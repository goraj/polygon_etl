import os

import datetime
import hashlib
import pandas as pd

import asyncio
import asyncpg

import socket
import aiohttp
from aiohttp.client_reqrep import ClientRequest
from aiohttp_retry import RetryClient, ExponentialRetry


import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JobStatus:
    COMPLETED = "COMPLETED"
    RUNNING = "RUNNING"
    FAILED = "FAILED"


class JobType:
    TRADES = "TRADES"
    QUOTES = "QUOTES"


class PolygonConfig:
    API_TOKEN = os.getenv("POLYGON_API_TOKEN")
    BASE_URL_V3 = "https://api.polygon.io/v3"

    CONCURRENT_CONNECTIONS = 250

    TICKER_LIMIT = 1_000
    TRADE_LIMIT = 50_000
    QUOTE_LIMIT = 50_000

    START_DATE = "2003-09-10"
    STATUS_SUCESS = "OK"
    STATUS_ERROR = "ERROR"

    TRADE_JSON_ATTRIBUTES = [
        "conditions",
        "exchange",
        "id",
        "participant_timestamp",
        "price",
        "sequence_number",
        "sip_timestamp",
        "size",
        "tape",
        "trf_id",
        "trf_timestamp",
    ]
    TRADE_DEFAULTS = {
        "conditions": [],
    }

    QUOTE_JSON_ATTRIBUTES = [
        "ask_exchange",
        "ask_price",
        "ask_size",
        "bid_exchange",
        "bid_price",
        "bid_size",
        "conditions",
        "indicators",
        "participant_timestamp",
        "sequence_number",
        "sip_timestamp",
        "tape",
    ]
    QUOTE_DEFAULTS = {"conditions": [], "indicators": []}

    @staticmethod
    def get_headers():
        return {"Authorization": f"Bearer {PolygonConfig.API_TOKEN}"}

    @staticmethod
    def trade_url() -> str:
        return f"{PolygonConfig.BASE_URL_V3}/trades"

    @staticmethod
    def quote_url() -> str:
        return f"{PolygonConfig.BASE_URL_V3}/quotes"

    @staticmethod
    def tickers_url() -> str:
        return f"{PolygonConfig.BASE_URL_V3}/reference/tickers"


async def is_job_completed_or_running(
    connection: asyncpg.connection,
    job_type: JobType,
    job_date: datetime.datetime,
    symbol: str,
):
    """Checks if job isn't already running or has been completed."""
    val = await connection.fetchval(
        """
        SELECT EXISTS(
            SELECT 
                * 
            FROM 
                jobs 
            WHERE 
                job=$1 AND
                job_date=$2 AND
                symbol=$3 AND  
                status IN ('COMPLETED', 'RUNNING')
        )""",
        job_type,
        job_date,
        symbol,
    )
    return val


async def set_job_status(
    connection: asyncpg.connection,
    job_type: JobType,
    job_date: datetime.datetime,
    job_status: JobStatus,
    symbol: str,
) -> JobStatus:
    """Sets job status."""
    await connection.execute(
        """
        INSERT INTO 
            jobs(job, status, symbol, job_date) 
        VALUES ($1, $2, $3, $4)
        ON CONFLICT 
            (symbol, job, job_date) 
        DO UPDATE
        SET status = $2
        """,
        job_type,
        job_status,
        symbol,
        job_date,
    )


async def get_data(
    session: aiohttp.ClientSession, url: str, limit: int, symbol: str, date: str
):
    params = {
        "limit": limit,
        "timestamp.gte": date,
        "timestamp.lte": date,
    }
    data = []
    while True:
        async with session.get(url=url, params=params) as response:
            json_data = await response.json()
            status = json_data["status"]
            if status == PolygonConfig.STATUS_ERROR:
                error_msg = json_data["error"]
                logger.error(f"Error msg: {error_msg=}")
                raise RuntimeError

            for result in json_data["results"]:
                data.append({"symbol": symbol} | result)

            try:
                url = json_data["next_url"]
            except KeyError:
                break

    return data


async def get_trades(session: aiohttp.ClientSession, symbol: str, date: str):
    url = f"{PolygonConfig.trade_url()}/{symbol}"
    limit = PolygonConfig.TRADE_LIMIT
    data = await get_data(
        session=session, url=url, limit=limit, symbol=symbol, date=date
    )

    for i in range(len(data)):
        data[i] = [symbol] + [
            data[i].get(k, PolygonConfig.TRADE_DEFAULTS.get(k, None))
            for k in PolygonConfig.TRADE_JSON_ATTRIBUTES
        ]
    return data


async def get_quotes(session: aiohttp.ClientSession, symbol: str, date: str):
    url = f"{PolygonConfig.quote_url()}/{symbol}"
    limit = PolygonConfig.QUOTE_LIMIT
    data = await get_data(
        session=session, url=url, limit=limit, symbol=symbol, date=date
    )

    for i in range(len(data)):
        data[i] = [symbol] + [
            data[i].get(k, PolygonConfig.QUOTE_DEFAULTS.get(k, None))
            for k in PolygonConfig.QUOTE_JSON_ATTRIBUTES
        ]
    return data


async def upsert(
    connection, date: datetime.datetime, table_name: str, columns: list[str], data: list
):
    async with connection.transaction():
        sha_hash = hashlib.sha1(
            f"{datetime.datetime.now()}".encode("UTF-8")
        ).hexdigest()
        temp_table_name = f"temp_table_{sha_hash}"
        await connection.execute(
            f"""
            CREATE TEMPORARY TABLE {temp_table_name}
            AS (
                SELECT * FROM {table_name} LIMIT 0
            );
            """
        )
        try:
            await connection.copy_records_to_table(
                temp_table_name, records=data, columns=columns
            )
            await connection.execute(
                f"""
                INSERT INTO {table_name}
                SELECT * FROM {temp_table_name}
                ON CONFLICT (symbol, sip_timestamp) DO NOTHING;
                """
            )
        except asyncpg.PostgresError as e:
            fn = f"{table_name}-{date}.parquet"
            pd.DataFrame(data=data, columns=columns).to_parquet(fn)
            logger.error(f"Error in copy_records_to_table: {e}. Export to {fn}")
            raise e


async def trade_update(connection, session, symbol, date):
    trade_data = await get_trades(session=session, symbol=symbol, date=date)
    if not trade_data:
        logger.info(f"No data available: {symbol = } {date = } {JobType.TRADES}")
        return

    columns = ["symbol"] + PolygonConfig.TRADE_JSON_ATTRIBUTES
    await upsert(
        connection=connection,
        date=date,
        table_name="trades",
        columns=columns,
        data=trade_data,
    )


async def quote_update(connection, session, symbol, date):
    quote_data = await get_quotes(session=session, symbol=symbol, date=date)
    if not quote_data:
        logger.info(f"No data available: {symbol = } {date = } {JobType.QUOTES}")
        return

    columns = ["symbol"] + PolygonConfig.QUOTE_JSON_ATTRIBUTES
    await upsert(
        connection=connection,
        date=date,
        table_name="quotes",
        columns=columns,
        data=quote_data,
    )


async def run_task(
    semaphore,
    pool,
    session: aiohttp.ClientSession,
    job_type: JobType,
    symbol: str,
    date: str,
):
    job_date = datetime.datetime.strptime(date, "%Y-%m-%d").date()

    match job_type:
        case JobType.TRADES:
            fn = trade_update
        case JobType.QUOTES:
            fn = quote_update
        case _:
            raise NotImplementedError

    async with semaphore:
        async with pool.acquire() as connection:
            if await is_job_completed_or_running(
                connection=connection,
                job_type=job_type,
                job_date=job_date,
                symbol=symbol,
            ):
                logger.info(
                    f"Skipping job: Already complete or in-progress: {symbol = } {date = } {job_date = } {job_type = }"
                )
                return
            try:
                await set_job_status(
                    connection=connection,
                    job_type=job_type,
                    job_date=job_date,
                    job_status=JobStatus.RUNNING,
                    symbol=symbol,
                )
                logger.info(
                    f"Starting job: {symbol = } {date = } {job_date = } {job_type = }"
                )
                await fn(
                    connection=connection, session=session, symbol=symbol, date=date
                )
            except (
                Exception
                # aiohttp.ClientError,
                # asyncpg.InternalClientError,
                # asyncpg.PostgresError,
            ) as e:
                logger.error(f"Error job failed: {symbol = } {date = } {e}.")
                await set_job_status(
                    connection=connection,
                    job_type=job_type,
                    job_date=job_date,
                    job_status=JobStatus.FAILED,
                    symbol=symbol,
                )
            else:
                logger.info(f"Job completed: {symbol = } {date = }  {job_type = }")
                await set_job_status(
                    connection=connection,
                    job_type=job_type,
                    job_date=job_date,
                    job_status=JobStatus.COMPLETED,
                    symbol=symbol,
                )


class KeepAliveClientRequest(aiohttp.client_reqrep.ClientRequest):
    async def send(self, conn: "Connection") -> aiohttp.ClientResponse:
        sock = conn.protocol.transport.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
        except AttributeError:
            # OSX
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, 60)

        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)

        return await super().send(conn)


async def etl():
    symbol = "QQQ"
    dt = datetime.datetime.strptime(PolygonConfig.START_DATE, "%Y-%m-%d")
    end_dt = str(datetime.date.today())
    end_dt = datetime.datetime.strptime(end_dt, "%Y-%m-%d").date()

    async with asyncpg.create_pool(
        database="postgres",
        user="postgres",
        password="password",
        min_size=10,
        max_size=PolygonConfig.CONCURRENT_CONNECTIONS,
    ) as pool:
        connector = aiohttp.TCPConnector(limit=PolygonConfig.CONCURRENT_CONNECTIONS)
        timeout = aiohttp.ClientTimeout(total=600)
        async with aiohttp.ClientSession(
            request_class=KeepAliveClientRequest,
            connector=connector,
            headers=PolygonConfig.get_headers() | {"Connection": "keep-alive"},
            timeout=timeout,
        ) as session:
            retry_options = ExponentialRetry(
                attempts=10, exceptions={aiohttp.ClientError}
            )
            retry_client = RetryClient(
                client_session=session,
                timeout=timeout,
                raise_for_status=False,
                retry_options=retry_options,
            )
            semaphore = asyncio.BoundedSemaphore(PolygonConfig.CONCURRENT_CONNECTIONS)
            tasks = []
            while dt.date() <= end_dt:
                for job_types in [JobType.QUOTES, JobType.TRADES]:
                    future = asyncio.ensure_future(
                        run_task(
                            semaphore=semaphore,
                            pool=pool,
                            session=retry_client,
                            job_type=job_types,
                            symbol=symbol,
                            date=str(dt.date()),
                        )
                    )
                    tasks.append(future)
                dt += datetime.timedelta(days=1)
            return await asyncio.gather(*tasks)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(etl())
