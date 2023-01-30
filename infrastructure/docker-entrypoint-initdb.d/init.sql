CREATE TABLE IF NOT EXISTS instruments (
    symbol TEXT PRIMARY KEY,
    name TEXT,
    cik TEXT
);

CREATE TABLE IF NOT EXISTS quotes (
    ask_exchange INTEGER,
    ask_price FLOAT,
    ask_size INTEGER,
    bid_exchange INTEGER,
    bid_price FLOAT,
    bid_size INTEGER,
    conditions INTEGER[] DEFAULT '{}'::INT[],
    indicators INTEGER[] DEFAULT '{}'::INT[],
    participant_timestamp BIGINT,
    sequence_number BIGINT,
    sip_timestamp BIGINT,
    tape INTEGER,
    symbol TEXT,
    PRIMARY KEY (symbol, sip_timestamp)
);

CREATE TABLE IF NOT EXISTS trades (
    conditions INTEGER[] DEFAULT '{}'::INT[],
    exchange INTEGER,
    id VARCHAR(64),
    participant_timestamp BIGINT,
    price FLOAT,
    sequence_number BIGINT,
    sip_timestamp BIGINT,
    size INTEGER,
    tape INTEGER,
    trf_id INTEGER DEFAULT NULL,
    trf_timestamp BIGINT DEFAULT NULL,
    symbol TEXT,
    PRIMARY KEY (symbol, sip_timestamp)
);

CREATE TYPE job_type AS ENUM ('TRADES', 'QUOTES');
CREATE TYPE job_status AS ENUM ('COMPLETED', 'RUNNING', 'FAILED');

CREATE TABLE IF NOT EXISTS jobs (
    job job_type,
    status job_status,
    symbol TEXT,
    job_date date,
    PRIMARY KEY (symbol, job, job_date)
);
