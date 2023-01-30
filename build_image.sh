
brew install libpq

docker build -t timescaledb .


docker run \
    --name timescaledb \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=password \
    -e TS_TUNE_MEMORY=4GB \
    -e TS_TUNE_NUM_CPUS=4 \
    timescaledb

    prefect orion start