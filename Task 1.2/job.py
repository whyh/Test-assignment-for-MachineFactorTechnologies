import os
import logging
from datetime import datetime
from time import time

import pandas as pd
from pandas.core.groupby import SeriesGroupBy
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import pandas.io.sql as psql

logging.basicConfig(level="INFO")
logger = logging.getLogger("process_bars_job")
logger.setLevel(level=os.getenv("LOGGING_LEVEL", "INFO"))

# Timestamp of the job, each job should have a unique timestamp
# It helps to keep track of what records are processed by what job
JOB_TIMESTAMP = int(time())


def log_errors(db: Engine, df_error: pd.DataFrame, message: str) -> None:
    """
    Stores error log to the database

    :param df_error: Dataframe with `date` and `symbol` columns
    :param message: Error message
    """
    df_error["message"] = df_error.apply(lambda row: message.format(**row), axis=1)
    df_error["date"] = df_error.apply(lambda row: pd.to_datetime(row["date"]), axis=1)
    df_error["launch_timestamp"] = datetime.now()
    with db.connect() as connection:
        df_error.to_sql("error_log", connection, index=False, if_exists="append", method="multi")
    logger.info(f"Job {JOB_TIMESTAMP} - Inserted {len(df_error)} records to `error_log` table")


def get_min_close(record: dict, df_bars1: SeriesGroupBy) -> int:
    """
    Calculates min close price for the record over the last 10 days from bars_1 table for record's symbol

    :param record: dict with
    :param df_bars1: Grouped by `symbol` DataFrame with `date`, `close` columns
    :return: Result of the calculation
    """
    group = df_bars1.get_group(record["symbol"])
    return group.loc[
        (record["date"] >= group["date"]) & (group["date"] >= record["date"] - pd.Timedelta(days=10)),
        "close",
    ].min()


def process_bars(db: Engine, batch_size: int = 20_000) -> None:
    """
    :param batch_size: Number of records to process
    """
    # Fetches records from bars_2 table and updates their job_timestamp to the current job timestamp
    with db.connect() as connection:
        cursor = connection.execute(
            """
            WITH batch AS (
                SELECT "date", symbol FROM bars_2
                WHERE job_timestamp IS NULL OR job_timestamp = %(job_timestamp)s LIMIT %(limit)s
            )
            UPDATE bars_2 AS b2 SET job_timestamp = %(job_timestamp)s FROM batch
            WHERE b2."date" = batch."date" AND b2.symbol = batch.symbol
            RETURNING b2."date", b2.symbol, b2.adj_close, b2."close", b2.high, b2.low, b2."open", b2.volume
            """,
            limit=batch_size,
            job_timestamp=JOB_TIMESTAMP,
        )
        df = pd.DataFrame.from_records(
            cursor.fetchall(),
            columns=("date", "symbol", "adj_close", "close", "high", "low", "open", "volume")
        )

    logger.info(f"Job {JOB_TIMESTAMP} - Fetched {len(df)} records from `bars_2` table")
    if df.empty:
        with db.connect() as connection:
            psql.execute(
                """INSERT INTO error_log (launch_timestamp, "message") VALUES (%(timestamp)s, %(message)s)""",
                connection,
                {"timestamp": datetime.now(), "message": "No values available in bars_2"},
            )
        return

    df_symbols = psql.read_sql("SELECT DISTINCT symbol from bars_1", db)["symbol"]

    log_errors(
        connection,
        df.loc[~df["symbol"].isin(df_symbols), ["date", "symbol"]],
        "{symbol} not present in tables_bars_1 on {date}",
    )

    # Calculates min_close values
    df_result = df.loc[df["symbol"].isin(df_symbols)]
    df_bars1 = psql.read_sql(
        """SELECT "date", symbol, "close" FROM bars_1""", db, coerce_float=False
    ).groupby("symbol")
    df_result["min_close"] = df.apply(get_min_close, df_bars1=df_bars1, axis=1)

    log_errors(
        db,
        df_result.loc[df["close"] <= df_result["min_close"], ["date", "symbol"]],
        "{symbol} close price is not bigger than the minimum over the past 10 days on {date}",
    )

    # Merges records on bars_1 table to mark potential duplicates
    df_result = df_result.loc[
        df_result["close"] > df_result["min_close"], df_result.columns != "min_close"
    ].merge(
        psql.read_sql("""SELECT "date", symbol from bars_1""", db),
        on=("date", "symbol"),
        how="left",
        indicator=True,
    )
    # Removes records that are already present in bars_1 table, to avoid collisions
    df_result = df_result.loc[df_result["_merge"] != "both", df_result.columns != "_merge"]
    with db.connect() as connection:
        df_result.to_sql("bars_1", connection, index=False, if_exists="append", method="multi")
    logger.info(f"Job {JOB_TIMESTAMP} - Inserted {len(df_result)} records to `bars_1` table")

    # Deletes processed bars_2 records
    with db.connect() as connection:
        psql.execute("DELETE FROM bars_2 WHERE job_timestamp = %s", connection, (JOB_TIMESTAMP,))
    logger.info(f"Job {JOB_TIMESTAMP} - Deleted {len(df)} records from `bars_2` table")


if __name__ == "__main__":
    with create_engine(os.environ["DB_URL"]).connect() as db:
        process_bars(db)
