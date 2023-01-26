-- The assignment:
-- Create PostgreSQL database with tables bars_1 and bars_2 and fill them with data from bars_1.csv and bars_2.csv respectively
-- A candidate should decide on tables’ structures and column types based on the data in CSV files
-- Also create error_log table with columns: launch_timestamp(datetime), date(datetime), symbol(str), message(str)


CREATE TABLE IF NOT EXISTS bars_1 (
    "date" DATE,
-- Typical max length for a Ticker symbol is 5, which is also true for given input data
    symbol VARCHAR(5),

-- The following fields in the CSV files have precision of 16 and undefined scale
    adj_close NUMERIC NOT NULL,
    "close" NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    "open" NUMERIC NOT NULL,
-- Volume in the CSV files has scale of 1, implying no fractional shares are being traded
    volume NUMERIC NOT NULL,

    PRIMARY KEY(symbol, "date")
);

CREATE TABLE IF NOT EXISTS bars_2 (
    LIKE bars_1 INCLUDING ALL,

    --  Unique job identifier for jobs from Task 1.2
    job_timestamp INTEGER DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS error_log (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    launch_timestamp TIMESTAMP NOT NULL,
    "date" TIMESTAMP,
    symbol VARCHAR(5),
    message TEXT NOT NULL
);