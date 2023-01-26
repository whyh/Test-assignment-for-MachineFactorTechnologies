-- Populates tables bars_1 and bars_2, skipping malformed data

COPY bars_1 ("date", symbol, adj_close, "close", high, low, "open", volume)
    FROM :'BARS_1_DATA_PATH' DELIMITER ',' CSV HEADER
    WHERE "date" IS NOT NULL
        AND symbol IS NOT NULL
        AND adj_close IS NOT NULL
        AND "close" IS NOT NULL
        AND high IS NOT NULL
        AND low IS NOT NULL
        AND "open" IS NOT NULL
        AND volume IS NOT NULL;

COPY bars_2 ("date", symbol, adj_close, "close", high, low, "open", volume)
    FROM :'BARS_2_DATA_PATH' DELIMITER ',' CSV HEADER
    WHERE "date" IS NOT NULL
        AND symbol IS NOT NULL
        AND adj_close IS NOT NULL
        AND "close" IS NOT NULL
        AND high IS NOT NULL
        AND low IS NOT NULL
        AND "open" IS NOT NULL
        AND volume IS NOT NULL;


