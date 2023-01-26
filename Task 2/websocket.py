import asyncio
import logging
import os

import pandas as pd
import uvicorn
from fastapi import FastAPI, WebSocket


LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
logging.basicConfig(level=LOGGING_LEVEL)
logger = logging.getLogger("trades_websocket")

app = FastAPI()
# This is ugly, but FastAPI docs recommend to store results of `on_event("startup")` tasks this way
# https://fastapi.tiangolo.com/advanced/events/
df_ts: pd.DataFrame | None = None

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


@app.on_event("startup")
async def prepare_trades_sample() -> None:
    """
    Loads and pre-processes trades sample data

    The data can also be validated with `pandera`, if needed
    """
    global df_ts

    parquet_path = os.environ["TRADES_SAMPLE_PATH"]
    logger.info(f"Reading trades sample data from {parquet_path}")
    df_ts = pd.read_parquet(parquet_path)
    df_ts.sort_values("timestamp", inplace=True)

    # Calculates `timeout = lag(t) - t`
    df_ts["timeout"] = df_ts["timestamp"].shift(-1) - df_ts["timestamp"]
    # Converts timestamp to a string
    df_ts["timestamp"] = df_ts.loc[:, "timestamp"].apply(lambda t: t.strftime(TIMESTAMP_FORMAT))

    df_ts = df_ts.groupby("timestamp")


@app.websocket("/live_trades")
async def replay_trades(websocket: WebSocket) -> None:
    """
    WebSocket imitating trading data provider

    Implements imitation of intervals of the original data provider
    """
    global df_ts

    await websocket.accept()
    logger.info(f"Connected")

    for timestamp, df_trades_with_timeout in df_ts:
        # Separates df_trades from timeout before sending
        df_trades = df_trades_with_timeout.loc[:, df_trades_with_timeout.columns != "timeout"]

        response = df_trades.to_dict("records")
        await websocket.send_json(response)

        # Calculates the amount of time we need to wait prior to sending the next batch of trades
        timeout = df_trades_with_timeout["timeout"].max()
        timeout_s = timeout.total_seconds()

        # Logs response
        message = {"timestamp": timestamp, "n_trades": len(response), "timeout": timeout_s}
        if LOGGING_LEVEL == "DEBUG":
            logger.debug(message | {"response": response})
        else:
            logger.info(message)

        # Imitates intervals of the original data provider
        await asyncio.sleep(timeout_s)

    logger.info(f"Disconnected")


if __name__ == "__main__":
    uvicorn.run(
        app, host="0.0.0.0", port=int(os.environ["WS_PORT"]), log_level=LOGGING_LEVEL.lower()
    )
