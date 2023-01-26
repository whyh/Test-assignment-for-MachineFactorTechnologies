from time import time, sleep
from unittest import TestCase, mock

import pandas as pd
from fastapi.testclient import TestClient

from .websocket import app, TIMESTAMP_FORMAT


class TestWebsocket(TestCase):
    @staticmethod
    def _create_timestamp(date_string: str) -> pd.Timestamp:
        return pd.to_datetime(date_string, format=TIMESTAMP_FORMAT)

    def test_send_valid_json(self) -> None:
        with mock.patch("pandas.read_parquet") as rp:
            rp.return_value = pd.DataFrame.from_records(
                [
                    {
                        "timestamp": self._create_timestamp("2021-12-01 00:01:01.326000"),
                        "price": 47476000,
                        "volume": 8227000000000,
                        "ticker": "1000SHIB-USDT-SWAP@BINANCE",
                    },
                ],
            )

            with TestClient(app) as client, client.websocket_connect("/live_trades") as websocket:
                response = websocket.receive_json()
                self.assertIsInstance(response, list)
                self.assertEqual(len(response), 1)
                record = response[0]
                self.assertIsInstance(record, dict)
                self.assertEqual(
                    record,
                    {
                        "timestamp": "2021-12-01 00:01:01.326000",
                        "price": 47476000,
                        "volume": 8227000000000,
                        "ticker": "1000SHIB-USDT-SWAP@BINANCE",
                    },
                )

    def test_send_together_records_with_identical_timestamp(self) -> None:
        with mock.patch("pandas.read_parquet") as rp:
            rp.return_value = pd.DataFrame.from_records(
                [
                    {
                        "timestamp": self._create_timestamp("2021-12-01 00:01:01.326001"),
                        "price": 47036000,
                        "volume": 12204000000000,
                        "ticker": "1000SHIB-BTC-SWAP@BINANCE",
                    },
                    {
                        "timestamp": self._create_timestamp("2021-12-01 00:01:01.326001"),
                        "price": 47031000,
                        "volume": 2037000000000,
                        "ticker": "1000SHIB-ETH-SWAP@BINANCE",
                    },
                ],
            )

            with TestClient(app) as client, client.websocket_connect("/live_trades") as websocket:
                response = websocket.receive_json()
                self.assertIsInstance(response, list)
                self.assertEqual(len(response), 2)
                self.assertIn(
                    {
                        "timestamp": "2021-12-01 00:01:01.326001",
                        "price": 47036000,
                        "volume": 12204000000000,
                        "ticker": "1000SHIB-BTC-SWAP@BINANCE",
                    },
                    response,
                )
                self.assertIn(
                    {
                        "timestamp": "2021-12-01 00:01:01.326001",
                        "price": 47031000,
                        "volume": 2037000000000,
                        "ticker": "1000SHIB-ETH-SWAP@BINANCE",
                    },
                    response,
                )

    def test_send_in_chronological_order(self) -> None:
        with mock.patch("pandas.read_parquet") as rp:
            records = [
                    {
                        "timestamp": self._create_timestamp("2021-12-01 00:01:01.326001"),
                        "price": 47036000,
                        "volume": 12204000000000,
                        "ticker": "1000SHIB-BTC-SWAP@BINANCE",

                    },
                    {
                        "timestamp": self._create_timestamp("2021-12-01 00:01:01.326003"),
                        "price": 28036000,
                        "volume": 13207000000000,
                        "ticker": "1000SHIB-ETC-SWAP@BINANCE",

                    },
                    {
                        "timestamp": self._create_timestamp("2021-12-01 00:01:01.326002"),
                        "price": 47031000,
                        "volume": 12204000000000,
                        "ticker": "1000SHIB-BTC-SWAP@BINANCE",

                    },
                    {
                        "timestamp": self._create_timestamp("2021-12-01 00:01:01.326004"),
                        "price": 47474000,
                        "volume": 2039000000000,
                        "ticker": "1000SHIB-ETH-SWAP@BINANCE",
                    },
                ]
            rp.return_value = pd.DataFrame.from_records(records)

            with TestClient(app) as client, client.websocket_connect("/live_trades") as websocket:
                prev_timestamp = self._create_timestamp("2001-01-01 01:01:01.000001")
                for _ in range(len(records)):
                    response = websocket.receive_json()
                    self.assertIsInstance(response, list)
                    self.assertEqual(len(response), 1)
                    record = response[0]
                    timestamp = self._create_timestamp(record["timestamp"])
                    self.assertLess(prev_timestamp, timestamp)
                    prev_timestamp = timestamp

    def test_await_for_about_timeout_before_sent_next(self) -> None:
        with mock.patch("pandas.read_parquet") as rp:
            rp.return_value = pd.DataFrame.from_records(
                [
                    {
                        "timestamp": self._create_timestamp("2021-12-01 00:00:01.000000"),
                        "price": 47036000,
                        "volume": 12204000000000,
                        "ticker": "1000SHIB-BTC-SWAP@BINANCE",
                    },
                    {
                        "timestamp": self._create_timestamp("2021-12-01 00:00:11.000000"),
                        "price": 47031000,
                        "volume": 2037000000000,
                        "ticker": "1000SHIB-ETH-SWAP@BINANCE",
                    },
                ],
            )

            with TestClient(app) as client:
                # Waits a bit for `prepare_dataframe` task to end execution
                sleep(1)
                with client.websocket_connect("/live_trades") as websocket:
                    websocket.receive_json()
                    t1 = time()
                    websocket.receive_json()
                    t2 = time()
                    timeout = t2 - t1
                    # Checks if WebSocked had awaited for about `timeout` seconds
                    self.assertAlmostEqual(10, timeout, delta=1)
