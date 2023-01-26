# Test assignment Machine Factor Technologies
My solutions to the [test assignment](Software_Engineer_Test_Case.pdf)

## Prerequisites
To work with this repo you will need
1. Docker >= v20.10.14
2. GNU Make >= v3.81
3. websocat or any other websocket client

Please make sure you have the correct version of Docker installed

Note: Because `docker-compose.yaml` uses env variables substitution,
`docker compose ...` should be called from the project root dir

## First run
1. Download [test_data](https://drive.google.com/drive/folders/1efXJOysiz_rWWsBLzr8XAB3qpM3NtP7W?usp=share_link)
and [trades_sample.parquet](https://drive.google.com/file/d/1T46Mo9lx1rDEBApptiJcT89jmE_hrXoh/view?usp=sharing)
from my Google Drive
2. Create `.env` file with `cp example.env .env`. Specify path to the downloaded files in [.env file](.env)
3. Run `make first_run` to initialize db and populate it with the sample data

Database structure is defined in [init.sql](db/scripts/init.sql)

I had decided to not include test files in to the repo, because they are not small.
And there is no straightforward way to download them with a script, because they are too large
(to be scanned for viruses, and Google Drive asks to press confirm to continue)

## Task 1.1
1. Run `docker compose up -d notebooks` to start Jupyter notebooks server 
2. In your browser go to http://127.0.0.1:8888/lab/tree/notebooks. You should be able to see and execute my solutions now

## Task 1.2
1. Run `docker compose up cron_job` to start cron job. You should be able to see some logs now

## Task 2
1. Run `docker compose up websocket` to start WebSocket. You should be able to see some logs now
2. Run in another terminal `websocat ws://127.0.0.1:8080/live_trades` to connect to the WebSocket. 
You should see streaming trades, and some logs in another terminal
3. Run `docker compose exec websocket python -m unittest /service/tests.py` to start tests

Note: It might take a few seconds for the endpoint to become available