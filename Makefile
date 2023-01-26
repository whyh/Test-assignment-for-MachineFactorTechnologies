start_db:
	echo "Starting database"
	docker compose up -d db

sleep_on_db_init:
	echo "Waiting 15s for database to initialize"
	sleep 15

populate_db: start_db
	echo "Populating database"
	docker compose exec db /scripts/populate.sh

# Running this command may result in
# `error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed`
# Give the database some time to start accepting connections and try again
first_run: start_db sleep_on_db_init populate_db

# Silences Make print-outs (to disable, call make like this `make V=1 <command>`)
$(V).SILENT: