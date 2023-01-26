FROM python:3.11-slim

# Quietly installs cron
RUN apt-get update -qq && apt-get install -qq cron

# This variable is used later in cron job
ARG SERVICE_ROOT=/service
ENV SERVICE_ROOT="${SERVICE_ROOT}"

COPY requirements.txt "${SERVICE_ROOT}/"
RUN pip install --quiet --no-cache-dir -r "${SERVICE_ROOT}/requirements.txt"

COPY job.py job.cron "${SERVICE_ROOT}/"

# Applies cron job configuration
RUN crontab "${SERVICE_ROOT}/job.cron"

# Exports env variables so they will be present in the crone shell and starts cron process
CMD printenv >> /etc/environment && cron -f