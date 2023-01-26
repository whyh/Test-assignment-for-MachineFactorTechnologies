FROM python:3.11-slim

# This variable is used later in CMD
ARG SERVICE_ROOT=/service
ENV SERVICE_ROOT="${SERVICE_ROOT}"

# This variable is used later in EXPOSE and uvicorn config
ARG WS_PORT=8080
ENV WS_PORT="${WS_PORT}"

COPY requirements.txt "${SERVICE_ROOT}/"
RUN pip install --quiet --no-cache-dir -r "${SERVICE_ROOT}/requirements.txt"

COPY websocket.py tests.py "${SERVICE_ROOT}/"

EXPOSE "${WS_PORT}"
CMD python "${SERVICE_ROOT}/websocket.py"