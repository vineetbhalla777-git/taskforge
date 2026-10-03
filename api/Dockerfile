FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 10002 taskforge
WORKDIR /app
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api/app ./app
COPY db/schema.sql ./db/schema.sql
USER taskforge
EXPOSE 8000
# Default command runs the REST API; docker-compose overrides `command` for the
# webhook-worker service to run `python -m app.webhook_worker` from this same image.
CMD ["uvicorn", "app.asgi:app", "--host", "0.0.0.0", "--port", "8000"]
