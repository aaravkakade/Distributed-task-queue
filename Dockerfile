FROM python:3.13-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY taskqueue/ taskqueue/

# default: coordinator; workers/reaper override `command` in compose
CMD ["uvicorn", "taskqueue.coordinator.app:app", "--host", "0.0.0.0", "--port", "8000"]
