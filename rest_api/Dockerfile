FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY rest_api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY rest_api /app

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
