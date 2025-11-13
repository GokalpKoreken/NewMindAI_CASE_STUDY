FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY proto /app/proto
COPY sentiment_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY sentiment_service /app

CMD ["python", "app.py"]
