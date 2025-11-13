FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY proto /app/proto
COPY producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN pip install --no-cache-dir grpcio==1.65.1 protobuf==5.26.1

COPY producer /app

CMD ["python", "app.py"]
