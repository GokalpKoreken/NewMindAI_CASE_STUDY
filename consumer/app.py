import json
import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

import grpc
import psycopg
import redis
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, ValidationError

from proto import sentiment_pb2
from proto import sentiment_pb2_grpc

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
LOGGER = logging.getLogger("consumer")


def parse_datetime(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


class RawComment(BaseModel):
    commentId: str
    text: str
    createdAt: datetime

    @classmethod
    def from_message(cls, payload: dict) -> "RawComment":
        if "createdAt" in payload and isinstance(payload["createdAt"], str):
            payload = dict(payload)
            payload["createdAt"] = parse_datetime(payload["createdAt"])
        return cls(**payload)


class ProcessedComment(BaseModel):
    commentId: str
    text: str
    sentiment: str
    createdAt: datetime
    processedAt: datetime

    def to_kafka(self) -> bytes:
        payload = {
            "commentId": self.commentId,
            "text": self.text,
            "sentiment": self.sentiment,
            "createdAt": self.createdAt.isoformat(),
            "processedAt": self.processedAt.isoformat(),
        }
        return json.dumps(payload).encode("utf-8")


class CacheWrapper:
    def __init__(self, client: redis.Redis, ttl_seconds: int) -> None:
        self._client = client
        self._ttl = ttl_seconds

    def get(self, key: str) -> Optional[str]:
        try:
            value = self._client.get(key)
            if value is None:
                return None
            return value.decode("utf-8")
        except redis.RedisError as exc:
            LOGGER.error("Redis get failed: %s", exc)
            return None

    def set(self, key: str, value: str) -> None:
        try:
            self._client.set(key, value, ex=self._ttl)
        except redis.RedisError as exc:
            LOGGER.error("Redis set failed: %s", exc)


class CommentConsumer:
    def __init__(self) -> None:
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.raw_topic = os.getenv("RAW_COMMENTS_TOPIC", "raw-comments")
        self.processed_topic = os.getenv("PROCESSED_COMMENTS_TOPIC", "processed-comments")
        self.group_id = os.getenv("CONSUMER_GROUP", "comment-processor")
        self.grpc_host = os.getenv("GRPC_HOST", "sentiment-service")
        self.grpc_port = int(os.getenv("GRPC_PORT", "50051"))
        self.grpc_timeout = float(os.getenv("GRPC_TIMEOUT_SECONDS", "5"))
        self.grpc_retry_attempts = int(os.getenv("GRPC_MAX_RETRIES", "4"))
        self.grpc_retry_backoff = float(os.getenv("GRPC_BASE_BACKOFF", "0.5"))

        self.pg_host = os.getenv("POSTGRES_HOST", "postgres")
        self.pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.pg_db = os.getenv("POSTGRES_DB", "comments")
        self.pg_user = os.getenv("POSTGRES_USER", "app")
        self.pg_password = os.getenv("POSTGRES_PASSWORD", "appsecret")

        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_ttl = int(os.getenv("CACHE_TTL_SECONDS", "86400"))

        self._consumer = None
        self._producer = None
        self._grpc_channel: Optional[grpc.Channel] = None
        self._grpc_stub: Optional[sentiment_pb2_grpc.SentimentServiceStub] = None
        self._db_conn: Optional[psycopg.Connection] = None
        self._cache: Optional[CacheWrapper] = None

    def _connect_kafka_consumer(self) -> KafkaConsumer:
        while True:
            try:
                consumer = KafkaConsumer(
                    self.raw_topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    enable_auto_commit=True,
                    auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "latest"),
                    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                    consumer_timeout_ms=1000,
                )
                LOGGER.info("Connected Kafka consumer to topic %s", self.raw_topic)
                return consumer
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.error("Unable to connect Kafka consumer: %s", exc)
                time.sleep(2)

    def _connect_kafka_producer(self) -> KafkaProducer:
        while True:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                    linger_ms=20,
                )
                LOGGER.info("Connected Kafka producer for topic %s", self.processed_topic)
                return producer
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.error("Unable to connect Kafka producer: %s", exc)
                time.sleep(2)

    def _connect_grpc(self) -> sentiment_pb2_grpc.SentimentServiceStub:
        target = f"{self.grpc_host}:{self.grpc_port}"
        while True:
            try:
                channel = grpc.insecure_channel(target)
                grpc.channel_ready_future(channel).result(timeout=5)
                stub = sentiment_pb2_grpc.SentimentServiceStub(channel)
                self._grpc_channel = channel
                LOGGER.info("Connected to gRPC sentiment service at %s", target)
                return stub
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.error("Waiting for gRPC service %s: %s", target, exc)
                time.sleep(2)

    def _connect_db(self) -> psycopg.Connection:
        dsn = (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )
        while True:
            try:
                conn = psycopg.connect(dsn)
                conn.autocommit = True
                LOGGER.info("Connected to PostgreSQL at %s:%s", self.pg_host, self.pg_port)
                return conn
            except psycopg.OperationalError as exc:
                LOGGER.error("Unable to connect to PostgreSQL: %s", exc)
                time.sleep(2)

    def _connect_cache(self) -> CacheWrapper:
        while True:
            try:
                client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=False)
                client.ping()
                LOGGER.info("Connected to Redis at %s:%s", self.redis_host, self.redis_port)
                return CacheWrapper(client, self.redis_ttl)
            except redis.RedisError as exc:
                LOGGER.error("Unable to connect to Redis: %s", exc)
                time.sleep(2)

    @contextmanager
    def _ensure_resources(self):
        self._consumer = self._consumer or self._connect_kafka_consumer()
        self._producer = self._producer or self._connect_kafka_producer()
        self._grpc_stub = self._grpc_stub or self._connect_grpc()
        self._db_conn = self._db_conn or self._connect_db()
        self._cache = self._cache or self._connect_cache()
        try:
            yield
        finally:
            pass

    def _ensure_table(self) -> None:
        with self._ensure_resources():
            assert self._db_conn is not None
            with self._db_conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS processed_comments (
                        id SERIAL PRIMARY KEY,
                        comment_id VARCHAR(128) UNIQUE NOT NULL,
                        comment_text TEXT NOT NULL,
                        sentiment VARCHAR(32) NOT NULL,
                        source_created_at TIMESTAMPTZ NOT NULL,
                        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    CREATE INDEX IF NOT EXISTS idx_processed_comments_sentiment
                        ON processed_comments (sentiment);
                    """
                )

    def _fetch_sentiment(self, comment: RawComment) -> str:
        assert self._cache is not None
        cached = self._cache.get(comment.text)
        if cached:
            LOGGER.debug("Cache hit for text of comment %s", comment.commentId)
            return cached

        assert self._grpc_stub is not None
        request = sentiment_pb2.AnalyzeCommentRequest(
            comment_id=comment.commentId,
            text=comment.text,
        )
        backoff = self.grpc_retry_backoff
        for attempt in range(self.grpc_retry_attempts):
            try:
                response = self._grpc_stub.AnalyzeComment(request, timeout=self.grpc_timeout)
                sentiment = response.sentiment
                self._cache.set(comment.text, sentiment)
                return sentiment
            except grpc.RpcError as exc:  # pylint: disable=broad-except
                status = exc.code()
                LOGGER.warning(
                    "gRPC call failed (attempt %s/%s) for %s: %s",
                    attempt + 1,
                    self.grpc_retry_attempts,
                    comment.commentId,
                    status,
                )
                if status in {grpc.StatusCode.INVALID_ARGUMENT}:
                    break
                time.sleep(backoff)
                backoff = min(backoff * 2, 5)
        LOGGER.error("Falling back to 'unknown' sentiment for comment %s", comment.commentId)
        return "unknown"

    def _persist(self, processed: ProcessedComment) -> None:
        assert self._db_conn is not None
        with self._db_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO processed_comments (comment_id, comment_text, sentiment, source_created_at, processed_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (comment_id) DO UPDATE SET
                    comment_text = EXCLUDED.comment_text,
                    sentiment = EXCLUDED.sentiment,
                    source_created_at = EXCLUDED.source_created_at,
                    processed_at = EXCLUDED.processed_at;
                """,
                (
                    processed.commentId,
                    processed.text,
                    processed.sentiment,
                    processed.createdAt,
                    processed.processedAt,
                ),
            )

    def _publish(self, processed: ProcessedComment) -> None:
        assert self._producer is not None
        payload = {
            "commentId": processed.commentId,
            "text": processed.text,
            "sentiment": processed.sentiment,
            "createdAt": processed.createdAt.isoformat(),
            "processedAt": processed.processedAt.isoformat(),
        }
        future = self._producer.send(self.processed_topic, payload)
        try:
            future.get(timeout=10)
            LOGGER.debug("Published processed comment %s", processed.commentId)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.error("Failed to publish processed comment %s: %s", processed.commentId, exc)

    def start(self) -> None:
        self._ensure_table()
        LOGGER.info("Consumer loop starting on topic %s", self.raw_topic)
        while True:
            with self._ensure_resources():
                assert self._consumer is not None
                try:
                    message_batch = self._consumer.poll(timeout_ms=1000)
                except Exception as exc:  # pylint: disable=broad-except
                    LOGGER.error("Kafka poll error: %s", exc)
                    time.sleep(1)
                    self._consumer = None
                    continue

                if not message_batch:
                    continue

                for records in message_batch.values():
                    for record in records:
                        try:
                            payload = record.value
                            raw_comment = RawComment.from_message(payload)
                        except (ValidationError, ValueError, json.JSONDecodeError) as exc:
                            LOGGER.error("Invalid message skipped: %s", exc)
                            continue

                        processed = ProcessedComment(
                            commentId=raw_comment.commentId,
                            text=raw_comment.text,
                            sentiment=self._fetch_sentiment(raw_comment),
                            createdAt=raw_comment.createdAt,
                            processedAt=datetime.now(timezone.utc),
                        )
                        self._persist(processed)
                        self._publish(processed)


def main() -> None:
    consumer_service = CommentConsumer()
    consumer_service.start()


if __name__ == "__main__":
    main()
