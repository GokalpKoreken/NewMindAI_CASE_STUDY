import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Deque

from kafka import KafkaProducer

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
LOGGER = logging.getLogger("producer")

WORDS = [
    "lorem",
    "ipsum",
    "dolor",
    "sit",
    "amet",
    "consectetur",
    "adipiscing",
    "elit",
    "fusce",
    "vehicula",
    "sapien",
    "risus",
    "semper",
    "ornare",
    "vivamus",
    "blandit",
    "porta",
    "felis",
    "mauris",
    "tincidunt",
    "turpis",
    "lobortis",
    "maximus",
    "placerat",
]


class ProducerService:
    def __init__(self) -> None:
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.topic = os.getenv("RAW_COMMENTS_TOPIC", "raw-comments")
        self.min_delay = float(os.getenv("MIN_DELAY_SECONDS", "0.1"))
        self.max_delay = float(os.getenv("MAX_DELAY_SECONDS", "10"))
        self.min_words = int(os.getenv("MIN_WORDS", "4"))
        self.max_words = int(os.getenv("MAX_WORDS", "30"))
        self.reuse_probability = float(os.getenv("REUSE_PROBABILITY", "0.25"))
        self._producer: KafkaProducer | None = None
        self._recent_texts: Deque[str] = deque(maxlen=int(os.getenv("TEXT_CACHE_SIZE", "200")))
        self._running = True

    def _ensure_producer(self) -> KafkaProducer:
        if self._producer:
            return self._producer
        backoff = 1
        while True:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                    linger_ms=10,
                    retries=5,
                )
                LOGGER.info("Connected to Kafka at %s", self.bootstrap_servers)
                self._producer = producer
                return producer
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.error("Failed to connect to Kafka: %s", exc)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _build_comment_text(self) -> str:
        if self._recent_texts and random.random() < self.reuse_probability:
            text = random.choice(list(self._recent_texts))
            LOGGER.debug("Reusing previous comment text: %s", text)
            return text
        word_count = random.randint(self.min_words, self.max_words)
        words = [random.choice(WORDS) for _ in range(word_count)]
        sentence = " ".join(words).capitalize() + "."
        self._recent_texts.append(sentence)
        LOGGER.debug("Generated new comment text: %s", sentence)
        return sentence

    def _emit_comment(self) -> None:
        producer = self._ensure_producer()
        comment_id = str(uuid.uuid4())
        text = self._build_comment_text()
        event = {
            "commentId": comment_id,
            "text": text,
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
        future = producer.send(self.topic, event)
        LOGGER.info("Sent comment %s", comment_id)
        try:
            future.get(timeout=10)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.error("Error while sending comment %s: %s", comment_id, exc)

    def run(self) -> None:
        LOGGER.info("Starting producer loop targeting topic %s", self.topic)
        while self._running:
            start = time.perf_counter()
            self._emit_comment()
            interval = random.uniform(self.min_delay, self.max_delay)
            elapsed = time.perf_counter() - start
            sleep_time = max(interval - elapsed, 0.0)
            time.sleep(sleep_time)

    def stop(self, *_args) -> None:
        self._running = False
        if self._producer:
            LOGGER.info("Flushing and closing Kafka producer")
            self._producer.flush()
            self._producer.close()


def main() -> None:
    service = ProducerService()
    signal.signal(signal.SIGINT, service.stop)
    signal.signal(signal.SIGTERM, service.stop)
    try:
        service.run()
    except KeyboardInterrupt:
        LOGGER.info("Producer interrupted; exiting.")
        service.stop()
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.exception("Fatal error in producer: %s", exc)
        service.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
