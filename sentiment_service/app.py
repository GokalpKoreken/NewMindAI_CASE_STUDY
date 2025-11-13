import logging
import os
import random
import threading
import time
from collections import deque
from concurrent import futures

import grpc

from proto import sentiment_pb2
from proto import sentiment_pb2_grpc


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
LOGGER = logging.getLogger("sentiment-service")


class RateLimiter:
    """Fixed window rate limiter that tracks requests per second."""

    def __init__(self, max_requests: int) -> None:
        self._max_requests = max_requests
        self._timestamps = deque()
        self._lock = threading.Lock()

    def allow(self) -> bool:
        if self._max_requests <= 0:
            return True
        now = time.time()
        with self._lock:
            while self._timestamps and now - self._timestamps[0] >= 1.0:
                self._timestamps.popleft()
            if len(self._timestamps) >= self._max_requests:
                return False
            self._timestamps.append(now)
            return True


class SentimentService(sentiment_pb2_grpc.SentimentServiceServicer):
    def __init__(
        self,
        drop_probability: float,
        delay_multiplier: float,
        base_delay: float,
        rate_limiter: RateLimiter,
    ) -> None:
        self._drop_prob = max(0.0, min(1.0, drop_probability))
        self._delay_multiplier = max(delay_multiplier, 0.0)
        self._base_delay = max(base_delay, 0.0)
        self._rate_limiter = rate_limiter
        self._cache: dict[str, str] = {}
        self._cache_lock = threading.Lock()
        self._rng = random.Random()

    def AnalyzeComment(self, request, context):  # pylint: disable=invalid-name
        text = request.text or ""
        comment_id = request.comment_id
        if not self._rate_limiter.allow():
            LOGGER.warning("Rate limit exceeded for comment %s", comment_id)
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Rate limit exceeded")

        if self._rng.random() < self._drop_prob:
            LOGGER.warning("Random drop triggered for comment %s", comment_id)
            context.abort(grpc.StatusCode.UNAVAILABLE, "Random drop")

        simulated_delay = self._base_delay + len(text) * self._delay_multiplier
        if simulated_delay > 0:
            time.sleep(simulated_delay)

        with self._cache_lock:
            sentiment = self._cache.get(text)
            if sentiment is None:
                sentiment = self._rng.choice(["positive", "negative", "neutral"])
                self._cache[text] = sentiment

        LOGGER.info("Comment %s analyzed as %s", comment_id, sentiment)
        response = sentiment_pb2.AnalyzeCommentResponse(
            comment_id=comment_id,
            sentiment=sentiment,
            original_text=text,
            analyzed_at=int(time.time()),
        )
        return response


def serve() -> None:
    port = int(os.getenv("GRPC_PORT", "50051"))
    drop_probability = float(os.getenv("DROP_PROBABILITY", "0.05"))
    delay_multiplier = float(os.getenv("DELAY_PER_CHAR_SECONDS", "0.005"))
    base_delay = float(os.getenv("BASE_DELAY_SECONDS", "0.05"))
    rate_limit = int(os.getenv("RATE_LIMIT_PER_SECOND", "100"))

    LOGGER.info(
        "Starting sentiment service on port %s with rate_limit=%s drop_probability=%.2f",
        port,
        rate_limit,
        drop_probability,
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    sentiment_service = SentimentService(
        drop_probability=drop_probability,
        delay_multiplier=delay_multiplier,
        base_delay=base_delay,
        rate_limiter=RateLimiter(rate_limit),
    )
    sentiment_pb2_grpc.add_SentimentServiceServicer_to_server(sentiment_service, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
