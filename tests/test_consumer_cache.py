from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from consumer.app import CommentConsumer, RawComment


class DummyCache:
    """Simple in-memory cache stub for testing."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def set(self, key: str, value: str) -> None:
        self.store[key] = value


class DummyStub:
    """gRPC stub that records calls and returns fixed sentiment."""

    def __init__(self, sentiment: str = "positive") -> None:
        self.sentiment = sentiment
        self.calls: list[tuple[str, str]] = []

    def AnalyzeComment(self, request, timeout: float | None = None):  # noqa: N802 - match gRPC stub naming
        self.calls.append((request.comment_id, request.text))
        return SimpleNamespace(sentiment=self.sentiment)


def make_comment(comment_id: str, text: str) -> RawComment:
    return RawComment(commentId=comment_id, text=text, createdAt=datetime.now(timezone.utc))


def test_duplicate_comments_reuse_cached_sentiment():
    consumer = CommentConsumer()
    cache = DummyCache()
    stub = DummyStub("neutral")

    consumer._cache = cache  # type: ignore[attr-defined]
    consumer._grpc_stub = stub  # type: ignore[attr-defined]

    first = make_comment("c1", "Repeated text for cache")
    second = make_comment("c2", "Repeated text for cache")

    first_sentiment = consumer._fetch_sentiment(first)
    second_sentiment = consumer._fetch_sentiment(second)

    assert first_sentiment == second_sentiment == "neutral"
    assert stub.calls == [(first.commentId, first.text)]