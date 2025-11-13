# Lounge Comments Pipeline

End-to-end pipeline that produces synthetic customer comments, enriches them with sentiment analysis, and serves processed feedback over a REST API. The stack uses Kafka for streaming, a gRPC microservice for sentiment analysis, PostgreSQL for persistence, and Redis for caching repeated analyses.

## Components

- **Producer (`producer/`)** – emits randomised comments into the `raw-comments` Kafka topic at variable intervals, reusing some texts with new `commentId` values to exercise cache paths.
- **Sentiment Service (`sentiment_service/`)** – gRPC microservice that applies deterministic sentiment per text, simulates latency proportional to text length, rate-limits at 100 req/s, and randomly drops requests.
- **Consumer (`consumer/`)** – subscribes to raw comments, retries gRPC calls with exponential backoff, caches sentiment results in Redis, republishes to `processed-comments`, and persists rows into PostgreSQL.
- **REST API (`rest_api/`)** – FastAPI service exposing processed comments with sentiment filtering.
- **Infrastructure (`docker-compose.yml`)** – orchestrates Kafka (KRaft), Redis, PostgreSQL, and all Python services.

## Prerequisites

- Docker 24+
- Docker Compose v2 (or the legacy `docker-compose` CLI used in the commands below)

### Linux setup

```bash
sudo apt update
sudo apt install docker.io docker-compose
docker --version
docker-compose --version
```

> Confirm both binaries print versions before continuing. If Docker requires elevated privileges, add your user to the `docker` group or run Compose commands with `sudo`.

### Windows setup

1. Install Docker Desktop for Windows from https://docs.docker.com/desktop/setup/install/windows-install/ (requires WSL 2 or Hyper-V).
2. After installation, open PowerShell or Windows Terminal and verify versions:

    ```powershell
    docker --version
    docker-compose --version
    ```

    Docker Desktop bundles Compose; if the legacy `docker-compose` binary is not available, use `docker compose` in subsequent commands.

## Bootstrapping

```bash
docker-compose build
docker-compose up
```

> The first start downloads base images and applies the database schema from `db/init.sql`.

## Useful Commands

- Regenerate gRPC stubs after editing `proto/sentiment.proto`:

  ```bash
  ./scripts/generate_proto.sh
  ```

- Tail processed comments as JSON:

  ```bash
  docker-compose logs -f comment-consumer
  ```

## Testing

- Run unit tests locally to verify cache behaviour:

  ```bash
  python -m pytest
  ```

- Continuous integration executes the same pytest suite on every push/pull request (see `.github/workflows/tests.yml`). The included `tests/test_consumer_cache.py` ensures duplicate comment texts reuse cached sentiment responses.

## REST API

Once the stack is running, the API is available at `http://localhost:8000`.

- `GET /healthz` – health probe.
- `GET /comments` – list processed comments. Supports `sentiment` (`positive`, `negative`, `neutral`, `unknown`) and `limit` query parameters.
- `GET /comments/export` – stream the same queryable dataset as CSV with `sentiment` and `limit` options, forcing a file download.

Example:

```bash
curl "http://localhost:8000/comments?sentiment=positive&limit=20"
```

Download as CSV:

```bash
curl -L "http://localhost:8000/comments/export?limit=100" -o comments.csv
```

## Environment Variables

Most defaults are set within `docker-compose.yml`. Adjust them there to tweak timings, drop probabilities, or connection settings.

## Development Notes

- Services automatically retry connections to Kafka, Redis, PostgreSQL, and the gRPC endpoint.
- Redis caches sentiments for 24 hours inside the consumer service; change `CACHE_TTL_SECONDS` to tune deduplication horizons.
- The REST API reads processed comments directly from PostgreSQL and does not use Redis or any additional caching layer.
- The gRPC service enforces a 100 rps rate limit and introduces latency of `BASE_DELAY_SECONDS + (DELAY_PER_CHAR_SECONDS × text length)`.

## Cleanup

```bash
docker-compose down 
```
