from __future__ import annotations

import os
import csv
import io
from datetime import datetime
from typing import Iterator, List, Optional

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

DATABASE_HOST = os.getenv("POSTGRES_HOST", "postgres")
DATABASE_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DATABASE_NAME = os.getenv("POSTGRES_DB", "comments")
DATABASE_USER = os.getenv("POSTGRES_USER", "app")
DATABASE_PASSWORD = os.getenv("POSTGRES_PASSWORD", "appsecret")

app = FastAPI(title="Comment Sentiment API", version="1.0.0")

_pool: Optional[asyncpg.Pool] = None


class Comment(BaseModel):
    commentId: str
    text: str
    sentiment: str
    createdAt: datetime
    processedAt: datetime


async def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise HTTPException(status_code=503, detail="Database pool not available")
    return _pool


@app.on_event("startup")
async def startup() -> None:
    global _pool  # pylint: disable=global-statement
    _pool = await asyncpg.create_pool(
        host=DATABASE_HOST,
        port=DATABASE_PORT,
        user=DATABASE_USER,
        password=DATABASE_PASSWORD,
        database=DATABASE_NAME,
        min_size=1,
        max_size=int(os.getenv("DB_POOL_MAX_SIZE", "10")),
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    global _pool  # pylint: disable=global-statement
    if _pool is not None:
        await _pool.close()
        _pool = None


@app.get("/healthz", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/comments", response_model=List[Comment], tags=["comments"])
async def list_comments(
    sentiment: Optional[str] = Query(None, description="Filter by sentiment label"),
    limit: int = Query(50, ge=1, le=200, description="Maximum number of comments to return"),
    pool: asyncpg.Pool = Depends(get_pool),
) -> List[Comment]:
    rows = await _fetch_comments(pool=pool, sentiment=sentiment, limit=limit)

    return [
        Comment(
            commentId=row["comment_id"],
            text=row["comment_text"],
            sentiment=row["sentiment"],
            createdAt=row["source_created_at"],
            processedAt=row["processed_at"],
        )
        for row in rows
    ]


async def _fetch_comments(
    pool: asyncpg.Pool,
    sentiment: Optional[str],
    limit: int,
) -> List[asyncpg.Record]:
    filters: list[str] = []
    values: list[str] = []
    if sentiment:
        filters.append(f"LOWER(sentiment) = LOWER(${len(values) + 1})")
        values.append(sentiment)

    where_clause = f"WHERE {' AND '.join(filters)} " if filters else ""
    order_clause = "ORDER BY processed_at DESC "
    limit_placeholder = len(values) + 1
    query = (
        "SELECT comment_id, comment_text, sentiment, source_created_at, processed_at "
        "FROM processed_comments "
        f"{where_clause}"
        f"{order_clause}"
        f"LIMIT ${limit_placeholder}"
    )

    async with pool.acquire() as connection:
        return await connection.fetch(query, *values, limit)


@app.get("/comments/export", tags=["comments"], response_class=StreamingResponse)
async def export_comments(
    sentiment: Optional[str] = Query(None, description="Filter by sentiment label"),
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of comments to include"),
    pool: asyncpg.Pool = Depends(get_pool),
) -> StreamingResponse:
    rows = await _fetch_comments(pool=pool, sentiment=sentiment, limit=limit)

    headers = {
        "Content-Disposition": "attachment; filename=comments.csv",
        "Cache-Control": "no-cache",
    }

    def row_stream() -> Iterator[str]:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["comment_id", "comment_text", "sentiment", "source_created_at", "processed_at"])
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        for row in rows:
            writer.writerow(
                [
                    row["comment_id"],
                    row["comment_text"],
                    row["sentiment"],
                    row["source_created_at"].isoformat() if row["source_created_at"] else "",
                    row["processed_at"].isoformat() if row["processed_at"] else "",
                ]
            )
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    return StreamingResponse(row_stream(), media_type="text/csv", headers=headers)
