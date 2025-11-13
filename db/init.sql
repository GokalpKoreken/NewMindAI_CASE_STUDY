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

CREATE INDEX IF NOT EXISTS idx_processed_comments_processed_at
    ON processed_comments (processed_at DESC);
