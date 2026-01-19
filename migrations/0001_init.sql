CREATE TABLE IF NOT EXISTS feedback_items (
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  author TEXT,
  text TEXT NOT NULL,
  created_at TEXT NOT NULL,
  sentiment TEXT,
  urgency TEXT,
  value TEXT,
  summary TEXT,
  tags TEXT,
  cluster_id TEXT,
  duplicate_of TEXT
);

CREATE INDEX IF NOT EXISTS idx_feedback_created_at ON feedback_items(created_at);
CREATE INDEX IF NOT EXISTS idx_feedback_cluster_id ON feedback_items(cluster_id);
CREATE INDEX IF NOT EXISTS idx_feedback_sentiment ON feedback_items(sentiment);
CREATE INDEX IF NOT EXISTS idx_feedback_urgency ON feedback_items(urgency);
CREATE INDEX IF NOT EXISTS idx_feedback_value ON feedback_items(value);

CREATE TABLE IF NOT EXISTS clusters (
  cluster_id TEXT PRIMARY KEY,
  title TEXT,
  theme_summary TEXT,
  count INTEGER NOT NULL DEFAULT 0,
  last_seen_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_clusters_last_seen_at ON clusters(last_seen_at);

CREATE TABLE IF NOT EXISTS digests (
  id TEXT PRIMARY KEY,
  date TEXT NOT NULL,
  content_md TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_digests_date ON digests(date);
