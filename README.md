# SignalBoard

SignalBoard is a Cloudflare Workers MVP that aggregates product feedback, clusters themes, classifies sentiment/urgency/value, and generates daily digest summaries.

## What’s included

- Cloudflare Worker API + UI
- D1 (SQLite) storage with migrations
- Vectorize index for embeddings and clustering
- Async processing (no Queues on free plan)
- Cron trigger for daily digest generation
- Minimal UI (HTML + vanilla JS)

## Local development

```bash
npm install
wrangler dev
```

## Cloudflare resources

### Create D1 database

```bash
wrangler d1 create signalboard-db
```

Update `wrangler.toml` with the returned `database_id`.

Run migrations:

```bash
wrangler d1 migrations apply signalboard-db --local
wrangler d1 migrations apply signalboard-db
```

### Create Vectorize index

```bash
wrangler vectorize create feedback_embeddings --dimensions=768 --metric=cosine
```

## Deploy

```bash
wrangler deploy
```

## Environment flags

- `DEV_MODE=true` allows deterministic mock classification if AI binding is missing.
- `FREE_PLAN=true` caps mock generation to 150 items per run to stay within free quotas.

## API endpoints

### Generate mock data

```bash
curl -X POST http://127.0.0.1:8787/api/mock/generate
```

### Ingest a single item

```bash
curl -X POST http://127.0.0.1:8787/api/ingest \
  -H "content-type: application/json" \
  -d '{"source":"github","author":"kim","text":"Billing page times out when exporting invoices."}'
```

### Dashboard overview

```bash
curl http://127.0.0.1:8787/api/dashboard/overview
```

### Cluster detail

```bash
curl http://127.0.0.1:8787/api/dashboard/cluster/c_example
```

### Digests

```bash
curl http://127.0.0.1:8787/api/digests
```

## UI pages

- Dashboard: http://127.0.0.1:8787/
- Cluster detail: http://127.0.0.1:8787/cluster/:id
- Inbox (daily digests): http://127.0.0.1:8787/inbox

## Notes on clustering

- Cluster counts exclude duplicates (items with `duplicate_of`).
- New clusters are created when similarity < 0.82.
- Duplicates are marked when similarity >= 0.90 and inherit the cluster.

## Free plan guidance

- Keep `FREE_PLAN=true` to limit mock ingestion volume.
- Workers AI usage is per item; avoid repeated bulk mock runs.

## Wrangler config

Update the placeholders in `wrangler.toml`:

- `database_id` for D1
- Vectorize index name (default: `feedback_embeddings`)

Cron trigger runs at `0 14 * * *` (14:00 UTC) which is 9am America/New_York during standard time.
