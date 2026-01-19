export interface Env {
  DB: D1Database;
  VECTORIZE: VectorizeIndex;
  FEEDBACK_QUEUE: Queue;
  AI: Ai;
  DEV_MODE: string;
}

type Sentiment = "positive" | "neutral" | "negative";
type Urgency = "low" | "medium" | "high";
type Value = "low" | "medium" | "high";

type Classification = {
  sentiment: Sentiment;
  urgency: Urgency;
  value: Value;
  summary: string;
  tags: string[];
};

type ClusterSummary = {
  title: string;
  theme_summary: string;
};

type OverviewResponse = {
  top_clusters: Array<{
    cluster_id: string;
    title: string | null;
    count: number;
    last_seen_at: string | null;
  }>;
  urgent_high_value: Array<{
    id: string;
    source: string;
    summary: string | null;
    urgency: Urgency | null;
    value: Value | null;
    cluster_id: string | null;
  }>;
  sentiment_breakdown: Record<Sentiment, number>;
};

type ClusterDetailResponse = {
  cluster: {
    cluster_id: string;
    title: string | null;
    theme_summary: string | null;
    count: number;
    last_seen_at: string | null;
  } | null;
  feedback: Array<{
    id: string;
    source: string;
    summary: string | null;
    text: string;
    created_at: string;
    sentiment: Sentiment | null;
    urgency: Urgency | null;
    value: Value | null;
  }>;
  sentiment_distribution: Record<Sentiment, number>;
  source_breakdown: Record<string, number>;
};

type DigestResponse = {
  digests: Array<{
    date: string;
    content_md: string;
  }>;
};

const SOURCE_OPTIONS = ["support", "discord", "github", "email", "twitter", "forum"] as const;

const DEFAULT_CLASSIFICATION: Classification = {
  sentiment: "neutral",
  urgency: "medium",
  value: "medium",
  summary: "General product feedback.",
  tags: ["general"],
};

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}

function notFound(): Response {
  return new Response("Not found", { status: 404 });
}

function methodNotAllowed(): Response {
  return new Response("Method not allowed", { status: 405 });
}

function parseJsonBody<T>(request: Request): Promise<T> {
  return request.json() as Promise<T>;
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function pick<T>(items: T[]): T {
  return items[randomInt(0, items.length - 1)];
}

function isoDaysAgo(daysAgo: number): string {
  const now = new Date();
  const offsetMs = randomInt(0, daysAgo * 24 * 60 * 60 * 1000);
  const date = new Date(now.getTime() - offsetMs);
  return date.toISOString();
}

function formatDateNY(date: Date): string {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return formatter.format(date);
}

function safeParseJson<T>(text: string): T | null {
  try {
    return JSON.parse(text) as T;
  } catch {
    const start = text.indexOf("{");
    const end = text.lastIndexOf("}");
    if (start !== -1 && end !== -1 && end > start) {
      try {
        return JSON.parse(text.slice(start, end + 1)) as T;
      } catch {
        return null;
      }
    }
    return null;
  }
}

function deterministicClassification(text: string): Classification {
  const lower = text.toLowerCase();
  const sentiment: Sentiment = lower.includes("love") || lower.includes("great") ? "positive"
    : lower.includes("hate") || lower.includes("bug") || lower.includes("broken") ? "negative"
    : "neutral";
  const urgency: Urgency = lower.includes("crash") || lower.includes("down") ? "high"
    : lower.includes("slow") || lower.includes("urgent") ? "medium"
    : "low";
  const value: Value = lower.includes("billing") || lower.includes("upgrade") ? "high"
    : lower.includes("feature") || lower.includes("api") ? "medium"
    : "low";
  return {
    sentiment,
    urgency,
    value,
    summary: text.length > 120 ? `${text.slice(0, 117)}...` : text,
    tags: ["mock"],
  };
}

async function classifyFeedback(env: Env, text: string): Promise<Classification> {
  if (!env.AI) {
    return env.DEV_MODE === "true" ? deterministicClassification(text) : DEFAULT_CLASSIFICATION;
  }
  const prompt = [
    {
      role: "system",
      content:
        "You are a classifier that returns ONLY valid JSON. No markdown. Schema: " +
        "{ sentiment: 'positive|neutral|negative', urgency: 'low|medium|high', value: 'low|medium|high', summary: string, tags: string[] }",
    },
    {
      role: "user",
      content: `Classify the feedback and respond with JSON only:\n${text}`,
    },
  ];
  try {
    const result = await env.AI.run("@cf/meta/llama-3-8b-instruct", {
      messages: prompt,
      max_tokens: 200,
    });
    const parsed = safeParseJson<Classification>(result.response ?? "");
    if (!parsed) return DEFAULT_CLASSIFICATION;
    return {
      sentiment: parsed.sentiment ?? DEFAULT_CLASSIFICATION.sentiment,
      urgency: parsed.urgency ?? DEFAULT_CLASSIFICATION.urgency,
      value: parsed.value ?? DEFAULT_CLASSIFICATION.value,
      summary: parsed.summary ?? DEFAULT_CLASSIFICATION.summary,
      tags: Array.isArray(parsed.tags) ? parsed.tags : DEFAULT_CLASSIFICATION.tags,
    };
  } catch {
    return DEFAULT_CLASSIFICATION;
  }
}

async function summarizeCluster(env: Env, summaries: string[]): Promise<ClusterSummary> {
  if (!env.AI) {
    return env.DEV_MODE === "true"
      ? { title: "Mixed feedback", theme_summary: summaries.slice(0, 3).join(" ") }
      : { title: "Mixed feedback", theme_summary: "General feedback theme." };
  }
  const prompt = [
    {
      role: "system",
      content:
        "Return ONLY valid JSON. Schema: { title: string, theme_summary: string }",
    },
    {
      role: "user",
      content: `Summarize these feedback summaries into a theme:\n${summaries.join("\n")}`,
    },
  ];
  try {
    const result = await env.AI.run("@cf/meta/llama-3-8b-instruct", {
      messages: prompt,
      max_tokens: 200,
    });
    const parsed = safeParseJson<ClusterSummary>(result.response ?? "");
    if (!parsed) {
      return { title: "Mixed feedback", theme_summary: "General feedback theme." };
    }
    return {
      title: parsed.title ?? "Mixed feedback",
      theme_summary: parsed.theme_summary ?? "General feedback theme.",
    };
  } catch {
    return { title: "Mixed feedback", theme_summary: "General feedback theme." };
  }
}

async function generateDailyDigest(env: Env, payload: unknown): Promise<string> {
  if (!env.AI) {
    return env.DEV_MODE === "true"
      ? `# Daily Digest\n\n## Top themes\n- General product feedback\n\n## Fires\n- None\n\n## Suggested next actions\n- Review top themes\n- Triage urgent items\n- Follow up with users\n`
      : "# Daily Digest\n\n(No data)\n";
  }
  const prompt = [
    {
      role: "system",
      content:
        "Generate a markdown digest with sections: Top themes, Fires, Suggested next actions. Return ONLY markdown.",
    },
    {
      role: "user",
      content: `Create a daily digest using this JSON payload:\n${JSON.stringify(payload, null, 2)}`,
    },
  ];
  try {
    const result = await env.AI.run("@cf/meta/llama-3-8b-instruct", {
      messages: prompt,
      max_tokens: 500,
    });
    return result.response?.trim() || "# Daily Digest\n";
  } catch {
    return "# Daily Digest\n\n(No data)\n";
  }
}

async function embedText(env: Env, text: string): Promise<number[] | null> {
  if (!env.AI) return null;
  try {
    const result = await env.AI.run("@cf/baai/bge-base-en-v1.5", { text });
    if (Array.isArray(result.data) && Array.isArray(result.data[0])) {
      return result.data[0] as number[];
    }
    if (Array.isArray(result.data)) {
      return result.data as number[];
    }
    return null;
  } catch {
    return null;
  }
}

async function processFeedbackMessage(env: Env, feedbackId: string): Promise<void> {
  const row = await env.DB.prepare(
    "SELECT id, source, author, text, created_at, cluster_id FROM feedback_items WHERE id = ?"
  )
    .bind(feedbackId)
    .first();
  if (!row) return;

  const embedding = await embedText(env, row.text);
  let clusterId = row.cluster_id as string | null;
  let duplicateOf: string | null = null;
  let shouldUpsertVector = true;

  if (!embedding) {
    shouldUpsertVector = false;
    if (!clusterId) {
      clusterId = `c_${crypto.randomUUID()}`;
      await env.DB.prepare(
        "INSERT INTO clusters (cluster_id, title, theme_summary, count, last_seen_at) VALUES (?, ?, ?, ?, ?)"
      )
        .bind(clusterId, "New theme", "Cluster created from feedback.", 0, row.created_at)
        .run();
    }
  } else {
    const queryResult = await env.VECTORIZE.query(embedding, {
      topK: 10,
      returnMetadata: true,
    });
    const best = queryResult.matches?.[0];

    if (best && typeof best.score === "number") {
      if (best.score >= 0.9) {
        duplicateOf = (best.metadata?.feedback_id as string) || best.id;
        clusterId = (best.metadata?.cluster_id as string) || null;
      } else if (best.score >= 0.82) {
        clusterId = (best.metadata?.cluster_id as string) || null;
      }
    }

    if (!clusterId) {
      if (best && best.metadata?.cluster_id) {
        clusterId = best.metadata.cluster_id as string;
      } else {
        clusterId = `c_${crypto.randomUUID()}`;
        await env.DB.prepare(
          "INSERT INTO clusters (cluster_id, title, theme_summary, count, last_seen_at) VALUES (?, ?, ?, ?, ?)"
        )
          .bind(clusterId, "New theme", "Cluster created from feedback.", 0, row.created_at)
          .run();
      }
    }
  }

  const classification = await classifyFeedback(env, row.text);
  await env.DB.prepare(
    "UPDATE feedback_items SET sentiment = ?, urgency = ?, value = ?, summary = ?, tags = ?, cluster_id = ?, duplicate_of = ? WHERE id = ?"
  )
    .bind(
      classification.sentiment,
      classification.urgency,
      classification.value,
      classification.summary,
      JSON.stringify(classification.tags),
      clusterId,
      duplicateOf,
      feedbackId
    )
    .run();

  if (!duplicateOf) {
    await env.DB.prepare(
      "UPDATE clusters SET count = count + 1, last_seen_at = ? WHERE cluster_id = ?"
    )
      .bind(row.created_at, clusterId)
      .run();
  }

  if (shouldUpsertVector && embedding) {
    await env.VECTORIZE.upsert([
      {
        id: feedbackId,
        values: embedding,
        metadata: {
          feedback_id: feedbackId,
          source: row.source,
          created_at: row.created_at,
          cluster_id: clusterId,
        },
      },
    ]);
  }

  const clusterRow = await env.DB.prepare(
    "SELECT count FROM clusters WHERE cluster_id = ?"
  )
    .bind(clusterId)
    .first();
  const clusterCount = clusterRow?.count as number | undefined;
  const shouldSummarize = !clusterCount || clusterCount % 25 === 0;
  if (shouldSummarize) {
    const summaries = await env.DB.prepare(
      "SELECT summary FROM feedback_items WHERE cluster_id = ? AND summary IS NOT NULL AND duplicate_of IS NULL ORDER BY created_at DESC LIMIT 20"
    )
      .bind(clusterId)
      .all();
    const summaryList = summaries.results
      .map((item) => item.summary as string)
      .filter(Boolean);
    if (summaryList.length > 0) {
      const clusterSummary = await summarizeCluster(env, summaryList);
      await env.DB.prepare(
        "UPDATE clusters SET title = ?, theme_summary = ? WHERE cluster_id = ?"
      )
        .bind(clusterSummary.title, clusterSummary.theme_summary, clusterId)
        .run();
    }
  }
}

async function generateMockFeedback(env: Env): Promise<{ inserted: number; enqueued: number }> {
  const total = randomInt(100, 500);
  const topics = [
    "authentication",
    "billing",
    "mobile app",
    "API rate limits",
    "dashboard UX",
    "onboarding",
    "docs",
    "performance",
    "exporting data",
    "notifications",
    "search",
    "integrations",
  ];
  const verbs = [
    "is confusing",
    "is slow",
    "is broken",
    "needs improvement",
    "works great",
    "feels clunky",
    "could be faster",
    "is missing key options",
    "fails intermittently",
    "looks great",
  ];
  const contexts = [
    "for our team",
    "during peak hours",
    "on mobile",
    "when using SSO",
    "after the last update",
    "for enterprise accounts",
    "with large datasets",
  ];
  const authors = ["Alex", "Sam", "Jordan", "Priya", "Lee", "Taylor", "Casey", "Morgan"];

  const inserts: D1PreparedStatement[] = [];
  const messages: { feedback_id: string }[] = [];

  for (let i = 0; i < total; i += 1) {
    const id = crypto.randomUUID();
    const source = pick([...SOURCE_OPTIONS]);
    const author = pick(authors);
    const text = `The ${pick(topics)} ${pick(verbs)} ${pick(contexts)}.`;
    const createdAt = isoDaysAgo(7);
    inserts.push(
      env.DB.prepare(
        "INSERT INTO feedback_items (id, source, author, text, created_at) VALUES (?, ?, ?, ?, ?)"
      ).bind(id, source, author, text, createdAt)
    );
    messages.push({ feedback_id: id });
  }

  await env.DB.batch(inserts);
  for (const message of messages) {
    await env.FEEDBACK_QUEUE.send(message);
  }
  return { inserted: total, enqueued: total };
}

async function handleOverview(env: Env): Promise<Response> {
  const topClusters = await env.DB.prepare(
    "SELECT cluster_id, title, count, last_seen_at FROM clusters ORDER BY count DESC LIMIT 10"
  ).all();
  const urgentHighValue = await env.DB.prepare(
    "SELECT id, source, summary, urgency, value, cluster_id FROM feedback_items WHERE urgency = 'high' AND value = 'high' AND duplicate_of IS NULL ORDER BY created_at DESC LIMIT 20"
  ).all();
  const sentimentRows = await env.DB.prepare(
    "SELECT sentiment, COUNT(*) as count FROM feedback_items WHERE sentiment IS NOT NULL AND duplicate_of IS NULL GROUP BY sentiment"
  ).all();

  const sentimentBreakdown: Record<Sentiment, number> = {
    positive: 0,
    neutral: 0,
    negative: 0,
  };
  for (const row of sentimentRows.results) {
    const key = row.sentiment as Sentiment;
    if (key in sentimentBreakdown) {
      sentimentBreakdown[key] = Number(row.count);
    }
  }

  const response: OverviewResponse = {
    top_clusters: topClusters.results.map((row) => ({
      cluster_id: row.cluster_id as string,
      title: row.title as string | null,
      count: Number(row.count ?? 0),
      last_seen_at: row.last_seen_at as string | null,
    })),
    urgent_high_value: urgentHighValue.results.map((row) => ({
      id: row.id as string,
      source: row.source as string,
      summary: row.summary as string | null,
      urgency: row.urgency as Urgency | null,
      value: row.value as Value | null,
      cluster_id: row.cluster_id as string | null,
    })),
    sentiment_breakdown: sentimentBreakdown,
  };

  return jsonResponse(response);
}

async function handleCluster(env: Env, clusterId: string): Promise<Response> {
  const cluster = await env.DB.prepare(
    "SELECT cluster_id, title, theme_summary, count, last_seen_at FROM clusters WHERE cluster_id = ?"
  )
    .bind(clusterId)
    .first();
  const feedbackRows = await env.DB.prepare(
    "SELECT id, source, summary, text, created_at, sentiment, urgency, value FROM feedback_items WHERE cluster_id = ? AND duplicate_of IS NULL ORDER BY created_at DESC LIMIT 20"
  )
    .bind(clusterId)
    .all();
  const sentimentRows = await env.DB.prepare(
    "SELECT sentiment, COUNT(*) as count FROM feedback_items WHERE cluster_id = ? AND sentiment IS NOT NULL AND duplicate_of IS NULL GROUP BY sentiment"
  )
    .bind(clusterId)
    .all();
  const sourceRows = await env.DB.prepare(
    "SELECT source, COUNT(*) as count FROM feedback_items WHERE cluster_id = ? AND duplicate_of IS NULL GROUP BY source"
  )
    .bind(clusterId)
    .all();

  const sentimentDistribution: Record<Sentiment, number> = {
    positive: 0,
    neutral: 0,
    negative: 0,
  };
  for (const row of sentimentRows.results) {
    const key = row.sentiment as Sentiment;
    if (key in sentimentDistribution) {
      sentimentDistribution[key] = Number(row.count);
    }
  }

  const sourceBreakdown: Record<string, number> = {};
  for (const row of sourceRows.results) {
    sourceBreakdown[row.source as string] = Number(row.count);
  }

  const response: ClusterDetailResponse = {
    cluster: cluster
      ? {
          cluster_id: cluster.cluster_id as string,
          title: cluster.title as string | null,
          theme_summary: cluster.theme_summary as string | null,
          count: Number(cluster.count ?? 0),
          last_seen_at: cluster.last_seen_at as string | null,
        }
      : null,
    feedback: feedbackRows.results.map((row) => ({
      id: row.id as string,
      source: row.source as string,
      summary: row.summary as string | null,
      text: row.text as string,
      created_at: row.created_at as string,
      sentiment: row.sentiment as Sentiment | null,
      urgency: row.urgency as Urgency | null,
      value: row.value as Value | null,
    })),
    sentiment_distribution: sentimentDistribution,
    source_breakdown: sourceBreakdown,
  };
  return jsonResponse(response);
}

async function handleDigests(env: Env): Promise<Response> {
  const rows = await env.DB.prepare(
    "SELECT date, content_md FROM digests ORDER BY date DESC LIMIT 30"
  ).all();
  const response: DigestResponse = {
    digests: rows.results.map((row) => ({
      date: row.date as string,
      content_md: row.content_md as string,
    })),
  };
  return jsonResponse(response);
}

function dashboardPage(): Response {
  const html = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>SignalBoard</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; }
      .section { margin-bottom: 24px; }
      .card { padding: 12px; border: 1px solid #e0e0e0; border-radius: 8px; margin-bottom: 12px; }
      button { padding: 8px 12px; }
    </style>
  </head>
  <body>
    <h1>SignalBoard Dashboard</h1>
    <div class="section">
      <button id="mockBtn">Generate Mock Data</button>
      <span id="mockResult"></span>
    </div>
    <div class="section">
      <h2>Top Clusters</h2>
      <div id="clusters"></div>
    </div>
    <div class="section">
      <h2>Urgent + High Value</h2>
      <div id="urgent"></div>
    </div>
    <div class="section">
      <h2>Sentiment Breakdown</h2>
      <div id="sentiment"></div>
    </div>
    <div class="section">
      <a href="/inbox">View Daily Digests</a>
    </div>
    <script>
      const mockBtn = document.getElementById("mockBtn");
      const mockResult = document.getElementById("mockResult");
      mockBtn.addEventListener("click", async () => {
        mockResult.textContent = "Generating...";
        const res = await fetch("/api/mock/generate", { method: "POST" });
        const data = await res.json();
        mockResult.textContent = "Inserted " + data.inserted;
        await loadOverview();
      });

      async function loadOverview() {
        const res = await fetch("/api/dashboard/overview");
        const data = await res.json();
        const clusters = document.getElementById("clusters");
        clusters.innerHTML = "";
        data.top_clusters.forEach((cluster) => {
          const div = document.createElement("div");
          div.className = "card";
          const title = cluster.title || "Untitled cluster";
          div.innerHTML = "<a href='/cluster/" + cluster.cluster_id + "'>" + title + "</a>" +
            " (" + cluster.count + ")<br/><small>Last seen: " + (cluster.last_seen_at || "n/a") + "</small>";
          clusters.appendChild(div);
        });

        const urgent = document.getElementById("urgent");
        urgent.innerHTML = "";
        data.urgent_high_value.forEach((item) => {
          const div = document.createElement("div");
          div.className = "card";
          div.innerHTML = "<strong>" + item.source + "</strong>: " + (item.summary || "No summary");
          urgent.appendChild(div);
        });

        const sentiment = document.getElementById("sentiment");
        sentiment.innerHTML = "";
        sentiment.innerHTML = "Positive: " + data.sentiment_breakdown.positive +
          " | Neutral: " + data.sentiment_breakdown.neutral +
          " | Negative: " + data.sentiment_breakdown.negative;
      }

      loadOverview();
    </script>
  </body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html; charset=utf-8" } });
}

function clusterPage(clusterId: string): Response {
  const html = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>Cluster ${clusterId}</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; }
      .card { padding: 12px; border: 1px solid #e0e0e0; border-radius: 8px; margin-bottom: 12px; }
    </style>
  </head>
  <body>
    <a href="/">Back</a>
    <h1 id="title">Cluster</h1>
    <p id="summary"></p>
    <div id="feedback"></div>
    <script>
      async function loadCluster() {
        const res = await fetch("/api/dashboard/cluster/${clusterId}");
        const data = await res.json();
        document.getElementById("title").textContent = data.cluster?.title || "Cluster ${clusterId}";
        document.getElementById("summary").textContent = data.cluster?.theme_summary || "";
        const feedback = document.getElementById("feedback");
        feedback.innerHTML = "";
        data.feedback.forEach((item) => {
          const div = document.createElement("div");
          div.className = "card";
          div.innerHTML = "<strong>" + item.source + "</strong>: " + (item.summary || item.text);
          feedback.appendChild(div);
        });
      }
      loadCluster();
    </script>
  </body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html; charset=utf-8" } });
}

function digestPage(): Response {
  const html = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>Daily Digests</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; }
      .card { padding: 12px; border: 1px solid #e0e0e0; border-radius: 8px; margin-bottom: 12px; }
    </style>
  </head>
  <body>
    <a href="/">Back</a>
    <h1>Daily Digests</h1>
    <div id="digests"></div>
    <script>
      function renderMarkdown(md) {
        return md
          .replace(/^### (.*)$/gm, "<h3>$1</h3>")
          .replace(/^## (.*)$/gm, "<h2>$1</h2>")
          .replace(/^# (.*)$/gm, "<h1>$1</h1>")
          .replace(/^\\- (.*)$/gm, "<li>$1</li>")
          .replace(/<li>(.*)<\\/li>/g, "<ul><li>$1</li></ul>")
          .replace(/\\n\\n/g, "<br/><br/>");
      }

      async function loadDigests() {
        const res = await fetch("/api/digests");
        const data = await res.json();
        const container = document.getElementById("digests");
        container.innerHTML = "";
        data.digests.forEach((digest) => {
          const div = document.createElement("div");
          div.className = "card";
          div.innerHTML = "<strong>" + digest.date + "</strong><div>" + renderMarkdown(digest.content_md) + "</div>";
          container.appendChild(div);
        });
      }
      loadDigests();
    </script>
  </body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html; charset=utf-8" } });
}

async function handleIngest(request: Request, env: Env): Promise<Response> {
  const body = await parseJsonBody<{ source: string; author: string; text: string }>(request);
  const id = crypto.randomUUID();
  const source = SOURCE_OPTIONS.includes(body.source as (typeof SOURCE_OPTIONS)[number])
    ? body.source
    : "support";
  const createdAt = new Date().toISOString();
  await env.DB.prepare(
    "INSERT INTO feedback_items (id, source, author, text, created_at) VALUES (?, ?, ?, ?, ?)"
  )
    .bind(id, source, body.author || "anon", body.text, createdAt)
    .run();
  await env.FEEDBACK_QUEUE.send({ feedback_id: id });
  return jsonResponse({ id });
}

async function handleRequest(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const path = url.pathname;

  if (path === "/") return dashboardPage();
  if (path === "/inbox") return digestPage();
  if (path.startsWith("/cluster/")) {
    const clusterId = path.split("/")[2];
    return clusterPage(clusterId);
  }

  if (path === "/api/mock/generate") {
    if (request.method !== "POST") return methodNotAllowed();
    const result = await generateMockFeedback(env);
    return jsonResponse(result);
  }

  if (path === "/api/ingest") {
    if (request.method !== "POST") return methodNotAllowed();
    return handleIngest(request, env);
  }

  if (path === "/api/dashboard/overview") {
    if (request.method !== "GET") return methodNotAllowed();
    return handleOverview(env);
  }

  if (path.startsWith("/api/dashboard/cluster/")) {
    if (request.method !== "GET") return methodNotAllowed();
    const clusterId = path.split("/")[4];
    return handleCluster(env, clusterId);
  }

  if (path === "/api/digests") {
    if (request.method !== "GET") return methodNotAllowed();
    return handleDigests(env);
  }

  return notFound();
}

async function handleScheduled(env: Env): Promise<void> {
  const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

  const clusterRows = await env.DB.prepare(
    "SELECT cluster_id, COUNT(*) as count FROM feedback_items WHERE created_at >= ? AND duplicate_of IS NULL GROUP BY cluster_id ORDER BY count DESC LIMIT 5"
  )
    .bind(since)
    .all();

  const clusters: Array<{ cluster_id: string; count: number; title: string | null }> = [];
  for (const row of clusterRows.results) {
    const cluster = await env.DB.prepare(
      "SELECT title FROM clusters WHERE cluster_id = ?"
    )
      .bind(row.cluster_id)
      .first();
    clusters.push({
      cluster_id: row.cluster_id as string,
      count: Number(row.count),
      title: (cluster?.title as string) || null,
    });
  }

  const urgentRows = await env.DB.prepare(
    "SELECT id, source, summary, cluster_id FROM feedback_items WHERE created_at >= ? AND urgency = 'high' AND value = 'high' AND duplicate_of IS NULL ORDER BY created_at DESC LIMIT 10"
  )
    .bind(since)
    .all();

  const payload = {
    window_hours: 24,
    top_clusters: clusters,
    urgent_high_value: urgentRows.results.map((row) => ({
      id: row.id as string,
      source: row.source as string,
      summary: row.summary as string | null,
      cluster_id: row.cluster_id as string | null,
    })),
  };

  const digestMd = await generateDailyDigest(env, payload);
  const dateKey = formatDateNY(new Date());
  await env.DB.prepare(
    "INSERT INTO digests (id, date, content_md) VALUES (?, ?, ?)"
  )
    .bind(crypto.randomUUID(), dateKey, digestMd)
    .run();
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    return handleRequest(request, env);
  },
  async queue(batch: MessageBatch<{ feedback_id: string }>, env: Env, ctx: ExecutionContext) {
    for (const message of batch.messages) {
      try {
        await processFeedbackMessage(env, message.body.feedback_id);
      } catch (error) {
        console.log("Queue processing error", error);
      }
    }
  },
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    try {
      await handleScheduled(env);
    } catch (error) {
      console.log("Scheduled digest error", error);
    }
  },
};
