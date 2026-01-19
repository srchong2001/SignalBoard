export interface Env {
  DB: D1Database;
  VECTORIZE: VectorizeIndex;
  AI: Ai;
  DEV_MODE: string;
  FREE_PLAN: string;
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
    theme_summary: string | null;
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
const SEEDED_MOCK_DATA = [
  {
    id: "fb-001",
    source: "github",
    author: "alice-dev",
    created_at: "2026-01-14T09:12:00Z",
    text:
      "API responses are intermittently returning 504 errors under moderate load. This started after the last deploy.",
  },
  {
    id: "fb-002",
    source: "support",
    author: "customer_4921",
    created_at: "2026-01-14T10:45:00Z",
    text: "Our production app is timing out when calling your API. We didn’t change anything on our end.",
  },
  {
    id: "fb-003",
    source: "discord",
    author: "latency_issues",
    created_at: "2026-01-14T11:02:00Z",
    text: "Anyone else seeing super slow API responses today? P95 latency feels way worse.",
  },
  {
    id: "fb-004",
    source: "forum",
    author: "new_user_88",
    created_at: "2026-01-13T16:21:00Z",
    text: "Getting started docs are confusing. I couldn’t figure out how to generate an API token.",
  },
  {
    id: "fb-005",
    source: "email",
    author: "cto@startup.io",
    created_at: "2026-01-13T17:10:00Z",
    text:
      "We struggled onboarding engineers because the documentation jumps between basic and advanced concepts.",
  },
  {
    id: "fb-006",
    source: "twitter",
    author: "@buildfast",
    created_at: "2026-01-13T18:03:00Z",
    text:
      "Love the product but the docs need a serious overhaul. Took us hours to find auth examples.",
  },
  {
    id: "fb-007",
    source: "support",
    author: "billing_user_123",
    created_at: "2026-01-12T09:40:00Z",
    text: "I was charged twice this month and support hasn’t resolved it yet.",
  },
  {
    id: "fb-008",
    source: "email",
    author: "finance@enterprise.com",
    created_at: "2026-01-12T10:15:00Z",
    text: "Our invoice includes usage we can’t account for. This is blocking internal approval.",
  },
  {
    id: "fb-009",
    source: "discord",
    author: "angry_payer",
    created_at: "2026-01-12T10:55:00Z",
    text: "Billing dashboard doesn’t match actual usage. This is pretty concerning.",
  },
  {
    id: "fb-010",
    source: "github",
    author: "mobile-dev",
    created_at: "2026-01-11T08:33:00Z",
    text: "The Android SDK crashes when initializing on Android 14 devices.",
  },
  {
    id: "fb-011",
    source: "support",
    author: "app_team_lead",
    created_at: "2026-01-11T09:02:00Z",
    text: "Our Android app is crashing on startup after upgrading the SDK.",
  },
  {
    id: "fb-012",
    source: "forum",
    author: "sdk_user",
    created_at: "2026-01-11T09:47:00Z",
    text: "Any workaround for Android 14 crashes? We’re blocked from shipping.",
  },
  {
    id: "fb-013",
    source: "twitter",
    author: "@frontendfan",
    created_at: "2026-01-10T14:12:00Z",
    text: "Dark mode for the dashboard would be amazing 👀",
  },
  {
    id: "fb-014",
    source: "forum",
    author: "ui_feedback",
    created_at: "2026-01-10T15:01:00Z",
    text: "The dashboard is really bright at night. A dark mode option would help.",
  },
  {
    id: "fb-015",
    source: "discord",
    author: "night_coder",
    created_at: "2026-01-10T15:22:00Z",
    text: "Please add dark mode. My eyes hurt 😅",
  },
  {
    id: "fb-016",
    source: "support",
    author: "security_team",
    created_at: "2026-01-09T11:30:00Z",
    text: "We need SSO support for Okta before we can roll this out company-wide.",
  },
  {
    id: "fb-017",
    source: "email",
    author: "it@enterprise.org",
    created_at: "2026-01-09T12:10:00Z",
    text: "Lack of SAML/SSO is currently a blocker for adoption.",
  },
  {
    id: "fb-018",
    source: "github",
    author: "api_user",
    created_at: "2026-01-08T09:05:00Z",
    text: "Rate limit errors are hard to debug. Can we get better error messages?",
  },
  {
    id: "fb-019",
    source: "forum",
    author: "dx_matters",
    created_at: "2026-01-08T10:18:00Z",
    text: "When hitting rate limits, it’s unclear how long to wait before retrying.",
  },
  {
    id: "fb-020",
    source: "twitter",
    author: "@happyuser",
    created_at: "2026-01-07T16:45:00Z",
    text: "Shoutout to the support team, super fast and helpful responses 🙌",
  },
] as const;

const MOCK_DIGEST_MD = `# Daily Digest

## Top themes
- API reliability: elevated 504s and latency under moderate load
- Documentation clarity: confusion around API tokens and onboarding
- Billing accuracy: invoice mismatches and double charges

## Fires
- API timeouts affecting production workloads
- Android 14 SDK crash blocking releases

## Suggested next actions
- Triage API latency regression and post status update
- Publish a short auth/token setup guide
- Audit recent billing changes and reconcile invoices
`;

const DEFAULT_CLASSIFICATION: Classification = {
  sentiment: "neutral",
  urgency: "medium",
  value: "medium",
  summary: "General product feedback.",
  tags: ["general"],
};

function deriveClusterLabel(text: string): { title: string; theme_summary: string } {
  const lower = text.toLowerCase();
  if (lower.includes("504") || lower.includes("timeout") || lower.includes("latency")) {
    return { title: "API reliability", theme_summary: "API timeouts and latency regressions." };
  }
  if (lower.includes("docs") || lower.includes("documentation") || lower.includes("token")) {
    return { title: "Documentation clarity", theme_summary: "Users struggle with docs and onboarding steps." };
  }
  if (lower.includes("billing") || lower.includes("invoice") || lower.includes("charged")) {
    return { title: "Billing accuracy", theme_summary: "Billing and invoice discrepancies reported." };
  }
  if (lower.includes("android") || lower.includes("sdk")) {
    return { title: "Android stability", theme_summary: "Android SDK crashes and stability issues." };
  }
  if (lower.includes("dark mode")) {
    return { title: "Dark mode request", theme_summary: "Requests for a darker dashboard theme." };
  }
  if (lower.includes("sso") || lower.includes("saml") || lower.includes("okta")) {
    return { title: "SSO adoption blockers", theme_summary: "Enterprise SSO requirements blocking rollout." };
  }
  if (lower.includes("rate limit")) {
    return { title: "Rate limit UX", theme_summary: "Unclear rate limit errors and retry guidance." };
  }
  if (lower.includes("support") && (lower.includes("fast") || lower.includes("helpful"))) {
    return { title: "Support praise", theme_summary: "Positive feedback on support responsiveness." };
  }
  return { title: "Product feedback", theme_summary: "General product feedback theme." };
}

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
    const result = (await env.AI.run("@cf/baai/bge-base-en-v1.5", { text })) as unknown;
    const data = (result as { data?: unknown }).data ?? result;
    if (Array.isArray(data) && Array.isArray(data[0])) {
      return data[0] as number[];
    }
    if (Array.isArray(data)) {
      return data as number[];
    }
    return null;
  } catch {
    return null;
  }
}

async function processFeedbackMessage(env: Env, feedbackId: string): Promise<void> {
  const row = (await env.DB.prepare(
    "SELECT id, source, author, text, created_at, cluster_id FROM feedback_items WHERE id = ?"
  )
    .bind(feedbackId)
    .first()) as
    | {
        id: string;
        source: string;
        author: string | null;
        text: string;
        created_at: string;
        cluster_id: string | null;
      }
    | null;
  if (!row) return;

  const embedding = await embedText(env, row.text);
  let clusterId = row.cluster_id as string | null;
  let duplicateOf: string | null = null;
  let shouldUpsertVector = true;
  const derived = deriveClusterLabel(row.text);

  if (!embedding) {
    shouldUpsertVector = false;
    if (!clusterId) {
      const existing = await env.DB.prepare(
        "SELECT cluster_id FROM clusters WHERE title = ? LIMIT 1"
      )
        .bind(derived.title)
        .first();
      if (existing?.cluster_id) {
        clusterId = existing.cluster_id as string;
      } else {
        clusterId = `c_${crypto.randomUUID()}`;
        await env.DB.prepare(
          "INSERT INTO clusters (cluster_id, title, theme_summary, count, last_seen_at) VALUES (?, ?, ?, ?, ?)"
        )
          .bind(clusterId, derived.title, derived.theme_summary, 0, row.created_at)
          .run();
      }
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
        const existing = await env.DB.prepare(
          "SELECT cluster_id FROM clusters WHERE title = ? LIMIT 1"
        )
          .bind(derived.title)
          .first();
        if (existing?.cluster_id) {
          clusterId = existing.cluster_id as string;
        } else {
          clusterId = `c_${crypto.randomUUID()}`;
          await env.DB.prepare(
            "INSERT INTO clusters (cluster_id, title, theme_summary, count, last_seen_at) VALUES (?, ?, ?, ?, ?)"
          )
            .bind(clusterId, derived.title, derived.theme_summary, 0, row.created_at)
            .run();
        }
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

  const clusterMeta = await env.DB.prepare(
    "SELECT title, theme_summary FROM clusters WHERE cluster_id = ?"
  )
    .bind(clusterId)
    .first();
  const title = (clusterMeta?.title as string | null) ?? null;
  const themeSummary = (clusterMeta?.theme_summary as string | null) ?? null;
  const isPlaceholder =
    !title ||
    !themeSummary ||
    title === "New theme" ||
    themeSummary === "Cluster created from feedback.";
  if (isPlaceholder) {
    await env.DB.prepare(
      "UPDATE clusters SET title = ?, theme_summary = ? WHERE cluster_id = ?"
    )
      .bind(derived.title, derived.theme_summary, clusterId)
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
        } as Record<string, VectorizeVectorMetadata>,
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

async function generateMockFeedback(
  env: Env
): Promise<{ inserted: number; enqueued: number; ids: string[] }> {
  const freePlan = env.FREE_PLAN === "true";
  const maxItems = freePlan ? 150 : 500;
  const total = randomInt(100, maxItems);
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
  const messages: string[] = [];

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
    messages.push(id);
  }

  await env.DB.batch(inserts);
  return { inserted: total, enqueued: 0, ids: messages };
}

async function processFeedbackIds(env: Env, ids: string[], ctx: ExecutionContext) {
  ctx.waitUntil(
    (async () => {
      for (const id of ids) {
        try {
          await processFeedbackMessage(env, id);
        } catch (error) {
          console.log("Processing error", error);
        }
      }
    })()
  );
}

async function handleOverview(env: Env): Promise<Response> {
  const topClusters = await env.DB.prepare(
    "SELECT cluster_id, title, theme_summary, count, last_seen_at FROM clusters ORDER BY count DESC LIMIT 10"
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
      theme_summary: row.theme_summary as string | null,
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

async function handleSeed(env: Env, ctx: ExecutionContext): Promise<Response> {
  const inserts: D1PreparedStatement[] = [];
  const ids: string[] = [];
  for (const item of SEEDED_MOCK_DATA) {
    inserts.push(
      env.DB.prepare(
        "INSERT OR IGNORE INTO feedback_items (id, source, author, text, created_at) VALUES (?, ?, ?, ?, ?)"
      ).bind(item.id, item.source, item.author, item.text, item.created_at)
    );
    ids.push(item.id);
  }
  await env.DB.batch(inserts);
  processFeedbackIds(env, ids, ctx);
  return jsonResponse({ inserted: SEEDED_MOCK_DATA.length, enqueued: 0 });
}

async function handleRefreshClusters(env: Env): Promise<Response> {
  const clusters = await env.DB.prepare(
    "SELECT cluster_id FROM clusters ORDER BY last_seen_at DESC"
  ).all();
  let updated = 0;
  for (const row of clusters.results) {
    const latest = await env.DB.prepare(
      "SELECT text FROM feedback_items WHERE cluster_id = ? AND duplicate_of IS NULL ORDER BY created_at DESC LIMIT 1"
    )
      .bind(row.cluster_id)
      .first();
    if (!latest) {
      await env.DB.prepare("DELETE FROM clusters WHERE cluster_id = ?")
        .bind(row.cluster_id)
        .run();
      continue;
    }
    const derived = deriveClusterLabel(latest.text as string);
    await env.DB.prepare(
      "UPDATE clusters SET title = ?, theme_summary = ? WHERE cluster_id = ?"
    )
      .bind(derived.title, derived.theme_summary, row.cluster_id)
      .run();
    updated += 1;
  }
  return jsonResponse({ updated });
}

async function recalcClusterStats(env: Env): Promise<void> {
  const rows = await env.DB.prepare("SELECT cluster_id FROM clusters").all();
  for (const row of rows.results) {
    const stats = await env.DB.prepare(
      "SELECT COUNT(*) as count, MAX(created_at) as last_seen_at FROM feedback_items WHERE cluster_id = ? AND duplicate_of IS NULL"
    )
      .bind(row.cluster_id)
      .first();
    const count = Number(stats?.count ?? 0);
    const lastSeen = (stats?.last_seen_at as string | null) ?? null;
    if (count === 0) {
      await env.DB.prepare("DELETE FROM clusters WHERE cluster_id = ?")
        .bind(row.cluster_id)
        .run();
      continue;
    }
    await env.DB.prepare(
      "UPDATE clusters SET count = ?, last_seen_at = ? WHERE cluster_id = ?"
    )
      .bind(count, lastSeen, row.cluster_id)
      .run();
  }
}

async function handleMergeClusters(env: Env): Promise<Response> {
  const rows = await env.DB.prepare(
    "SELECT cluster_id, title, theme_summary FROM clusters ORDER BY last_seen_at DESC"
  ).all();

  const canonicalByTitle = new Map<string, string>();
  const toDelete: string[] = [];

  for (const row of rows.results) {
    let title = (row.title as string | null) ?? null;
    const theme = (row.theme_summary as string | null) ?? null;
    const isPlaceholder =
      !title ||
      !theme ||
      title === "New theme" ||
      theme === "Cluster created from feedback.";

    if (isPlaceholder) {
      const latest = await env.DB.prepare(
        "SELECT text FROM feedback_items WHERE cluster_id = ? AND duplicate_of IS NULL ORDER BY created_at DESC LIMIT 1"
      )
        .bind(row.cluster_id)
        .first();
      if (!latest) {
        toDelete.push(row.cluster_id as string);
        continue;
      }
      const derived = deriveClusterLabel(latest.text as string);
      title = derived.title;
      await env.DB.prepare(
        "UPDATE clusters SET title = ?, theme_summary = ? WHERE cluster_id = ?"
      )
        .bind(derived.title, derived.theme_summary, row.cluster_id)
        .run();
    }

    const key = (title ?? "").trim().toLowerCase();
    if (!key) {
      toDelete.push(row.cluster_id as string);
      continue;
    }
    if (!canonicalByTitle.has(key)) {
      canonicalByTitle.set(key, row.cluster_id as string);
    } else {
      const canonical = canonicalByTitle.get(key) as string;
      await env.DB.prepare(
        "UPDATE feedback_items SET cluster_id = ? WHERE cluster_id = ?"
      )
        .bind(canonical, row.cluster_id)
        .run();
      toDelete.push(row.cluster_id as string);
    }
  }

  for (const clusterId of toDelete) {
    await env.DB.prepare("DELETE FROM clusters WHERE cluster_id = ?")
      .bind(clusterId)
      .run();
  }

  await recalcClusterStats(env);
  return jsonResponse({ merged: toDelete.length });
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

async function handleDigestMock(env: Env): Promise<Response> {
  const dateKey = formatDateNY(new Date());
  await env.DB.prepare(
    "INSERT INTO digests (id, date, content_md) VALUES (?, ?, ?)"
  )
    .bind(crypto.randomUUID(), dateKey, MOCK_DIGEST_MD)
    .run();
  return jsonResponse({ inserted: 1, date: dateKey });
}

function dashboardPage(): Response {
  const html = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>SignalBoard</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f7f7f8;
        --panel: #ffffff;
        --text: #1f2933;
        --muted: #667085;
        --border: #e5e7eb;
        --accent: #1d4ed8;
        --accent-soft: #e0e7ff;
        --success: #16a34a;
        --warning: #d97706;
        --danger: #dc2626;
      }
      * { box-sizing: border-box; }
      body {
        font-family: "Inter", "Segoe UI", Arial, sans-serif;
        margin: 0;
        background: var(--bg);
        color: var(--text);
      }
      header {
        background: var(--panel);
        border-bottom: 1px solid var(--border);
        padding: 20px 24px;
        position: sticky;
        top: 0;
        z-index: 10;
      }
      header h1 { margin: 0; font-size: 24px; }
      header p { margin: 4px 0 0; color: var(--muted); font-size: 14px; }
      .container { max-width: 1100px; margin: 0 auto; padding: 24px; }
      .section { margin-bottom: 24px; }
      .section-title { font-size: 18px; margin: 0 0 12px; }
      .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; }
      .card {
        padding: 16px;
        border: 1px solid var(--border);
        border-radius: 12px;
        background: var(--panel);
        box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04);
      }
      .cluster-link {
        color: var(--accent);
        font-weight: 600;
        text-decoration: none;
      }
      .cluster-meta { color: var(--muted); font-size: 12px; margin-top: 6px; }
      .pill {
        display: inline-flex;
        align-items: center;
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 600;
        background: var(--accent-soft);
        color: var(--accent);
      }
      .pill.positive { background: #dcfce7; color: var(--success); }
      .pill.neutral { background: #fef9c3; color: var(--warning); }
      .pill.negative { background: #fee2e2; color: var(--danger); }
      .empty {
        padding: 16px;
        border: 1px dashed var(--border);
        border-radius: 12px;
        color: var(--muted);
        background: #fafafa;
      }
      footer { padding: 16px 24px 32px; color: var(--muted); font-size: 12px; }
    </style>
  </head>
  <body>
    <header>
      <h1>SignalBoard Dashboard</h1>
      <p>Live view of feedback signals, urgent issues, and sentiment trends.</p>
    </header>
    <div class="container">
    <div class="section">
      <div class="section-title">Top clusters</div>
      <div id="clusters" class="grid"></div>
    </div>
    <div class="section">
      <div class="section-title">Urgent + high value</div>
      <div id="urgent" class="grid"></div>
    </div>
    <div class="section">
      <div class="section-title">Sentiment breakdown</div>
      <div id="sentiment" class="card"></div>
    </div>
    <div class="section">
      <a class="cluster-link" href="/inbox">View daily digests</a>
    </div>
    </div>
    <footer class="container">Data refreshes when the page reloads.</footer>
    <script>
      const clusters = document.getElementById("clusters");
      const urgent = document.getElementById("urgent");
      const sentiment = document.getElementById("sentiment");

      function renderEmpty(container, message) {
        container.innerHTML = "<div class='empty'>" + message + "</div>";
      }

      async function loadOverview() {
        clusters.innerHTML = "<div class='card'>Loading clusters...</div>";
        urgent.innerHTML = "<div class='card'>Loading urgent items...</div>";
        sentiment.innerHTML = "Loading sentiment...";
        const res = await fetch("/api/dashboard/overview");
        const data = await res.json();

        clusters.innerHTML = "";
        if (!data.top_clusters.length) {
          renderEmpty(clusters, "No clusters yet. Seed or ingest feedback to get started.");
        } else {
          data.top_clusters.forEach((cluster) => {
            const div = document.createElement("div");
            div.className = "card";
            const title = cluster.theme_summary || cluster.title || "Untitled cluster";
            div.innerHTML =
              "<a class='cluster-link' href='/cluster/" + cluster.cluster_id + "'>" + title + "</a>" +
              " <span class='pill'>" + cluster.count + " items</span>" +
              "<div class='cluster-meta'>Last seen: " + (cluster.last_seen_at || "n/a") + "</div>";
            clusters.appendChild(div);
          });
        }

        urgent.innerHTML = "";
        if (!data.urgent_high_value.length) {
          renderEmpty(urgent, "No urgent high-value items yet.");
        } else {
          data.urgent_high_value.forEach((item) => {
            const div = document.createElement("div");
            div.className = "card";
            div.innerHTML =
              "<strong>" + item.source + "</strong>" +
              "<div style='margin-top:6px;color:var(--muted);'>" + (item.summary || "No summary") + "</div>";
            urgent.appendChild(div);
          });
        }

        sentiment.innerHTML =
          "<span class='pill positive'>Positive " + data.sentiment_breakdown.positive + "</span> " +
          "<span class='pill neutral'>Neutral " + data.sentiment_breakdown.neutral + "</span> " +
          "<span class='pill negative'>Negative " + data.sentiment_breakdown.negative + "</span>";
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
      body { font-family: "Inter", "Segoe UI", Arial, sans-serif; margin: 0; background: #f7f7f8; color: #1f2933; }
      header { padding: 20px 24px; border-bottom: 1px solid #e5e7eb; background: #ffffff; }
      a { color: #1d4ed8; text-decoration: none; }
      .container { max-width: 900px; margin: 0 auto; padding: 24px; }
      .card { padding: 16px; border: 1px solid #e5e7eb; border-radius: 12px; margin-bottom: 12px; background: #ffffff; }
      .muted { color: #667085; }
    </style>
  </head>
  <body>
    <header>
      <a href="/">Back to dashboard</a>
    </header>
    <div class="container">
      <h1 id="title">Cluster</h1>
      <p id="summary" class="muted"></p>
      <h3>Recent feedback</h3>
      <div id="feedback"></div>
    </div>
    <script>
      async function loadCluster() {
        const res = await fetch("/api/dashboard/cluster/${clusterId}");
        const data = await res.json();
        document.getElementById("title").textContent = data.cluster?.title || "Cluster ${clusterId}";
        document.getElementById("summary").textContent = data.cluster?.theme_summary || "";
        const feedback = document.getElementById("feedback");
        feedback.innerHTML = "";
        if (!data.feedback.length) {
          feedback.innerHTML = "<div class='card muted'>No feedback yet for this cluster.</div>";
          return;
        }
        data.feedback.forEach((item) => {
          const div = document.createElement("div");
          div.className = "card";
          div.innerHTML =
            "<strong>" + item.source + "</strong>" +
            "<div class='muted' style='margin-top:6px;'>" + (item.summary || item.text) + "</div>";
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
      :root {
        --bg: #f7f7f8;
        --panel: #ffffff;
        --text: #1f2933;
        --muted: #667085;
        --border: #e5e7eb;
        --accent: #1d4ed8;
        --accent-soft: #e0e7ff;
      }
      body { font-family: "Inter", "Segoe UI", Arial, sans-serif; margin: 0; background: var(--bg); color: var(--text); }
      header { padding: 20px 24px; border-bottom: 1px solid var(--border); background: var(--panel); }
      a { color: var(--accent); text-decoration: none; }
      .container { max-width: 900px; margin: 0 auto; padding: 24px; }
      .card { padding: 20px; border: 1px solid var(--border); border-radius: 16px; margin-bottom: 16px; background: var(--panel); box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04); }
      .muted { color: var(--muted); }
      .digest-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; }
      .digest-date { background: var(--accent-soft); color: var(--accent); padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; }
      .digest-content h1 { font-size: 22px; margin: 8px 0 12px; }
      .digest-content h2 { font-size: 18px; margin: 18px 0 8px; }
      .digest-content h3 { font-size: 16px; margin: 14px 0 6px; }
      .digest-content p { margin: 6px 0; color: var(--text); }
      .digest-content ul { padding-left: 18px; margin: 8px 0; }
      .digest-content li { margin: 6px 0; }
      .empty { padding: 16px; border: 1px dashed var(--border); border-radius: 12px; background: #fafafa; color: var(--muted); }
    </style>
  </head>
  <body>
    <header>
      <a href="/">Back to dashboard</a>
    </header>
    <div class="container">
      <h1>Daily Digests</h1>
      <p class="muted">Generated at 9am America/New_York. Each digest highlights themes, fires, and suggested actions.</p>
      <div id="digests"></div>
    </div>
    <script>
      function renderMarkdown(md) {
        const lines = md.split(/\\n/);
        let html = "";
        let inList = false;
        for (const line of lines) {
          if (line.startsWith("# ")) {
            continue;
          }
          if (line.startsWith("- ")) {
            if (!inList) {
              html += "<ul>";
              inList = true;
            }
            html += "<li>" + line.slice(2) + "</li>";
          } else {
            if (inList) {
              html += "</ul>";
              inList = false;
            }
            if (line.startsWith("### ")) {
              html += "<h3>" + line.slice(4) + "</h3>";
            } else if (line.startsWith("## ")) {
              html += "<h2>" + line.slice(3) + "</h2>";
            } else if (line.startsWith("# ")) {
              html += "<h1>" + line.slice(2) + "</h1>";
            } else if (line.trim() === "") {
              html += "<br/>";
            } else {
              html += "<p>" + line + "</p>";
            }
          }
        }
        if (inList) html += "</ul>";
        return html;
      }

      async function loadDigests() {
        const res = await fetch("/api/digests");
        const data = await res.json();
        const container = document.getElementById("digests");
        container.innerHTML = "";
        if (!data.digests.length) {
          container.innerHTML = "<div class='empty'>No digests yet.</div>";
          return;
        }
        data.digests.forEach((digest) => {
          const div = document.createElement("div");
          div.className = "card";
          div.innerHTML =
            "<div class='digest-header'>" +
            "<div class='digest-date'>" + digest.date + "</div>" +
            "</div>" +
            "<div class='digest-content'>" + renderMarkdown(digest.content_md) + "</div>";
          container.appendChild(div);
        });
      }
      loadDigests();
    </script>
  </body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html; charset=utf-8" } });
}

async function handleIngest(
  request: Request,
  env: Env,
  ctx: ExecutionContext
): Promise<Response> {
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
  processFeedbackIds(env, [id], ctx);
  return jsonResponse({ id });
}

async function handleRequest(
  request: Request,
  env: Env,
  ctx: ExecutionContext
): Promise<Response> {
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
    processFeedbackIds(env, result.ids, ctx);
    return jsonResponse({ inserted: result.inserted, enqueued: result.enqueued });
  }
  if (path === "/api/mock/seed") {
    if (request.method !== "POST") return methodNotAllowed();
    return handleSeed(env, ctx);
  }
  if (path === "/api/mock/refresh-clusters") {
    if (request.method !== "POST") return methodNotAllowed();
    return handleRefreshClusters(env);
  }
  if (path === "/api/mock/merge-clusters") {
    if (request.method !== "POST") return methodNotAllowed();
    return handleMergeClusters(env);
  }

  if (path === "/api/ingest") {
    if (request.method !== "POST") return methodNotAllowed();
    return handleIngest(request, env, ctx);
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
  if (path === "/api/digests/mock") {
    if (request.method !== "POST") return methodNotAllowed();
    return handleDigestMock(env);
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
    return handleRequest(request, env, ctx);
  },
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    try {
      await handleScheduled(env);
    } catch (error) {
      console.log("Scheduled digest error", error);
    }
  },
};
