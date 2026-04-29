import { authenticate } from "../auth/apiKey";
import { requireScope } from "../auth/scopes";
import { json, openAiError } from "../utils/json";
import type { Env } from "../types";

interface CacheHealthRow {
  created_at: string;
  model: string | null;
  cache_read_tokens: number | null;
  cache_creation_tokens: number | null;
  input_tokens: number | null;
  client_system_hash: string | null;
  cache_anchor_block: string | null;
}

interface ModelAgg {
  model: string;
  requests: number;
  cache_creation_tokens: number;
  cache_read_tokens: number;
  input_tokens: number;
}

interface HashAgg {
  client_system_hash: string;
  requests: number;
  cache_read_tokens: number;
}

interface CacheHealthSummary {
  total_requests: number;
  cache_creation_total_tokens: number;
  cache_read_total_tokens: number;
  input_total_tokens: number;
  cache_read_ratio: number;
  by_model: ModelAgg[];
  by_client_system_hash: HashAgg[];
  recent: CacheHealthRow[];
}

// All queries filter to Anthropic/Claude traffic only.
const ANTHROPIC_FILTER = "(provider = 'anthropic' OR lower(model) LIKE 'anthropic/%' OR lower(model) LIKE '%claude%')";

export async function handleCacheHealth(request: Request, env: Env): Promise<Response> {
  const auth = await authenticate(request, env);
  if (!auth.ok) return openAiError("Unauthorized", 401, "authentication_error");

  const scopeError = requireScope(auth.profile, "debug:read");
  if (scopeError) return scopeError;

  try {
    const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

    const summary = await env.DB.prepare(
      `SELECT
         COUNT(*) as total_requests,
         COALESCE(SUM(cache_creation_tokens), 0) as cache_creation_total_tokens,
         COALESCE(SUM(cache_read_tokens), 0) as cache_read_total_tokens,
         COALESCE(SUM(input_tokens), 0) as input_total_tokens
       FROM usage_logs
       WHERE created_at >= ? AND ${ANTHROPIC_FILTER}`
    ).bind(since).first<{
      total_requests: number;
      cache_creation_total_tokens: number;
      cache_read_total_tokens: number;
      input_total_tokens: number;
    }>();

    const totalRequests = summary?.total_requests ?? 0;
    const cacheCreationTotal = summary?.cache_creation_total_tokens ?? 0;
    const cacheReadTotal = summary?.cache_read_total_tokens ?? 0;
    const inputTotal = summary?.input_total_tokens ?? 0;

    const byModel = await env.DB.prepare(
      `SELECT
         model,
         COUNT(*) as requests,
         COALESCE(SUM(cache_creation_tokens), 0) as cache_creation_tokens,
         COALESCE(SUM(cache_read_tokens), 0) as cache_read_tokens,
         COALESCE(SUM(input_tokens), 0) as input_tokens
       FROM usage_logs
       WHERE created_at >= ? AND ${ANTHROPIC_FILTER}
       GROUP BY model
       ORDER BY requests DESC`
    ).bind(since).all<ModelAgg>();

    const byHash = await env.DB.prepare(
      `SELECT
         client_system_hash,
         COUNT(*) as requests,
         COALESCE(SUM(cache_read_tokens), 0) as cache_read_tokens
       FROM usage_logs
       WHERE created_at >= ? AND client_system_hash IS NOT NULL AND ${ANTHROPIC_FILTER}
       GROUP BY client_system_hash
       ORDER BY requests DESC`
    ).bind(since).all<HashAgg>();

    const recent = await env.DB.prepare(
      `SELECT
         created_at, model, cache_read_tokens, cache_creation_tokens,
         input_tokens, client_system_hash, cache_anchor_block
       FROM usage_logs
       WHERE created_at >= ? AND ${ANTHROPIC_FILTER}
       ORDER BY created_at DESC
       LIMIT 10`
    ).bind(since).all<CacheHealthRow>();

    const result: CacheHealthSummary = {
      total_requests: totalRequests,
      cache_creation_total_tokens: cacheCreationTotal,
      cache_read_total_tokens: cacheReadTotal,
      input_total_tokens: inputTotal,
      cache_read_ratio: inputTotal > 0 ? cacheReadTotal / inputTotal : 0,
      by_model: byModel.results ?? [],
      by_client_system_hash: byHash.results ?? [],
      recent: recent.results ?? []
    };

    return json(result);
  } catch (error) {
    console.error("cache_health query failed", error);
    return json({ error: "cache_health_query_failed" }, { status: 500 });
  }
}
