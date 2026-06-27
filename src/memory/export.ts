import { listMemoriesPage } from "../db/memories";
import { fetchMemoryLifecycleRows } from "../db/v2";
import type { Env, MemoryApiRecord, MemoryRecord } from "../types";
import { createEmbedding } from "./embedding";
import { toMemoryApiRecord } from "./search";

export interface MemoryExportInput {
  namespace: string;
  type?: string | null;
  format?: string | null;
}

export interface MemoryExportRecord {
  id: string;
  content: string;
  metadata: Record<string, unknown>;
}

export interface MemoryExportResult {
  data: MemoryExportRecord[];
  meta: {
    namespace: string;
    type: string | null;
    format: "json";
    count: number;
    exported_at: string;
    source: "vectorize" | "d1";
    vectorize: {
      index_name: string;
      requested_top_k: number;
      fetched_count: number;
      unavailable?: string;
      fallback?: string;
    };
  };
}

const EXPORT_PAGE_SIZE = 1000;
const VECTORIZE_EXPORT_TOP_K = 1000;

function getIndexName(env: Env): string {
  return env.VECTORIZE_INDEX_NAME?.trim() || "memo-kb";
}

function readMetadataText(metadata: Record<string, unknown>): string {
  const fields = ["content", "text", "memory", "summary", "document", "chunk", "value", "title"];
  for (const field of fields) {
    const value = metadata[field];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return "";
}

function readMetadataString(metadata: Record<string, unknown>, field: string): string | null {
  const value = metadata[field];
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function readMetadataNumber(metadata: Record<string, unknown>, field: string, fallback: number): number {
  const value = metadata[field];
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return fallback;
}

function readMetadataBoolean(metadata: Record<string, unknown>, field: string): boolean {
  const value = metadata[field];
  return value === true || value === "true";
}

function readMetadataStringArray(metadata: Record<string, unknown>, field: string): string[] {
  const value = metadata[field];
  if (Array.isArray(value)) return value.filter((item): item is string => typeof item === "string");
  if (typeof value === "string" && value.trim()) {
    try {
      const parsed = JSON.parse(value) as unknown;
      if (Array.isArray(parsed)) return parsed.filter((item): item is string => typeof item === "string");
    } catch {
      return [value.trim()];
    }
  }
  return [];
}

function vectorMatchToExportRecord(match: VectorizeMatch): MemoryExportRecord {
  const metadata = ((match.metadata || {}) as Record<string, unknown>) || {};
  const refId = readMetadataString(metadata, "ref_id");
  const id = refId || (match.id.startsWith("mem_") ? match.id.slice("mem_".length) : match.id);
  const content = readMetadataText(metadata);

  return {
    id,
    content,
    metadata: {
      namespace: readMetadataString(metadata, "namespace"),
      type: readMetadataString(metadata, "type") || "note",
      summary: readMetadataString(metadata, "summary"),
      importance: readMetadataNumber(metadata, "importance", 0.5),
      confidence: readMetadataNumber(metadata, "confidence", 0.8),
      status: readMetadataString(metadata, "status") || "active",
      pinned: readMetadataBoolean(metadata, "pinned"),
      tags: readMetadataStringArray(metadata, "tags"),
      source: readMetadataString(metadata, "source_id") || readMetadataString(metadata, "source"),
      source_message_ids: readMetadataStringArray(metadata, "source_message_ids"),
      vector_id: match.id,
      created_at: readMetadataString(metadata, "created_at"),
      updated_at: readMetadataString(metadata, "updated_at"),
      expires_at: readMetadataString(metadata, "expires_at"),
      score: match.score,
      raw: metadata
    }
  };
}

async function exportFromVectorize(
  env: Env,
  input: { namespace: string; type?: string | null }
): Promise<{ records: MemoryExportRecord[]; unavailable?: string }> {
  if (!env.VECTORIZE) return { records: [], unavailable: "missing_vectorize_binding" };

  const probe = await createEmbedding(env, "memory export probe");
  if (!probe) return { records: [], unavailable: "embedding_unavailable_for_vectorize_export" };
  const zeroVector = probe.map(() => 0);

  const filter: VectorizeVectorMetadataFilter = { namespace: input.namespace };
  if (input.type) filter.type = input.type;

  try {
    const result = await env.VECTORIZE.query(zeroVector, {
      topK: VECTORIZE_EXPORT_TOP_K,
      namespace: input.namespace,
      returnMetadata: true,
      filter
    });
    return { records: result.matches.map(vectorMatchToExportRecord) };
  } catch (error) {
    return {
      records: [],
      unavailable: error instanceof Error ? error.message : "vectorize_export_query_failed"
    };
  }
}

async function listAllMemoryRecords(
  env: Env,
  input: { namespace: string; type?: string | null }
): Promise<MemoryRecord[]> {
  const records: MemoryRecord[] = [];
  let offset = 0;

  for (;;) {
    const page = await listMemoriesPage(env.DB, {
      namespace: input.namespace,
      type: input.type || undefined,
      limit: EXPORT_PAGE_SIZE,
      offset
    });

    records.push(...page.records);
    if (!page.hasMore || page.nextOffset === null) break;
    offset = page.nextOffset;
  }

  return records;
}

function toD1ExportRecord(record: MemoryApiRecord): MemoryExportRecord {
  const { id, content, score: _score, ...metadata } = record;
  return { id, content, metadata };
}

async function exportFromD1(env: Env, input: { namespace: string; type?: string | null }): Promise<MemoryExportRecord[]> {
  const records = await listAllMemoryRecords(env, input);
  const lifecycleRows = await fetchMemoryLifecycleRows(env.DB, records.map((record) => record.id));
  const lifecycleByMemoryId = new Map(lifecycleRows.map((row) => [row.memory_id, row]));

  return records.map((record) =>
    toD1ExportRecord(toMemoryApiRecord(record, undefined, lifecycleByMemoryId.get(record.id) ?? null))
  );
}

export async function exportMemories(env: Env, input: MemoryExportInput): Promise<MemoryExportResult> {
  const format = (input.format || "json").trim().toLowerCase();
  if (format !== "json") {
    throw new Error("Only format=json is supported");
  }

  const vectorize = await exportFromVectorize(env, {
    namespace: input.namespace,
    type: input.type
  });
  const usedVectorize = !vectorize.unavailable;
  const data = usedVectorize
    ? vectorize.records
    : await exportFromD1(env, { namespace: input.namespace, type: input.type });

  return {
    data,
    meta: {
      namespace: input.namespace,
      type: input.type || null,
      format: "json",
      count: data.length,
      exported_at: new Date().toISOString(),
      source: usedVectorize ? "vectorize" : "d1",
      vectorize: {
        index_name: getIndexName(env),
        requested_top_k: VECTORIZE_EXPORT_TOP_K,
        fetched_count: vectorize.records.length,
        ...(vectorize.unavailable ? { unavailable: vectorize.unavailable, fallback: "d1" } : {})
      }
    }
  };
}
