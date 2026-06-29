#!/usr/bin/env node
import assert from "node:assert/strict";
import { mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { build } from "esbuild";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const tmp = resolve(tmpdir(), `aelios-extract-verify-${process.pid}`);
await rm(tmp, { recursive: true, force: true });
await mkdir(tmp, { recursive: true });

async function bundle(entry, outfile) {
  await build({
    entryPoints: [resolve(root, entry)],
    outfile: resolve(tmp, outfile),
    bundle: true,
    platform: "node",
    format: "esm",
    target: "node20",
    logLevel: "silent"
  });
  return import(pathToFileURL(resolve(tmp, outfile)).href);
}

class MockStatement {
  constructor(db, sql) {
    this.db = db;
    this.sql = sql;
    this.args = [];
  }

  bind(...args) {
    this.args = args;
    return this;
  }

  async first() {
    if (this.sql.includes("SELECT value FROM processing_cursors")) {
      const value = this.db.cursors.get(this.args[0]);
      return value === undefined ? null : { value };
    }
    if (this.sql.includes("JOIN memory_lifecycle") && this.sql.includes("lc.fact_key")) {
      const [namespace, factKey] = this.args;
      const row = this.db.activeFact;
      return row && row.namespace === namespace && row.fact_key === factKey ? row : null;
    }
    if (this.sql.includes("SELECT id, status, vector_id FROM memories")) {
      const [, id] = this.args;
      return this.db.memories.get(id) ?? null;
    }
    if (this.sql.includes("SELECT * FROM memories WHERE namespace = ? AND id = ?")) {
      const [, id] = this.args;
      return this.db.memories.get(id) ?? null;
    }
    return null;
  }

  async all() {
    if (this.sql.includes("FROM messages")) {
      this.db.messageQueries.push([...this.args]);
      const hasAfter = this.args.length === 5;
      const namespace = this.args[0];
      const start = this.args[1];
      const end = this.args[2];
      const after = hasAfter ? this.args[3] : null;
      const limit = this.args[hasAfter ? 4 : 3];
      const results = this.db.messages
        .filter((m) => m.namespace === namespace)
        .filter((m) => m.role === "user" || m.role === "assistant")
        .filter((m) => m.created_at >= start && m.created_at < end)
        .filter((m) => !after || m.created_at > after)
        .sort((a, b) => a.created_at.localeCompare(b.created_at))
        .slice(0, limit);
      return { results };
    }
    return { results: [] };
  }

  async run() {
    this.db.runs.push({ sql: this.sql, args: [...this.args] });
    if (this.sql.includes("INSERT INTO processing_cursors")) {
      this.db.cursors.set(this.args[0], this.args[1]);
    }
    if (this.sql.includes("UPDATE memory_lifecycle") && this.sql.includes("seen_count = seen_count + 1")) {
      this.db.seenIds.push(this.args[1]);
    }
    if (this.sql.includes("UPDATE memories SET status = 'superseded'")) {
      this.db.supersededIds.push(this.args[1]);
    }
    return { meta: { changes: 1 } };
  }
}

class MockDB {
  constructor(messages = []) {
    this.messages = messages;
    this.cursors = new Map();
    this.memories = new Map();
    this.messageQueries = [];
    this.runs = [];
    this.batchCount = 0;
    this.seenIds = [];
    this.supersededIds = [];
    this.activeFact = null;
  }

  prepare(sql) {
    return new MockStatement(this, sql);
  }

  async batch(statements) {
    this.batchCount += 1;
    return Promise.all(statements.map((statement) => statement.run()));
  }
}

function msg(id, createdAt, namespace = "default") {
  return {
    id,
    conversation_id: "c1",
    namespace,
    role: "user",
    content: `message ${id}`,
    source: "test",
    created_at: createdAt
  };
}

function okFetch(memories = []) {
  let calls = 0;
  const fn = async () => {
    calls += 1;
    return new Response(JSON.stringify({ choices: [{ message: { content: JSON.stringify({ memories }) } }] }), {
      status: 200,
      headers: { "content-type": "application/json" }
    });
  };
  fn.calls = () => calls;
  return fn;
}

function baseEnv(db) {
  return {
    DB: db,
    AI_GATEWAY_BASE_URL: "https://gateway.test",
    EXTRACT_MODEL: "unit-extract-model",
    DEDUP_COSINE: "0.9"
  };
}

const { runMemoryExtractionWindow } = await bundle("src/memory/extractPipeline.ts", "extractPipeline.mjs");
const { handleMemories } = await bundle("src/api/memories.ts", "memoriesApi.mjs");

{
  const db = new MockDB([
    msg("old", "2026-06-28T23:59:00.000Z"),
    msg("in1", "2026-06-29T00:10:00.000Z"),
    msg("in2", "2026-06-29T03:59:00.000Z"),
    msg("edge", "2026-06-29T04:00:00.000Z")
  ]);
  globalThis.fetch = okFetch([]);
  const result = await runMemoryExtractionWindow(baseEnv(db), "default", {
    scheduledTime: Date.parse("2026-06-29T04:00:00.000Z")
  });
  assert.equal(result.ran, true);
  assert.equal(result.stats.processedMessages, 2);
  assert.equal(db.messageQueries[0][1], "2026-06-29T00:00:00.000Z");
  assert.equal(db.messageQueries[0][2], "2026-06-29T04:00:00.000Z");
  assert.equal(db.cursors.get("extract:default"), "2026-06-29T03:59:00.000Z");
  assert.equal(db.cursors.get("extract:default:2026-06-29T04:00:00.000Z"), "done:2026-06-29T03:59:00.000Z");
}

{
  const db = new MockDB([
    msg("m1", "2026-06-29T00:10:00.000Z"),
    msg("m2", "2026-06-29T00:20:00.000Z"),
    msg("m3", "2026-06-29T00:30:00.000Z")
  ]);
  globalThis.fetch = okFetch([]);
  const env = { ...baseEnv(db), EXTRACT_MAX_MESSAGES: "2" };
  const first = await runMemoryExtractionWindow(env, "default", {
    scheduledTime: Date.parse("2026-06-29T04:00:00.000Z")
  });
  assert.equal(first.ran, true);
  assert.equal(first.stats.hasMore, true);
  assert.equal(db.cursors.has("extract:default:2026-06-29T04:00:00.000Z"), false);
  const second = await runMemoryExtractionWindow(env, "default", {
    scheduledTime: Date.parse("2026-06-29T04:00:00.000Z")
  });
  assert.equal(second.ran, true);
  assert.equal(second.stats.processedMessages, 1);
  assert.equal(db.messageQueries[1][1], "2026-06-29T00:20:00.000Z");
  assert.equal(db.messageQueries[1][3], "2026-06-29T00:20:00.000Z");
  const queryCount = db.messageQueries.length;
  const third = await runMemoryExtractionWindow(env, "default", {
    scheduledTime: Date.parse("2026-06-29T04:00:00.000Z")
  });
  assert.equal(third.ran, false);
  assert.equal(third.reason, "already_done");
  assert.equal(db.messageQueries.length, queryCount);
}

{
  const db = new MockDB([msg("m1", "2026-06-29T00:10:00.000Z")]);
  let calls = 0;
  globalThis.fetch = async () => {
    calls += 1;
    return new Response("nope", { status: 500 });
  };
  const env = baseEnv(db);
  const first = await runMemoryExtractionWindow(env, "default", {
    scheduledTime: Date.parse("2026-06-29T04:00:00.000Z")
  });
  const second = await runMemoryExtractionWindow(env, "default", {
    scheduledTime: Date.parse("2026-06-29T04:00:00.000Z")
  });
  assert.equal(first.ran, false);
  assert.equal(first.reason, "model_error");
  assert.equal(second.reason, "model_error");
  assert.equal(calls, 2);
  assert.equal(db.cursors.has("extract:default"), false);
  assert.equal(db.cursors.has("extract:default:2026-06-29T04:00:00.000Z"), false);
}

{
  const db = new MockDB([msg("m1", "2026-06-29T00:10:00.000Z")]);
  db.activeFact = {
    id: "mem_existing",
    namespace: "default",
    type: "project",
    content: "你正在重构 Aelios v2 记忆写入流水线。",
    fact_key: "project:aelios-memory-v2"
  };
  globalThis.fetch = okFetch([
    {
      content: "你在重做 Aelios v2 的记忆写入流程。",
      type: "project",
      fact_key: "project:aelios-memory-v2",
      importance: 0.8,
      confidence: 0.9,
      source_message_ids: ["m1"]
    }
  ]);
  const env = {
    ...baseEnv(db),
    AI: {
      run: async () => ({ data: [[1, 0, 0]] })
    }
  };
  const result = await runMemoryExtractionWindow(env, "default", {
    scheduledTime: Date.parse("2026-06-29T04:00:00.000Z")
  });
  assert.equal(result.ran, true);
  assert.equal(result.stats.duplicate, 1);
  assert.equal(result.stats.superseded, 0);
  assert.deepEqual(db.seenIds, ["mem_existing"]);
  assert.deepEqual(db.supersededIds, []);
  assert.equal(db.batchCount >= 1, true);
}

{
  const request = new Request("https://memory.test/v1/memories", {
    method: "POST",
    headers: {
      authorization: "Bearer test-key",
      "content-type": "application/json"
    },
    body: JSON.stringify({ content: "没有 fact_key 的 v2 create" })
  });
  const response = await handleMemories(request, { ...baseEnv(new MockDB()), CHATBOX_API_KEY: "test-key" }, {
    waitUntil() {}
  });
  assert.equal(response.status, 400);
  assert.match(await response.text(), /fact_key is required in v2/);
}

await rm(tmp, { recursive: true, force: true });
console.log("verify-extract-pipeline: all checks passed");
