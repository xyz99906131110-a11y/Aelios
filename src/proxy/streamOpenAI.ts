import { saveAssistantMessage } from "../db/messages";
import { saveUsageLog } from "../db/usageLogs";
import { enqueueMemoryMaintenanceIfNeeded } from "../queue/producer";
import type { Env, KeyProfile, TokenUsage } from "../types";
import { getSseData, splitSseEvents } from "../utils/sseParser";

interface StreamOpenAIOptions {
  env: Env;
  ctx: ExecutionContext;
  profile: KeyProfile;
  conversationId: string;
  fromMessageId?: string;
  requestModel: string;
  upstreamModel: string;
  provider: string;
  clientSystemHash?: string | null;
  cacheAnchorBlock?: string | null;
}

interface StreamState {
  assistantText: string;
  finishReason: string | null;
  usage?: TokenUsage;
}

function consumeOpenAIStreamData(data: string, state: StreamState): void {
  if (data === "[DONE]") return;

  try {
    const parsed = JSON.parse(data) as {
      choices?: Array<{
        delta?: {
          content?: string;
        };
        finish_reason?: string | null;
      }>;
      usage?: TokenUsage;
    };

    const choice = parsed.choices?.[0];
    const content = choice?.delta?.content;
    if (content) state.assistantText += content;
    if (choice?.finish_reason) state.finishReason = choice.finish_reason;
    if (parsed.usage) state.usage = parsed.usage;
  } catch {
    // Ignore malformed provider chunks while continuing to proxy bytes to the client.
  }
}

async function persistStreamResult(options: StreamOpenAIOptions, state: StreamState): Promise<void> {
  const messageId = await saveAssistantMessage(options.env.DB, {
    conversationId: options.conversationId,
    namespace: options.profile.namespace,
    source: options.profile.source,
    content: state.assistantText,
    requestModel: options.requestModel,
    upstreamModel: options.upstreamModel,
    provider: options.provider,
    stream: true,
    finishReason: state.finishReason,
    usage: state.usage
  });

  await saveUsageLog(options.env.DB, {
    messageId,
    namespace: options.profile.namespace,
    provider: options.provider,
    model: options.upstreamModel,
    usage: state.usage,
    clientSystemHash: options.clientSystemHash ?? null,
    cacheAnchorBlock: options.cacheAnchorBlock ?? null
  });

  await enqueueMemoryMaintenanceIfNeeded(options.env, {
    namespace: options.profile.namespace,
    conversationId: options.conversationId,
    fromMessageId: options.fromMessageId,
    toMessageId: messageId,
    source: options.profile.source
  });
}

export function streamOpenAIWithTee(upstream: Response, options: StreamOpenAIOptions): Response {
  if (!upstream.body) {
    return new Response(upstream.body, {
      status: upstream.status,
      headers: upstream.headers
    });
  }

  const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
  const reader = upstream.body.getReader();
  const writer = writable.getWriter();
  const decoder = new TextDecoder();
  const state: StreamState = {
    assistantText: "",
    finishReason: null
  };

  void (async () => {
    let buffered = "";

    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        if (!value) continue;

        await writer.write(value);
        buffered += decoder.decode(value, { stream: true });

        const parsed = splitSseEvents(buffered);
        buffered = parsed.rest;

        for (const event of parsed.events) {
          const data = getSseData(event);
          if (data) consumeOpenAIStreamData(data, state);
        }
      }

      buffered += decoder.decode();
      const parsed = splitSseEvents(buffered);
      for (const event of parsed.events) {
        const data = getSseData(event);
        if (data) consumeOpenAIStreamData(data, state);
      }

      await writer.close();
      options.ctx.waitUntil(
        persistStreamResult(options, state).catch((error) => {
          console.error("failed to persist stream result", error);
        })
      );
    } catch (error) {
      console.error("openai stream proxy error", error);
      await writer.abort(error);
    } finally {
      reader.releaseLock();
      writer.releaseLock();
    }
  })();

  const headers = new Headers(upstream.headers);
  headers.set("content-type", "text/event-stream; charset=utf-8");
  headers.set("cache-control", "no-cache");

  return new Response(readable, {
    status: upstream.status,
    headers
  });
}
