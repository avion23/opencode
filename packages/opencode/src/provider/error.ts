import { APICallError } from "ai"
import { STATUS_CODES } from "http"
import { iife } from "@/util/iife"
import type { ProviderV2 } from "@opencode-ai/core/provider"
import { isContextOverflow } from "@opencode-ai/llm"

export class HeaderTimeoutError extends Error {
  public override readonly name = "ProviderHeaderTimeoutError"

  constructor(public readonly ms: number) {
    super(`Provider response headers timed out after ${ms}ms`)
  }
}

export class ResponseStreamError extends Error {
  public override readonly name = "ProviderResponseStreamError"

  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
  }
}

function isOpenAiErrorRetryable(e: APICallError) {
  const status = e.statusCode
  if (!status) return e.isRetryable
  // openai sometimes returns 404 for models that are actually available
  return status === 404 || e.isRetryable
}

// Providers not reliably handled in this function:
// - z.ai: can accept overflow silently (needs token-count/context-window checks)
function message(providerID: ProviderV2.ID, e: APICallError) {
  return iife(() => {
    const msg = e.message
    // A whitespace-only message carries no information; treat it like an empty
    // message so the fallback derivation supplies a safe non-empty value.
    if (msg.trim() === "") {
      const detail = providerErrorDetail(e)
      const prefix = e.statusCode === undefined ? "Provider request failed" : `Provider request failed with HTTP ${e.statusCode}`
      if (detail.message) return detail.message
      if (detail.code) return `${prefix}: ${detail.code}`
      if (e.statusCode) {
        const err = STATUS_CODES[e.statusCode]
        if (err) return err
      }
      return prefix
    }

    if (!e.responseBody || (e.statusCode && msg !== STATUS_CODES[e.statusCode])) {
      return msg
    }

    try {
      const body = JSON.parse(e.responseBody)
      // try to extract common error message fields
      const errMsg = body.message || body.error || body.error?.message
      if (errMsg && typeof errMsg === "string") {
        return `${msg}: ${errMsg}`
      }
    } catch {}

    // If responseBody is HTML (e.g. from a gateway or proxy error page),
    // provide a human-readable message instead of dumping raw markup
    if (/^\s*<!doctype|^\s*<html/i.test(e.responseBody)) {
      if (e.statusCode === 401) {
        return "Unauthorized: request was blocked by a gateway or proxy. Your authentication token may be missing or expired — try running `opencode auth login <your provider URL>` to re-authenticate."
      }
      if (e.statusCode === 403) {
        return "Forbidden: request was blocked by a gateway or proxy. You may not have permission to access this resource — check your account and provider settings."
      }
      return msg
    }

    // No recognized message field in the body: the message is just the HTTP
    // status text. Surface that text alone — never append the raw body, which
    // can contain credentials (e.g. a 401 body echoing the Authorization
    // header) or other provider-internal detail.
    return msg
  }).trim()
}

function json(input: unknown) {
  if (typeof input === "string") {
    try {
      const result = JSON.parse(input)
      if (result && typeof result === "object") return result
      return undefined
    } catch {
      return undefined
    }
  }
  if (typeof input === "object" && input !== null) {
    return input
  }
  return undefined
}

// AI SDK errors such as AI_APICallError can carry an empty message while still
// holding structured provider details in `data` or `responseBody`. Extract a
// safe non-empty fallback from recognized fields only; never surface the raw
// response payload (upstream #41450).
function providerErrorDetail(e: APICallError) {
  const data = json(e.data)
  const body = json(e.responseBody)
  const detail = (value: unknown) => (value && typeof value === "object" ? (value as Record<string, unknown>) : undefined)
  const candidates = [detail(data?.error), detail(data), detail(body?.error), detail(body)].filter(
    (value): value is Record<string, unknown> => value !== undefined,
  )
  const message = candidates.map((candidate) => candidate.message).find((value) => typeof value === "string" && value.trim())
  const code = candidates.map((candidate) => candidate.code).find((value) => value !== undefined)
  return {
    message: typeof message === "string" ? message : undefined,
    code: code === undefined ? undefined : String(code),
  }
}

export type ParsedStreamError =
  | {
      type: "context_overflow"
      message: string
      responseBody: string
    }
  | {
      type: "api_error"
      message: string
      isRetryable: boolean
      responseBody: string
    }

export function parseStreamError(input: unknown): ParsedStreamError | undefined {
  const raw = json(input)
  const body = typeof raw?.message === "string" ? (json(raw.message) ?? raw) : raw
  if (!body) return

  const responseBody = JSON.stringify(body)
  if (body.type !== "error") return

  switch (body?.error?.code) {
    case "context_length_exceeded":
      return {
        type: "context_overflow",
        message: "Input exceeds context window of this model",
        responseBody,
      }
    case "insufficient_quota":
      return {
        type: "api_error",
        message: "Quota exceeded. Check your plan and billing details.",
        isRetryable: false,
        responseBody,
      }
    case "usage_not_included":
      return {
        type: "api_error",
        message: "To use Codex with your ChatGPT plan, upgrade to Plus: https://chatgpt.com/explore/plus.",
        isRetryable: false,
        responseBody,
      }
    case "invalid_prompt":
      return {
        type: "api_error",
        message: typeof body?.error?.message === "string" ? body?.error?.message : "Invalid prompt.",
        isRetryable: false,
        responseBody,
      }
    case "server_is_overloaded":
    case "server_error":
      return {
        type: "api_error",
        message: typeof body?.error?.message === "string" ? body?.error?.message : "Server error.",
        isRetryable: true,
        responseBody,
      }
  }

  return {
    type: "api_error",
    message: typeof body?.error?.message === "string" ? body.error.message : "Server error.",
    isRetryable: true,
    responseBody,
  }
}

export type ParsedAPICallError =
  | {
      type: "context_overflow"
      message: string
      responseBody?: string
    }
  | {
      type: "api_error"
      message: string
      statusCode?: number
      isRetryable: boolean
      responseHeaders?: Record<string, string>
      responseBody?: string
      metadata?: Record<string, string>
    }

export function parseAPICallError(input: { providerID: ProviderV2.ID; error: APICallError }): ParsedAPICallError {
  const m = message(input.providerID, input.error)
  const body = json(input.error.responseBody)
  const data = json(input.error.data)
  if (
    isContextOverflow(m) ||
    input.error.statusCode === 413 ||
    body?.error?.code === "context_length_exceeded" ||
    data?.error?.code === "context_length_exceeded"
  ) {
    return {
      type: "context_overflow",
      message: m,
      responseBody: input.error.responseBody,
    }
  }

  // Providers don't always set isRetryable on APICallError, but 429 and 5xx
  // are inherently transient; derive retryability from the status alone so an
  // empty or uninformative body can't block the retry (upstream #41450).
  const status = input.error.statusCode
  const retryable = status !== undefined && (status === 429 || status >= 500)
  const metadata = input.error.url ? { url: input.error.url } : undefined
  return {
    type: "api_error",
    message: m,
    statusCode: input.error.statusCode,
    isRetryable: retryable || (input.providerID.startsWith("openai") ? isOpenAiErrorRetryable(input.error) : input.error.isRetryable),
    responseHeaders: input.error.responseHeaders,
    responseBody: input.error.responseBody,
    metadata,
  }
}

export * as ProviderError from "./error"
