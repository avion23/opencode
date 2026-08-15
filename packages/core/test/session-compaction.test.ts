import { expect, test } from "bun:test"
import { LLM, LLMEvent, Model, type LLMRequest } from "@opencode-ai/llm"
import * as OpenAIChat from "@opencode-ai/llm/protocols/openai-chat"
import { Config } from "@opencode-ai/core/config"
import { ConfigCompaction } from "@opencode-ai/core/config/compaction"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionCompaction } from "@opencode-ai/core/session/compaction"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { DateTime, Effect, Stream } from "effect"

const created = DateTime.makeUnsafe(0)

const entry = (seq: number, text: string) => ({
  seq,
  message: SessionMessage.User.make({
    id: SessionMessage.ID.make(`msg_${seq}`),
    type: "user",
    text,
    time: { created },
  }),
})

test("compaction prompt preserves detailed work state and relevant files", () => {
  const prompt = SessionCompaction.buildPrompt({ context: ["conversation history"] })

  expect(prompt).toStartWith(
    "Here is the conversation so far:\n\n<conversation>\nconversation history\n</conversation>",
  )
  expect(prompt.indexOf("</conversation>")).toBeLessThan(prompt.indexOf("Create a new anchored summary"))
  expect(prompt).toContain("conversation history in the <conversation> tags above")
  expect(prompt).toContain("## Work State\n### Completed")
  expect(prompt).toContain("### Active")
  expect(prompt).toContain("### Blocked")
  expect(prompt).toContain("## Relevant Files")
})

test("compaction prompt gives update instructions for a prior summary", () => {
  const prompt = SessionCompaction.buildPrompt({
    context: ["new conversation"],
    previousSummary: "existing summary",
  })

  expect(prompt.indexOf("<conversation>")).toBeLessThan(prompt.indexOf("<prior-summary>"))
  expect(prompt.indexOf("</prior-summary>")).toBeLessThan(prompt.indexOf("The <prior-summary> summarizes"))
  expect(prompt).toContain(
    "Carry forward objectives, constraints, user directives, decisions, and parallel workstreams from the <prior-summary>",
  )
  expect(prompt).toContain('Move completed work from "Active" to "Completed".')
  expect(prompt).toContain('Update "Objective" and "Next Move" to reflect the current work state.')
})

test("compaction describes tool media without embedding base64", () => {
  const base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAAB"
  const serialized = SessionCompaction.serializeToolContent([
    { type: "text", text: "Image read successfully" },
    {
      type: "file",
      uri: `data:image/png;base64,${base64}`,
      mime: "image/png",
      name: "pixel.png",
    },
  ])

  expect(serialized).toBe("Image read successfully\n[Attached image/png: pixel.png]")
  expect(serialized).not.toContain(base64)
})

test("compaction invokes the provider when an oversized newest message has a bounded prefix", () => {
  const requests: LLMRequest[] = []
  const published: Array<{ readonly type: string; readonly data: unknown }> = []
  const events = {
    publish: (definition: { readonly type: string }, data: unknown) =>
      Effect.sync(() => {
        published.push({ type: definition.type, data })
        return data
      }),
  } as unknown as EventV2.Interface
  const llm = {
    stream: (request: LLMRequest) => {
      requests.push(request)
      return Stream.make(LLMEvent.textDelta({ id: "summary", text: "summary" }))
    },
  }
  const model = Model.make({
    id: "compaction-test",
    provider: "fake",
    route: OpenAIChat.route.with({ limits: { context: 20_000, output: 4_096 } }),
  })
  const entries = [entry(0, "Earlier context"), entry(1, `LATEST_INSTRUCTION ${"x".repeat(76_000)}`)]
  const selected = SessionCompaction.select(entries, 8_000)
  const compaction = SessionCompaction.make({
    events,
    llm,
    config: [
      new Config.Document({
        type: "document",
        info: new Config.Info({
          compaction: new ConfigCompaction.Info({
            keep: new ConfigCompaction.Keep({ tokens: 8_000 }),
          }),
        }),
      }),
    ],
    owner: () => Effect.succeed(EventV2.strictOwner("compaction-test")),
  })

  const result = Effect.runSync(
    compaction.compactAfterOverflow({
      sessionID: SessionSchema.ID.make("ses_compaction_test"),
      entries,
      model,
      request: LLM.request({ model, messages: [], tools: [], generation: { maxTokens: 4_096 } }),
    }),
  )

  expect(result).toBe(true)
  expect(selected).toBeDefined()
  expect(requests).toHaveLength(1)
  expect(requests[0]!.messages[0]!.content[0]).toMatchObject({
    type: "text",
    text: expect.stringContaining(selected!.head),
  })
  expect(requests[0]!.messages[0]!.content[0]).not.toMatchObject({
    text: expect.stringContaining(selected!.recent),
  })
  expect(published).toEqual(
    expect.arrayContaining([
      expect.objectContaining({ type: SessionEvent.Compaction.Started.type }),
      expect.objectContaining({
        type: SessionEvent.Compaction.Ended.type,
        data: expect.objectContaining({ recent: selected!.recent, text: "summary" }),
      }),
    ]),
  )
})
