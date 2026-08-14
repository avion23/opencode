import { expect, test } from "bun:test"
import { SessionCompaction } from "@opencode-ai/core/session/compaction"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { DateTime } from "effect"

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

test("compaction split does not duplicate the straddling message into the head", () => {
  // "c" alone exceeds the token budget, so the split point never enters it:
  // it stays whole in `head` for summarization and no fragment of it is
  // retained in `recent`. The full message must appear exactly once.
  const large = "c".repeat(80_000)
  const selected = SessionCompaction.select([entry(0, "a".repeat(400)), entry(1, "b".repeat(400)), entry(2, large)], 8000)

  expect(selected).toBeDefined()
  expect(selected!.head).toBe(`[User]: ${"a".repeat(400)}\n\n[User]: ${"b".repeat(400)}\n\n[User]: ${large}`)
  expect(selected!.recent).toBe("")
})

test("compaction split keeps everything in recent when the conversation fits the budget", () => {
  const selected = SessionCompaction.select([entry(0, "hello"), entry(1, "world")], 100_000)

  expect(selected).toBeDefined()
  expect(selected!.head).toBe("")
  expect(selected!.recent).toContain("hello")
  expect(selected!.recent).toContain("world")
})
