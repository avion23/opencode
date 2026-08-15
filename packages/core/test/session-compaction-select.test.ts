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

test("compaction split keeps a bounded suffix of an oversized message exactly once", () => {
  const large = `OVERSIZED_START ${"c".repeat(80_000)} OVERSIZED_END`
  const selected = SessionCompaction.select([entry(0, "a".repeat(400)), entry(1, large)], 8000)

  expect(selected).toBeDefined()
  expect(selected!.recent).toBe(`[User]: ${large}`.slice(-32_000))
  expect(selected!.recent.length).toBe(32_000)
  expect(selected!.head).toContain(`[User]: ${"a".repeat(400)}`)
  expect(selected!.head).toContain("OVERSIZED_START")
  expect(selected!.head).not.toContain("OVERSIZED_END")

  const currentPrefix = selected!.head.slice(selected!.head.lastIndexOf("[User]:"))
  const reconstructed = currentPrefix + selected!.recent
  expect(reconstructed).toBe(`[User]: ${large}`)
  expect(reconstructed.split(large)).toHaveLength(2)
  expect(selected!.head + selected!.recent).toBe(`[User]: ${"a".repeat(400)}\n\n[User]: ${large}`)
})

test("compaction split keeps everything in recent when the conversation fits the budget", () => {
  const selected = SessionCompaction.select([entry(0, "hello"), entry(1, "world")], 100_000)

  expect(selected).toBeDefined()
  expect(selected!.head).toBe("")
  expect(selected!.recent).toContain("hello")
  expect(selected!.recent).toContain("world")
})
