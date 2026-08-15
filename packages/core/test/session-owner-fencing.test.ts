import { describe, expect } from "bun:test"
import { Cause, DateTime, Effect, Exit, Layer, Schema, Stream } from "effect"
import { LLM, LLMEvent, Model, type LLMRequest } from "@opencode-ai/llm"
import * as OpenAIChat from "@opencode-ai/llm/protocols/openai-chat"
import { eq } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { EventTable } from "@opencode-ai/core/event/sql"
import { Location } from "@opencode-ai/core/location"
import { ProjectV2 } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionCompaction } from "@opencode-ai/core/session/compaction"
import { SessionContextEpoch } from "@opencode-ai/core/session/context-epoch"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { Prompt } from "@opencode-ai/core/session/prompt"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SystemContext } from "@opencode-ai/core/system-context/index"
import { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { testEffect } from "./lib/effect"

const projects = Layer.succeed(
  ProjectV2.Service,
  ProjectV2.Service.of({
    resolve: (directory) => Effect.succeed({ id: ProjectV2.ID.global, directory }),
    directories: () => Effect.succeed([]),
    commit: () => Effect.void,
  }),
)
const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Database.node, EventV2.node, SessionProjector.node, SessionStore.node, SessionV2.node]),
    [
      [ProjectV2.node, projects],
      [SessionExecution.node, SessionExecution.noopLayer],
    ],
  ),
)
const location = Location.Ref.make({ directory: AbsolutePath.make("/project") })
const otherWorkspace = WorkspaceV2.ID.make("wrk_new")

const moveRecordToWorkspace = (db: Database.Interface["db"], sessionID: SessionSchema.ID) =>
  db
    .update(SessionTable)
    .set({ workspace_id: otherWorkspace })
    .where(eq(SessionTable.id, sessionID))
    .run()
    .pipe(Effect.orDie)

describe("Session owner fencing", () => {
  it.effect("prompt admission uses the current owner after a claim transition", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const events = yield* EventV2.Service
      const session = yield* SessionV2.Service
      const created = yield* session.create({ id: SessionV2.ID.make("ses_admit_claimed"), location })

      // The session moved to another workspace and the destination claimed the
      // aggregate. Admission resolves the owner from the authoritative session
      // record, so the new owner can admit without a fence violation.
      yield* events.claim(created.id, otherWorkspace)
      yield* moveRecordToWorkspace(db, created.id)

      const admitted = yield* session.prompt({
        sessionID: created.id,
        prompt: Prompt.make({ text: "hi" }),
        resume: false,
      })
      expect(admitted.sessionID).toBe(created.id)
    }),
  )

  it.effect("prompt admission rejects when the durable owner moved without a claim", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const session = yield* SessionV2.Service
      const created = yield* session.create({ id: SessionV2.ID.make("ses_admit_reject"), location })

      // The session record now points at a new workspace, but nobody claimed the
      // aggregate: the durable owner is still the project. The stale location must
      // not silently adopt the admission as already-admitted.
      yield* moveRecordToWorkspace(db, created.id)

      const exit = yield* session
        .prompt({ sessionID: created.id, prompt: Prompt.make({ text: "hi" }), resume: false })
        .pipe(Effect.exit)
      expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toBeInstanceOf(EventV2.OwnerFenceError)
    }),
  )

  it.effect("switchAgent publishes under the current owner after a claim transition", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const events = yield* EventV2.Service
      const session = yield* SessionV2.Service
      const created = yield* session.create({ id: SessionV2.ID.make("ses_switch_claimed"), location })

      yield* events.claim(created.id, otherWorkspace)
      yield* moveRecordToWorkspace(db, created.id)

      const exit = yield* session.switchAgent({ sessionID: created.id, agent: "build" }).pipe(Effect.exit)
      expect(Exit.isSuccess(exit)).toBe(true)
    }),
  )

  it.effect("switchAgent rejects when the durable owner moved without a claim", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const session = yield* SessionV2.Service
      const created = yield* session.create({ id: SessionV2.ID.make("ses_switch_reject"), location })

      yield* moveRecordToWorkspace(db, created.id)

      const exit = yield* session.switchAgent({ sessionID: created.id, agent: "build" }).pipe(Effect.exit)
      expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toBeInstanceOf(EventV2.OwnerFenceError)
    }),
  )

  it.effect("context-epoch ContextUpdated rejects a stale owner", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const events = yield* EventV2.Service
      const sessionID = SessionSchema.ID.make("ses_epoch_fence")
      // The epoch table references the session row, so seed it before use.
      yield* db
        .insert(ProjectTable)
        .values({ id: ProjectV2.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: ProjectV2.ID.global,
          slug: "epoch-fence",
          directory: "/project",
          title: "epoch fence",
          version: "test",
        })
        .run()
        .pipe(Effect.orDie)
      // Stamp the durable owner with a valid local publish.
      yield* events.publish(
        SessionEvent.Compaction.Started,
        { sessionID, messageID: SessionMessage.ID.create(), timestamp: yield* DateTime.now, reason: "auto" },
        EventV2.strictOwner("owner-a"),
      )
      let value = "v1"
      const context = Effect.sync(() =>
        SystemContext.make({
          key: SystemContext.Key.make("test/context"),
          codec: Schema.toCodecJson(Schema.String),
          load: Effect.sync(() => value),
          baseline: String,
          update: () => "changed",
        }),
      )
      yield* SessionContextEpoch.initialize(db, context, sessionID)
      value = "v2"
      yield* SessionContextEpoch.prepare(db, events, context, sessionID, EventV2.strictOwner("owner-a"))
      value = "v3"
      const exit = yield* SessionContextEpoch.prepare(
        db,
        events,
        context,
        sessionID,
        EventV2.strictOwner("owner-b"),
      ).pipe(Effect.exit)
      expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toBeInstanceOf(EventV2.OwnerFenceError)
    }),
  )

  it.effect("compaction Started rejects a stale owner", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const sessionID = SessionSchema.ID.make("ses_compaction_fence")
      yield* events.publish(
        SessionEvent.Compaction.Started,
        { sessionID, messageID: SessionMessage.ID.create(), timestamp: yield* DateTime.now, reason: "auto" },
        EventV2.strictOwner("owner-a"),
      )
      const compaction = SessionCompaction.make({
        events,
        llm: { stream: () => Stream.make(LLMEvent.textDelta({ id: "summary", text: "summary" })) },
        config: [],
      })
      const model = Model.make({
        id: "compaction-fence",
        provider: "fake",
        route: OpenAIChat.route.with({ limits: { context: 20_000, output: 4_096 } }),
      })
      const request = LLM.request({ model, messages: [], tools: [], generation: { maxTokens: 4_096 } })
      const entry = (seq: number, text: string) => ({
        seq,
        message: SessionMessage.User.make({
          id: SessionMessage.ID.make(`msg_${seq}`),
          type: "user",
          text,
          time: { created: DateTime.makeUnsafe(0) },
        }),
      })
      const exit = yield* compaction
        .compactAfterOverflow({
          sessionID,
          entries: [entry(0, "Earlier context"), entry(1, `LATEST_INSTRUCTION ${"x".repeat(76_000)}`)],
          model,
          request,
          owner: EventV2.strictOwner("owner-b"),
          validateLocation: () => Effect.void,
        })
        .pipe(Effect.exit)
      expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toBeInstanceOf(EventV2.OwnerFenceError)
    }),
  )

  it.effect("stale compaction revalidates its location before the Ended append", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { db } = yield* Database.Service
      const sessionID = SessionSchema.ID.make("ses_compaction_moved")
      const sourceOwner = EventV2.strictOwner("owner-a")
      yield* events.publish(
        SessionEvent.Compaction.Started,
        { sessionID, messageID: SessionMessage.ID.create(), timestamp: yield* DateTime.now, reason: "auto" },
        sourceOwner,
      )

      let checks = 0
      const compaction = SessionCompaction.make({
        events,
        llm: { stream: () => Stream.make(LLMEvent.textDelta({ id: "summary", text: "summary" })) },
        config: [],
      })
      const model = Model.make({
        id: "compaction-moved",
        provider: "fake",
        route: OpenAIChat.route.with({ limits: { context: 20_000, output: 4_096 } }),
      })
      const entry = (seq: number, text: string) => ({
        seq,
        message: SessionMessage.User.make({
          id: SessionMessage.ID.make(`msg_moved_${seq}`),
          type: "user",
          text,
          time: { created: DateTime.makeUnsafe(0) },
        }),
      })
      const exit = yield* compaction
        .compactAfterOverflow({
          sessionID,
          entries: [entry(0, "Earlier context"), entry(1, `LATEST_INSTRUCTION ${"x".repeat(76_000)}`)],
          model,
          request: LLM.request({ model, messages: [], tools: [], generation: { maxTokens: 4_096 } }),
          owner: sourceOwner,
          validateLocation: () =>
            Effect.gen(function* () {
              checks++
              if (checks === 2) {
                yield* events.claim(sessionID, "owner-b")
                return yield* Effect.interrupt
              }
            }),
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
      expect(checks).toBe(2)
      expect(
        (yield* db
          .select({ type: EventTable.type })
          .from(EventTable)
          .where(eq(EventTable.aggregate_id, sessionID))
          .all()).map((event) => event.type),
      ).toEqual(["session.next.compaction.started.1", "session.next.compaction.started.1"])
    }),
  )
})
