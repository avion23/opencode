import { afterEach, describe, expect, mock } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Context, Effect, Fiber, Layer } from "effect"
import { asc, eq } from "drizzle-orm"
import { Flag } from "@opencode-ai/core/flag/flag"
import { WorkspaceTable } from "@opencode-ai/core/control-plane/workspace.sql"
import { Database } from "@opencode-ai/core/database/database"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { EventTable, EventSequenceTable } from "@opencode-ai/core/event/sql"
import { SyncPaths } from "../../src/server/routes/instance/httpapi/groups/sync"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { Session } from "@/session/session"
import { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { EventV2 } from "@opencode-ai/core/event"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { withFixedWorkspaceID } from "../fixture/flag"
import { testEffect } from "../lib/effect"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"

const originalWorkspaces = Flag.OPENCODE_EXPERIMENTAL_WORKSPACES
const context = Context.empty() as Context.Context<unknown>
const it = testEffect(
  Layer.mergeAll(
    LayerNode.compile(LayerNode.group([Session.node, EventV2.node, Database.node])),
    httpApiLayer,
  ),
)

afterEach(async () => {
  mock.restore()
  Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = originalWorkspaces
  await disposeAllInstances()
  await resetDatabase()
})

describe("sync HttpApi", () => {
  it.instance(
    "serves sync routes",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const session = yield* Session.use.create({ title: "sync" })
        const unrelated = yield* Session.use.create({ title: "unrelated" })
        yield* Session.use.setTitle({ sessionID: session.id, title: "sync updated" })

        const started = yield* requestInDirectory(SyncPaths.start, tmp.directory, { method: "POST", headers })
        expect(started.status).toBe(200)
        expect(yield* started.json).toBe(true)

        const history = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ [session.id]: 0 }),
        })
        expect(history.status).toBe(200)
        const rows = (yield* history.json) as Array<{
          id: string
          aggregate_id: string
          seq: number
          type: string
          data: Record<string, unknown>
        }>
        expect(rows.map((row) => row.aggregate_id)).toEqual([session.id])
        expect(rows.map((row) => row.aggregate_id)).not.toContain(unrelated.id)

        const replayed = yield* requestInDirectory(SyncPaths.replay, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({
            directory: tmp.directory,
            events: rows
              .filter((row) => row.aggregate_id === session.id)
              .map((row) => ({
                id: row.id,
                aggregateID: row.aggregate_id,
                seq: row.seq,
                type: row.type,
                data: row.data,
              })),
          }),
        })
        expect(replayed.status).toBe(200)
        expect(yield* replayed.json).toEqual({ sessionID: session.id })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "waits for an aggregate commit before returning its history",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const events = yield* EventV2.Service
        const session = yield* Session.use.create({ title: "sync history lock" })

        const request = yield* events.exclusive(
          session.id,
          Effect.gen(function* () {
            const fiber = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ [session.id]: 0 }),
            }).pipe(Effect.forkChild)
            yield* Effect.sleep("100 millis")
            expect((yield* Fiber.join(fiber).pipe(Effect.timeoutOption("50 millis")))._tag).toBe("None")
            yield* Session.use.setTitle({ sessionID: session.id, title: "committed" })
            return fiber
          }),
        )

        const response = yield* Fiber.join(request)
        expect(response.status).toBe(200)
        const rows = (yield* response.json) as Array<{ aggregate_id: string; seq: number }>
        expect(rows).toMatchObject([{ aggregate_id: session.id, seq: 1 }])
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "validates seq values",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const cases = [
          {
            path: SyncPaths.history,
            body: { aggregate: -1 },
          },
          {
            path: SyncPaths.history,
            body: { aggregate: 1.5 },
          },
          {
            path: SyncPaths.replay,
            body: {
              directory: tmp.directory,
              events: [{ id: "event", aggregateID: "session", seq: -1, type: "session.created", data: {} }],
            },
          },
          {
            path: SyncPaths.replay,
            body: {
              directory: tmp.directory,
              events: [{ id: "event", aggregateID: "session", seq: 1.5, type: "session.created", data: {} }],
            },
          },
          {
            path: SyncPaths.replay,
            body: {
              directory: tmp.directory,
              events: [{ id: "event", aggregateID: "session", seq: 0, type: "session.created", data: {} }],
            },
          },
        ]

        for (const item of cases) {
          const response = yield* requestInDirectory(item.path, tmp.directory, {
            method: "POST",
            headers,
            body: JSON.stringify(item.body),
          })
          expect(response.status).toBe(400)
        }
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "rejects stale session steals",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const session = yield* Session.use.create({ title: "sync steal" })
        const workspaceID = WorkspaceV2.ID.ascending()
        const warpID = EventV2.ID.create()
        yield* withFixedWorkspaceID(workspaceID)
        // The steal handler's currentScope requires the workspace to exist as
        // a real WorkspaceTable row owned by this project.
        const { db } = yield* Database.Service
        yield* db
          .insert(WorkspaceTable)
          .values({ id: workspaceID, type: "test", project_id: (yield* Session.use.get(session.id)).projectID })
          .run()
          .pipe(Effect.orDie)

        const stale = yield* requestInDirectory(SyncPaths.steal, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ sessionID: session.id, seq: 1, warpID, ownerID: workspaceID }),
        })
        expect(stale.status).toBe(409)
        expect((yield* Session.use.get(session.id)).workspaceID).toBeUndefined()

        const committed = yield* requestInDirectory(SyncPaths.steal, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ sessionID: session.id, seq: 0, warpID, ownerID: workspaceID }),
        })
        expect(committed.status).toBe(200)
        const result = (yield* committed.json) as { sessionID: string; event: { id: string; seq: number } }
        expect(result).toMatchObject({ sessionID: session.id, event: { id: warpID, seq: 1 } })
        expect((yield* Session.use.get(session.id)).workspaceID).toBe(workspaceID)

        const retried = yield* requestInDirectory(SyncPaths.steal, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ sessionID: session.id, seq: 0, warpID, ownerID: workspaceID }),
        })
        expect(retried.status).toBe(200)
        expect(yield* retried.json).toEqual(result)

        // A steal for a DIFFERENT warpID must not recover the committed warp:
        // the seq+1 event is only adoptable when its id is the requested warpID.
        const recovered = yield* requestInDirectory(SyncPaths.steal, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ sessionID: session.id, seq: 0, warpID: EventV2.ID.create(), ownerID: workspaceID }),
        })
        expect(recovered.status).toBe(409)
        expect((yield* Session.use.get(session.id)).workspaceID).toBe(workspaceID)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "does not delete an already-owned destination session when a stale divergent replay fails",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const workspaceID = WorkspaceV2.ID.ascending()
        yield* withFixedWorkspaceID(workspaceID)
        const session = yield* Session.use.create({ title: "sync replay owned", workspaceID })
        yield* Session.use.setTitle({ sessionID: session.id, title: "sync replay owned updated" })
        const events = yield* EventV2.Service
        // The aggregate-history query filters on owner_id: stamp the destination
        // as the workspace owner so the history rows are visible to the handler.
        yield* events.claim(session.id, workspaceID)
        // history/replay handlers' currentScope requires the workspace to
        // exist as a real WorkspaceTable row owned by this project.
        const { db } = yield* Database.Service
        yield* db
          .insert(WorkspaceTable)
          .values({ id: workspaceID, type: "test", project_id: (yield* Session.use.get(session.id)).projectID })
          .run()
          .pipe(Effect.orDie)

        // The destination already owns the aggregate: fetch its real rows.
        const history = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ scope: "aggregate", state: { [session.id]: -1 } }),
        })
        expect(history.status).toBe(200)
        const owned = (yield* history.json) as Array<{
          id: string
          aggregate_id: string
          seq: number
          type: string
          data: Record<string, unknown>
        }>
        expect(owned.map((row) => row.seq)).toEqual([0, 1])

        // A stale source replays the same aggregate with a DIFFERENT event id
        // at the newest sequence: replayAll diverges and fails, and the D4 fix
        // must NOT roll the destination's legitimate state back.
        const divergent = owned.map((row, index) =>
          index === owned.length - 1
            ? {
                id: `evt_divergent_${index}`,
                aggregateID: row.aggregate_id,
                seq: row.seq,
                type: row.type,
                data: row.data,
              }
            : {
                id: row.id,
                aggregateID: row.aggregate_id,
                seq: row.seq,
                type: row.type,
                data: row.data,
              },
        )
        const replayed = yield* requestInDirectory(SyncPaths.replay, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({
            directory: tmp.directory,
            events: divergent,
            ownerID: workspaceID,
            warpID: EventV2.ID.create(),
          }),
        })
        expect(replayed.status).toBeGreaterThanOrEqual(400)

        // The destination session, its events, and its ownership survive the
        // failed replay (rollbackReplay only runs when the destination had no
        // prior state).
        expect((yield* Session.use.get(session.id)).title).toBe("sync replay owned updated")
        expect((yield* Session.use.get(session.id)).workspaceID).toBe(workspaceID)
        const after = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ scope: "aggregate", state: { [session.id]: -1 } }),
        })
        expect(after.status).toBe(200)
        const remaining = (yield* after.json) as Array<{ id: string; seq: number }>
        expect(remaining.map((row) => row.seq)).toEqual([0, 1])
        expect(remaining.map((row) => row.id)).not.toContain(divergent.at(-1)!.id)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "sessionWarp round-trips a session A→B→A: D3 replay authorization re-adopts the returning history without stranding",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const { db } = yield* Database.Service
        const events = yield* EventV2.Service

        // Source workspace A owns the session; destination workspace B is the
        // remote owner the warp transfers to. Both must exist as real
        // WorkspaceTable rows so the replay handler's currentScope resolves.
        const workspaceA = WorkspaceV2.ID.ascending()
        const workspaceB = WorkspaceV2.ID.ascending()
        yield* withFixedWorkspaceID(workspaceA)
        const session = yield* Session.use.create({ title: "round trip", workspaceID: workspaceA })
        const projectID = (yield* Session.use.get(session.id)).projectID
        yield* db
          .insert(WorkspaceTable)
          .values([
            { id: workspaceA, type: "test", project_id: projectID },
            { id: workspaceB, type: "test", project_id: projectID },
          ])
          .run()
          .pipe(Effect.orDie)

        // Ensure the source aggregate is fenced to workspace A, and add one
        // local write so the history is non-trivial (seq 0, 1).
        yield* events.claim(session.id, workspaceA)
        yield* Session.use.setTitle({ sessionID: session.id, title: "round trip in a" })

        const sourceEvents = (
          yield* db
            .select({ id: EventTable.id, aggregate_id: EventTable.aggregate_id, seq: EventTable.seq, type: EventTable.type, data: EventTable.data })
            .from(EventTable)
            .where(eq(EventTable.aggregate_id, session.id))
            .orderBy(asc(EventTable.seq))
            .all()
            .pipe(Effect.orDie)
        )
        expect(sourceEvents.map((row) => row.seq)).toEqual([0, 1])
        const replayEvents = sourceEvents.map((row) => ({
          id: row.id,
          aggregateID: row.aggregate_id,
          seq: row.seq,
          type: row.type,
          data: row.data,
        }))

        // First leg A → B: the destination store now owns the aggregate with
        // workspace B and byte-identical history (one in-process store holds
        // both sides), mirroring the steal/claim a real warp performs.
        yield* events.claim(session.id, workspaceB)
        yield* db
          .update(SessionTable)
          .set({ workspace_id: workspaceB })
          .where(eq(SessionTable.id, session.id))
          .run()
          .pipe(Effect.orDie)

        // A foreign source (owner C, neither the requesting workspace A nor
        // the recorded owner B) is rejected before the return warp: the D3
        // predicate must NOT let an unrelated owner re-adopt.
        const foreign = yield* requestInDirectory(SyncPaths.replay, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({
            directory: tmp.directory,
            events: replayEvents,
            ownerID: WorkspaceV2.ID.ascending(),
            warpID: EventV2.ID.create(),
          }),
        })
        expect(foreign.status).toBe(409)

        // Return leg B → A: the transferring source's recorded owner (B) is
        // sent as the payload ownerID. The D3 predicate authorizes it because
        // sequence.ownerID === ctx.payload.ownerID, then claims back to A.
        const replayed = yield* requestInDirectory(SyncPaths.replay, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({
            directory: tmp.directory,
            events: replayEvents,
            ownerID: workspaceB,
            warpID: EventV2.ID.create(),
          }),
        })
        expect(replayed.status).toBe(200)
        expect(yield* replayed.json).toEqual({ sessionID: session.id })

        // The aggregate ownership is back on A (claimed by the replay).
        const sequence = yield* db
          .select({ ownerID: EventSequenceTable.owner_id })
          .from(EventSequenceTable)
          .where(eq(EventSequenceTable.aggregate_id, session.id))
          .get()
          .pipe(Effect.orDie)
        expect(sequence?.ownerID).toBe(workspaceA)

        // Complete the return (the steal/setWorkspace side of the warp writes
        // the session back to A) and verify a local write is no longer fenced:
        // the session must not be stranded.
        yield* db
          .update(SessionTable)
          .set({ workspace_id: workspaceA })
          .where(eq(SessionTable.id, session.id))
          .run()
          .pipe(Effect.orDie)
        yield* Session.use.setTitle({ sessionID: session.id, title: "back in a" })
        expect((yield* Session.use.get(session.id)).title).toBe("back in a")
        expect((yield* Session.use.get(session.id)).workspaceID).toBe(workspaceA)
        const after = yield* db
          .select({ seq: EventSequenceTable.seq })
          .from(EventSequenceTable)
          .where(eq(EventSequenceTable.aggregate_id, session.id))
          .get()
          .pipe(Effect.orDie)
        expect(after?.seq).toBe(2)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "does not hold the database write lock while waiting to steal a session",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const events = yield* EventV2.Service
        const session = yield* Session.use.create({ title: "locked steal" })
        const unrelated = yield* Session.use.create({ title: "unrelated" })
        const workspaceID = WorkspaceV2.ID.ascending()
        yield* withFixedWorkspaceID(workspaceID)

        const request = yield* events.exclusive(
          session.id,
          Effect.gen(function* () {
            const fiber = yield* requestInDirectory(SyncPaths.steal, tmp.directory, {
              method: "POST",
              headers: { "x-opencode-directory": tmp.directory, "content-type": "application/json" },
              body: JSON.stringify({ sessionID: session.id, seq: 0, warpID: EventV2.ID.create() }),
            }).pipe(Effect.forkChild)
            yield* Effect.sleep("100 millis")
            expect((yield* Fiber.join(fiber).pipe(Effect.timeoutOption("50 millis")))._tag).toBe("None")
            expect(
              (yield* Session.use
                .setTitle({ sessionID: unrelated.id, title: "write completed" })
                .pipe(Effect.timeoutOption("500 millis")))._tag,
            ).toBe("Some")
            return fiber
          }),
        )

        expect((yield* Fiber.join(request)).status).toBe(200)
        expect((yield* Session.use.get(unrelated.id)).title).toBe("write completed")
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance.skip(
    "returns structured validation errors",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const response = yield* Effect.promise(() =>
          HttpApiApp.webHandler().handler(
            new Request(`http://localhost${SyncPaths.history}`, {
              method: "POST",
              headers: { "x-opencode-directory": tmp.directory, "content-type": "application/json" },
              body: JSON.stringify({ aggregate: -1 }),
            }),
            context,
          ),
        )

        expect(response.status).toBe(400)
        expect(response.headers.get("content-type") ?? "").toContain("application/json")
        const body = (yield* Effect.promise(() => response.json())) as Record<string, unknown>
        expect(body.success).toBe(false)
        expect(Array.isArray(body.error) || Array.isArray(body.errors)).toBe(true)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )
})
