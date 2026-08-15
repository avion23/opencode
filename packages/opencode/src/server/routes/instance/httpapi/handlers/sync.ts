import { Workspace } from "@/control-plane/workspace"
import * as InstanceState from "@/effect/instance-state"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { EventV2Bridge } from "@/event-v2-bridge"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { WorkspaceTable } from "@opencode-ai/core/control-plane/workspace.sql"
import { ProjectV2 } from "@opencode-ai/core/project"
import { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { Durable } from "@opencode-ai/schema/durable-event-manifest"
import { asc, and, eq, gt, inArray } from "drizzle-orm"
import { Effect, Schema, Scope } from "effect"
import { HttpApiBuilder, HttpApiError } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"
import {
  AggregateHistoryPayload,
  HistoryPayload,
  ReplayPayload,
  SessionPayload,
  WorkspaceHistoryPayload,
} from "../groups/sync"
import { isCommittedSessionWarp } from "@/session/warp"

export const syncHandlers = HttpApiBuilder.group(InstanceHttpApi, "sync", (handlers) =>
  Effect.gen(function* () {
    const workspace = yield* Workspace.Service
    const session = yield* Session.Service
    const scope = yield* Scope.Scope
    const events = yield* EventV2Bridge.Service
    const { db } = yield* Database.Service

    const currentScope = Effect.fn("SyncHttpApi.currentScope")(function* () {
      const context = yield* InstanceState.context
      const workspaceID = yield* InstanceState.workspaceID
      if (!workspaceID) return yield* new HttpApiError.BadRequest({})

      const ownedWorkspace = yield* db
        .select({ id: WorkspaceTable.id })
        .from(WorkspaceTable)
        .where(and(eq(WorkspaceTable.id, workspaceID), eq(WorkspaceTable.project_id, context.project.id)))
        .get()
        .pipe(Effect.orDie)
      if (!ownedWorkspace) return yield* new HttpApiError.BadRequest({})
      return { workspaceID, projectID: context.project.id }
    })

    const sessionInProject = Effect.fn("SyncHttpApi.sessionInProject")(function* (
      sessionID: SessionID,
      projectID: ProjectV2.ID,
    ) {
      return yield* db
        .select({ id: SessionTable.id, workspaceID: SessionTable.workspace_id, projectID: SessionTable.project_id })
        .from(SessionTable)
        .where(and(eq(SessionTable.id, sessionID), eq(SessionTable.project_id, projectID)))
        .get()
        .pipe(Effect.orDie)
    })

    const ownerInProject = Effect.fn("SyncHttpApi.ownerInProject")(function* (
      ownerID: WorkspaceV2.ID,
      projectID: ProjectV2.ID,
    ) {
      if (String(ownerID) === String(projectID)) return true
      return Boolean(
        yield* db
          .select({ id: WorkspaceTable.id })
          .from(WorkspaceTable)
          .where(and(eq(WorkspaceTable.id, ownerID), eq(WorkspaceTable.project_id, projectID)))
          .get()
          .pipe(Effect.orDie),
      )
    })

    const decodeReplay = (payload: typeof ReplayPayload.Type) => {
      const source = payload.events[0]?.aggregateID
      if (!source || payload.events.some((event) => event.aggregateID !== source)) return
      const start = payload.events[0]?.seq
      if (start === undefined || payload.events.some((event, index) => event.seq !== start + index)) return

      for (const event of payload.events) {
        const definition = Durable.get(event.type)
        if (!definition?.durable) return
        try {
          Schema.decodeUnknownSync(definition.data)(event.data)
        } catch {
          return
        }
      }
      return source
    }

    const rollbackReplay = Effect.fn("SyncHttpApi.rollbackReplay")(function* (sessionID: SessionID) {
      yield* events.remove(sessionID)
      // EventV2.remove deletes the durable rows; the session projector is a
      // separate projection and must be removed as part of the same rollback.
      yield* db.delete(SessionTable).where(eq(SessionTable.id, sessionID)).run().pipe(Effect.orDie)
    })

    const start = Effect.fn("SyncHttpApi.start")(function* () {
      yield* workspace
        .startWorkspaceSyncing((yield* InstanceState.context).project.id)
        .pipe(Effect.ignore, Effect.forkIn(scope))
      return true
    })

    const replay = Effect.fn("SyncHttpApi.replay")(function* (ctx: { payload: typeof ReplayPayload.Type }) {
      const owner = yield* currentScope()
      const source = decodeReplay(ctx.payload)
      if (!source) return yield* new HttpApiError.BadRequest({})

      const existing = yield* sessionInProject(SessionID.make(source), owner.projectID)
      if (!existing) {
        const first = ctx.payload.events[0]
        const info = (first?.data as { info?: { projectID?: unknown } }).info
        if (
          first?.type !== EventV2.versionedType(Session.Event.Created.type, 1) ||
          info?.projectID !== owner.projectID
        )
          return yield* new HttpApiError.BadRequest({})
      }

      // D4: only roll back a replay that the destination created itself. When
      // the destination already had a session/sequence row before this replay,
      // a failure (e.g. a divergent snapshot from a stale source) must not
      // delete the legitimate destination state.
      let hadDestinationState = Boolean(existing)

      const replay = events
        .exclusive(
          source,
          Effect.gen(function* () {
            const sequence = yield* db
              .select({ seq: EventSequenceTable.seq, ownerID: EventSequenceTable.owner_id })
              .from(EventSequenceTable)
              .where(eq(EventSequenceTable.aggregate_id, source))
              .get()
              .pipe(Effect.orDie)
            hadDestinationState ||= Boolean(sequence)
            // D3: authorize when the recorded owner is undefined, the requesting
            // workspace, or the transferring source's current owner (A→B→A).
            if (
              sequence?.ownerID &&
              sequence.ownerID !== owner.workspaceID &&
              sequence.ownerID !== ctx.payload.ownerID
            )
              return yield* new HttpApiError.Conflict({})

            // Claim the aggregate before replay so a returning warp can
            // re-adopt its own history. Reentrant-safe: this runs inside the
            // exclusive lock for the same aggregate.
            yield* events.claim(source, owner.workspaceID)
            return yield* events.replayAll([...ctx.payload.events], { ownerID: owner.workspaceID, strictOwner: true })
          }),
        )
        .pipe(
          Effect.catchCause((cause) =>
            (ctx.payload.warpID && !hadDestinationState
              ? rollbackReplay(SessionID.make(source))
              : Effect.succeed(undefined)
            ).pipe(Effect.andThen(Effect.failCause(cause))),
          ),
        )

      yield* replay
      yield* Effect.logInfo("sync replay complete", {
        sessionID: source,
        events: ctx.payload.events.length,
        first: ctx.payload.events[0]?.seq,
        last: ctx.payload.events.at(-1)?.seq,
      })
      return { sessionID: source }
    })

    const steal = Effect.fn("SyncHttpApi.steal")(function* (ctx: { payload: typeof SessionPayload.Type }) {
      const owner = yield* currentScope()
      const current = yield* sessionInProject(ctx.payload.sessionID, owner.projectID)
      if (!current) return yield* new HttpApiError.BadRequest({})

      const event = yield* events.exclusive(
        ctx.payload.sessionID,
        Effect.gen(function* () {
          const latest = yield* EventV2.latestSequence(db, ctx.payload.sessionID)
          const candidate = yield* db
            .select()
            .from(EventTable)
            .where(
              and(
                eq(EventTable.aggregate_id, ctx.payload.sessionID),
                eq(EventTable.seq, ctx.payload.seq + 1),
                eq(EventTable.type, EventV2.versionedType(Session.Event.Updated.type, 1)),
              ),
            )
            .get()
            .pipe(Effect.orDie)

          // A seq+1 event is recoverable only when it is the terminal event.
          // In particular, an old matching warp must not be resurrected after
          // a later event has already committed.
          if (latest !== ctx.payload.seq) {
            if (
              latest !== ctx.payload.seq + 1 ||
              !candidate ||
              !isCommittedSessionWarp(candidate, {
                sessionID: ctx.payload.sessionID,
                seq: ctx.payload.seq,
                workspaceID: owner.workspaceID,
                warpID: ctx.payload.warpID,
              })
            )
              return undefined
            return candidate
          }

          const ownerRow = yield* db
            .select({ ownerID: EventSequenceTable.owner_id })
            .from(EventSequenceTable)
            .where(eq(EventSequenceTable.aggregate_id, ctx.payload.sessionID))
            .get()
            .pipe(Effect.orDie)
          if (ownerRow?.ownerID && ownerRow.ownerID !== owner.workspaceID) return undefined

          yield* session.setWorkspace({
            sessionID: ctx.payload.sessionID,
            workspaceID: owner.workspaceID,
            eventID: ctx.payload.warpID,
            ownerID: owner.workspaceID,
          })
          return yield* db
            .select()
            .from(EventTable)
            .where(eq(EventTable.id, ctx.payload.warpID))
            .get()
            .pipe(Effect.orDie)
        }),
      )
      if (!event) return yield* new HttpApiError.Conflict({})

      if (
        !isCommittedSessionWarp(event, {
          sessionID: ctx.payload.sessionID,
          seq: ctx.payload.seq,
          workspaceID: owner.workspaceID,
          warpID: ctx.payload.warpID,
        })
      )
        return yield* new HttpApiError.Conflict({})

      yield* Effect.logInfo("sync session stolen", { sessionID: ctx.payload.sessionID, workspaceID: owner.workspaceID })

      return {
        sessionID: ctx.payload.sessionID,
        event: {
          id: EventV2.ID.make(event.id),
          aggregateID: event.aggregate_id,
          seq: event.seq,
          type: event.type,
          data: event.data,
        },
      }
    })

    const history = Effect.fn("SyncHttpApi.history")(function* (ctx: { payload: typeof HistoryPayload.Type }) {
      const owner = yield* currentScope()

      if (ctx.payload.scope === "workspace") {
        const aggregates = yield* db
          .select({ id: EventSequenceTable.aggregate_id })
          .from(EventSequenceTable)
          .innerJoin(SessionTable, eq(EventSequenceTable.aggregate_id, SessionTable.id))
          .where(
            and(
              eq(EventSequenceTable.owner_id, owner.workspaceID),
              eq(SessionTable.project_id, owner.projectID),
            ),
          )
          .all()
          .pipe(Effect.orDie)
        if (!aggregates.length) return []
        return yield* db
          .select()
          .from(EventTable)
          .where(inArray(EventTable.aggregate_id, aggregates.map((item) => item.id)))
          .orderBy(asc(EventTable.aggregate_id), asc(EventTable.seq))
          .all()
          .pipe(Effect.orDie)
      }

      const request = ctx.payload as typeof AggregateHistoryPayload.Type
      if (request.fence) {
        if (!(yield* ownerInProject(WorkspaceV2.ID.make(request.fence.ownerID), owner.projectID)))
          return yield* new HttpApiError.BadRequest({})
        if (request.fence.sessionID !== Object.keys(request.state)[0] || Object.keys(request.state).length !== 1)
          return yield* new HttpApiError.BadRequest({})

        return yield* events.exclusive(
          request.fence.sessionID,
          Effect.gen(function* () {
            const current = yield* sessionInProject(request.fence!.sessionID, owner.projectID)
            if (!current || current.workspaceID !== owner.workspaceID) return yield* new HttpApiError.Conflict({})

            const sequence = yield* db
              .select({ ownerID: EventSequenceTable.owner_id })
              .from(EventSequenceTable)
              .where(eq(EventSequenceTable.aggregate_id, request.fence!.sessionID))
              .get()
              .pipe(Effect.orDie)
            if (sequence?.ownerID && sequence.ownerID !== owner.workspaceID && sequence.ownerID !== request.fence!.ownerID)
              return yield* new HttpApiError.Conflict({})

            const rows = yield* db
              .select()
              .from(EventTable)
              .where(
                and(
                  eq(EventTable.aggregate_id, request.fence!.sessionID),
                  gt(EventTable.seq, request.state[request.fence!.sessionID] ?? -1),
                ),
              )
              .orderBy(asc(EventTable.seq))
              .all()
              .pipe(Effect.orDie)
            yield* events.claim(request.fence!.sessionID, request.fence!.ownerID)
            return rows
          }),
        )
      }

      return yield* Effect.forEach(Object.entries(request.state), ([id, seq]) =>
        events.exclusive(
          id,
          db
            .select({ event: EventTable })
            .from(EventTable)
            .innerJoin(EventSequenceTable, eq(EventTable.aggregate_id, EventSequenceTable.aggregate_id))
            .innerJoin(SessionTable, eq(EventTable.aggregate_id, SessionTable.id))
            .where(
              and(
                eq(EventTable.aggregate_id, id),
                gt(EventTable.seq, seq),
                eq(EventSequenceTable.owner_id, owner.workspaceID),
                eq(SessionTable.project_id, owner.projectID),
              ),
            )
            .orderBy(asc(EventTable.seq))
            .all()
            .pipe(Effect.orDie, Effect.map((rows) => rows.map((row) => row.event)),
          ),
        ),
      ).pipe(Effect.map((rows) => rows.flat()))
    })

    return handlers.handle("start", start).handle("replay", replay).handle("steal", steal).handle("history", history)
  }),
)
