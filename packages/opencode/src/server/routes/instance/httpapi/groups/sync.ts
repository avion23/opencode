import { NonNegativeInt } from "@opencode-ai/core/schema"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionID } from "@/session/schema"
import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiError, HttpApiGroup, OpenApi } from "effect/unstable/httpapi"
import { Authorization } from "../middleware/authorization"
import { InstanceContextMiddleware } from "../middleware/instance-context"
import { WorkspaceRoutingMiddleware, WorkspaceRoutingQuery } from "../middleware/workspace-routing"
import { described } from "./metadata"

const root = "/sync"
export const ReplayEvent = Schema.Struct({
  id: EventV2.ID,
  aggregateID: Schema.String,
  seq: NonNegativeInt,
  type: Schema.String,
  data: Schema.Record(Schema.String, Schema.Unknown),
})
export const ReplayPayload = Schema.Struct({
  directory: Schema.String,
  events: Schema.NonEmptyArray(ReplayEvent),
  /** The owner that created the replay snapshot. The destination transfers it during steal. */
  ownerID: Schema.String,
  /** Identifies the in-progress warp so a failed replay can be rolled back. */
  warpID: Schema.optional(EventV2.ID),
})
export const ReplayResponse = Schema.Struct({
  sessionID: Schema.String,
})
export const SessionPayload = Schema.Struct({
  sessionID: SessionID,
  seq: NonNegativeInt,
  warpID: EventV2.ID,
  /** The owner currently fencing the aggregate, before compare-and-transfer. */
  ownerID: Schema.String,
})
export const StealResponse = Schema.Struct({
  sessionID: SessionID,
  event: ReplayEvent,
})
export const WorkspaceHistoryPayload = Schema.Struct({
  scope: Schema.Literal("workspace"),
})
export const AggregateHistoryPayload = Schema.Struct({
  scope: Schema.Literal("aggregate"),
  // -1 is the state fence used when the caller has no local event yet.
  state: Schema.Record(Schema.String, Schema.Int.check(Schema.isGreaterThanOrEqualTo(-1))),
  /** Read history only when this owner still owns the aggregate. */
  ownerID: Schema.optional(Schema.String),
  /** Fence one aggregate while returning its newest history. */
  fence: Schema.optional(
    Schema.Struct({
      sessionID: SessionID,
      /** The owner to install after the history snapshot is read. */
      ownerID: Schema.String,
    }),
  ),
})
export const HistoryPayload = Schema.Union([WorkspaceHistoryPayload, AggregateHistoryPayload])
export const HistoryEvent = Schema.Struct({
  id: EventV2.ID,
  aggregate_id: Schema.String,
  seq: NonNegativeInt,
  type: Schema.String,
  data: Schema.Record(Schema.String, Schema.Unknown),
})

export const SyncPaths = {
  start: `${root}/start`,
  replay: `${root}/replay`,
  steal: `${root}/steal`,
  history: `${root}/history`,
} as const

export const SyncApi = HttpApi.make("sync")
  .add(
    HttpApiGroup.make("sync")
      .add(
        HttpApiEndpoint.post("start", SyncPaths.start, {
          query: WorkspaceRoutingQuery,
          success: described(Schema.Boolean, "Workspace sync started"),
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "sync.start",
            summary: "Start workspace sync",
            description: "Start sync loops for workspaces in the current project that have active sessions.",
          }),
        ),
        HttpApiEndpoint.post("replay", SyncPaths.replay, {
          query: WorkspaceRoutingQuery,
          payload: ReplayPayload,
          success: described(ReplayResponse, "Replayed sync events"),
          error: [HttpApiError.BadRequest, HttpApiError.Conflict],
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "sync.replay",
            summary: "Replay sync events",
            description: "Validate and replay a complete sync event history.",
          }),
        ),
        HttpApiEndpoint.post("steal", SyncPaths.steal, {
          query: WorkspaceRoutingQuery,
          payload: SessionPayload,
          success: described(StealResponse, "Session stolen into workspace"),
          error: [HttpApiError.BadRequest, HttpApiError.Conflict],
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "sync.steal",
            summary: "Steal session into workspace",
            description: "Update a session to belong to the current workspace through the sync event system.",
          }),
        ),
        HttpApiEndpoint.post("history", SyncPaths.history, {
          query: WorkspaceRoutingQuery,
          payload: HistoryPayload,
          success: described(Schema.Array(HistoryEvent), "Sync events"),
          error: [HttpApiError.BadRequest, HttpApiError.Conflict],
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "sync.history.list",
            summary: "List sync events",
            description:
              "List newer sync events for the requested aggregates. Keys are aggregate IDs and values are the last known sequence ID.",
          }),
        ),
      )
      .annotateMerge(
        OpenApi.annotations({
          title: "sync",
          description: "Experimental HttpApi sync routes.",
        }),
      )
      .middleware(InstanceContextMiddleware)
      .middleware(WorkspaceRoutingMiddleware)
      .middleware(Authorization),
  )
  .annotateMerge(
    OpenApi.annotations({
      title: "opencode experimental HttpApi",
      version: "0.0.1",
      description: "Experimental HttpApi surface for selected instance routes.",
    }),
  )
