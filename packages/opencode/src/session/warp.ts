import { EventV2 } from "@opencode-ai/core/event"
import { SessionV1 } from "@opencode-ai/core/v1/session"

export function isCommittedSessionWarp(
  event: {
    id: string
    aggregate_id: string
    seq: number
    type: string
    data: unknown
  },
  input: { sessionID: string; seq: number; workspaceID: string; warpID: string },
) {
  // A warp commit is only recoverable when the candidate event is the exact
  // warp event this client generated. A non-warp session.updated.1 (a title
  // change, a move, …) that merely matches seq+1/type/workspace must not be
  // adopted as if the steal committed.
  if (event.id !== input.warpID) return false
  if (event.aggregate_id !== input.sessionID) return false
  if (event.seq !== input.seq + 1) return false
  if (event.type !== EventV2.versionedType(SessionV1.Event.Updated.type, 1)) return false
  const data = event.data as { info?: { workspaceID?: unknown } } | undefined
  return data?.info?.workspaceID === input.workspaceID
}
