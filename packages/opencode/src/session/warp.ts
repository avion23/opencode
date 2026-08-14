import { EventV2 } from "@opencode-ai/core/event"
import { SessionV1 } from "@opencode-ai/core/v1/session"

export function isCommittedSessionWarp(
  event: {
    aggregate_id: string
    seq: number
    type: string
    data: unknown
  },
  input: { sessionID: string; seq: number; workspaceID: string },
) {
  if (event.aggregate_id !== input.sessionID) return false
  if (event.seq !== input.seq + 1) return false
  if (event.type !== EventV2.versionedType(SessionV1.Event.Updated.type, 1)) return false
  const data = event.data as { info?: { workspaceID?: unknown } } | undefined
  return data?.info?.workspaceID === input.workspaceID
}
