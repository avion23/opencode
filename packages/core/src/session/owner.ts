export * as SessionOwner from "./owner"

import type { SessionSchema } from "./schema"

/**
 * Durable aggregate owner used to fence local Session-event appends.
 *
 * A Session inside a workspace is owned by that workspace; workspace-less Sessions
 * fall back to their project. Both map to exactly one coordination domain, so a
 * stale runner at an old location cannot append after ownership moves.
 */
export const ownerOf = (session: SessionSchema.Info): string =>
  session.location.workspaceID ?? session.projectID
