#!/usr/bin/env bun
import { fileURLToPath } from "url"

const dir = fileURLToPath(new URL("..", import.meta.url))
process.chdir(dir)

import { $ } from "bun"
import path from "path"

import { createClient } from "@hey-api/openapi-ts"

const opencode = path.resolve(dir, "../../opencode")
const openapi = path.resolve(dir, "../openapi.json")

await $`bun dev generate > ${openapi}`.cwd(opencode)

const document = (await Bun.file(openapi).json()) as {
  components?: { schemas?: Record<string, unknown> }
  [key: string]: unknown
}
const schemas = document.components?.schemas
if (schemas) {
  const reachable = new Set<string>()
  const visit = (value: unknown) => {
    if (Array.isArray(value)) {
      value.forEach(visit)
      return
    }
    if (typeof value !== "object" || value === null) return
    for (const [key, child] of Object.entries(value)) {
      if (key === "$ref" && typeof child === "string" && child.startsWith("#/components/schemas/")) {
        const name = child.slice("#/components/schemas/".length)
        if (reachable.has(name)) continue
        reachable.add(name)
        visit(schemas[name])
      } else {
        visit(child)
      }
    }
  }
  visit({ ...document, components: { ...document.components, schemas: undefined } })
  for (const name of Object.keys(schemas)) {
    if (/^SessionNext\w+1$/.test(name) && !reachable.has(name)) delete schemas[name]
  }
  await Bun.write(openapi, JSON.stringify(document, null, 2) + "\n")
}

await createClient({
  input: openapi,
  output: {
    path: "./src/v2/gen",
    tsConfigPath: path.join(dir, "tsconfig.json"),
    clean: true,
  },
  plugins: [
    {
      name: "@hey-api/typescript",
      exportFromIndex: false,
    },
    {
      name: "@hey-api/sdk",
      instance: "OpencodeClient",
      exportFromIndex: false,
      auth: false,
      paramsStructure: "flat",
    },
    {
      name: "@hey-api/client-fetch",
      exportFromIndex: false,
      baseUrl: "http://localhost:4096",
    },
  ],
})

const generatedTypes = await Bun.file("./src/v2/gen/types.gen.ts").text()
if (/export type SessionNext\w+1 =/.test(generatedTypes)) {
  throw new Error("Session history generated duplicate Session event variants")
}
const historyTypesPatched = generatedTypes.replace(
  /(export type V2SessionHistoryData = \{[\s\S]*?query\?: \{\s*limit\?: )string([;,]\s*after\?: )string/,
  "$1number$2number",
)
if (historyTypesPatched === generatedTypes) {
  throw new Error("Session history numeric query patch did not apply")
}
await Bun.write("./src/v2/gen/types.gen.ts", historyTypesPatched)

const generatedSdk = await Bun.file("./src/v2/gen/sdk.gen.ts").text()
const historySdkPatched = generatedSdk.replace(
  /(Get session history[\s\S]*?parameters: \{\s*sessionID: string[;,]\s*limit\?: )string([;,]\s*after\?: )string/,
  "$1number$2number",
)
if (historySdkPatched === generatedSdk) {
  throw new Error("Session history numeric SDK patch did not apply")
}
const syncStealSdkPatched = historySdkPatched.replace(
  /(Steal session into workspace[\s\S]*?parameters)\?: \{([\s\S]*?sessionID)\?: string([;,]\s*seq)\?: number([;,]\s*warpID)\?: string([;,]\s*ownerID)\?: string/,
  "$1: {$2: string$3: number$4: string$5: string",
)
if (syncStealSdkPatched === historySdkPatched) {
  throw new Error("Sync steal required parameter SDK patch did not apply")
}
// /sync/replay: like steal, hey-api still emits optional flat parameters
// despite required=true in the spec, so force the required protocol body
// fields (body_directory, events, ownerID). warpID stays optional.
const syncReplaySdkPatched = syncStealSdkPatched.replace(
  /(Replay sync events[\s\S]*?parameters)\?: \{([\s\S]*?body_directory)\?: string([\s\S]*?events)\?: (Array<\{[\s\S]*?\}>)([\s\S]*?ownerID)\?: string/,
  "$1: {$2: string$3: $4$5: string",
)
if (syncReplaySdkPatched === syncStealSdkPatched) {
  throw new Error("Sync replay required parameter SDK patch did not apply")
}
// /sync/history: unlike steal/replay, hey-api already honours required=true
// here and emits parameters/body as required, so no transform is needed --
// assert the invariant instead so a future codegen change fails the build.
const syncHistorySdkPatched = syncReplaySdkPatched
if (!/List sync events[\s\S]*?parameters: \{\s*directory\?: string;?\s*workspace\?: string;?\s*body: (?!\?)/.test(syncHistorySdkPatched)) {
  throw new Error("Sync history required parameter SDK patch did not apply")
}
await Bun.write("./src/v2/gen/sdk.gen.ts", syncHistorySdkPatched)

// Patch a @hey-api/openapi-ts codegen bug: SseFn incorrectly passes the
// endpoint's TError into the second generic of ServerSentEventsResult, which
// is the AsyncGenerator's TReturn slot. Iterator return values have nothing
// to do with HTTP errors, and any consumer that calls `.return()` or returns
// from a mock generator gets type-checked against the wrong shape. Drop the
// arg so TReturn defaults to void.
const sseTypesPath = "./src/v2/gen/client/types.gen.ts"
const sseTypesFile = Bun.file(sseTypesPath)
const sseTypesSource = await sseTypesFile.text()
const sseTypesPatched = sseTypesSource.replace(
  "=> Promise<ServerSentEventsResult<TData, TError>>",
  "=> Promise<ServerSentEventsResult<TData>>",
)
if (sseTypesPatched === sseTypesSource) {
  throw new Error(`SseFn patch did not apply; @hey-api/openapi-ts output may have changed (${sseTypesPath})`)
}
await Bun.write(sseTypesPath, sseTypesPatched)

await $`bun prettier --write src/gen`
await $`bun prettier --write src/v2`
await $`rm -rf dist`
await $`bun tsc`
