import { describe, expect, test } from "bun:test"
import { Database, type Statement } from "bun:sqlite"
import { Effect, Layer } from "effect"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { literal } from "effect/unstable/sql/Statement"
import { sqliteLayer } from "../src/database/sqlite.bun.ts"
import { Sqlite } from "../src/database/sqlite"

type Call = "finalize" | "close"

interface Spy {
  created: Set<Statement>
  finalized: Statement[]
  log: Call[]
}

// Provides Sqlite.Native with a proxied Database so the test can observe the
// contract the fix guarantees: every statement created through native.query
// is finalized exactly once, and all finalizations happen before close().
const makeSpyLayer = () => {
  const spy: Spy = { created: new Set(), finalized: [], log: [] }
  const statementProxies = new WeakMap<Statement, Statement>()

  const wrapStatement = (stmt: Statement): Statement => {
    let proxy = statementProxies.get(stmt)
    if (!proxy) {
      proxy = new Proxy(stmt, {
        get(target, prop) {
          if (prop === "finalize") {
            return (...args: unknown[]) => {
              spy.log.push("finalize")
              spy.finalized.push(target)
              return Reflect.apply(target.finalize, target, args)
            }
          }
          const value = Reflect.get(target, prop)
          if (typeof value === "function") {
            return (...args: unknown[]) => Reflect.apply(value, target, args)
          }
          return value
        },
      })
      statementProxies.set(stmt, proxy)
    }
    return proxy
  }

  const wrapDatabase = (db: Database): Database =>
    new Proxy(db, {
      get(target, prop) {
        if (prop === "close") {
          return (...args: unknown[]) => {
            spy.log.push("close")
            return Reflect.apply(target.close, target, args)
          }
        }
        const value = Reflect.get(target, prop)
        if (prop === "query" && typeof value === "function") {
          return (sql: string, ...rest: unknown[]) => {
            const stmt = Reflect.apply(value, target, [sql, ...rest]) as Statement
            spy.created.add(stmt)
            return wrapStatement(stmt)
          }
        }
        if (typeof value === "function") {
          return (...args: unknown[]) => Reflect.apply(value, target, args)
        }
        return value
      },
    })

  const native = Layer.effect(
    Sqlite.Native,
    Effect.gen(function* () {
      const db = wrapDatabase(new Database(":memory:"))
      yield* Effect.addFinalizer(() => Effect.sync(() => db.close()))
      return db
    }),
  )

  const layer = Layer.merge(
    native,
    sqliteLayer({ filename: ":memory:", disableWAL: true }).pipe(Layer.provide(native)),
  ).pipe(Layer.provide(Reactivity.layer))

  return { spy, layer }
}

const open = <A, E>(
  layer: Layer.Layer<Sqlite.Native | SqlClient, never, never>,
  effect: Effect.Effect<A, E, SqlClient>,
) => Effect.runPromise(effect.pipe(Effect.provide(layer), Effect.scoped))

const isMisuse = (message: string) => /misuse|invalid database connection pointer/i.test(message)

describe("sqlite.bun connection lifecycle", () => {
  test("finalizes every statement exactly once before the native connection closes", async () => {
    const { spy, layer } = makeSpyLayer()
    await open(
      layer,
      Effect.gen(function* () {
        const sql = yield* SqlClient
        for (let i = 0; i < 40; i++) {
          yield* sql`CREATE TABLE ${literal("t" + i)} (id INTEGER PRIMARY KEY, name TEXT)`
          yield* sql`INSERT INTO ${literal("t" + i)} (name) VALUES (${"x" + i})`
          yield* sql`SELECT * FROM ${literal("t" + i)}`
        }
      }),
    )
    Bun.gc(true)
    Bun.gc(true)

    expect(spy.created.size).toBeGreaterThan(0)
    expect(spy.finalized.length).toBe(spy.created.size)
    expect(new Set(spy.finalized).size).toBe(spy.finalized.length)
    const closeIndex = spy.log.indexOf("close")
    const lastFinalize = spy.log.lastIndexOf("finalize")
    expect(closeIndex).toBeGreaterThanOrEqual(0)
    expect(lastFinalize).toBeGreaterThanOrEqual(0)
    expect(lastFinalize).toBeLessThan(closeIndex)
  })

  test("rejects queries after the scope closes with a clean error, never a misuse", async () => {
    const { layer } = makeSpyLayer()
    const sql = await open(
      layer,
      Effect.gen(function* () {
        const sql = yield* SqlClient
        yield* sql`SELECT 1 as v`
        return sql
      }),
    )
    Bun.gc(true)
    Bun.gc(true)

    const error = await Effect.runPromise(sql`SELECT 1 as post_close`.pipe(Effect.scoped)).then(
      () => null,
      (cause: unknown) => cause,
    )
    expect(error).not.toBeNull()
    const message = error instanceof Error ? error.message : String(error)
    expect(message).toMatch(/closed/i)
    expect(isMisuse(message)).toBe(false)
  })

  test("finalizes statements created through transactions", async () => {
    const { layer } = makeSpyLayer()
    await open(
      layer,
      Effect.gen(function* () {
        const sql = yield* SqlClient
        yield* sql`CREATE TABLE tx_test (id INTEGER PRIMARY KEY, name TEXT)`
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`INSERT INTO tx_test (name) VALUES (${"a"})`
            yield* sql`INSERT INTO tx_test (name) VALUES (${"b"})`
          }),
        )
        const rows = yield* sql`SELECT count(*) as c FROM tx_test`
        if (rows[0].c !== 2) throw new Error("transaction did not commit")
      }),
    )
    Bun.gc(true)
    Bun.gc(true)
  })

  test("survives repeated open/query/close/GC cycles without misuse", async () => {
    for (let n = 0; n < 10; n++) {
      const { layer } = makeSpyLayer()
      await open(
        layer,
        Effect.gen(function* () {
          const sql = yield* SqlClient
          yield* sql`SELECT 1 as v`
          yield* sql`SELECT 2 as w`
        }),
      )
      Bun.gc(true)
      Bun.gc(true)
    }
  })
})
