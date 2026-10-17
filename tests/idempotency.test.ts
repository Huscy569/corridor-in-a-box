import { describe, expect, it } from "vitest";
import { parseCorridor, type Corridor } from "@corridor/manifest";
import { createMockAdapter } from "@corridor/adapter-kit";
import { StaticRouteResolver } from "@corridor/router";
import {
  InMemoryIdempotencyStore,
  PostgresIdempotencyStore,
  createMockSubmitter,
  execute,
  hasRequestedRefund,
  migrate,
  type EngineDeps,
  type Queryable,
  type QueryResult,
  type StoredRun,
} from "@corridor/engine";
import type { PaymentIntent } from "@corridor/types";

function corridor(): Corridor {
  const r = parseCorridor({
    id: "test",
    source: { name: "S", asset: "USDC", endpoints: { home_domain: "s.example" } },
    dest:{
      name: "D",
      asset: "iso4217:ARS",
      endpoints: {
        home_domain: "d.example",
        transfer_server_sep31: "https://d.example/sep31",
        endpoints_verified_at: "1970-01-01",
      },
    },
    fx: { path: ["ARS", "USDC", "ARS"], who_holds_risk: "receiving_anchor" },
    compliance: { source_jurisdiction: "AR", dest_jurisdiction: "AR" },
    settlement: { network: "public", asset_issuer: "GISSUER" },
    recovery: {},
    proof: {
      canary_completed_at: "1970-01-01T00:00:00Z",
      stellar_tx_hash: "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90",
      anchor_transaction_id: "canary-test",
      amount: "1",
      max_age_days: 50000,
    },
  });
  if (!r.ok) throw new Error("fixture invalid");
  return r.value;
}

const intent: PaymentIntent = {
  idempotencyKey: "resume-1",
  corridorId: "test",
  sender: { id: "s" },
  recipient: { id: "r" },
  sourceAmount: { asset: "USDC", amount: "100.00" },
};

function deps(store: InMemoryIdempotencyStore): EngineDeps {
  return {
    resolver: new StaticRouteResolver(() => createMockAdapter(), {
      trustManifestWithoutAttestation: true,
    }),
    submitter: createMockSubmitter(),
    idempotency: store,
    sleep: async () => {},
    trustManifestWithoutAttestation: true,
    unsafeSkipPreSettleGate: true,
  };
}

describe("crash resume", () => {
  it("resumes a run persisted in 'settled' by reconciling, then completing", async () => {
    const store = new InMemoryIdempotencyStore();
    // Simulate a crash right after the on-chain payment but before reconcile.
    await store.put({
      idempotencyKey: "resume-1",
      corridorId: "test",
      state: "settled",
      version: 5,
      transactionId: "tx_crashed",
      stellarTxHash: "mocktx0001",
    });

    const r = await execute(intent, corridor(), deps(store));
    expect(r.ok).toBe(true);
    if (r.ok) {
      expect(r.value.state).toBe("completed");
      expect(r.value.transactionId).toBe("tx_crashed");
      expect(r.value.trail).toEqual(["settled", "reconciled", "completed"]);
    }
  });

  it("does NOT auto-resume an ambiguous 'settling' run (no double-settle)", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.put({
      idempotencyKey: "resume-1",
      corridorId: "test",
      state: "settling",
      version: 4,
    });
    const r = await execute(intent, corridor(), deps(store));
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.code).toBe("IDEMPOTENCY_CONFLICT");
  });
});

describe("concurrent claim", () => {
  it("two concurrent runs of the same key settle exactly once", async () => {
    const store = new InMemoryIdempotencyStore();
    // Count how many times the submitter actually moves money. A correct gate
    // lets exactly one of the two concurrent runs reach settle().
    let settlements = 0;
    const submitter = createMockSubmitter();
    const counting = {
      ...submitter,
      submit: (req: Parameters<typeof submitter.submit>[0]) => {
        settlements += 1;
        return submitter.submit(req);
      },
    };
    const d: EngineDeps = {
      resolver: new StaticRouteResolver(() => createMockAdapter(), {
        trustManifestWithoutAttestation: true,
      }),
      submitter: counting,
      idempotency: store,
      sleep: async () => {},
      trustManifestWithoutAttestation: true,
      unsafeSkipPreSettleGate: true,
    };

    const [a, b] = await Promise.all([
      execute(intent, corridor(), d),
      execute(intent, corridor(), d),
    ]);

    const outcomes = [a, b];
    expect(outcomes.filter((r) => r.ok)).toHaveLength(1);
    const loser = outcomes.find((r) => !r.ok);
    expect(loser && !loser.ok && loser.error.code).toBe("IDEMPOTENCY_CONFLICT");
    expect(settlements).toBe(1);
  });
});

describe("held run resolution", () => {
  it("lists only held runs with limits and corridor filtering", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.put({ idempotencyKey: "held-a", corridorId: "a", state: "held", version: 4 });
    await store.put({ idempotencyKey: "held-b", corridorId: "b", state: "held", version: 2 });
    await store.put({
      idempotencyKey: "done",
      corridorId: "a",
      state: "completed",
      version: 8,
    });

    expect(await store.listByState("held", { corridorId: "a" })).toMatchObject([
      { idempotencyKey: "held-a", state: "held" },
    ]);
    expect(await store.listByState("held", { limit: 1 })).toHaveLength(1);
  });

  it("records one append-only resolution for a held run only", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.put({ idempotencyKey: "held", corridorId: "a", state: "held", version: 4 });
    await store.put({
      idempotencyKey: "done",
      corridorId: "a",
      state: "completed",
      version: 8,
    });
    const resolution = {
      idempotencyKey: "held",
      outcome: "refunded-offchain" as const,
      note: "Operations confirmed bank refund",
      resolvedBy: "operator-1",
      resolvedAt: 1700000000000,
    };

    expect(await store.recordResolution(resolution)).toBe(true);
    expect(await store.recordResolution(resolution)).toBe(false);
    expect(await store.recordResolution({ ...resolution, idempotencyKey: "done" })).toBe(
      false,
    );
    expect(await store.getResolution("held")).toEqual(resolution);
    expect((await store.get("held"))?.state).toBe("held");
  });
});

describe("PostgresIdempotencyStore.create", () => {
  it("claims a fresh key once and rejects a second claim", async () => {
    const db = fakeDb();
    const store = new PostgresIdempotencyStore(db);
    const run: StoredRun = {
      idempotencyKey: "k",
      corridorId: "c",
      state: "created",
      version: 0,
    };
    expect(await store.create(run)).toBe(true);
    expect(await store.create(run)).toBe(false);
    // The losing claim must not have clobbered the row.
    const got = await store.get("k");
    expect(got?.state).toBe("created");
    expect(got?.version).toBe(0);
  });
});

describe("PostgresIdempotencyStore.list", () => {
  it("returns the owner's runs newest first with a stable cursor", async () => {
    const db = fakeDb();
    const store = new PostgresIdempotencyStore(db);
    await store.put({
      idempotencyKey: "a",
      corridorId: "c",
      state: "held",
      version: 1,
      owner: "owner-1",
    });
    await store.put({
      idempotencyKey: "b",
      corridorId: "c",
      state: "held",
      version: 1,
      owner: "owner-1",
    });
    await store.put({
      idempotencyKey: "x",
      corridorId: "c",
      state: "held",
      version: 1,
      owner: "other",
    });

    const first = await store.list({ owner: "owner-1", state: "held", limit: 1 });
    expect(first.runs.map((r) => r.idempotencyKey)).toEqual(["b"]);
    expect(first.nextCursor).Function();

    const second = await store.list({
      owner: "owner-1",
      state: "held",
      limit: 10,
      cursor: first.nextCursor,
    });
    expect(second.runs.map((r) => r.idempotencyKey)).toEqual(["a"]);
    expect(second.nextCursor).toBeNull();
  });
});

describe("InMemoryIdempotencyStore.list", () => {
  it("scopes to owner and paginates newest first", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.put({
      idempotencyKey: "a",
      corridorId: "c",
      state: "held",
      version: 1,
      owner: "owner-1",
    });
    await store.put({
      idempotencyKey: "b",
      corridorId: "c",
      state: "held",
      version: 1,
      owner: "owner-1",
    });
    await store.put({
      idempotencyKey: "x",
      corridorId: "c",
      state: "held",
      version: 1,
      owner: "other",
    });

    const all = await store.list({ owner: "owner-1", limit: 10 });
    expect(all.runs.map((r) => r.idempotencyKey).sort()).toEqual(["a", "b"]);
    expect(all.nextCursor).toBeNull();

    const page = await store.list({ owner: "owner-1", limit: 1 });
    expect(page.runs).toHaveLength(1);
    expect(page.nextCursor).Function();
    const rest = await store.list({
      owner: "owner-1",
      limit: 10,
      cursor: page.nextCursor ?? undefined,
    });
    expect(rest.runs).toHaveLength(1);
    expect(rest.runs[0]?.idempotencyKey).not.toBe(page.runs[0]?.idempotencyKey);
  });

  it("rejects a malformed cursor", async () => {
    const store = new InMemoryIdempotencyStore();
    await expect(store.list(store.list({ owner: "owner-1", limit: 10, cursor: "not-a-cursor" }))).rejects.toThrow();
  });
});

// A tiny in-memory fake that honours the version-guarded upsert semantics of the
// real SQL, so we can test PostgresIdempotencyStore's mapping + concurrency rule
// without a live database.
function fakeDb(): Queryable & { table: Map<string, Record<string, unknown>> } {
  const table = new Map<string, Record<string, unknown>>();
  return {
    table,
    async query<T = Record<string, unknown>>(
      text: string,
      params: unknown[] = [],
    ): Promise<QueryResult<T>> {
      if (text.trimStart().startsWith("select")) {
        const row = table.get(params[0] as string);
        return { rows: (row ? [row] : []) as T? };
      }
      const key = params[0] as string;
      const incoming = {
        idempotency_key: key,
        corridor_id: params[1],
        state: params[2],
        version: params[3] as number,
        transaction_id: params[4],
        quote_id: params[5],
        quote_expires_at: params[6],
        quote_firm: params[7],
        settlement_amount: params[8],
        stellar_tx_hash: params[9],
        deposit_address: params[10],
        memo: params[11],
        memo_type: params[12],
        refund_id: params[13],
        last_error: params[14],
        owner: params[15],
      };
      // create(): INSERT … ON CONFLICT DO NOTHING RETURNING — only the first
      // writer for a key lands a row and gets it back; a conflict returns [].
      if (text.includes("do nothing")) {
        if (table.has(key)) return { rows: [] as T[] };
        table.set(key, incoming);
        return { rows: [{ idempotency_key: key }] as T[] };
      }
      // put(): upsert with version guard. `refund_id` is coalesced, not
      // overwritten, mirroring the real SQL: once a refund has been requested
      // the stored id must survive a writer that lost it.
      const current = table.get(key);
      if (!current || (current.version as number) < incoming.version) {
        table.set(key, {
          ...incoming,
          refund_id: current?.refund_id ?? incoming.refund_id,
        });
      }
      return { rows: [] as T[] };
    },
  };
}

describe("state round-trips", () => {
  // refund_pending is just a string to both stores — but a run parked in it
  // is a run with money in motion, so "it comes back exactly as written and
  // still reads as non-terminal" is asserted rather than assumed.
  it("InMemoryIdempotencyStore persists and reads back refund_pending", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.put({
      idempotencyKey: "k",
      corridorId: "c",
      state: "refund_pending",
      version: 4,
      stellarTxHash: "hash_rp",
    });
    const got = await store.get("k");
    expect(got?.state).toBe("refund_pending");
    expect(got?.stellarTxHash).toBe("hash_rp");
  });
});
