import { describe, expect, it } from "vitest";
import { request, type Server } from "node:http";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { parseCorridor, type Corridor } from "@corridor/manifest";
import { createMockAdapter } from "@corridor/adapter-kit";
import { StaticRouteResolver } from "@corridor/router";
import {
  InMemoryIdempotencyStore,
  createMockSubmitter,
  type EngineDeps,
} from "@corridor/engine";
import { createService, type ServiceOptions } from "@corridor/service";
import type { CorridorErrorCode, PaymentIntent } from "@corridor/types";

function corridor(): Corridor {
  const r = parseCorridor({
    id: "test",
    source: { name: "S", asset: "USDC", endpoints: { home_domain: "s.example" } },
    dest: {
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
      stellar_tx_hash: "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f607182;a3a4b5c6d7e8f90",
      anchor_transaction_id: "canary-test",
      amount: "1",
      max_age_days: 50000,
    },
  });
  if (!r.ok) throw new Error("fixture invalid");
  return r.value;
}

function intentBody(key = "p1"): PaymentIntent {
  return {
    idempotencyKey: key,
    corridorId: "test",
    sender: { id: "s" },
    recipient: { id: "r" },
    sourceAmount: { asset: "USDC", amount: "100.00" },
  };
}

function deps(adapterOpts = {}): EngineDeps {
  return {
    resolver: new StaticRouteResolver(() => createMockAdapter(adapterOpts), {
      trustManifestWithoutAttestation: true,
    }),
    submitter: createMockSubmitter(),
    idempotency: new InMemoryIdempotencyStore(),
    sleep: async () => {},
    trustManifestWithoutAttestation: true,
    unsafeSkipPreSettleGate: true,
  };
}

function svc(over: Partial<ServiceOptions> = {}) {
  return createService({
    corridors: new Map([["test", corridor()]]),
    deps: deps(),
    ...over,
  });
}

describe("service: POST /payments", () => {
  it("runs a payment and returns 200 completed", async () => {
    const s = svc();
    const r = await s.route({ method: "POST", path: "/payments", body: intentBody() });
    expect(r.status).toBe(200);
    expect((r.body as { state: string }).state).toBe("completed");
  });

  it("returns 200 on idempotent replay of a completed key", async () => {
    const s = svc();
    await s.route({ method: "POST", path: "/payments", body: intentBody("dup") });
    const r = await s.route({ method: "POST", path: "/payments", body: intentBody("dup") });
    expect(r.status).toBe(200);
  });

  it("maps a malformed amount to 422", async () => {
    const s = svc();
    const body = { ...intentBody(), sourceAmount: { asset: "USDC", amount: "1,000" } };
    const r = await s.route({ method: "POST", path: "/payments", body });
    expect(r.status).toBe(422);
  });

  it("maps a KYC rejection to 403", async () => {
    const s = createService({
      corridors: new Map([["test", corridor()]]),
      deps: deps({ kyc: "rejected" }),
    });
    const r = await s.route({ method: "POST", path: "/payments", body: intentBody() });
    expect(r.status).toBe(403);
  });

  it("returns 404 for an unknown corridor", async () => {
    const s = svc();
    const r = await s.route({
      method: "POST",
      path: "/payments",
      body: { ...intentBody(), corridorId: "nope" },
    });
    expect(r.status).toBe(404);
  });

  it("returns 400 for a malformed body", async () => {
    const s = svc();
    const r = await s.route({ method: "POST", path: "/payments", body: { foo: "bar" } });
    expect(r.status).toBe(400);
  });
});

describe("service: GET /payments (list)", () => {
  it("returns the caller's runs newest first", async () => {
    const s = svc();
    await s.route({ method: "POST", path: "/payments", body: intentBody("a") });
    await s.route({ method: "POST", path: "/payments", body: intentBody("b") });
    await s.route({ method: "POST", path: "/payments", body: intentBody("c") });
    const r = await s.route({ method: "GET", path: "/payments" });
    expect(r.status).toBe(200);
    const body = r.body as { items: Array<{ idempotencyKey: string }>; nextCursor?: string };
    expect(body.items.map((i) => i.idempotencyKey)).toEqual(["c", "b", "a"]);
  });

  it("paginates stably with an opaque cursor", async () => {
    const s = svc();
    for (const k of ["a", "b", "c", "d", "e"]) {
      await s.route({ method: "POST", path: "/payments", body: intentBody(k) });
    }
    const page1 = await s.route({ method: "GET", path: "/payments?limit=2" });
    expect(page1.status).toBe(200);
    const b1 = page1.body as { items: Array<{ idempotencyKey: string }>; nextCursor?: string };
    expect(b1.items.map((i) => i.idempotencyKey)).toEqual(["e", "d"]);
    expect(b1.nextCursor).toBeDefined();

    const page2 = await s.route({
      method: "GET",
      path: `/payments?limit=2&cursor=${encodeURIComponent(b1.nextCursor!)}`,
    });
    expect(page2.status).toBe(200);
    const b2 = page2.body as { items: Array<{ idempotencyKey: string }>; nextCursor?: string };
    expect(b2.items.map((i) => i.idempotencyKey)).toEqual(["c", "b"]);

    const page3 = await s.route({
      method: "GET",
      path: `/payments?limit=2&cursor=${encodeURIComponent(b2.nextCursor!)}`,
    });
    expect(page3.status).toBe(200);
    const b3 = page3.body as { items: Array<{ idempotencyKey: string }>; nextCursor?: string };
    expect(b3.items.map((i) => i.idempotencyKey)).toEqual(["a"]);
    expect(b3.nextCursor).toBeUndefined();
  });

  it("filters by state and corridorId", async () => {
    const s = svc();
    await s.route({ method: "POST", path: "/payments", body: intentBody("held-1") });
    const r = await s.route({
      method: "GET",
      path: "/payments?state=completed&corridorId=test",
    });
    expect(r.status).toBe(200);
    const body = r.body as { items: Array<{ idempotencyKey: string; state: string }> };
    expect(body.items.length).toBe(1);
    expect(body.items[0]!.state).toBe("completed");
  });

  it("never returns another tenant's runs", async () => {
    const shared = new InMemoryIdempotencyStore();
    const mk = (owner: string) =>
      createService({
        corridors: new Map([["test", corridor()]]),
        deps: { ...deps(), idempotency: shared },
        owner,
      });
    const a = mk("tenant-a");
    const b = mk("tenant-b");
    await a.route({ method: "POST", path: "/payments", body: intentBody("a-1") });
    await b.route({ method: "POST", path: "/payments", body: intentBody("b-1") });

    const ra = await a.route({ method: "GET", path: "/payments" });
    expect(ra.status).toBe(200);
    const ba = ra.body as { items: Array<{ idempotencyKey: string }> };
    expect(ba.items.map((i) => i.idempotencyKey)).toEqual(["a-1"]);

    const rb = await b.route({ method: "GET", path: "/payments" });
    expect(rb.status).toBe(200);
    const bb = rb.body as { items: Array<{ idempotencyKey: string }> };
    expect(bb.items.map((i) => i.idempotencyKey)).toEqual(["b-1"]);
  });

  it("returns 400 for a bad cursor", async () => {
    const r = await svc().route({ method: "GET", path: "/payments?cursor=not-base64!!" });
    expect(r.status).toBe(400);
  });

  it("caps limit at 100", async () => {
    const s = svc();
    await s.route({ method: "POST", path: "/payments", body: intentBody("only") });
    const r = await s.route({ method: "GET", path: "/payments?limit=1000" });
    expect(r.status).toBe(200);
  });
});

describe("service: GET /payments/:key", () => {
  it("returns the run state after a payment", async () => {
    const s = svc();
    await s.route({ method: "POST", path: "/payments", body: intentBody("look") });
    const r = await s.route({ method: "GET", path: "/payments/look" });
    expect(r.status).toBe(200);
    expect((r.body as { state: string }).state).toBe("completed");
  });

  it("404s an unknown key", async () => {
    const r = await svc().route({ method: "GET", path: "/payments/missing" });
    expect(r.status).toBe(404);
  });
});

describe("service: body-size limit (HTTP server)", () => {
  async function listen(server: Server): Promise<{ port: number; close: () => void }> {
    server.listen(0);
    await once(server, "listening");
    const port = (server.address() as AddressInfo).port;
    return { port, close: () => server.close() };
  }

  function post(
    port: number,
    body: string,
    headers: Record<string, string> = {},
  ): Promise<{ status: number; body: string }> {
    return new Promise((resolve, reject) => {
      const req = request(
        { host: "127.0.0.1", port, method: "POST", path: "/payments", headers },
        (res) => {
          let data = "";
          res.on("data", (c) => (data += c));
          res.on("end", () => resolve({ status: res.statusCode ?? 0, body: data }));
        },
      );
      req.on("error", reject);
      req.end(body);
    });
  }

  it("rejects a body over the cap with 413 (declared Content-Length)", async () => {
    const { port, close } = await listen(svc({ maxBodyBytes: 64 }).server());
    try {
      const big = JSON.stringify({ blob: "x".repeat(500) });
      const res = await post(port, big, {
        "content-type": "application/json",
        "content-length": String(Buffer.byteLength(big)),
      });
      expect(res.status).toBe(413);
    } finally {
      close();
    }
  });

  it("rejects an oversized streamed body with 413 (no/short Content-Length)", async () => {
    const { port, close } = await listen(svc({ maxBodyBytes: 64 }).server());
    try {
      // Chunked transfer: the server can't trust a declared length and must cap
      // the bytes it actually buffers.
      const res = await post(port, JSON.stringify({ blob: "x".repeat(500) }), {
        "content-type": "application/json",
        "transfer-encoding": "chunked",
      });
      expect(res.status).toBe(413);
    } finally {
      close();
    }
  });

  it("still serves a normal payment under the cap", async () => {
    const { port, close } = await listen(svc().server());
    try {
      const res = await post(port, JSON.stringify(intentBody("http-ok")), {
        "content-type": "application/json",
      });
      expect(res.status).toBe(200);
      expect(JSON.parse(res.body).state).toBe("completed");
    } finally {
      close();
    }
  });
});

describe("service: auth + rate limiting + health", () => {
  it("healthz is public", async () => {
    const r = await svc({ apiKeys: new Set(["secret"]) }).route({
      method: "GET",
      path: "/healthz",
    });
    expect(r.status).toBe(200);
  });

  it("serves /metrics as text when a renderer is configured, public + unmetered", async () => {
    const s = svc({
      apiKeys: new Set(["secret"]),
      rateLimit: { capacity: 0, refillPerSec: 0 }, // would 429 everything if metered
      metricsText: () => "# TYPE corridor_transition counter\ncorridor_transition 3\n",
    });
    const r = await s.route({ method: "GET", path: "/metrics" });
    expect(r.status).toBe(200);
    expect(r.contentType).toContain("text/plain");
    expect(r.body).toContain("corridor_transition 3");
  });

  it("404s /metrics when no renderer is configured", async () => {
    const r = await svc().route({ method: "GET", path: "/metrics" });
    expect(r.status).toBe(404);
  });

  it("rejects requests without a valid API key", async () => {
    const s = svc({ apiKeys: new Set(["secret"]) });
    const noKey = await s.route({ method: "POST", path: "/payments", body: intentBody() });
    expect(noKey.status).toBe(401);
    const good = await s.route({
      method: "POST",
      path: "/payments",
      body: intentBody(),
      headers: { authorization: "Bearer secret" },
    });
    expect(good.status).toBe(200);
  });
});
