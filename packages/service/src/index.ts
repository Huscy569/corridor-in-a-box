// @corridor/service — a thin HTTP layer over the engine. The engine is a
// library; this turns it into a service with the things a real endpoint needs:
// JSON in/out, sensible HTTP status mapping, optional API-key auth, and a simple
// in-memory rate limiter. Zero runtime dependencies — Node's built-in http.
//
// The core is a pure `route()` you can unit-test without binding a socket;
// `server()` is a thin node:http adapter around it.

import {
  createServer as httpCreateServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import * as crypto from "node:crypto";
import type { Corridor } from "@corridor/manifest";
import { execute, type EngineDeps } from "@corridor/engine";
import {
  constantTimeEqual,
  isSettleableAmount,
  type CorridorErrorCode,
  type PaymentIntent,
} from "@corridor/types";

export interface RateLimitOptions {
  /** Bucket size (max burst). */
  capacity: number;
  /** Tokens refilled per second. */
  refillPerSec: number;
}

/**
 * Pluggable rate limiter. The default `TokenBucket` is per-process — fine for a
 * single replica. For multiple replicas, inject a shared implementation (e.g. a
 * Redis token bucket via a Lua script) so the limit is enforced across the
 * fleet rather than per-instance. `take()` may be sync or async (Redis is
 * async). See docs/operations.md §5.
 */
export interface RateLimiter {
  /** Consume one unit for `key`. Return false to reject (429). */
  take(key: string): boolean | Promise<boolean>;
}

/**
 * A single named readiness check. `check()` must resolve when the dependency is
 * healthy and throw (or reject) when it is not. Throwing with an `Error` is
 * preferred so the log entry carries a message; the body returned to callers
 * never includes the error text.
 */
export interface ReadinessCheck {
  name: string;
  check: () => Promise<void>;
}

export interface ServiceOptions {
  /** Corridor manifests, keyed by corridor id. */
  corridors: Map<string, Corridor>;
  /** Engine dependencies (resolver, submitter, store, …). */
  deps: EngineDeps;
  /** If set, requests must carry `Authorization: Bearer <key>` with a known key. */
  apiKeys?: Set<string>;
  /** If set, requests are rate-limited per client (API key, else resolved client IP)
   *  using the default in-process TokenBucket. Ignored when `rateLimiter` is given. */
  rateLimit?: RateLimitOptions;
  /** Inject a custom (e.g. shared/Redis) limiter. Takes precedence over `rateLimit`. */
  rateLimiter?: RateLimiter;
  /**
   * If set, `GET /metrics` serves Prometheus text exposition (public + unmetered).
   * Pass `() => prometheusMetrics.render()` and hand the SAME `PrometheusMetrics`
   * to the engine as `deps.metrics` so scrapes see live counters/timings.
   */
  metricsText?: () => string;
  /** Max request body size in bytes. Larger bodies are rejected with 413. Default 64 KiB. */
  maxBodyBytes?: number;
  /**
   * Trust `X-Forwarded-For` for the client IP. Default `false`: the header is
   * client-controlled, so trusting it lets an attacker forge a fresh identity
   * per request and slip the rate limiter. Enable ONLY when a trusted reverse
   * proxy (your ingress) sets the header and strips any inbound value. When
   * false, the transport uses the socket's peer address instead.
   */
  trustProxy?: boolean;
  now?: () => number;
  /**
   * Optional list of readiness checks. `GET /readyz` runs each with a 5-second
   * timeout and returns 200 when all pass, 503 when any fails. The response body
   * lists per-check pass/fail status but never exposes internal error messages.
   * While the service is draining (after `gracefulShutdown()` is called) `/readyz`
   * always returns 503 so load balancers stop routing new traffic immediately.
   */
  readinessChecks?: ReadinessCheck[];
  /**
   * Timeout in milliseconds for each individual readiness check. Default 5000 ms.
   */
  readinessTimeoutMs?: number;
}

/** Default cap on a buffered request body. A payment intent is well under 1 KiB. */
const DEFAULT_MAX_BODY_BYTES = 64 * 1024;

/** Default per-check timeout for /readyz. */
const DEFAULT_READINESS_TIMEOUT_MS = 5_000;

/**
 * Build a `ReadinessCheck` for a `PostgresIdempotencyStore` (or any object with
 * a `ping(): Promise<void>` method). Wire this into `ServiceOptions.readinessChecks`
 * so `/readyz` reports store reachability before the load balancer routes traffic:
 *
 *   const store = new PostgresIdempotencyStore(pool);
 *   createService({ …, readinessChecks: [storeCheck(store)] });
 */
export function storeCheck(store: { ping(): Promise<void> }, name = "store"): ReadinessCheck {
  return { name, check: () => store.ping() };
}

export interface RouteResponse {
  status: number;
  body: unknown;
  /** Defaults to application/json (body is JSON-encoded). Set e.g. text/plain to
   *  send a string body verbatim (used by /metrics). */
  contentType?: string;
}

export interface RouteRequest {
  method: string;
  path: string;
  body?: unknown;
  rawBody?: string;
  headers?: Record<string, string>;
  /** Resolved client IP from the transport (see `trustProxy`). Used for rate-limit keying. */
  clientIp?: string;
}

const STATUS_BY_CODE: Record<CorridorErrorCode, number> = {
  MANIFEST_INVALID: 422,
  MANIFEST_CHANGED: 409,
  CAPABILITY_MISMATCH: 422,
  AMOUNT_INVALID: 422,
  QUOTE_UNAVAILABLE: 502,
  QUOTE_EXPIRED: 409,
  KYC_REQUIRED: 409,
  KYC_REJECTED: 403,
  ANCHOR_UNAVAILABLE: 502,
  SETTLEMENT_FAILED: 500,
  SETTLEMENT_TIMEOUT: 504,
  REFUND_UNSUPPORTED: 501,
  RECONCILE_MISMATCH: 500,
  RECONCILE_STALLED: 504,
  IDEMPOTENCY_CONFLICT: 409,
  PRESETTLE_ANCHOR_DRIFT: 502,
  PRESETTLE_TX_MISMATCH: 409,
  PRESETTLE_DESTINATION_UNSAFE: 422,
  PRESETTLE_INSUFFICIENT_FUNDS: 422,
  PRESETTLE_QUOTE_WINDOW: 409,
  PRESETTLE_AMOUNT_OUT_OF_RANGE: 422,
  PRESETTLE_RECEIVER_NOT_ACCEPTED: 403,
  CORRIDOR_UNPROVEN: 422,
  // The lane halted itself after repeated failures. 503 rather than 409: the
  // request was well-formed and the same one would be accepted again once the
  // lane is reset, so this is a dependency being unavailable, not a conflict
  // the caller can resolve.
  CORRIDOR_HALTED: 503,
  ENGINE_MISCONFIGURED: 500,
};

/** Token-bucket rate limiter, keyed per client. In-memory; swap for Redis at scale. */
export class TokenBucket implements RateLimiter {
  private readonly state = new Map<string, { tokens: number; last: number }>();
  /**
   * How long an idle bucket takes to refill back to full capacity. Once a bucket
   * has been idle that long it's indistinguishable from a brand-new one, so it
   * can be dropped — this is what bounds memory. `Infinity` when refill is
   * disabled (a drained bucket never recovers, so we must never evict it).
   */
  private readonly fullRefillMs: number;
  private lastSweep = 0;

  constructor(
    private readonly opts: RateLimitOptions,
    private readonly now: () => number,
  ) {
    this.fullRefillMs =
      opts.refillPerSec > 0 ? (opts.capacity / opts.refillPerSec) * 1000 : Infinity;
  }

  take(key: string): boolean {
    const t = this.now();
    // Without eviction the map grows once per distinct client (API key, or a
    // spoofable x-forwarded-for) and never shrinks — an unbounded-memory vector.
    // Sweep at most once per refill window so the cost stays amortized O(1).
    this.evictFullyRefilled(t);
    const s = this.state.get(key) ?? { tokens: this.opts.capacity, last: t };
    s.tokens = Math.min(
      this.opts.capacity,
      s.tokens + ((t - s.last) / 1000) * this.opts.refillPerSec,
    );
    s.last = t;
    const allowed = s.tokens >= 1;
    if (allowed) s.tokens -= 1;
    this.state.set(key, s);
    return allowed;
  }

  /** Drop buckets idle long enough to have fully refilled (behaviour-neutral:
   *  a full bucket and a fresh one are identical). Throttled to one pass per
   *  refill window to keep the hot path cheap. */
  private evictFullyRefilled(t: number): void {
    if (!Number.isFinite(this.fullRefillMs)) return;
    if (t - this.lastSweep < this.fullRefillMs) return;
    this.lastSweep = t;
    for (const [k, s] of this.state) {
      if (t - s.last >= this.fullRefillMs) this.state.delete(k);
    }
  }
}

function isPaymentBody(b: unknown): b is PaymentIntent {
  if (typeof b !== "object" || b === null) return false;
  const o = b as Record<string, unknown>;
  const amount = o.sourceAmount as Record<string, unknown> | undefined;
  return (
    typeof o.idempotencyKey === "string" &&
    typeof o.corridorId === "string" &&
    typeof (o.sender as Record<string, unknown>)?.id === "string" &&
    typeof (o.recipient as Record<string, unknown>)?.id === "string" &&
    typeof amount?.asset === "string" &&
    typeof amount?.amount === "string"
  );
}

export interface Service {
  route(req: RouteRequest): Promise<RouteResponse>;
  /** A node:http server that delegates to route(). Call .listen(port). */
  server(): ReturnType<typeof httpCreateServer>;
  /**
   * Signal the service to start draining. After this call:
   * - `GET /readyz` returns 503 immediately (load balancers stop routing).
   * - New `POST /payments` return 503 `{error:"draining"}`.
   * - In-flight `execute()` calls are allowed to finish.
   * Returns a Promise that resolves once all in-flight executions have settled.
   */
  drain(): Promise<void>;
  /** Idempotency keys of the `execute()` calls still running right now. */
  inFlightKeys(): string[];
}

export function createService(options: ServiceOptions): Service {
  const now = options.now ?? (() => Date.now());
  const limiter: RateLimiter | undefined =
    options.rateLimiter ??
    (options.rateLimit ? new TokenBucket(options.rateLimit, now) : undefined);
  const cbVerifier = new CallbackVerifier();
  const readinessTimeoutMs = options.readinessTimeoutMs ?? DEFAULT_READINESS_TIMEOUT_MS;

  // --- draining state --------------------------------------------------
  // Set true by drain(). Once set it is never cleared: a draining instance
  // keeps its load-balancer weight at zero and rejects new payments until the
  // process exits.
  let draining = false;

  // Track every in-flight execute() promise so drain() can wait for them all.
  // Keys are idempotency keys (for logging on grace-timer fire).
  const inFlight = new Map<string, Promise<unknown>>();

  function trackExecution(idempotencyKey: string, p: Promise<unknown>): void {
    inFlight.set(idempotencyKey, p);
    void p.finally(() => inFlight.delete(idempotencyKey));
  }

  function drain(): Promise<void> {
    draining = true;
    if (inFlight.size === 0) return Promise.resolve();
    return Promise.all([...inFlight.values()]).then(() => undefined);
  }

  // Rate-limit identity: a VALIDATED API key if present (most specific), else
  // the client IP resolved by the transport.
  //
  // The key must be one we actually recognise. Keying off the raw bearer let a
  // client mint a fresh bucket per request just by rotating the token — with
  // auth disabled (the documented dev mode) the limiter was a no-op, and each
  // forged token also left a map entry behind until the next sweep. An
  // unrecognised bearer now falls through to the IP bucket, which a client
  // cannot forge. Same reasoning as X-Forwarded-For, which server() only honours
  // under `trustProxy`.
  const clientKey = (req: RouteRequest): string => {
    const key = bearer(req.headers);
    if (key && matchesKey(options.apiKeys, key)) return `key:${key}`;
    return `ip:${req.clientIp ?? "anon"}`;
  };

  async function route(req: RouteRequest): Promise<RouteResponse> {
    const headers = req.headers ?? {};
    const path = req.path.replace(/\/+$/, "") || "/";

    // Health is public and unmetered. Always 200 — liveness only.
    if (req.method === "GET" && path === "/healthz") {
      return { status: 200, body: { status: "ok" } };
    }

    // Readiness is public and unmetered. Returns 503 while draining or when any
    // check fails, 200 when all checks pass. The body lists per-check results but
    // never surfaces internal error messages — no anchor URLs, no stack traces.
    if (req.method === "GET" && path === "/readyz") {
      if (draining) {
        return { status: 503, body: { status: "draining", checks: [] } };
      }
      const checks = options.readinessChecks ?? [];
      const results = await Promise.all(
        checks.map(async ({ name, check }) => {
          try {
            await Promise.race([
              check(),
              new Promise<never>((_, reject) =>
                setTimeout(() => reject(new Error("timeout")), readinessTimeoutMs),
              ),
            ]);
            return { name, ok: true };
          } catch {
            return { name, ok: false };
          }
        }),
      );
      const allOk = results.every((r) => r.ok);
      return {
        status: allOk ? 200 : 503,
        body: { status: allOk ? "ok" : "degraded", checks: results },
      };
    }

    // Metrics are public and unmetered (scrapers don't carry an API key). Only
    // exposed when a renderer is configured.
    if (req.method === "GET" && path === "/metrics" && options.metricsText) {
      return {
        status: 200,
        body: options.metricsText(),
        contentType: "text/plain; version=0.0.4",
      };
    }

    // --- rate limit ---
    // BEFORE auth, deliberately. Returning 401 first meant failed credentials
    // were never metered, so API keys could be brute-forced at line rate — a
    // wrong key cost the attacker nothing. An unrecognised bearer keys to the
    // caller's IP bucket (see clientKey), so guessing is now rate-limited per
    // client while legitimate keyed traffic keeps its own bucket.
    if (limiter && !(await limiter.take(clientKey(req)))) {
      return { status: 429, body: { error: "rate_limited" } };
    }

    // --- auth ---
    const presentedKey = bearer(headers);
    if (options.apiKeys) {
      if (!matchesKey(options.apiKeys, presentedKey)) {
        return { status: 401, body: { error: "unauthorized" } };
      }
    }

    // --- POST /payments ---
    if (req.method === "POST" && path === "/payments") {
      // Refuse new payments while draining. In-flight ones that already started
      // will run to completion; only new arrivals are blocked.
      if (draining) {
        return { status: 503, body: { error: "draining" } };
      }
      if (!isPaymentBody(req.body)) {
        return { status: 400, body: { error: "invalid payment body" } };
      }
      const intent = req.body;
      // isSettleableAmount, not isValidAmount: the latter is a signed syntax
      // check (subAmounts needs negatives) and let "-100.00" through to the
      // engine, which settled it.
      if (!isSettleableAmount(intent.sourceAmount.amount)) {
        return {
          status: 422,
          body: { error: "AMOUNT_INVALID", message: "amount must be a positive decimal" },
        };
      }
      const corridor = options.corridors.get(intent.corridorId);
      if (!corridor) {
        return {
          status: 404,
          body: { error: "unknown corridor", corridorId: intent.corridorId },
        };
      }
      // Owner comes from the VALIDATED bearer, never from the body — a client
      // must not be able to claim another tenant's runs by asking.
      const p = execute(intent, corridor, options.deps, { owner: presentedKey });
      trackExecution(intent.idempotencyKey, p);
      const result = await p;
      if (result.ok) {
        return { status: 200, body: result.value };
      }
      return {
        status: STATUS_BY_CODE[result.error.code] ?? 500,
        body: { error: result.error.code, message: result.error.message },
      };
    }

    // --- POST /callbacks/sep31/:corridorId ---
    const cbMatch = path.match(/^\/callbacks\/sep31\/([^/]+)$/);
    if (req.method === "POST" && cbMatch) {
      const corridorId = decodeURIComponent(cbMatch[1]);
      const corridor = options.corridors.get(corridorId);
      if (!corridor) {
        return { status: 404, body: { error: "unknown corridor" } };
      }
      const sigHeader = headers["signature"] || headers["x-stellar-signature"];
      if (!sigHeader) {
        return { status: 401, body: { error: "missing signature header" } };
      }
      // Linear-time parse (no backtracking regex): "t=<unix>, s=<base64>".
      const commaAt = sigHeader.indexOf(",");
      const tPart = commaAt < 0 ? "" : sigHeader.slice(0, commaAt).trim();
      const sPart = commaAt < 0 ? "" : sigHeader.slice(commaAt + 1).trim();
      if (!tPart.startsWith("t=") || !sPart.startsWith("s=") || sigHeader.length > 2048) {
        return { status: 401, body: { error: "invalid signature format" } };
      }
      const tStr = tPart.slice(2);
      const s = sPart.slice(2);
      if (!/^\d{1,15}$/.test(tStr) || s.length === 0) {
        return { status: 401, body: { error: "invalid signature format" } };
      }
      const t = parseInt(tStr, 10);
      const currentT = Math.floor(now() / 1000);
      if (isNaN(t) || Math.abs(currentT - t) > 120) {
        return { status: 401, body: { error: "signature expired or invalid timestamp" } };
      }
      const payload = Buffer.from(tStr + "." + (req.rawBody ?? ""));
      const valid = await cbVerifier.verify(corridor.dest.endpoints.home_domain, payload, s);
      if (!valid) {
        return { status: 401, body: { error: "invalid signature" } };
      }
      const body = req.body as { transaction?: { id?: string } };
      const txId = body?.transaction?.id;
      if (txId && options.deps.waker) {
        options.deps.waker.wake(txId);
      }
      return { status: 200, body: {} };
    }

    // --- GET /payments/:key ---
    const m = path.match(/^\/payments\/([^/]+)$/);
    if (req.method === "GET" && m) {
      const store = options.deps.idempotency;
      // A caller-chosen idempotency key is often guessable ("demo-0001"), so the
      // key alone cannot be the authorization. Scope the read to the tenant that
      // created the run.
      let key: string;
      try {
        key = decodeURIComponent(m[1]);
      } catch {
        // Malformed percent-encoding — nothing can match it.
        return { status: 404, body: { error: "not found" } };
      }
      const run = store ? await store.get(key) : undefined;
      // 404 rather than 403 on an ownership mismatch: a 403 would confirm the
      // key exists, which is itself the enumeration oracle we are closing.
      if (!run || (options.apiKeys && !ownerMatches(run.owner, presentedKey))) {
        return { status: 404, body: { error: "not found" } };
      }
      return {
        status: 200,
        body: {
          idempotencyKey: run.idempotencyKey,
          corridorId: run.corridorId,
          state: run.state,
          transactionId: run.transactionId,
          stellarTxHash: run.stellarTxHash,
          // Only the error CODE, never the stored message: lastError is
          // `${code}: ${message}` and the message carries anchor URLs, upstream
          // HTTP bodies and other internals the caller has no business seeing.
          error: errorCode(run.lastError),
        },
      };
    }

    return { status: 404, body: { error: "not found" } };
  }

  const maxBodyBytes = options.maxBodyBytes ?? DEFAULT_MAX_BODY_BYTES;

  function server() {
    return httpCreateServer((incoming: IncomingMessage, res: ServerResponse) => {
      const sendJson = (status: number, body: unknown) => {
        res.writeHead(status, { "content-type": "application/json" });
        res.end(JSON.stringify(body));
      };

      // Reject oversized bodies up front when the client declares Content-Length,
      // before reading a single byte.
      const declared = Number(incoming.headers["content-length"]);
      if (Number.isFinite(declared) && declared > maxBodyBytes) {
        sendJson(413, { error: "payload too large" });
        incoming.destroy();
        return;
      }

      const chunks: Buffer[] = [];
      let size = 0;
      let aborted = false;
      incoming.on("data", (c: Buffer) => {
        if (aborted) return;
        size += c.length;
        // Guard against a lying/absent Content-Length: cap the actual bytes read
        // so a stream can't exhaust memory.
        if (size > maxBodyBytes) {
          aborted = true;
          sendJson(413, { error: "payload too large" });
          incoming.destroy();
          return;
        }
        chunks.push(c);
      });
      incoming.on("end", async () => {
        if (aborted) return;
        let body: unknown;
        const raw = Buffer.concat(chunks).toString("utf8");
        if (raw) {
          try {
            body = JSON.parse(raw);
          } catch {
            res.writeHead(400, { "content-type": "application/json" });
            res.end(JSON.stringify({ error: "invalid JSON" }));
            return;
          }
        }
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(incoming.headers)) {
          if (typeof v === "string") headers[k.toLowerCase()] = v;
        }
        const url = new URL(incoming.url ?? "/", "http://localhost");
        try {
          const out = await route({
            method: incoming.method ?? "GET",
            path: url.pathname,
            body,
            rawBody: raw,
            headers,
            clientIp: resolveClientIp(incoming, headers, options.trustProxy ?? false),
          });
          if (out.contentType) {
            res.writeHead(out.status, { "content-type": out.contentType });
            res.end(typeof out.body === "string" ? out.body : JSON.stringify(out.body));
          } else {
            res.writeHead(out.status, { "content-type": "application/json" });
            res.end(JSON.stringify(out.body));
          }
        } catch {
          // An unexpected throw (e.g. the idempotency DB is unreachable) must not
          // hang the socket or crash the process. A throw here means no payment
          // was committed past its last persisted state, so the run is safe to
          // retry; surface a 500 and let the caller back off. Settlement-time
          // store failures degrade to the same `settling`/IDEMPOTENCY_CONFLICT
          // path as a crash — never a silent double-settle.
          if (!res.headersSent) {
            res.writeHead(500, { "content-type": "application/json" });
            res.end(JSON.stringify({ error: "internal", message: "unexpected error" }));
          }
        }
      });
    });
  }

  return { route, server, drain, inFlightKeys: () => [...inFlight.keys()] };
}

/** Extract a bearer token from an Authorization header, if present. */
function bearer(headers: Record<string, string> | undefined): string | undefined {
  const auth = headers?.["authorization"];
  return auth?.startsWith("Bearer ") ? auth.slice(7) : undefined;
}

/** `Set.has()` short-circuits on hash-bucket lookup, which leaks timing
 *  about the presented key. Iterate and compare every candidate in constant
 *  time instead — the set of configured API keys is small (operator-sized,
 *  not user-sized), so this stays cheap. */
function matchesKey(keys: Set<string> | undefined, presented: string | undefined): boolean {
  if (!keys || !presented) return false;
  let matched = false;
  for (const key of keys) {
    if (constantTimeEqual(key, presented)) matched = true;
  }
  return matched;
}

/** Same constant-time treatment for the ownership check on GET /payments/:key. */
function ownerMatches(owner: string | undefined, presented: string | undefined): boolean {
  if (owner === undefined || presented === undefined) return false;
  return constantTimeEqual(owner, presented);
}

/** `lastError` is stored as "CODE: message". Surface only the code — the message
 *  carries anchor URLs and upstream response text that must not leave the box. */
function errorCode(lastError: string | undefined): string | undefined {
  if (!lastError) return undefined;
  const idx = lastError.indexOf(":");
  return idx > 0 ? lastError.slice(0, idx) : lastError;
}

/** First hop in an X-Forwarded-For list (the original client). */
function firstForwarded(xff: string | undefined): string | undefined {
  const first = xff?.split(",")[0]?.trim();
  return first || undefined;
}

/**
 * Resolve the client IP for rate-limit keying. With `trustProxy`, prefer the
 * left-most X-Forwarded-For entry set by a trusted ingress; otherwise use the
 * socket peer address, which a client cannot forge.
 */
function resolveClientIp(
  incoming: IncomingMessage,
  headers: Record<string, string>,
  trustProxy: boolean,
): string | undefined {
  if (trustProxy) {
    const fwd = firstForwarded(headers["x-forwarded-for"]);
    if (fwd) return fwd;
  }
  return incoming.socket?.remoteAddress ?? undefined;
}

/**
 * Stop accepting new connections and drain in-flight requests, then force-close
 * any that outlast the grace period. Wire this to SIGTERM/SIGINT in your entry
 * point so a deploy/rollout doesn't sever an in-flight payment mid-settle:
 *
 *   const svc = createService(opts);
 *   const srv = svc.server();
 *   srv.listen(8080);
 *   for (const sig of ["SIGTERM", "SIGINT"] as const) {
 *     process.on(sig, () => void gracefulShutdown(srv, svc).then(() => process.exit(0)));
 *   }
 *
 * Pass `svc` as the second argument so the shutdown sequence:
 *   1. Signals the service to drain — `/readyz` immediately returns 503 and new
 *      POST /payments are rejected, stopping the load balancer from routing new
 *      traffic while existing payments finish.
 *   2. Waits for all in-flight `execute()` calls to settle.
 *   3. Stops the HTTP server from accepting new connections and closes idle sockets.
 *   4. Force-closes any remaining connections once the grace window expires,
 *      logging the idempotency keys still running so operators can investigate.
 */
export function gracefulShutdown(
  srv: ReturnType<typeof httpCreateServer>,
  svcOrOpts?: Service | { graceMs?: number },
  legacyOpts: { graceMs?: number } = {},
): Promise<void> {
  // Support both old signature gracefulShutdown(srv, opts) and new
  // gracefulShutdown(srv, svc, opts) so existing callers keep working.
  let svc: Service | undefined;
  let opts: { graceMs?: number };
  if (svcOrOpts && "drain" in svcOrOpts) {
    svc = svcOrOpts as Service;
    opts = legacyOpts;
  } else {
    svc = undefined;
    opts = (svcOrOpts as { graceMs?: number } | undefined) ?? {};
  }

  const graceMs = opts.graceMs ?? 10_000;

  return new Promise<void>((resolve) => {
    let settled = false;
    const done = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve();
    };

    // 1. Tell the service to drain — /readyz goes 503 and new payments are
    //    blocked immediately, before we wait for anything.
    if (svc) void svc.drain();

    // 2. Stop accepting new HTTP connections; wait for existing ones to close.
    srv.close(() => done());
    // Free idle keep-alive sockets so srv.close() can complete promptly.
    srv.closeIdleConnections?.();

    // 3. Backstop: after the grace window, force-close everything still open.
    const timer = setTimeout(() => {
      // Log the keys still running so operators know what was mid-settle when we cut.
      if (svc) {
        const keys = svc.inFlightKeys();
        process.stderr.write(
          `[corridor/service] graceful-shutdown grace period (${graceMs}ms) expired, ` +
            `force-closing connections. In-flight idempotency keys: ` +
            `${keys.length > 0 ? keys.join(", ") : "none"}\n`,
        );
      }
      srv.closeAllConnections?.();
      done();
    }, graceMs);
    timer.unref?.();
  });
}

function decodeBase32(str: string): Buffer {
  const ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  const buf = Buffer.alloc(Math.ceil((str.length * 5) / 8));
  let bits = 0,
    val = 0,
    idx = 0;
  for (let i = 0; i < str.length; i++) {
    if (str[i] === "=") break;
    val = (val << 5) | ALPHABET.indexOf(str[i]);
    bits += 5;
    if (bits >= 8) {
      buf[idx++] = (val >>> (bits - 8)) & 255;
      bits -= 8;
    }
  }
  return buf.subarray(0, idx);
}

export class CallbackVerifier {
  private readonly keys = new Map<string, { key: crypto.KeyObject; expires: number }>();

  async verify(
    homeDomain: string,
    payload: Buffer,
    signatureBase64: string,
  ): Promise<boolean> {
    const now = Date.now();
    let cached = this.keys.get(homeDomain);
    if (!cached || cached.expires < now) {
      try {
        const res = await fetch(`https://${homeDomain}/.well-known/stellar.toml`);
        if (!res.ok) return false;
        const text = await res.text();
        const match = text.match(/^SIGNING_KEY\s*=\s*"([^"]+)"/m);
        if (!match) return false;
        const rawPub = decodeBase32(match[1]).subarray(1, 33);
        const der = Buffer.concat([Buffer.from("302a300506032b6570032100", "hex"), rawPub]);
        const key = crypto.createPublicKey({ key: der, format: "der", type: "spki" });
        cached = { key, expires: now + 5 * 60_000 };
        this.keys.set(homeDomain, cached);
      } catch {
        return false;
      }
    }
    try {
      return crypto.verify(null, payload, cached.key, Buffer.from(signatureBase64, "base64"));
    } catch {
      return false;
    }
  }
}
