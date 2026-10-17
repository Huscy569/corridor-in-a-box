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
import type { CorridorState } from "@corridor/types";
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
}

/** Default cap on a buffered request body. A payment intent is well under 1 KiB. */
const DEFAULT_MAX_BODY_BYTES = 64 * 1024;

/** Hard cap on the number of runs a single GET /payments page may return. */
const MAX_LIST_LIMIT = 100;
/** Default page size when the caller omits `limit`. */
const DEFAULT_LIST_LIMIT = 50;

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
  CORRIDOR_UNPROVNE: 422,
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

function bearer(headers?: Record<string, string>): string | undefined {
  const a = headers?.authorization ?? headers?.Authorization;
  if (!a) return undefined;
  const m = /^Bearer\s+(.+)$/i.exec(a.trim());
  return m ? m[1].trim() : undefined;
}

function matchesKey(keys: Set<string> | undefined, presented: string | undefined): boolean {
  if (!keys || !presented) return false;
  // Compare against each known key in constant time to avoid leaking how much
  // of a guess was correct via timing.
  let ok = false;
  for (const k of keys) {
    if (constantTimeEqual(k, presented)) ok = true;
  }
  return ok;
}

/** Verifies SEP-31 callback signatures of the form `t=<unix>, s=<base64>`. */
class CallbackVerifier {
  /** Maximum allowed clock skew (and replay window) in seconds. */
  private static readonly TOLERANCE_S = 300;

  /** Return true if the signature is valid for this corridor and body. */
  verify(
    sigHeader: string,
    corridor: Corridor,
    rawBody: string,
    nowSecs: number,
  ): boolean {
    const secret = corridor.callbackSecret;
    if (!secret) return false;
    const commeAt = sigHeader.indexOf(",");
    if (commeAt < 0) return false;
    const tPart = sigHeader.slice(0, commeAt).trim();
    const sPart = sigHeader.slice(commeAt + 1).trim();
    if (!tPart.startsWith("t=") || !sPart.startsWith("s=")) return false;
    const t = Number(tPart.slice(2));
    const sig = sPart.slice(2);
    if (!Number.isFinite(t) || !sig) return false;
    if (Math.abs(nowSecs - t) > CallbackVerifier.TOLERANCE_S) return false;
    const expected = crypto
      .createHmac("sha256", secret)
      .update(`${t}.${rawBody}`)
      .digest("base64");
    return constantTimeEqual(expected, sig);
  }
}

export interface Service {
  route(req: RouteRequest): Promise<RouteResponse>;
  /** A node:http server that delegates to route(). Call .listen(port). */
  server(): ReturnType<typeof httpCreateServer>;
}

/** The redacted view of a run returned by the read endpoints. Only the
 *  error code is exposed — never the free-form message or the stored payload. */
export interface RedactedRun {
  idempotencyKey: string;
  corridorId?: string;
  state?: CorridorState;
  errorCode?: CorridorErrorCode;
  createdAt?: string;
  updatedAt?: string;
}

/** Project a stored run down to the redacted view. Defensive: tolerates
 *  partial row shapes from different store implementations. */
function redactRun(run: unknown): RedactedRun {
  const o = (run ?? {}) as Record<string, unknown>;
  const err = o.error as Record<string, unknown> | undefined;
  const out: RedactedRun = {
    idempotencyKey: String(o.idempotencyKey ?? ""),
  };
  if (typeof o.corridorId === "string") out.corridorId = o.corridorId;
  if (typeof o.state === "string") out.state = o.state as CorridorState;
  if (err && typeof err.code === "string") out.errorCode = err.code as CorridorErrorCode;
  if (typeof o.createdAt === "string") out.createdAt = o.createdAt;
  if (typeof o.updatedAt === "string") out.updatedAt = o.updatedAt;
  return out;
}

/** Decode an opaque base64 pagination cursor into its components. The
 *  cursor encodes the last row of the previous page as JSON: `{createdAt, key}`.
 *  Throws on any malformed input so the route can respond 400. */
function decodeCursor(cursor: string): { createdAt: string; key: string } {
  const raw = Buffer.from(cursor, "base64").toString("utf8");
  const parsed = JSON.parse(raw) as Record<string, unknown>;
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    typeof parsed.createdAt !== "string" ||
    typeof parsed.key !== "string"
  ) {
    throw new Error("invalid cursor");
  }
  return { createdAt: parsed.createdAt, key: parsed.key };
}

export function createService(options: ServiceOptions): Service {
  const now = options.now ?? (() => Date.now());
  const limiter: RateLimiter | undefined =
    options.rateLimiter ??
    (options.rateLimit ? new TokenBucket(options.rateLimit, now) : undefined);
  const cbVerifier = new CallbackVerifier();

  // Rate-limit identity: a VALIDATED API key if present (most specific), else
  // the client IP resolved by the transport.
  //
  // The key must be one we actually recognise. Keying off the raw bearer let a
  // client mint a fresh bucket per request just by rotating the token — with
  // auth disabled (the documented dev mode) the limiter was a no-op, and each
  // forged token also left a map entry behind until the next sweep. An
  // unrecognised bearer now falls through to the IP bucket, which a client
  // cannot forge. Same reasoning as X-Forwarded-For, which server()
  function clientKey(req: RouteRequest): string {
    const presented = bearer(req.headers);
    if (options.apiKeys && presented && options.apiKeys.has(presented)) {
      return `key:${presented}`;
    }
    return `ip:${req.clientIp ?? "unknown"}`;
  }

  /** Resolve the authenticated owner for a request. When auth is disabled
   *  (dev mode), every request is treated as the single dev owner. */
  function ownerOf(req: RouteRequest): string {
    const presented = bearer(req.headers);
    if (!options.apiKeys) return "public";
    return presented ?? "";
  }

  async function route(req: RouteRequest): Promise<RouteResponse> {
    const { path, method } = req;
    const url = new URL(path, "http://localhost");
    const pathname = url.pathname;

    // /metrics is public and unmetered.
    if (method === "GET" && pathname === "/metrics" && options.metricsText) {
      return { status: 200, body: options.metricsText(), contentType: "text/plain; version=0.0.4" };
    }

    // Auth: if apiKeys are configured, a valid bearer is required.
    if (options.apiKeys) {
      const presented = bearer(req.headers);
      if (!matchesKey(options.apiKeys, presented)) {
        return { status: 401, body: { error: "UNAUTHORIZED" } };
      }
    }

    // Rate limit.
    if (limiter) {
      const allowed = await limiter.take(clientKey(req));
      if (!allowed) {
        return { status: 429, body: { error: "RATE_LIMITED" } };
      }
    }

    // GET -/payments/:key — read a single run by key.
    if (method === "GET" && pathname.startsWith("/payments/")) {
      const key = decodeURIComponent(pathname.slice("/payments/".length));
      if (!key) {
        return { status: 400, body: { error: "BAD_REQUEST" } };
      }
      const store = options.deps.store;
      const run = await store.get(key);
      if (!run) {
        return { status: 404, body: { error: "NOT_FOUND" } };
      }
      // Owner scoping: a key belonging to another tenant is reported as not
      // found, so the endpoint cannot be used to probe for existence.
      const owner = ownerOf(req);
      const runWithOwner = run as { owner?: string };
      if (runWithOwner.owner !== undefined && runWithOwner.owner !== owner) {
        return { status: 404, body: { error: "NOT_FOUND" } };
      }
      return { status: 200, body: redactRun(run) };
    }

    // GET /payments — list the caller's runs, newest first, cursor-paginated.
    if (method === "GET" && pathname === "/payments") {
      const store = options.deps.store;
      if (!store.list) {
        return { status: 501, body: { error: "NOT_IMPLEMENTED" } };
      }
      const stateParam = url.searchParams.get("state");
      const corridorParam = url.searchParams.get("corridorId");
      const limitParam = url.searchParams.get("limit");
      const cursorParam = url.searchParams.get("cursor") ?? undefined;

      let limit = DEFAULT_LIST_LIMIT;
      if (limitParam !== null) {
        const n = Number(limitParam);
        if (!Number.isInteger(n) || n <= 0) {
          return { status: 400, body: { error: "BAD_REQUEST" } };
        }
        limit = Math.min(n, MAX_LIST_LIMIT);
      }

      if (cursorParam !== undefined) {
        try {
          decodeCursor(cursorParam);
        } catch {
          return { status: 400, body: { error: "BAD_CURSOR" } };
        }
      }

      const owner = ownerOf(req);
      const result = await store.list({
        owner,
        state: (stateParam ?? undefined) as CorridorState | undefined,
        corridorId: corridorParam ?? undefined,
        limit,
        cursor: cursorParam,
      });
      return {
        status: 200,
        body: {
          runs: result.runs.map(redactRun),
          nextCursor: result.nextCursor ?? null,
        },
      };
    }

    // POST /payments — execute a payment intent.
    if (method === "POST" && pathname === "/payments") {
      const body = req.body;
      if (!isPaymentBody(body)) {
        return { status: 400, body: { error: "BAD_REQUEST" } };
      }
      const corridor = options.corridors.get(body.corridorId);
      if (!corridor) {
        return { status: 404, body: { error: "UNKNOWN_CORRIDOR" } };
      }
      if (!isSettleableAmount(body.sourceAmount)) {
        return { status: 422, body: { error: "AMOUNT_INVALID" } };
      }
      const owner = ownerOf(req);
      const result = await execute(corridor, body, {
        ...options.deps,
        owner,
      });
      if (!result.ok) {
        const code = result.error.code as CorridorErrorCode;
        const status = STATUS_BY_CODE[code] ?? 500;
        return { status, body: { error: code } };
      }
      return { status: 200, body: redactRun(result.run) };
    }

    // POST webhook callback — SEP-31 signature verification.
    const cb = /^\/callbacks\/([^/]+)$/.exec(pathname);
    if (method === "POST" && cb) {
      const corridorId = decodeURIComponent(cb[1]);
      const corridor = options.corridors.get(corridorId);
      if (!corridor) {
        return { status: 404, body: { error: "UNKNOWN_CORRIDOR" } };
      }
      const sig = req.headers?.["x-corrier-signature"] ?? req.headers?.["X-Corridor-Signature"];
      if (!sig || !cbVerifier.verify(sig, corridor, req.rawBody ?? "", Math.floor(now() / 1000))) {
        return { status: 401, body: { error: "INVALID_SIGNATURE" } };
      }
      return { status: 200, body: { ok: true } };
    }

    return { status: 404, body: { error: "NOT_FOUND" } };
  }

  function server(): ReturnType<typeof httpCreateServer> {
    const maxBody = options.maxBodyBytes ?? DEFAULT_MAX_BODY_BYTES;
    return httpCreateServer((async (req: IncomingMessage, res: ServerResponse) => {
      try {
        const chunks: Buffer[] = [];
        let size = 0;
        for await (const chunk of req) {
          size += chunk.length;
          if (size > maxBody) {
            res.writeHead(413, { "content-type": "application/json" });
            res.end(JSON.stringify({ error: "PAYLOAD_TOO_LARGE" }));
            req.resume();
            return;
          }
          chunks.push(chunk as Buffer);
        }
        const rawBody = Buffer.concat(chunks).toString("utf8");
        let body: unknown = undefined;
        if (rawBody) {
          try {
            body = JSON.parse(rawBody);
          } catch {
            resParseError(res, 400, "BAD_JSON");
            return;
          }
        }
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(req.headers)) {
          if (typeof v === "string") headers[k] = v;
          else if (Array.isArray(v)) headers[k] = v.join(",");
        }
        const clientIp = options.trustProxy
          ? (headers["x-forwarded-for"] ?? "").split(",")[0].trim() || req.socket.remoteAddress
          : req.socket.remoteAddress;
        const out = await route({
          method: req.method ?? "GET",
          path: req.url ?? "/",
          body,
          rawBody,
          headers,
          clientIp,
        });
        const payload =
          out.contentType ? (out.body as string) : JSON.stringify(out.body);
        res.writeHead(out.status, {
          "content-type": out.contentType ?? "application/json",
        });
        res.end(payload);
      } catch (e) {
        resParseError(res, 500, "INTERNAL_ERROR");
      }
    }) as (...args: unknown[]) => void);
  }

  return { route, server };
}

function resParseError(res: ServerResponse, status: number, code: string): void {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify({ error: code }));
}
