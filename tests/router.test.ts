import { describe, expect, it } from "vitest";
import {
  RegistryRouteResolver,
  StaticRouteResolver,
  createAdapterRegistry,
} from "@corridor/router";
import { parseCorridor, type Corridor } from "@corridor/manifest";
import type { AnchorAdapter } from "@corridor/adapter-kit";
import type { PaymentIntent } from "@corridor/types";

function corridor(id: string): Corridor {
  const r = parseCorridor({
    id,
    source: { name: "S", asset: "USDC", endpoints: { home_domain: "s.example" } },
    dest: { name: "D", asset: "iso4217:ARS", endpoints: { home_domain: "d.example" } },
    fx: { path: ["ARS", "USDC", "ARS"], who_holds_risk: "receiving_anchor" },
    compliance: { source_jurisdiction: "AR", dest_jurisdiction: "AR" },
    settlement: { network: "public", asset_issuer: "GISSUER" },
    recovery: {},
  });
  if (!r.ok) throw new Error("fixture invalid");
  return r.value;
}

const intent = {
  idempotencyKey: "k",
  corridorId: "test",
  sender: { id: "s" },
} as PaymentIntent;
const adapterStub = { name: "stub" } as AnchorAdapter;

describe("StaticRouteResolver", () => {
  it("throws without explicit opt-in naming RegistryRouteResolver", () => {
    expect(() => new StaticRouteResolver(() => adapterStub)).toThrowError(
      /RegistryRouteResolver/,
    );
    expect(() => new StaticRouteResolver(() => adapterStub, {})).toThrowError(
      /RegistryRouteResolver/,
    );
    expect(
      () =>
        new StaticRouteResolver(() => adapterStub, {
          trustManifestWithoutAttestation: false,
        }),
    ).toThrowError(/RegistryRouteResolver/);
  });

  it("reports trust: 'manifest'", async () => {
    const resolver = new StaticRouteResolver(() => adapterStub, {
      trustManifestWithoutAttestation: true,
    });
    const decision = await resolver.resolve(intent, corridor("c1"));
    expect(decision.trust).toBe("manifest");
  });

  it("registry resolver reports trust: 'attested'", async () => {
    const registry = {
      servesSep31: async () => true,
      staleness: async () => 0,
      tomlHash: async () => "0".repeat(64),
    };
    const resolver = new RegistryRouteResolver({
      registry,
      adapterFor: () => adapterStub,
    });
    const decision = await resolver.resolve(intent, corridor("c1"));
    expect(decision.trust).toBe("attested");
  });

  it("returns exactly the adapter the injected factory produced", async () => {
    const resolver = new StaticRouteResolver(() => adapterStub, {
      trustManifestWithoutAttestation: true,
    });
    const decision = await resolver.resolve(intent, corridor("c1"));
    expect(decision.receiving).toBe(adapterStub);
  });

  it("passes the actual corridor argument through to the factory", async () => {
    const seen: string[] = [];
    const resolver = new StaticRouteResolver(
      (c) => {
        seen.push(c.id);
        return adapterStub;
      },
      { trustManifestWithoutAttestation: true },
    );
    await resolver.resolve(intent, corridor("c1"));
    await resolver.resolve(intent, corridor("c2"));
    expect(seen).toEqual(["c1", "c2"]);
  });

  it("never sets split", async () => {
    const resolver = new StaticRouteResolver(() => adapterStub, {
      trustManifestWithoutAttestation: true,
    });
    const decision = await resolver.resolve(intent, corridor("c1"));
    expect(decision.split).toBeUndefined();
  });

  it("invokes the factory fresh on every call — no memoization", async () => {
    let calls = 0;
    const resolver = new StaticRouteResolver(
      () => {
        calls++;
        return adapterStub;
      },
      { trustManifestWithoutAttestation: true },
    );
    const c = corridor("c1");
    await resolver.resolve(intent, c);
    await resolver.resolve(intent, c);
    expect(calls).toBe(2);
  });
});

function corridorFor(id: string, dest: Record<string, unknown>): Corridor {
  const parsed = parseCorridor({
    id,
    source: { name: "S", asset: "USDC", endpoints: { home_domain: "s.example" } },
    dest: { name: "D", asset: "iso4217:ARS", ...dest },
    fx: { path: ["ARS", "USDC", "ARS"], who_holds_risk: "receiving_anchor" },
    compliance: { source_jurisdiction: "AR", dest_jurisdiction: "AR" },
    settlement: { network: "public", asset_issuer: "GISSUER" },
    recovery: {},
  });
  if (!parsed.ok) throw new Error(`fixture invalid: ${parsed.error.message}`);
  return parsed.value;
}

const sep31Corridor = (id: string) =>
  corridorFor(id, { endpoints: { home_domain: "d.example" } });
const sep6Corridor = (id: string) =>
  corridorFor(id, {
    protocol: "sep6",
    endpoints: { home_domain: "d.example", transfer_server: "https://d.example/sep6" },
  });
const customCorridor = (id: string, name: string) =>
  corridorFor(id, {
    protocol: `custom:${name}`,
    endpoints: { home_domain: "d.example", base_url: "https://d.example/api" },
  });

describe("createAdapterRegistry", () => {
  it("dispatches sep31 corridors (including a legacy manifest with no protocol)", () => {
    const sep31Adapter = { name: "sep31" } as AnchorAdapter;
    const registry = createAdapterRegistry();
    registry.register("sep31", () => sep31Adapter);
    expect(registry.adapterFor(sep31Corridor("c1"))).toBe(sep31Adapter);
  });

  it("dispatches sep6 corridors to the sep6 factory", () => {
    const sep6Adapter = { name: "sep6" } as AnchorAdapter;
    const registry = createAdapterRegistry();
    registry.register("sep31", () => ({ name: "sep31" }) as AnchorAdapter);
    registry.register("sep6", () => sep6Adapter);
    expect(registry.adapterFor(sep6Corridor("c2"))).toBe(sep6Adapter);
  });

  it("dispatches custom protocols to their registered factory", () => {
    const customAdapter = { name: "custom" } as AnchorAdapter;
    const registry = createAdapterRegistry();
    registry.register("custom:my-anchor", () => customAdapter);
    expect(registry.adapterFor(customCorridor("c3", "my-anchor"))).toBe(customAdapter);
  });

  it("passes the corridor to the factory", () => {
    const seen: string[] = [];
    const registry = createAdapterRegistry();
    registry.register("sep31", (c) => {
      seen.push(c.id);
      return { name: "sep31" } as AnchorAdapter;
    });
    registry.adapterFor(sep31Corridor("c4"));
    expect(seen).toEqual(["c4"]);
  });

  it("has() reports whether the corridor's protocol is registered", () => {
    const registry = createAdapterRegistry();
    registry.register("sep31", () => ({ name: "sep31" }) as AnchorAdapter);
    expect(registry.has(sep31Corridor("c5"))).toBe(true);
    expect(registry.has(sep6Corridor("c5"))).toBe(false);
  });

  it("throws a clear startup error naming the protocol and the corridor id", () => {
    const registry = createAdapterRegistry();
    registry.register("sep31", () => ({ name: "sep31" }) as AnchorAdapter);
    expect(() => registry.adapterFor(sep6Corridor("corridor-x"))).toThrowError(
      /protocol "sep6".*corridor "corridor-x"/,
    );
    expect(() => registry.adapterFor(customCorridor("corridor-y", "nope"))).toThrowError(
      /protocol "custom:nope".*corridor "corridor-y"/,
    );
  });

  it("drives a StaticRouteResolver, so an unregistered protocol never builds a sep31 adapter", async () => {
    const registry = createAdapterRegistry();
    registry.register("sep31", () => ({ name: "sep31" }) as AnchorAdapter);
    const resolver = new StaticRouteResolver((c) => registry.adapterFor(c), {
      trustManifestWithoutAttestation: true,
    });
    await expect(resolver.resolve(intent, sep6Corridor("c6"))).rejects.toThrow(/sep6/);
  });
});
