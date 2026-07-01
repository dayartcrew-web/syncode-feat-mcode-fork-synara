// FILE: syncode-vendor-augmentations.d.ts
// Purpose: Ambient module + global declarations needed to make the vendored
//          MCode UI compile against the STABLE npm `effect` (3.x) and standard
//          browser libs.
// Layer: Web type declarations (vendor-bridge)
//
// MCode's production `apps/web` imports two experimental subpaths from its
// custom `effect` fork (pkg.pr.new/Effect-TS/effect-smol/effect@8881a9b):
//   - `effect/unstable/rpc`            (RpcClient, RpcSerialization)
//   - `effect/unstable/socket/Socket`  (Socket namespace)
// Stable `effect@3.x` does NOT export these (they live in `@effect/rpc` and
// `@effect/platform` upstream). Rather than pull those packages now (T5 strips
// effect entirely), we declare ambient modules so the imports RESOLVE —
// converting module-resolution errors into ordinary type errors (the same
// hole-driving signal the contracts shim produces). See
// docs/CONTRACTS-BRIDGE-DESIGN.md §3.1.
//
// TODO T5: remove these declarations when `effect` is stripped from the
// frontend (the wsTransport will be rewritten to a plain WebSocket client).

// `effect/unstable/rpc` — stubbed. Real types live in @effect/rpc upstream.
declare module "effect/unstable/rpc" {
  // RpcClient.Protocol / RpcSerialization are referenced as types in
  // wsTransport.ts; stubbing as unknown keeps resolution green.
  export const RpcClient: unknown;
  export const RpcSerialization: unknown;
  export namespace RpcClient {
    export type Protocol = unknown;
  }
  export namespace RpcSerialization {
    export type Options = unknown;
  }
}

// `effect/unstable/socket/Socket` — stubbed namespace.
declare module "effect/unstable/socket/Socket" {
  export type Socket = unknown;
  export const makeWebSocket: unknown;
  export const net: unknown;
  export const typeLiteral: unknown;
  const _default: unknown;
  export default _default;
}

// `cookieStore` — the Cookie Store API global. Not yet in TS's lib.dom.d.ts
// (TS lib < 5.7 gap); used by shadcn sidebar.tsx for persistence.
interface CookieStore {
  get(name: string): Promise<{ value: string } | null>;
  set(details: {
    name: string;
    value: string;
    expires?: number;
    path?: string;
  }): Promise<void>;
  delete(name: string): Promise<void>;
}

interface Window {
  cookieStore?: CookieStore;
}

declare const cookieStore: CookieStore | undefined;
