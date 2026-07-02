/**
 * Minimal runtime guards — replaces Effect `Schema.is` / safe-decode usage.
 *
 * Agent measurement (CONTRACTS-BRIDGE-DESIGN.md §5): runtime Effect-Schema use
 * in the cloned MCode UI is minimal — ~6 files, ~15 call sites: type guards
 * (`Schema.is`), localStorage JSON serde (`fromJsonString`/`decodeSync`),
 * and safe-decode-with-defaults. ~95% of contract usage is type-only.
 *
 * Decision: ship hand-written guards for exactly those patterns here. Do NOT
 * pull in zod/valibot globally — the surface is too small to justify the
 * dependency. Revisit only if runtime-validation demand grows.
 *
 * Intentionally tiny: `isObject`, `hasKey`, `isString`, `safeParse`, and a
 * `decodeWithDefault` helper. Everything else stays type-only.
 */

/**
 * Type guard: is `value` a non-null plain object (not an array)?
 *
 * Narrows `unknown` to `Record<string, unknown>`. Use this in place of
 * `Schema.is(SomeObjectSchema)` for ad-hoc shape checks at trust boundaries
 * (e.g. `event.data` from a WebSocket push).
 */
export const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/**
 * Type guard: does `value` have an own property `key`?
 *
 * Narrows to `{ [K in key]: unknown }`. Combine with `isObject` first:
 *   if (isObject(data) && hasKey(data, "token")) { ... data.token ... }
 */
export const hasKey = <K extends string>(
  value: object,
  key: K,
): value is Record<K, unknown> =>
  Object.prototype.hasOwnProperty.call(value, key);

/** Type guard: is `value` a `string`? */
export const isString = (value: unknown): value is string =>
  typeof value === "string";

/** Type guard: is `value` a `number` (finite, not NaN)? */
export const isNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

/** Type guard: is `value` a `boolean`? */
export const isBoolean = (value: unknown): value is boolean =>
  typeof value === "boolean";

/**
 * Safe JSON parse — never throws. Returns `null` on invalid JSON.
 *
 * Replaces Effect's `fromJsonString`. Use when reading untrusted JSON (e.g.
 * from localStorage or a raw WS frame) where a parse failure should be
 * tolerated rather than crash the UI.
 */
export const safeParse = <T = unknown>(text: string): T | null => {
  try {
    return JSON.parse(text) as T;
  } catch {
    return null;
  }
};

/**
 * Safe JSON parse with a typed fallback. Returns `fallback` on invalid JSON
 * or on a successful parse that fails the optional `guard`.
 *
 * Replaces Effect's `decodeUnknownSync`-with-defaults pattern:
 *   const cfg = decodeWithDefault(raw, DEFAULT_CFG, isMyConfig);
 */
export const decodeWithDefault = <T>(
  text: string,
  fallback: T,
  guard?: (value: unknown) => value is T,
): T => {
  const parsed = safeParse(text);
  if (parsed === null) return fallback;
  if (guard && !guard(parsed)) return fallback;
  return parsed as T;
};

/**
 * Minimal JSON codec contract — replaces Effect `Schema.Codec` for the
 * localStorage round-trip helpers (`useLocalStorage`, `getLocalStorageItem`,
 * `setLocalStorageItem`).
 *
 * A codec is just a typed pair of `encode` (value -> JSON string) and
 * `decode` (JSON string -> value). The default {@link jsonCodec} round-trips
 * via `JSON.stringify` / `JSON.parse`. Callers that previously passed an
 * Effect `Schema.String`, `Schema.Finite`, or `Schema.Struct({...})` now
 * pass a tiny codec built with {@link stringCodec}, {@link numberCodec}, or
 * {@link objectCodec}.
 */
export interface Codec<T> {
  readonly encode: (value: T) => string;
  readonly decode: (text: string) => T;
}

/** Default codec: plain `JSON.stringify` / `JSON.parse`. */
export const jsonCodec: Codec<unknown> = {
  encode: (value) => JSON.stringify(value),
  decode: (text) => JSON.parse(text) as unknown,
};

/**
 * String codec: the value is stored as a raw JSON string (so `Schema.String`
 * semantics — `JSON.stringify("hi")` -> `"hi"` -> `JSON.parse` -> `"hi"`).
 */
export const stringCodec: Codec<string> = {
  encode: (value) => JSON.stringify(value),
  decode: (text) => JSON.parse(text) as string,
};

/** Number codec for finite numbers (replaces `Schema.Finite`). */
export const numberCodec: Codec<number> = {
  encode: (value) => JSON.stringify(value),
  decode: (text) => {
    const parsed = JSON.parse(text);
    if (typeof parsed !== "number" || !Number.isFinite(parsed)) {
      throw new Error("Expected a finite number");
    }
    return parsed;
  },
};

/**
 * Object codec with an optional guard + default-filling. Mirrors the
 * `decodeUnknownSync`-with-defaults pattern used by `AppSettings`:
 *   objectCodec<AppSettings>(DEFAULT_APP_SETTINGS, isAppSettings)
 *
 * On decode, the parsed value is merged over the fallback (so missing keys
 * pick up defaults) and the optional guard may reject entirely.
 */
export const objectCodec = <T>(
  fallback: T,
  guard?: (value: unknown) => value is T,
): Codec<T> => ({
  encode: (value) => JSON.stringify(value),
  decode: (text) => {
    const parsed = safeParse<T>(text);
    if (parsed === null) return fallback;
    if (guard && !guard(parsed)) return fallback;
    if (isObject(fallback) && isObject(parsed)) {
      return { ...fallback, ...parsed } as T;
    }
    return parsed;
  },
});
