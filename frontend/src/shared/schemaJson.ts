/**
 * Schema-JSON helpers — formerly Effect Schema `decodeExit` / `Result` /
 * `SchemaIssue` wrappers.
 *
 * The original module wrapped Effect `Schema.fromJsonString(...)` decode exits
 * into `Result`/`Cause` values and formatted `SchemaError`s via `SchemaIssue`.
 * Those APIs (`Schema.Codec`, `Result`, `SchemaIssue`, `SchemaError`) are absent
 * from the installed stable `effect@3.21.4`, so this module now ships plain-TS
 * equivalents that preserve the public signatures for any caller that imports
 * them via the `@t3tools/shared/schemaJson` subpath.
 *
 * Semantics:
 *   - `decodeJsonResult(codec)` returns a `(input: string) => Result<unknown>`.
 *   - `decodeUnknownJsonResult(codec)` returns a `(input: unknown) => Result<unknown>`.
 *   - `formatSchemaError(cause)` stringifies the cause.
 *
 * `Result<T>` is modelled as a discriminated union (`{ ok: true; value } |
 * `{ ok: false; error }`) so callers can branch without the Effect runtime.
 */

import type { Codec } from "@t3tools/contracts";

export type Result<T> = { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: unknown };

/**
 * Build a decoder that JSON-parses a string and runs it through `codec.decode`.
 * Returns `Result.fail` on parse or decode error.
 */
export const decodeJsonResult = (codec: Codec<unknown>) => {
  return (input: string): Result<unknown> => {
    try {
      const parsed = JSON.parse(input) as unknown;
      return { ok: true, value: codec.decode(JSON.stringify(parsed)) };
    } catch (error) {
      return { ok: false, error };
    }
  };
};

/**
 * Build a decoder that accepts an already-parsed unknown value and runs it
 * through `codec.decode` (after re-stringifying, since the plain Codec.decode
 * takes a string). Returns `Result.fail` on decode error.
 */
export const decodeUnknownJsonResult = (codec: Codec<unknown>) => {
  return (input: unknown): Result<unknown> => {
    try {
      return { ok: true, value: codec.decode(JSON.stringify(input)) };
    } catch (error) {
      return { ok: false, error };
    }
  };
};

/**
 * Format a schema/decode error cause. Formerly rendered `SchemaIssue`s; now
 * falls back to `Error.message` / `String(cause)`.
 */
export const formatSchemaError = (cause: unknown): string => {
  if (cause instanceof Error) {
    return cause.message;
  }
  if (typeof cause === "string") {
    return cause;
  }
  try {
    return JSON.stringify(cause);
  } catch {
    return String(cause);
  }
};
