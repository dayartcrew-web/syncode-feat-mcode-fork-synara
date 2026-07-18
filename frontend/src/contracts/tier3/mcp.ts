/**
 * MCP (Model Context Protocol) server catalog contracts.
 *
 * Mirrors `crates/syncode-ws/src/mcp_catalog.rs` wire shapes 1:1.
 *
 * SECURITY: env-var VALUES never cross the wire. Only the names are exposed
 * (`McpEnvVar` has no `value` field). Values live on disk in
 * `~/.syncode/mcp.json` for syncode-owned entries; they are re-read at
 * `session/new` time and forwarded directly to ACP providers.
 */

export type McpTransport = "stdio" | "http" | "sse";

export type McpScope = "user" | "project" | "syncode";

export type McpStatus = "reachable" | "unreachable";

/** Name-only env var descriptor. Values are intentionally NOT serialized. */
export interface McpEnvVar {
  readonly name: string;
}

/**
 * Wire descriptor for one MCP server. Source of truth for the Settings panel
 * and for `provider/list-mcp-catalog`.
 */
export interface McpServerDescriptor {
  readonly name: string;
  readonly transport: McpTransport;
  readonly command: string | null;
  readonly args: readonly string[];
  readonly env: readonly McpEnvVar[];
  readonly url: string | null;
  readonly scope: McpScope;
  readonly sourcePath: string;
  /** `false` for discovered entries (read-only), `true` for syncode-owned. */
  readonly editable: boolean;
  readonly enabled: boolean;
  /** Populated only by `mcp/test-connection`. `null` in catalog responses. */
  readonly status: McpStatus | null;
  /** Populated only by `mcp/test-connection` when status === "unreachable". */
  readonly error?: string | null;
  /** Populated only by `mcp/test-connection` when status === "reachable". */
  readonly latencyMs?: number | null;
}

/** Payload for `provider/list-mcp-catalog`. */
export interface McpCatalogResponse {
  readonly servers: readonly McpServerDescriptor[];
  readonly syncodeMcpPath: string | null;
}

/** Input for `provider/list-mcp-catalog`. */
export interface ProviderListMcpCatalogInput {
  readonly cwd?: string;
  readonly forceReload?: boolean;
}

/** Result of `mcp/create`. */
export interface McpCreateResult {
  readonly server: McpServerDescriptor;
}

/** Input for `mcp/update`. */
export interface McpUpdateInput {
  readonly name: string;
  readonly patch: McpServerPatch;
}

/** Result of `mcp/update`. */
export interface McpUpdateResult {
  readonly server: McpServerDescriptor;
}

/** Input for `mcp/delete`. */
export interface McpDeleteInput {
  readonly name: string;
}

/** Result of `mcp/delete`. */
export interface McpDeleteResult {
  readonly ok: true;
}

/**
 * Input for `mcp/test-connection`.
 *
 * Either pass `name` to probe a catalog entry in-place, OR pass full
 * transport details to probe ad-hoc (e.g. from the editor before save).
 */
export interface McpTestConnectionInput {
  readonly name?: string;
  readonly transport?: McpTransport;
  readonly command?: string;
  readonly args?: readonly string[];
  /** `[name, value]` tuples — needed so the handshake can pass env through. */
  readonly env?: readonly Readonly<[string, string]>[];
  readonly url?: string;
  readonly timeoutMs?: number;
}

/** Input for `mcp/create`. */
export interface McpServerInput {
  readonly name: string;
  readonly transport: McpTransport;
  readonly command?: string | null;
  readonly args?: readonly string[];
  /** `[name, value]` tuples — values are persisted to ~/.syncode/mcp.json. */
  readonly env?: readonly Readonly<[string, string]>[];
  readonly url?: string | null;
}

/**
 * Input for `mcp/update`. All fields optional except `name`. Only fields
 * present in the patch are mutated on the stored entry.
 */
export interface McpServerPatch {
  readonly name?: string;
  readonly transport?: McpTransport;
  readonly command?: string | null;
  readonly args?: readonly string[];
  readonly env?: readonly Readonly<[string, string]>[];
  readonly url?: string | null;
}

/** Result of `mcp/test-connection`. */
export interface McpTestConnectionResult {
  readonly status: McpStatus;
  readonly latencyMs?: number;
  readonly error?: string;
}
