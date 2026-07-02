/**
 * Tier 3 — Auth domain.
 *
 * Hand-ported from MCode `packages/contracts/src/auth.ts` (Effect Schema →
 * plain TS types). Adds real shapes for the auth-session/pairing surface
 * that shell.ts declared opaque: AuthBootstrapInput, AuthBearerBootstrapResult,
 * AuthWebSocketTokenResult, AuthSessionState, AuthCreatePairingCredentialInput,
 * AuthPairingCredentialResult, AuthPairingLink, AuthRevokePairingLinkInput,
 * AuthClientSession, AuthRevokeClientSessionInput.
 *
 * NOTE: `AuthBootstrapResult` is the ts-rs-generated Tier 1 DTO
 * (`../types/AuthBootstrapResult`) — already re-exported from index.ts;
 * this module does not re-declare it.
 *
 * Source of truth: /home/vibe-dev/mcode/packages/contracts/src/auth.ts
 */

import type { TrimmedNonEmptyString } from "./base";
import type { AuthSessionId } from "./base";

// ─── Auth enums ───────────────────────────────────────────────────────

export type ServerAuthPolicy =
  | "desktop-managed-local"
  | "loopback-browser"
  | "remote-reachable"
  | "unsafe-no-auth";

export type ServerAuthBootstrapMethod =
  | "desktop-bootstrap"
  | "one-time-token";

export type ServerAuthSessionMethod =
  | "browser-session-cookie"
  | "bearer-session-token";

export type AuthSessionRole = "owner" | "client";

export interface ServerAuthDescriptor {
  policy: ServerAuthPolicy;
  bootstrapMethods: readonly ServerAuthBootstrapMethod[];
  sessionMethods: readonly ServerAuthSessionMethod[];
  sessionCookieName: TrimmedNonEmptyString;
}

// ─── Bootstrap inputs / results ───────────────────────────────────────

export interface AuthBootstrapInput {
  credential: TrimmedNonEmptyString;
}

export interface AuthBearerBootstrapResult {
  authenticated: true;
  role: AuthSessionRole;
  sessionMethod: "bearer-session-token";
  expiresAt: string;
  sessionToken: TrimmedNonEmptyString;
}

export interface AuthWebSocketTokenResult {
  token: TrimmedNonEmptyString;
  expiresAt: string;
}

// ─── Pairing ──────────────────────────────────────────────────────────

export interface AuthPairingCredentialResult {
  id: TrimmedNonEmptyString;
  credential: TrimmedNonEmptyString;
  label?: TrimmedNonEmptyString;
  expiresAt: string;
}

export interface AuthPairingLink {
  id: TrimmedNonEmptyString;
  credential: TrimmedNonEmptyString;
  role: AuthSessionRole;
  subject: TrimmedNonEmptyString;
  label?: TrimmedNonEmptyString;
  createdAt: string;
  expiresAt: string;
}

export interface AuthCreatePairingCredentialInput {
  label?: TrimmedNonEmptyString;
}

export interface AuthRevokePairingLinkInput {
  id: TrimmedNonEmptyString;
}

// ─── Client sessions ──────────────────────────────────────────────────

export type AuthClientMetadataDeviceType =
  | "desktop"
  | "mobile"
  | "tablet"
  | "bot"
  | "unknown";

export interface AuthClientMetadata {
  label?: TrimmedNonEmptyString;
  ipAddress?: TrimmedNonEmptyString;
  userAgent?: TrimmedNonEmptyString;
  deviceType: AuthClientMetadataDeviceType;
  os?: TrimmedNonEmptyString;
  browser?: TrimmedNonEmptyString;
}

export interface AuthClientSession {
  sessionId: AuthSessionId;
  subject: TrimmedNonEmptyString;
  role: AuthSessionRole;
  method: ServerAuthSessionMethod;
  client: AuthClientMetadata;
  issuedAt: string;
  expiresAt: string;
  lastConnectedAt: string | null;
  connected: boolean;
  current: boolean;
}

export interface AuthRevokeClientSessionInput {
  sessionId: AuthSessionId;
}

// ─── Session state (aggregate) ────────────────────────────────────────

export interface AuthSessionState {
  authenticated: boolean;
  auth: ServerAuthDescriptor;
  role?: AuthSessionRole;
  sessionMethod?: ServerAuthSessionMethod;
  expiresAt?: string;
}
