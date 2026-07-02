/**
 * Tier 3 — Provider discovery + agent-mention aliases.
 *
 * Hand-ported from MCode `packages/contracts/src/providerDiscovery.ts` and
 * `agentMentions.ts` (Effect Schema → plain TS types). Covers the provider
 * discovery RPC surface (skills/plugins/commands/models/agents descriptors +
 * results), the composer-capabilities descriptor, the provider-option
 * descriptor/selection pair, and the agent-mention alias resolution helpers
 * (`resolveAgentAlias`, `getAgentMentionAutocompleteAliases`) used by the
 * composer UI's @-mention autocomplete.
 *
 * Source of truth:
 *   /home/vibe-dev/mcode/packages/contracts/src/providerDiscovery.ts
 *   /home/vibe-dev/mcode/packages/contracts/src/agentMentions.ts
 */

import type { ProviderKind } from "./orchestration";
import type { ModelSlug } from "./orchestration";
import type { TrimmedNonEmptyString } from "./base";

// ─── Provider option descriptor + selection ───────────────────────────

export interface ProviderOptionChoice {
  id: TrimmedNonEmptyString;
  label: TrimmedNonEmptyString;
  description?: TrimmedNonEmptyString;
  isDefault?: true;
}

export interface SelectProviderOptionDescriptor {
  id: TrimmedNonEmptyString;
  label: TrimmedNonEmptyString;
  description?: TrimmedNonEmptyString;
  type: "select";
  options: readonly ProviderOptionChoice[];
  currentValue?: TrimmedNonEmptyString;
  promptInjectedValues?: readonly TrimmedNonEmptyString[];
}

export interface BooleanProviderOptionDescriptor {
  id: TrimmedNonEmptyString;
  label: TrimmedNonEmptyString;
  description?: TrimmedNonEmptyString;
  type: "boolean";
  currentValue?: boolean;
}

export type ProviderOptionDescriptor =
  | SelectProviderOptionDescriptor
  | BooleanProviderOptionDescriptor;

export interface ProviderOptionSelection {
  id: TrimmedNonEmptyString;
  value: TrimmedNonEmptyString | boolean;
}

// ─── Skill / mention / plugin / model / agent descriptors ─────────────

export interface ProviderSkillInterface {
  displayName?: TrimmedNonEmptyString;
  shortDescription?: TrimmedNonEmptyString;
}

export interface ProviderSkillDescriptor {
  name: TrimmedNonEmptyString;
  description?: TrimmedNonEmptyString;
  path: TrimmedNonEmptyString;
  enabled: boolean;
  scope?: TrimmedNonEmptyString;
  interface?: ProviderSkillInterface;
  dependencies?: unknown;
}

export interface ProviderMentionReference {
  name: TrimmedNonEmptyString;
  path: TrimmedNonEmptyString;
}
export interface ProviderSkillReference {
  name: TrimmedNonEmptyString;
  path: TrimmedNonEmptyString;
}

export interface ProviderComposerCapabilities {
  provider: ProviderKind;
  supportsSkillMentions: boolean;
  supportsSkillDiscovery: boolean;
  supportsNativeSlashCommandDiscovery: boolean;
  supportsPluginMentions: boolean;
  supportsPluginDiscovery: boolean;
  supportsRuntimeModelList: boolean;
  supportsThreadCompaction?: boolean;
  supportsThreadImport?: boolean;
}

export interface ProviderNativeCommandDescriptor {
  name: TrimmedNonEmptyString;
  description?: TrimmedNonEmptyString;
}

export interface ProviderPluginMarketplaceInterface {
  displayName?: TrimmedNonEmptyString;
}

export type ProviderPluginInstallPolicy =
  | "NOT_AVAILABLE"
  | "AVAILABLE"
  | "INSTALLED_BY_DEFAULT";
export type ProviderPluginAuthPolicy = "ON_INSTALL" | "ON_USE";

export interface ProviderPluginSource {
  type: "local";
  path: TrimmedNonEmptyString;
}

export interface ProviderPluginInterface {
  displayName?: TrimmedNonEmptyString;
  shortDescription?: TrimmedNonEmptyString;
  longDescription?: TrimmedNonEmptyString;
  developerName?: TrimmedNonEmptyString;
  category?: TrimmedNonEmptyString;
  capabilities?: readonly TrimmedNonEmptyString[];
  websiteUrl?: TrimmedNonEmptyString;
  privacyPolicyUrl?: TrimmedNonEmptyString;
  termsOfServiceUrl?: TrimmedNonEmptyString;
  defaultPrompt?: readonly TrimmedNonEmptyString[];
  brandColor?: TrimmedNonEmptyString;
  composerIcon?: TrimmedNonEmptyString;
  logo?: TrimmedNonEmptyString;
  screenshots?: readonly TrimmedNonEmptyString[];
}

export interface ProviderPluginDescriptor {
  id: TrimmedNonEmptyString;
  name: TrimmedNonEmptyString;
  source: ProviderPluginSource;
  installed: boolean;
  enabled: boolean;
  installPolicy: ProviderPluginInstallPolicy;
  authPolicy: ProviderPluginAuthPolicy;
  interface?: ProviderPluginInterface;
}

export interface ProviderPluginMarketplaceLoadError {
  marketplacePath: TrimmedNonEmptyString;
  message: TrimmedNonEmptyString;
}

export interface ProviderPluginMarketplaceDescriptor {
  name: TrimmedNonEmptyString;
  path: TrimmedNonEmptyString;
  interface?: ProviderPluginMarketplaceInterface;
  plugins: readonly ProviderPluginDescriptor[];
}

export interface ProviderPluginAppSummary {
  id: TrimmedNonEmptyString;
  name: TrimmedNonEmptyString;
  description?: TrimmedNonEmptyString;
  installUrl?: TrimmedNonEmptyString;
  needsAuth: boolean;
}

export interface ProviderPluginDetail {
  marketplaceName: TrimmedNonEmptyString;
  marketplacePath: TrimmedNonEmptyString;
  summary: ProviderPluginDescriptor;
  description?: TrimmedNonEmptyString;
  skills: readonly ProviderSkillDescriptor[];
  apps: readonly ProviderPluginAppSummary[];
  mcpServers: readonly TrimmedNonEmptyString[];
}

export interface ProviderReasoningEffortDescriptor {
  value: TrimmedNonEmptyString;
  label?: TrimmedNonEmptyString;
  description?: TrimmedNonEmptyString;
}

export interface ProviderContextWindowDescriptor {
  value: TrimmedNonEmptyString;
  label: TrimmedNonEmptyString;
  isDefault?: true;
}

export interface ProviderModelDescriptor {
  slug: TrimmedNonEmptyString;
  name: TrimmedNonEmptyString;
  upstreamProviderId?: TrimmedNonEmptyString;
  upstreamProviderName?: TrimmedNonEmptyString;
  optionDescriptors?: readonly ProviderOptionDescriptor[];
  supportedReasoningEfforts?: readonly ProviderReasoningEffortDescriptor[];
  defaultReasoningEffort?: TrimmedNonEmptyString;
  supportsFastMode?: boolean;
  supportsThinkingToggle?: boolean;
  contextWindowOptions?: readonly ProviderContextWindowDescriptor[];
  defaultContextWindow?: TrimmedNonEmptyString;
}

export interface ProviderAgentDescriptor {
  name: TrimmedNonEmptyString;
  displayName: TrimmedNonEmptyString;
  description?: TrimmedNonEmptyString;
  model?: TrimmedNonEmptyString;
}

// ─── Agent mention aliases ────────────────────────────────────────────

type AgentAliasColor =
  | "violet"
  | "fuchsia"
  | "teal"
  | "cyan"
  | "amber"
  | "orange";

interface BaseAgentAliasDefinition {
  readonly provider: ProviderKind;
  readonly displayName: string;
  readonly color: AgentAliasColor;
}

export interface CodexAgentAliasDefinition extends BaseAgentAliasDefinition {
  readonly provider: "codex";
  readonly kind: "model";
  readonly model: ModelSlug;
}

export interface ClaudeSubagentAliasDefinition extends BaseAgentAliasDefinition {
  readonly provider: "claudeAgent";
  readonly kind: "claude-subagent";
  readonly agentName: string;
  readonly description: string;
  readonly prompt: string;
  readonly tools?: readonly string[];
  readonly disallowedTools?: readonly string[];
  readonly model?: string;
}

export type AgentAliasDefinition =
  | CodexAgentAliasDefinition
  | ClaudeSubagentAliasDefinition;

export type ResolvedAgentAlias = AgentAliasDefinition & {
  readonly alias: string;
};

const CODEX_AGENT_MENTION_ALIASES: Record<string, CodexAgentAliasDefinition> = {
  "5.5": { provider: "codex", kind: "model", model: "gpt-5.5", displayName: "GPT-5.5", color: "violet" },
  "5.4": { provider: "codex", kind: "model", model: "gpt-5.4", displayName: "GPT-5.4", color: "violet" },
  mini: { provider: "codex", kind: "model", model: "gpt-5.4-mini", displayName: "GPT-5.4 Mini", color: "fuchsia" },
  "5.4-mini": { provider: "codex", kind: "model", model: "gpt-5.4-mini", displayName: "GPT-5.4 Mini", color: "fuchsia" },
  codex: { provider: "codex", kind: "model", model: "gpt-5.3-codex", displayName: "GPT-5.3 Codex", color: "teal" },
  "5.3-codex": { provider: "codex", kind: "model", model: "gpt-5.3-codex", displayName: "GPT-5.3 Codex", color: "teal" },
  spark: { provider: "codex", kind: "model", model: "gpt-5.3-codex-spark", displayName: "GPT-5.3 Codex Spark", color: "cyan" },
  "5.3-spark": { provider: "codex", kind: "model", model: "gpt-5.3-codex-spark", displayName: "GPT-5.3 Codex Spark", color: "cyan" },
  "5.2": { provider: "codex", kind: "model", model: "gpt-5.2", displayName: "GPT-5.2", color: "amber" },
  "5.2-codex": { provider: "codex", kind: "model", model: "gpt-5.2-codex", displayName: "GPT-5.2 Codex", color: "orange" },
};

const CLAUDE_AGENT_MENTION_ALIASES: Record<string, ClaudeSubagentAliasDefinition> = {
  explore: {
    provider: "claudeAgent", kind: "claude-subagent", agentName: "explore", displayName: "Explore", color: "cyan",
    description: "Read-only codebase explorer. Use for file discovery, code search, and gathering context before implementation.",
    prompt: "You are a focused codebase exploration specialist. Search broadly, gather the most relevant findings, and return a concise summary with the key files, evidence, and risks. Do not make code changes.",
    tools: ["Read", "Grep", "Glob"], model: "haiku",
  },
  review: {
    provider: "claudeAgent", kind: "claude-subagent", agentName: "review", displayName: "Code Review", color: "amber",
    description: "Bug and risk reviewer. Use for code review, regression hunting, and edge-case analysis.",
    prompt: "You are a senior code reviewer. Focus on behavioral regressions, correctness bugs, edge cases, and missing tests. Return findings first, then open questions, then a brief summary.",
    tools: ["Read", "Grep", "Glob"], model: "sonnet",
  },
  reviewer: {
    provider: "claudeAgent", kind: "claude-subagent", agentName: "review", displayName: "Code Review", color: "amber",
    description: "Bug and risk reviewer. Use for code review, regression hunting, and edge-case analysis.",
    prompt: "You are a senior code reviewer. Focus on behavioral regressions, correctness bugs, edge cases, and missing tests. Return findings first, then open questions, then a brief summary.",
    tools: ["Read", "Grep", "Glob"], model: "sonnet",
  },
  build: {
    provider: "claudeAgent", kind: "claude-subagent", agentName: "build", displayName: "Implementer", color: "violet",
    description: "Implementation teammate. Use for scoped code changes, debugging, and hands-on execution tasks.",
    prompt: "You are an implementation-focused coding teammate. Make targeted changes, validate assumptions with the available tools, and return a short implementation summary plus any remaining risks.",
    tools: ["Read", "Grep", "Glob", "Bash", "Edit", "Write", "MultiEdit"], model: "sonnet",
  },
  implement: {
    provider: "claudeAgent", kind: "claude-subagent", agentName: "build", displayName: "Implementer", color: "violet",
    description: "Implementation teammate. Use for scoped code changes, debugging, and hands-on execution tasks.",
    prompt: "You are an implementation-focused coding teammate. Make targeted changes, validate assumptions with the available tools, and return a short implementation summary plus any remaining risks.",
    tools: ["Read", "Grep", "Glob", "Bash", "Edit", "Write", "MultiEdit"], model: "sonnet",
  },
  plan: {
    provider: "claudeAgent", kind: "claude-subagent", agentName: "plan", displayName: "Planner", color: "fuchsia",
    description: "Planning specialist. Use for breaking work into steps, evaluating approaches, and preparing execution plans.",
    prompt: "You are a planning specialist. Clarify goals, evaluate tradeoffs, identify edge cases, and return a concrete ordered plan with the main risks called out explicitly.",
    tools: ["Read", "Grep", "Glob", "TodoWrite"], model: "sonnet",
  },
  planner: {
    provider: "claudeAgent", kind: "claude-subagent", agentName: "plan", displayName: "Planner", color: "fuchsia",
    description: "Planning specialist. Use for breaking work into steps, evaluating approaches, and preparing execution plans.",
    prompt: "You are a planning specialist. Clarify goals, evaluate tradeoffs, identify edge cases, and return a concrete ordered plan with the main risks called out explicitly.",
    tools: ["Read", "Grep", "Glob", "TodoWrite"], model: "sonnet",
  },
};

const OPENCODE_AGENT_MENTION_ALIASES: Record<string, AgentAliasDefinition> = {};

export const AGENT_MENTION_ALIASES_BY_PROVIDER: Record<
  ProviderKind,
  Record<string, AgentAliasDefinition>
> = {
  codex: CODEX_AGENT_MENTION_ALIASES,
  claudeAgent: CLAUDE_AGENT_MENTION_ALIASES,
  cursor: {},
  gemini: {},
  grok: {},
  kilo: OPENCODE_AGENT_MENTION_ALIASES,
  opencode: OPENCODE_AGENT_MENTION_ALIASES,
  pi: {},
};

export const AGENT_MENTION_ALIASES: Record<string, AgentAliasDefinition> =
  Object.assign({}, ...Object.values(AGENT_MENTION_ALIASES_BY_PROVIDER));

const AGENT_MENTION_AUTOCOMPLETE_ALIASES_BY_PROVIDER: Record<
  ProviderKind,
  readonly string[]
> = {
  codex: ["5.5", "5.4", "mini", "5.3-codex", "spark", "5.2", "5.2-codex"],
  claudeAgent: ["explore", "review", "build", "plan"],
  cursor: [],
  gemini: [],
  grok: [],
  kilo: [],
  opencode: [],
  pi: [],
};

function mapAgentEntries(
  input: Record<string, AgentAliasDefinition>,
): ResolvedAgentAlias[] {
  return Object.entries(input)
    .map(([alias, definition]) => ({ alias, ...definition }))
    .sort((a, b) => a.alias.localeCompare(b.alias));
}

export function getAgentMentionAliases(
  provider?: ProviderKind,
): ResolvedAgentAlias[] {
  if (provider) {
    return mapAgentEntries(AGENT_MENTION_ALIASES_BY_PROVIDER[provider]);
  }
  return Object.values(AGENT_MENTION_ALIASES_BY_PROVIDER).flatMap((d) =>
    mapAgentEntries(d),
  );
}

export function getAgentMentionAutocompleteAliases(
  provider: ProviderKind,
): ResolvedAgentAlias[] {
  return AGENT_MENTION_AUTOCOMPLETE_ALIASES_BY_PROVIDER[provider].map(
    (alias) => {
      const definition = AGENT_MENTION_ALIASES_BY_PROVIDER[provider][alias];
      if (!definition) {
        throw new Error(`Unknown autocomplete alias for ${provider}: ${alias}`);
      }
      return { alias, ...definition };
    },
  );
}

export function resolveAgentAlias(
  alias: string,
  provider?: ProviderKind,
): AgentAliasDefinition | null {
  const normalized = alias.toLowerCase();
  if (provider) {
    return AGENT_MENTION_ALIASES_BY_PROVIDER[provider][normalized] ?? null;
  }
  for (const definitions of Object.values(AGENT_MENTION_ALIASES_BY_PROVIDER)) {
    const resolved = definitions[normalized];
    if (resolved) {
      return resolved;
    }
  }
  return null;
}

export function isValidAgentAlias(alias: string, provider?: ProviderKind): boolean {
  return resolveAgentAlias(alias, provider) !== null;
}

export function getAgentAliasNames(provider?: ProviderKind): string[] {
  if (provider) {
    return Object.keys(AGENT_MENTION_ALIASES_BY_PROVIDER[provider]);
  }
  return Object.values(AGENT_MENTION_ALIASES_BY_PROVIDER).flatMap((d) =>
    Object.keys(d),
  );
}
