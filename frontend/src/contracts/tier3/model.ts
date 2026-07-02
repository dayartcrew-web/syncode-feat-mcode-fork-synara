/**
 * Tier 3 — Model catalog constants + provider display names.
 *
 * Ported verbatim from MCode `packages/contracts/src/model.ts` (the
 * non-Schema const declarations). The vendored UI imports these to render
 * the model picker and provider menus, so the values must match MCode's
 * exactly. The catalog is large but mechanical; real type-checking value
 * lives in the `ModelCapabilities` interface + `ProviderKind`-keyed records.
 *
 * Source of truth: /home/vibe-dev/mcode/packages/contracts/src/model.ts
 */

import type {
  ProviderKind,
  ProviderWithDefaultModel,
  ModelCapabilities,
  ModelSlug,
} from "./orchestration";

interface ModelDefinition {
  readonly slug: string;
  readonly name: string;
  readonly capabilities: ModelCapabilities;
}

const GEMINI_2_5_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [
    { value: "-1", label: "Dynamic", isDefault: true },
    { value: "512", label: "512 Tokens" },
  ],
  supportsFastMode: false,
  supportsThinkingToggle: false,
  promptInjectedEffortLevels: [],
  contextWindowOptions: [],
};

const CODEX_GPT_5_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [
    { value: "low", label: "Low" },
    { value: "medium", label: "Medium" },
    { value: "high", label: "High", isDefault: true },
    { value: "xhigh", label: "Extra High" },
  ],
  supportsFastMode: true,
  supportsThinkingToggle: false,
  promptInjectedEffortLevels: [],
  contextWindowOptions: [],
};

const CODEX_GPT_5_5_CAPABILITIES: ModelCapabilities = {
  ...CODEX_GPT_5_CAPABILITIES,
  reasoningEffortLevels: [
    { value: "low", label: "Low" },
    { value: "medium", label: "Medium", isDefault: true },
    { value: "high", label: "High" },
    { value: "xhigh", label: "Extra High" },
  ],
};

const GROK_BUILD_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [
    { value: "none", label: "None" },
    { value: "low", label: "Low", isDefault: true },
    { value: "medium", label: "Medium" },
    { value: "high", label: "High" },
  ],
  supportsFastMode: false,
  supportsThinkingToggle: false,
  promptInjectedEffortLevels: [],
  contextWindowOptions: [],
};

const CLAUDE_DUAL_CONTEXT_WINDOW = [
  { value: "200k", label: "200k", isDefault: true },
  { value: "1m", label: "1M" },
] as const;

const CLAUDE_FABLE_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [
    { value: "low", label: "Low" },
    { value: "medium", label: "Medium" },
    { value: "high", label: "High", isDefault: true },
    { value: "xhigh", label: "Extra High" },
    { value: "max", label: "Max" },
    { value: "ultracode", label: "Ultracode", description: "xhigh + workflows" },
  ],
  supportsFastMode: false,
  supportsThinkingToggle: false,
  promptInjectedEffortLevels: [],
  contextWindowOptions: CLAUDE_DUAL_CONTEXT_WINDOW,
};

const CLAUDE_FLAGSHIP_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [
    { value: "low", label: "Low" },
    { value: "medium", label: "Medium" },
    { value: "high", label: "High", isDefault: true },
    { value: "xhigh", label: "Extra High" },
    { value: "max", label: "Max" },
    { value: "ultrathink", label: "Ultrathink" },
    { value: "ultracode", label: "Ultracode" },
  ],
  supportsFastMode: true,
  supportsThinkingToggle: false,
  promptInjectedEffortLevels: ["ultrathink"],
  contextWindowOptions: CLAUDE_DUAL_CONTEXT_WINDOW,
};

const CLAUDE_EXTENDED_THINKING_CAPABILITIES: ModelCapabilities = {
  ...CLAUDE_FLAGSHIP_CAPABILITIES,
  reasoningEffortLevels: [
    { value: "low", label: "Low" },
    { value: "medium", label: "Medium" },
    { value: "high", label: "High", isDefault: true },
    { value: "max", label: "Max" },
    { value: "ultrathink", label: "Ultrathink" },
  ],
};

const EMPTY_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [],
  supportsFastMode: false,
  supportsThinkingToggle: false,
  promptInjectedEffortLevels: [],
  contextWindowOptions: [],
};

/**
 * TODO: This should not be a static array, each provider should return its own
 * model list over the WS API.
 */
export const MODEL_OPTIONS_BY_PROVIDER: Record<
  ProviderKind,
  readonly ModelDefinition[]
> = {
  codex: [
    { slug: "gpt-5.5", name: "GPT-5.5", capabilities: CODEX_GPT_5_5_CAPABILITIES },
    { slug: "gpt-5.4", name: "GPT-5.4", capabilities: CODEX_GPT_5_CAPABILITIES },
    { slug: "gpt-5.4-mini", name: "GPT-5.4 Mini", capabilities: CODEX_GPT_5_CAPABILITIES },
    { slug: "gpt-5.3-codex", name: "GPT-5.3 Codex", capabilities: CODEX_GPT_5_CAPABILITIES },
    { slug: "gpt-5.3-codex-spark", name: "GPT-5.3 Codex Spark", capabilities: CODEX_GPT_5_CAPABILITIES },
    { slug: "gpt-5.2-codex", name: "GPT-5.2 Codex", capabilities: CODEX_GPT_5_CAPABILITIES },
    { slug: "gpt-5.2", name: "GPT-5.2", capabilities: CODEX_GPT_5_CAPABILITIES },
  ],
  claudeAgent: [
    { slug: "claude-fable-5", name: "Claude Fable 5", capabilities: CLAUDE_FABLE_CAPABILITIES },
    { slug: "claude-opus-4-8", name: "Claude Opus 4.8", capabilities: CLAUDE_FLAGSHIP_CAPABILITIES },
    { slug: "claude-opus-4-7", name: "Claude Opus 4.7", capabilities: CLAUDE_FLAGSHIP_CAPABILITIES },
    { slug: "claude-opus-4-6", name: "Claude Opus 4.6", capabilities: CLAUDE_EXTENDED_THINKING_CAPABILITIES },
    {
      slug: "claude-opus-4-5",
      name: "Claude Opus 4.5",
      capabilities: {
        reasoningEffortLevels: [
          { value: "low", label: "Low" },
          { value: "medium", label: "Medium" },
          { value: "high", label: "High", isDefault: true },
        ],
        supportsFastMode: false,
        supportsThinkingToggle: false,
        promptInjectedEffortLevels: [],
        contextWindowOptions: CLAUDE_DUAL_CONTEXT_WINDOW,
      },
    },
    {
      slug: "claude-sonnet-4-6",
      name: "Claude Sonnet 4.6",
      capabilities: { ...CLAUDE_EXTENDED_THINKING_CAPABILITIES, supportsFastMode: false },
    },
    {
      slug: "claude-haiku-4-5",
      name: "Claude Haiku 4.5",
      capabilities: { ...EMPTY_CAPABILITIES, supportsThinkingToggle: true },
    },
  ],
  gemini: [
    {
      slug: "auto-gemini-3",
      name: "Auto Gemini 3",
      capabilities: {
        reasoningEffortLevels: [
          { value: "HIGH", label: "High", isDefault: true },
          { value: "LOW", label: "Low" },
        ],
        supportsFastMode: false,
        supportsThinkingToggle: false,
        promptInjectedEffortLevels: [],
        contextWindowOptions: [],
      },
    },
    { slug: "auto-gemini-2.5", name: "Auto Gemini 2.5", capabilities: GEMINI_2_5_CAPABILITIES },
    {
      slug: "gemini-3.1-pro-preview",
      name: "Gemini 3.1 Pro Preview",
      capabilities: {
        reasoningEffortLevels: [
          { value: "HIGH", label: "High", isDefault: true },
          { value: "LOW", label: "Low" },
        ],
        supportsFastMode: false,
        supportsThinkingToggle: false,
        promptInjectedEffortLevels: [],
        contextWindowOptions: [],
      },
    },
    {
      slug: "gemini-3-flash-preview",
      name: "Gemini 3 Flash Preview",
      capabilities: {
        reasoningEffortLevels: [
          { value: "HIGH", label: "High", isDefault: true },
          { value: "LOW", label: "Low" },
        ],
        supportsFastMode: false,
        supportsThinkingToggle: false,
        promptInjectedEffortLevels: [],
        contextWindowOptions: [],
      },
    },
    {
      slug: "gemini-3.1-flash-lite-preview",
      name: "Gemini 3.1 Flash Lite Preview",
      capabilities: {
        reasoningEffortLevels: [
          { value: "HIGH", label: "High", isDefault: true },
          { value: "LOW", label: "Low" },
        ],
        supportsFastMode: false,
        supportsThinkingToggle: false,
        promptInjectedEffortLevels: [],
        contextWindowOptions: [],
      },
    },
    { slug: "gemini-2.5-pro", name: "Gemini 2.5 Pro", capabilities: GEMINI_2_5_CAPABILITIES },
    { slug: "gemini-2.5-flash", name: "Gemini 2.5 Flash", capabilities: GEMINI_2_5_CAPABILITIES },
    { slug: "gemini-2.5-flash-lite", name: "Gemini 2.5 Flash Lite", capabilities: GEMINI_2_5_CAPABILITIES },
  ],
  grok: [
    { slug: "grok-build-0.1", name: "Grok Build 0.1", capabilities: GROK_BUILD_CAPABILITIES },
    { slug: "grok-build", name: "Grok 4.3", capabilities: GROK_BUILD_CAPABILITIES },
  ],
  opencode: [
    { slug: "openai/gpt-5", name: "OpenAI GPT-5", capabilities: EMPTY_CAPABILITIES },
  ],
  kilo: [
    { slug: "kilo/kilo-auto/free", name: "Kilo Auto Free", capabilities: EMPTY_CAPABILITIES },
  ],
  pi: [],
  cursor: [
    { slug: "auto", name: "Auto", capabilities: EMPTY_CAPABILITIES },
    { slug: "composer-2", name: "Composer 2", capabilities: EMPTY_CAPABILITIES },
    {
      slug: "claude-opus-4-6",
      name: "Claude Opus 4.6",
      capabilities: {
        reasoningEffortLevels: [
          { value: "low", label: "Low" },
          { value: "medium", label: "Medium" },
          { value: "high", label: "High", isDefault: true },
          { value: "max", label: "Max" },
        ],
        supportsFastMode: false,
        supportsThinkingToggle: false,
        promptInjectedEffortLevels: [],
        contextWindowOptions: [],
      },
    },
    { slug: "gpt-5.3-codex", name: "GPT-5.3 Codex", capabilities: CODEX_GPT_5_CAPABILITIES },
    { slug: "gemini-3-pro", name: "Gemini 3 Pro", capabilities: EMPTY_CAPABILITIES },
  ],
};

export const DEFAULT_MODEL_BY_PROVIDER: Record<
  ProviderWithDefaultModel,
  ModelSlug
> = {
  codex: "gpt-5.5",
  claudeAgent: "claude-sonnet-4-6",
  cursor: "auto",
  gemini: "auto-gemini-3",
  grok: "grok-build",
  kilo: "kilo/kilo-auto/free",
  opencode: "openai/gpt-5",
};

export const DEFAULT_GIT_TEXT_GENERATION_MODEL = "gpt-5.4-mini" as const;

export const MODEL_SLUG_ALIASES_BY_PROVIDER: Record<
  ProviderKind,
  Record<string, ModelSlug>
> = {
  codex: {
    "5.5": "gpt-5.5",
    "5.4": "gpt-5.4",
    "5.3": "gpt-5.3-codex",
    "gpt-5.3": "gpt-5.3-codex",
    "5.3-spark": "gpt-5.3-codex-spark",
    "gpt-5.3-spark": "gpt-5.3-codex-spark",
  },
  claudeAgent: {
    fable: "claude-fable-5",
    "fable-5": "claude-fable-5",
    opus: "claude-opus-4-8",
    "opus-4.8": "claude-opus-4-8",
    "claude-opus-4.8": "claude-opus-4-8",
    "claude-opus-4-8-20260528": "claude-opus-4-8",
    "opus-4.7": "claude-opus-4-7",
    "claude-opus-4.7": "claude-opus-4-7",
    "claude-opus-4-7-20260416": "claude-opus-4-7",
    "opus-4.6": "claude-opus-4-6",
    "claude-opus-4.6": "claude-opus-4-6",
    "claude-opus-4-6-20251117": "claude-opus-4-6",
    "opus-4.5": "claude-opus-4-5",
    "claude-opus-4.5": "claude-opus-4-5",
    "claude-opus-4-5-20250120": "claude-opus-4-5",
    sonnet: "claude-sonnet-4-6",
    "sonnet-4.6": "claude-sonnet-4-6",
    "claude-sonnet-4.6": "claude-sonnet-4-6",
    "claude-sonnet-4-6-20251117": "claude-sonnet-4-6",
    haiku: "claude-haiku-4-5",
    "haiku-4.5": "claude-haiku-4-5",
    "claude-haiku-4.5": "claude-haiku-4-5",
    "claude-haiku-4-5-20251001": "claude-haiku-4-5",
  },
  cursor: {
    auto: "auto",
    composer: "composer-2",
    "composer-2": "composer-2",
    "composer-1.5": "composer-1.5",
    "composer-1": "composer-1.5",
    "opus-4.6": "claude-opus-4-6",
    "opus-4.6-thinking": "claude-opus-4-6",
    "gpt-5.3": "gpt-5.3-codex",
    "codex-5.3": "gpt-5.3-codex",
    "gemini-3": "gemini-3-pro",
  },
  gemini: {
    auto: "auto-gemini-3",
    "auto-gemini-3": "auto-gemini-3",
    "auto-gemini-2.5": "auto-gemini-2.5",
    "gemini-3-pro-preview": "gemini-3.1-pro-preview",
    "gemini-3.1-pro-preview": "gemini-3.1-pro-preview",
    "gemini-3-flash-preview": "gemini-3-flash-preview",
    "gemini-3.1-flash-lite-preview": "gemini-3.1-flash-lite-preview",
    "gemini-2.5-pro": "gemini-2.5-pro",
    "gemini-2.5-flash": "gemini-2.5-flash",
    "gemini-2.5-flash-lite": "gemini-2.5-flash-lite",
  },
  grok: {
    grok: "grok-build-0.1",
    build: "grok-build-0.1",
    "grok-build-0.1": "grok-build-0.1",
    "grok-build": "grok-build",
    "4.3": "grok-build",
    "grok-4": "grok-build",
    "grok-4.3": "grok-build",
    "grok-latest": "grok-build",
    "grok-code-fast": "grok-build-0.1",
    "grok-code-fast-1": "grok-build-0.1",
    "grok-code-fast-1-0825": "grok-build-0.1",
    "code-fast": "grok-build-0.1",
  },
  kilo: {},
  opencode: {},
  pi: {},
};

export const MODEL_CAPABILITIES_INDEX = Object.fromEntries(
  Object.entries(MODEL_OPTIONS_BY_PROVIDER).map(([provider, models]) => [
    provider,
    Object.fromEntries(models.map((m) => [m.slug, m.capabilities])),
  ]),
) as unknown as Record<ProviderKind, Record<string, ModelCapabilities>>;

export const PROVIDER_DISPLAY_NAMES: Record<ProviderKind, string> = {
  codex: "Codex",
  claudeAgent: "Claude",
  cursor: "Cursor",
  gemini: "Gemini",
  grok: "Grok",
  kilo: "Kilo",
  opencode: "OpenCode",
  pi: "Pi",
};

// Re-export types referenced by the model surface so vendored UI imports of
// `ModelCapabilities`, `ModelSlug`, `ProviderWithDefaultModel`, `ProviderKind`
// resolve. The local `import type` above keeps `as const satisfies` blocks
// honest; the re-export makes the names visible at the barrel boundary.
export type {
  ModelCapabilities,
  ModelSlug,
  ProviderWithDefaultModel,
  ProviderKind,
} from "./orchestration";
