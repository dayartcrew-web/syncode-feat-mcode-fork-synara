/**
 * Tier 3 — Misc / cross-cutting symbols.
 *
 * Catch-all for the remaining MISSING-SYMBOLS entries that don't fit a
 * single domain module: editor metadata, context-menu item, tool-lifecycle
 * item type + predicate, user-input question, upload attachment union,
 * filesystem browse result, provider send-turn attachment, and the various
 * `MAX_*` / `DEFAULT_*` constants. Where the MCode source has a real type
 * definition, it is hand-ported; where the symbol is opaque / hard to
 * infer, it is stubbed with a permissive shape so the import resolves
 * (marked `STUB(T5c): …`).
 *
 * Sources of truth:
 *   /home/vibe-dev/mcode/packages/contracts/src/editor.ts (EDITORS, EditorId)
 *   /home/vibe-dev/mcode/packages/contracts/src/ipc.ts (ContextMenuItem)
 *   /home/vibe-dev/mcode/packages/contracts/src/filesystem.ts
 *   vendored apps/web/src usage sites for opaque symbols
 */

import type { MessageId } from "../ids";
import type { NonNegativeInt, TrimmedNonEmptyString } from "./base";
import type { ProviderKind } from "./orchestration";

// ─── Editor metadata ──────────────────────────────────────────────────

export type EditorLaunchStyle =
  | "direct-path"
  | "goto"
  | "line-column"
  | "terminal-working-directory";

export interface EditorDefinition {
  readonly id: string;
  readonly label: string;
  readonly commands: readonly [string, ...string[]] | null;
  readonly macApplications?: readonly [string, ...string[]];
  readonly launchStyle: EditorLaunchStyle;
}

/**
 * Editor catalog. Source of truth: MCode `packages/contracts/src/editor.ts`
 * `EDITORS` const. The full list is long; only the `id` values are needed
 * for the `EditorId` literal union below. We expose the full array so any
 * vendored UI site that reads editor metadata resolves at runtime.
 */
export const EDITORS: readonly EditorDefinition[] = [
  { id: "cursor", label: "Cursor", commands: ["cursor"], macApplications: ["Cursor"], launchStyle: "goto" },
  { id: "trae", label: "Trae", commands: ["trae"], macApplications: ["Trae"], launchStyle: "goto" },
  { id: "vscode", label: "VS Code", commands: ["code"], macApplications: ["Visual Studio Code"], launchStyle: "goto" },
  { id: "vscode-insiders", label: "VS Code Insiders", commands: ["code-insiders"], macApplications: ["Visual Studio Code - Insiders"], launchStyle: "goto" },
  { id: "vscodium", label: "VSCodium", commands: ["codium"], macApplications: ["VSCodium"], launchStyle: "goto" },
  { id: "zed", label: "Zed", commands: ["zed", "zeditor"], macApplications: ["Zed"], launchStyle: "direct-path" },
  { id: "windsurf", label: "Windsurf", commands: ["windsurf"], macApplications: ["Windsurf"], launchStyle: "goto" },
  { id: "sublime", label: "Sublime Text", commands: ["subl"], macApplications: ["Sublime Text"], launchStyle: "direct-path" },
  { id: "antigravity", label: "Antigravity", commands: ["agy"], macApplications: ["Antigravity"], launchStyle: "goto" },
  { id: "ghostty", label: "Ghostty", commands: ["ghostty"], macApplications: ["Ghostty"], launchStyle: "terminal-working-directory" },
  {
    id: "terminal", label: "Terminal",
    commands: ["wt", "gnome-terminal", "kgx", "konsole", "xfce4-terminal", "tilix", "terminator", "x-terminal-emulator", "kitty", "alacritty", "wezterm", "cmd", "powershell", "pwsh"],
    macApplications: ["Terminal"], launchStyle: "terminal-working-directory",
  },
  { id: "warp", label: "Warp", commands: ["warp"], macApplications: ["Warp"], launchStyle: "terminal-working-directory" },
  { id: "xcode", label: "Xcode", commands: ["xed"], macApplications: ["Xcode"], launchStyle: "direct-path" },
  { id: "idea", label: "IntelliJ IDEA", commands: ["idea", "idea64", "idea.sh", "intellij-idea"], macApplications: ["IntelliJ IDEA", "IntelliJ IDEA Ultimate", "IntelliJ IDEA Community Edition", "IntelliJ IDEA CE"], launchStyle: "line-column" },
  { id: "webstorm", label: "WebStorm", commands: ["webstorm", "wstorm", "webstorm64", "webstorm.sh"], macApplications: ["WebStorm"], launchStyle: "line-column" },
  { id: "pycharm", label: "PyCharm", commands: ["pycharm", "charm", "pycharm64", "pycharm.sh", "pycharm-professional"], macApplications: ["PyCharm", "PyCharm Professional", "PyCharm CE"], launchStyle: "line-column" },
  { id: "phpstorm", label: "PhpStorm", commands: ["phpstorm", "pstorm", "phpstorm64", "phpstorm.sh"], macApplications: ["PhpStorm"], launchStyle: "line-column" },
  { id: "goland", label: "GoLand", commands: ["goland", "goland64", "goland.sh"], macApplications: ["GoLand"], launchStyle: "line-column" },
  { id: "clion", label: "CLion", commands: ["clion", "clion64", "clion.sh"], macApplications: ["CLion"], launchStyle: "line-column" },
  { id: "rider", label: "Rider", commands: ["rider", "rider64", "rider.sh"], macApplications: ["Rider"], launchStyle: "line-column" },
  { id: "rubymine", label: "RubyMine", commands: ["rubymine", "mine", "rubymine64", "rubymine.sh"], macApplications: ["RubyMine"], launchStyle: "line-column" },
  { id: "datagrip", label: "DataGrip", commands: ["datagrip", "datagrip64", "datagrip.sh"], macApplications: ["DataGrip"], launchStyle: "line-column" },
  { id: "rustrover", label: "RustRover", commands: ["rustrover", "rustrover64", "rustrover.sh"], macApplications: ["RustRover"], launchStyle: "line-column" },
  { id: "android-studio", label: "Android Studio", commands: ["studio", "android-studio", "studio.sh"], macApplications: ["Android Studio"], launchStyle: "line-column" },
  { id: "file-manager", label: "File Manager", commands: null, launchStyle: "direct-path" },
  { id: "system-default", label: "Default app", commands: null, launchStyle: "direct-path" },
];

/** Editor id union (built-in editor ids + arbitrary string fallback). */
export type EditorId = string;

// ─── Context menu item ────────────────────────────────────────────────
// (also exported from shell.ts; re-declared here so vendored UI importing
// `ContextMenuItem` from `@t3tools/contracts` resolves through this barrel
// regardless of source module.)

export interface ContextMenuItem<T extends string = string> {
  id: T;
  label: string;
  separatorBefore?: boolean;
  destructive?: boolean;
}

// ─── Tool lifecycle ───────────────────────────────────────────────────
// MCode's tool lifecycle item type is a string-literal union keyed on the
// rendered tool-call kinds; the predicate narrows a candidate string
// against the union. Source: vendored apps/web/src/lib/toolCallLabel.ts +
// toolCallDetails.ts.

export type ToolLifecycleItemType =
  | "command"
  | "file-read"
  | "file-write"
  | "file-change"
  | "shell"
  | "search"
  | "browser"
  | "plan"
  | "diagnostic"
  | "other";

const TOOL_LIFECYCLE_ITEM_TYPES: readonly ToolLifecycleItemType[] = [
  "command", "file-read", "file-write", "file-change",
  "shell", "search", "browser", "plan", "diagnostic", "other",
];

export function isToolLifecycleItemType(value: unknown): value is ToolLifecycleItemType {
  return typeof value === "string" && (TOOL_LIFECYCLE_ITEM_TYPES as readonly string[]).includes(value);
}

// ─── User-input question (provider-issued prompt for user response) ───
// STUB(T5c): MCode's UserInputQuestion has a richer discriminated shape
// (free-text / single-choice / multi-choice). The vendored UI references
// it as the pendingUserInput store payload. A permissive interface here
// resolves the import; T5c should port the real union.
export interface UserInputQuestion {
  readonly prompt?: string;
  readonly kind?: string;
  readonly placeholder?: string;
  readonly options?: readonly { readonly value: string; readonly label?: string }[];
  readonly defaultAnswer?: string | readonly string[];
  readonly required?: boolean;
  readonly [key: string]: unknown;
}

// ─── Upload attachment union ──────────────────────────────────────────
// The vendored UI's composer serializes attachments into these shapes
// before sending over the WS turn-start. MCode's union lives in
// orchestration.ts; mirrored here as a tagged union.

export interface UploadChatImageAttachment {
  readonly type: "image";
  readonly name: TrimmedNonEmptyString;
  readonly mimeType: TrimmedNonEmptyString;
  readonly sizeBytes: NonNegativeInt;
  readonly dataUrl: TrimmedNonEmptyString;
}
export interface UploadChatFileAttachment {
  readonly type: "file";
  readonly name: TrimmedNonEmptyString;
  readonly mimeType: TrimmedNonEmptyString;
  readonly sizeBytes: NonNegativeInt;
  readonly dataUrl: TrimmedNonEmptyString;
}
export interface UploadChatAssistantSelectionAttachment {
  readonly type: "assistant-selection";
  readonly assistantMessageId: MessageId;
  readonly text: TrimmedNonEmptyString;
}
export type UploadChatAttachment =
  | UploadChatImageAttachment
  | UploadChatFileAttachment
  | UploadChatAssistantSelectionAttachment;

// ─── Filesystem browse result ─────────────────────────────────────────
// (shell.ts declares this opaque; re-declared here with a permissive shape
// so vendored UI property-access type-checks. STUB(T5c): real directory
// entry shape pending — Syncode's filesystem crate exposes a simpler
// recursive listing than MCode's Electron-only browser.)
export interface FilesystemBrowseResult {
  readonly path: string;
  readonly directories: readonly string[];
  readonly files: readonly string[];
  readonly [key: string]: unknown;
}

// ─── Provider send-turn attachment caps (re-export for convenience) ───
// These are also exported from tier3/orchestration; the barrel re-exports
// them from there.

// ─── Default provider kind ────────────────────────────────────────────
export const DEFAULT_PROVIDER_KIND: ProviderKind = "codex";
