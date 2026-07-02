/**
 * Tier 3 — Keybinding domain.
 *
 * Hand-ported from MCode `packages/contracts/src/keybindings.ts` (Effect
 * Schema → plain TS types). Covers the keybinding-command union (static
 * commands + `script.<id>.run` template literal), the rule/shortcut/when-AST
 * triad, the resolved rule/config pair, and the thread-jump command list.
 *
 * Source of truth: /home/vibe-dev/mcode/packages/contracts/src/keybindings.ts
 */

export const MAX_KEYBINDING_VALUE_LENGTH = 64;
export const MAX_SCRIPT_ID_LENGTH = 24;

export const THREAD_JUMP_KEYBINDING_COMMANDS = [
  "thread.jump.1",
  "thread.jump.2",
  "thread.jump.3",
  "thread.jump.4",
  "thread.jump.5",
  "thread.jump.6",
  "thread.jump.7",
  "thread.jump.8",
  "thread.jump.9",
] as const;
export type ThreadJumpKeybindingCommand =
  (typeof THREAD_JUMP_KEYBINDING_COMMANDS)[number];

/** Pattern for the dynamic `script.<id>.run` command form. */
export const SCRIPT_RUN_COMMAND_PATTERN = /^script\.[a-z0-9][a-z0-9-]*\.run$/;

const STATIC_KEYBINDING_COMMANDS = [
  "sidebar.toggle",
  "sidebar.search",
  "sidebar.addProject",
  "sidebar.importThread",
  "terminal.toggle",
  "terminal.split",
  "terminal.splitRight",
  "terminal.splitLeft",
  "terminal.splitDown",
  "terminal.splitUp",
  "terminal.new",
  "terminal.close",
  "terminal.workspace.newFullWidth",
  "terminal.workspace.closeActive",
  "terminal.workspace.terminal",
  "terminal.workspace.chat",
  "browser.toggle",
  "diff.toggle",
  "composer.focus.toggle",
  "modelPicker.toggle",
  "traitsPicker.toggle",
  "settings.usage",
  "chat.new",
  "chat.newLatestProject",
  "chat.newChat",
  "chat.newLocal",
  "chat.newTerminal",
  "chat.newClaude",
  "chat.newCodex",
  "chat.newCursor",
  "chat.newGemini",
  "chat.split",
  "view.recent.next",
  "view.recent.previous",
  "thread.jump.1",
  "thread.jump.2",
  "thread.jump.3",
  "thread.jump.4",
  "thread.jump.5",
  "thread.jump.6",
  "thread.jump.7",
  "thread.jump.8",
  "thread.jump.9",
  "chat.visible.next",
  "chat.visible.previous",
  "editor.openFavorite",
] as const;

/**
 * Keybinding command — either a static command string or a
 * `script.<id>.run` template-literal form. The MCode source models this as
 * a Schema.Union over `Schema.Literals(STATIC_KEYBINDING_COMMANDS)` and a
 * template-literal pattern; here we use a branded string alias so the
 * runtime value is preserved while callers can still narrow via the const
 * tuple above.
 */
export type KeybindingCommand =
  | (typeof STATIC_KEYBINDING_COMMANDS)[number]
  | (string & { readonly __keybindingScriptCommand?: true });

export interface KeybindingRule {
  key: string;
  command: KeybindingCommand;
  when?: string;
}

export interface KeybindingShortcut {
  key: string;
  metaKey: boolean;
  ctrlKey: boolean;
  shiftKey: boolean;
  altKey: boolean;
  modKey: boolean;
}

export type KeybindingWhenNode =
  | { readonly type: "identifier"; readonly name: string }
  | { readonly type: "not"; readonly node: KeybindingWhenNode }
  | { readonly type: "and"; readonly left: KeybindingWhenNode; readonly right: KeybindingWhenNode }
  | { readonly type: "or"; readonly left: KeybindingWhenNode; readonly right: KeybindingWhenNode };

export interface ResolvedKeybindingRule {
  command: KeybindingCommand;
  shortcut: KeybindingShortcut;
  whenAst?: KeybindingWhenNode;
}

export type ResolvedKeybindingsConfig = readonly ResolvedKeybindingRule[];
