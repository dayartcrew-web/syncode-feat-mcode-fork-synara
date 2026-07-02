import {
  MAX_KEYBINDING_VALUE_LENGTH,
  SCRIPT_RUN_COMMAND_PATTERN,
  type KeybindingCommand,
  type KeybindingRule,
  type ResolvedKeybindingsConfig,
} from "@t3tools/contracts";

export const PROJECT_SCRIPT_KEYBINDING_INVALID_MESSAGE = "Invalid keybinding.";

function normalizeProjectScriptKeybindingInput(
  keybinding: string | null | undefined,
): string | null {
  const trimmed = keybinding?.trim() ?? "";
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * Validate a candidate keybinding rule the way the former Effect
 * `KeybindingRuleSchema` did: the `key` must be non-empty and within
 * `MAX_KEYBINDING_VALUE_LENGTH`, and the `command` must match the
 * `script.<id>.run` pattern (the only dynamic command project scripts emit).
 */
function isValidKeybindingRule(key: string, command: KeybindingCommand): boolean {
  if (key.length === 0 || key.length > MAX_KEYBINDING_VALUE_LENGTH) {
    return false;
  }
  // Project-script keybindings always target a `script.<id>.run` command; the
  // static `KeybindingCommand` literals are not produced by this path.
  return typeof command === "string" && SCRIPT_RUN_COMMAND_PATTERN.test(command);
}

export function decodeProjectScriptKeybindingRule(input: {
  keybinding: string | null | undefined;
  command: KeybindingCommand;
}): KeybindingRule | null {
  const normalizedKey = normalizeProjectScriptKeybindingInput(input.keybinding);
  if (!normalizedKey) return null;

  // The original Schema returned None on shape failure and threw on a
  // structural rule violation. We mirror that: invalid key length or command
  // pattern throws.
  if (!isValidKeybindingRule(normalizedKey, input.command)) {
    throw new Error(PROJECT_SCRIPT_KEYBINDING_INVALID_MESSAGE);
  }
  return { key: normalizedKey, command: input.command };
}

export function keybindingValueForCommand(
  keybindings: ResolvedKeybindingsConfig,
  command: KeybindingCommand,
): string | null {
  for (let index = keybindings.length - 1; index >= 0; index -= 1) {
    const binding = keybindings[index];
    if (!binding || binding.command !== command) continue;

    const parts: string[] = [];
    if (binding.shortcut.modKey) parts.push("mod");
    if (binding.shortcut.ctrlKey) parts.push("ctrl");
    if (binding.shortcut.metaKey) parts.push("meta");
    if (binding.shortcut.altKey) parts.push("alt");
    if (binding.shortcut.shiftKey) parts.push("shift");
    const keyToken =
      binding.shortcut.key === " "
        ? "space"
        : binding.shortcut.key === "escape"
          ? "esc"
          : binding.shortcut.key;
    parts.push(keyToken);
    return parts.join("+");
  }
  return null;
}
