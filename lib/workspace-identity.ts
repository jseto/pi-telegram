/**
 * Durable Workspace identity value construction
 * Zones: workspace identity, session identity, key encoding
 * Owns exact session/CWD normalization, bounded keys and local instance-slot encoding.
 * Excludes allocation, bindings/targets, storage-reference approval, custody and effects.
 */
import { createHash } from "node:crypto";
import { posix, resolve } from "node:path";

export interface TelegramWorkspaceBindingIdentity {
  cwd: string;
  workspaceKey: string;
  /** Exact durable Pi session identity; absent only on legacy cwd-only bindings. */
  sessionId?: string;
  /** Full SHA-256 index component for session-qualified bindings. */
  sessionKey?: string;
  /** Immutable legacy binding-key component, not the displayed global letter. */
  instanceSlot: string;
  bindingKey: string;
  /** Profile-wide letter reserved by the transient claim. */
  slot?: string;
}

export const TELEGRAM_WORKSPACE_KEY_MAX_LENGTH = 180;
const TELEGRAM_SESSION_ID_MAX_LENGTH = 256;

export function normalizeTelegramSessionId(
  sessionId: string,
): string | undefined {
  if (typeof sessionId !== "string") return undefined;
  const normalized = sessionId.trim();
  return normalized &&
    Buffer.byteLength(normalized, "utf8") <= TELEGRAM_SESSION_ID_MAX_LENGTH
    ? normalized
    : undefined;
}

export function createTelegramSessionKey(
  sessionId: string,
): string | undefined {
  const normalized = normalizeTelegramSessionId(sessionId);
  return normalized
    ? createHash("sha256").update(normalized).digest("hex")
    : undefined;
}

export function normalizeTelegramWorkspacePath(
  cwd: string,
): string | undefined {
  const trimmed = cwd.trim();
  if (!trimmed) return undefined;
  const normalized = trimmed.startsWith("/")
    ? posix.normalize(trimmed)
    : resolve(trimmed).replaceAll("\\", "/");
  const withoutTrailingSeparators =
    normalized.length > 1 ? normalized.replace(/\/+$/u, "") : normalized;
  return process.platform === "win32"
    ? withoutTrailingSeparators.replace(
        /^([A-Z]):/u,
        (_, drive: string) => `${drive.toLowerCase()}:`,
      )
    : withoutTrailingSeparators;
}

export function createTelegramWorkspaceDirectoryKey(
  cwd: string,
): string | undefined {
  const normalized = normalizeTelegramWorkspacePath(cwd);
  if (!normalized) return undefined;
  const readable =
    normalized.replace(/[^\p{L}\p{N}._-]+/gu, "-").replace(/^-+|-+$/gu, "") ||
    "root";
  const candidate = `--${readable}--`;
  if (candidate.length <= TELEGRAM_WORKSPACE_KEY_MAX_LENGTH) return candidate;
  const digest = createHash("sha256")
    .update(normalized)
    .digest("hex")
    .slice(0, 12);
  const prefixLength = TELEGRAM_WORKSPACE_KEY_MAX_LENGTH - digest.length - 5;
  return `--${readable.slice(0, prefixLength)}-${digest}--`;
}

function createTelegramWorkspaceInstanceSlot(
  ordinal: number,
): string | undefined {
  if (!Number.isSafeInteger(ordinal) || ordinal < 0) return undefined;
  let value = ordinal + 1;
  let slot = "";
  while (value > 0) {
    value -= 1;
    slot = String.fromCharCode(97 + (value % 26)) + slot;
    value = Math.floor(value / 26);
  }
  return slot;
}

export function createTelegramWorkspaceBindingIdentityWithKey(
  cwd: string,
  workspaceKey: string,
  ordinal: number,
  sessionId?: string,
): TelegramWorkspaceBindingIdentity | undefined {
  const instanceSlot = createTelegramWorkspaceInstanceSlot(ordinal);
  if (!instanceSlot) return undefined;
  const normalizedSessionId =
    sessionId === undefined ? undefined : normalizeTelegramSessionId(sessionId);
  const sessionKey = normalizedSessionId
    ? createTelegramSessionKey(normalizedSessionId)
    : undefined;
  if (sessionId !== undefined && (!normalizedSessionId || !sessionKey)) {
    return undefined;
  }
  const legacyBindingKey =
    instanceSlot === "a" ? workspaceKey : `${workspaceKey}${instanceSlot}`;
  return {
    cwd,
    workspaceKey,
    ...(normalizedSessionId && sessionKey
      ? { sessionId: normalizedSessionId, sessionKey }
      : {}),
    instanceSlot,
    bindingKey: sessionKey
      ? `${legacyBindingKey}-s-${sessionKey}`
      : legacyBindingKey,
  };
}

export function createTelegramWorkspaceBindingIdentity(
  cwd: string,
  ordinal = 0,
  sessionId?: string,
): TelegramWorkspaceBindingIdentity | undefined {
  const normalized = normalizeTelegramWorkspacePath(cwd);
  const workspaceKey = normalized
    ? createTelegramWorkspaceDirectoryKey(normalized)
    : undefined;
  if (!normalized || !workspaceKey) return undefined;
  return createTelegramWorkspaceBindingIdentityWithKey(
    normalized,
    workspaceKey,
    ordinal,
    sessionId,
  );
}
