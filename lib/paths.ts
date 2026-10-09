/**
 * Telegram bridge path resolution for Pi-compatible runtimes
 * Zones: telemetry paths, filesystem, runtime identity
 * Owns agent-dir detection and extension-local path derivation
 */

import { createHash } from "node:crypto";
import { realpathSync } from "node:fs";
import { homedir } from "node:os";
import {
  basename,
  dirname,
  isAbsolute,
  join,
  relative,
  resolve,
  sep,
} from "node:path";

export const TELEGRAM_DEFAULT_PROFILE_NAME = "default";

export interface TelegramAgentDirResolutionInput {
  env?: Partial<Pick<NodeJS.ProcessEnv, "PI_CODING_AGENT_DIR">>;
  execPath?: string;
  argv?: readonly string[];
}

/**
 * Resolve the agent data directory for the current Pi-compatible runtime.
 *
 * Precedence:
 * 1. `PI_CODING_AGENT_DIR` env variable, when explicitly set.
 * 2. Detect Pi-compatible runtime identity from the executable or argv[1]
 *    (e.g. OMP vs standard Pi agent).
 * 3. Fallback: `~/.pi/agent`.
 */
export function resolveAgentDir(
  input: TelegramAgentDirResolutionInput = {},
): string {
  const env = input.env ?? process.env;
  if (env.PI_CODING_AGENT_DIR) return resolve(env.PI_CODING_AGENT_DIR);
  const execPath = input.execPath ?? process.execPath;
  const argv = input.argv ?? process.argv;
  const execBasename = execPath.toLowerCase().split(/[\\/]/u).pop() ?? "";
  const argv1Last = (argv[1] ?? "").toLowerCase().split(/[\\/]/u).pop() ?? "";
  if (execBasename.startsWith("omp") || argv1Last.startsWith("omp")) {
    return join(homedir(), ".omp", "agent");
  }
  return join(homedir(), ".pi", "agent");
}

/**
 * Pure reference preflight against an independently approved resource path.
 * Reject aliases; never repair relative historical references or redirect storage.
 * Equality proves spelling only, not file identity, consumer closure or migration readiness.
 */
export function requireTelegramStoragePathReference(
  path: string,
  expectedPath: string,
): string {
  if (
    typeof path !== "string" ||
    typeof expectedPath !== "string" ||
    !isAbsolute(path) ||
    resolve(path) !== path ||
    !isAbsolute(expectedPath) ||
    resolve(expectedPath) !== expectedPath ||
    path !== expectedPath
  ) {
    throw new Error(
      "Telegram storage reference does not match its approved absolute path.",
    );
  }
  return path;
}

/** Telegram bridge configuration file (<agentDir>/telegram.json). */
export function resolveTelegramConfigPath(): string {
  return join(resolveAgentDir(), "telegram.json");
}

/**
 * Resolve symlinks in the longest existing prefix of `path` and keep the missing suffix. Strict journal reads require
 * canonical anchors, so a symlinked agent directory (for example macOS `/var` → `/private/var`) must not leak in.
 * Relative input is returned unchanged so callers' exact-absolute-path guards still reject it. On Windows only
 * drive-letter and UNC paths resolve; resolving a drive-less root would add the current drive.
 */
function canonicalizeExistingPrefix(path: string): string {
  if (
    !isAbsolute(path) ||
    (process.platform === "win32" && !/^(?:[a-zA-Z]:[\\/]|\\\\)/u.test(path))
  )
    return path;
  const absolute = resolve(path);
  const missing: string[] = [];
  for (let current = absolute; ; current = dirname(current)) {
    try {
      return join(realpathSync(current), ...missing.reverse());
    } catch (error) {
      if (
        (error as { code?: unknown }).code !== "ENOENT" ||
        dirname(current) === current
      )
        return absolute;
      missing.push(basename(current));
    }
  }
}

/** Telegram bridge temporary directory (<agentDir>/tmp/pi-telegram); releases before 0.52.0 used `tmp/telegram`. */
export function resolveTelegramTempDir(agentDir = resolveAgentDir()): string {
  return join(canonicalizeExistingPrefix(agentDir), "tmp", "pi-telegram");
}

/** Consolidated-root service artifacts (transaction guards, staging and IPC), never session custody. */
export function resolveTelegramRuntimeDir(
  agentDir = resolveAgentDir(),
): string {
  return join(resolveTelegramTempDir(agentDir), "runtime");
}

/** Rotated shared runtime event segments, outside the persistent root-file census. */
export function resolveTelegramRuntimeLogsDir(
  agentDir = resolveAgentDir(),
): string {
  return join(resolveTelegramTempDir(agentDir), "logs");
}

/** Consolidated runtime state for every logical profile (`tmp/pi-telegram/state.json`). */
export function resolveTelegramStatePath(agentDir = resolveAgentDir()): string {
  return join(resolveTelegramTempDir(agentDir), "state.json");
}

/** Flat attachment scratch directory (<agentDir>/tmp/pi-telegram/attachments). */
export function resolveTelegramAttachmentsDir(
  agentDir = resolveAgentDir(),
): string {
  return join(resolveTelegramTempDir(agentDir), "attachments");
}

/** Read-only pre-0.52.0 ownership file, consulted only to refuse a second poller beside a live older leader. */
export function resolveLegacyTelegramOwnersPath(
  agentDir = resolveAgentDir(),
): string {
  return join(agentDir, "tmp", "telegram", "owners.json");
}

/** Telegram transport ownership store (<agentDir>/tmp/pi-telegram/owners.json). */
export function resolveTelegramOwnersPath(): string {
  return join(resolveTelegramTempDir(), "owners.json");
}

export function getTelegramProfilePathSuffix(profileName?: string): string {
  if (!profileName || profileName === TELEGRAM_DEFAULT_PROFILE_NAME) return "";
  return `.${profileName.replace(/[^a-zA-Z0-9._-]+/g, "_")}`;
}

export function resolveTelegramProfileTempFilePath(
  baseName: string,
  extension: string,
  agentDir = resolveAgentDir(),
  profileName?: string,
): string {
  return join(
    resolveTelegramTempDir(agentDir),
    `${baseName}${getTelegramProfilePathSuffix(profileName)}.${extension}`,
  );
}

export function getTelegramDiagnosticsDisplayPaths(_profileName?: string): {
  state: string;
  logs: string;
} {
  return {
    state: "~/.pi/agent/tmp/pi-telegram/state.json",
    logs: "~/.pi/agent/tmp/pi-telegram/logs.jsonl",
  };
}

/** Durable Workspace admission ledger (<agentDir>/tmp/pi-telegram/workspace-admission[.<profile>].json). */
export function resolveTelegramWorkspaceAdmissionPath(
  agentDir = resolveAgentDir(),
  profileName?: string,
): string {
  return resolveTelegramProfileTempFilePath(
    "workspace-admission",
    "json",
    agentDir,
    profileName,
  );
}

/** Profile-only callback shape; binds storage to the configured agent directory. */
export function resolveTelegramWorkspaceAdmissionPathForProfile(
  profileName?: string,
): string {
  return resolveTelegramWorkspaceAdmissionPath(resolveAgentDir(), profileName);
}

/** Consolidated non-session service journal (`journals/<kind>.<profile sha256>.json`) and its runtime namespace. */
export function resolveTelegramServiceJournalStorage(
  kind: "thread-cleanup" | "channel-posts",
  agentDir = resolveAgentDir(),
  profileName = TELEGRAM_DEFAULT_PROFILE_NAME,
): { path: string; runtimeDir: string } {
  const profileHash = createHash("sha256").update(profileName).digest("hex");
  return {
    path: join(
      resolveTelegramTempDir(agentDir),
      "journals",
      `${kind}.${profileHash}.json`,
    ),
    runtimeDir: resolveTelegramRuntimeDir(agentDir),
  };
}

/** Bind one non-session journal to its exact colocated-root service namespace, never a format fallback. */
export function getTelegramJournalPublicationPaths(
  path: string,
  runtimeDir?: string,
): {
  transactionPath: string;
  temporaryBasePath: string;
} {
  if (runtimeDir === undefined)
    return { transactionPath: `${path}.transaction`, temporaryBasePath: path };
  requireTelegramStoragePathReference(path, resolve(path));
  requireTelegramStoragePathReference(
    runtimeDir,
    join(dirname(dirname(path)), "runtime"),
  );
  if (basename(dirname(path)) !== "journals")
    throw new Error(
      "Telegram service journal must use its journals namespace.",
    );
  return {
    transactionPath: join(runtimeDir, `${basename(path)}.transaction`),
    temporaryBasePath: join(runtimeDir, basename(path)),
  };
}

/** Durable inactive Thread cleanup work-set journal. */
export function resolveTelegramThreadCleanupWorkPath(
  agentDir = resolveAgentDir(),
  profileName?: string,
): string {
  return resolveTelegramProfileTempFilePath(
    "thread-cleanup",
    "json",
    agentDir,
    profileName,
  );
}

/** Durable agent-authored channel post journal. */
export function resolveTelegramChannelPostJournalPath(
  agentDir = resolveAgentDir(),
  profileName?: string,
): string {
  return resolveTelegramProfileTempFilePath(
    "channel-posts",
    "json",
    agentDir,
    profileName,
  );
}

/** Durable inbound update journal (<agentDir>/tmp/pi-telegram/inbox[.<profile>].json). */
export function resolveTelegramUpdateJournalPath(
  agentDir = resolveAgentDir(),
  profileName?: string,
): string {
  return resolveTelegramProfileTempFilePath(
    "inbox",
    "json",
    agentDir,
    profileName,
  );
}

/** Profile-only callback shape; binds storage to the configured agent directory. */
export function resolveTelegramUpdateJournalPathForProfile(
  profileName?: string,
): string {
  return resolveTelegramUpdateJournalPath(resolveAgentDir(), profileName);
}

/** Durable follower delivery journal, isolated by stable recipient binding. */
export function resolveTelegramFollowerJournalPath(
  recipientBindingKey: string,
  agentDir = resolveAgentDir(),
  profileName?: string,
): string {
  if (!recipientBindingKey) {
    throw new Error("Telegram follower journal binding key is required.");
  }
  const bindingHash = createHash("sha256")
    .update(recipientBindingKey)
    .digest("hex")
    .slice(0, 16);
  return resolveTelegramProfileTempFilePath(
    `follower-inbox-${bindingHash}`,
    "json",
    agentDir,
    profileName,
  );
}

const TELEGRAM_SESSION_DIRECTORY_MAX_LENGTH = 200;
const WINDOWS_RESERVED_NAME =
  /^(?:con|prn|aux|nul|com[0-9]|lpt[0-9])(?:\..*)?$/u;

/**
 * Directory name for a session id. Lowercase letters, digits, `_`, `-` and inner `.` stay verbatim, so every real
 * (UUID-shaped) id is unchanged; any other UTF-8 byte is percent-encoded. Uppercase is encoded so case-insensitive
 * filesystems never merge two sessions; a leading or trailing dot and Windows reserved names are encoded so the name
 * can never be `.`/`..`, hidden, or a device. The encoding is reversible; an empty or over-long result is refused.
 */
export function encodeTelegramSessionDirectoryName(
  sessionId: string,
): string | undefined {
  if (typeof sessionId !== "string" || sessionId.length === 0) return undefined;
  const bytes = Buffer.from(sessionId, "utf8");
  if (bytes.toString("utf8") !== sessionId) return undefined;
  let name = "";
  bytes.forEach((byte, index) => {
    const char = String.fromCharCode(byte);
    const safe =
      /[a-z0-9_-]/u.test(char) ||
      (char === "." && index > 0 && index < bytes.length - 1);
    name += safe
      ? char
      : `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
  });
  if (WINDOWS_RESERVED_NAME.test(name))
    name = `%${name.charCodeAt(0).toString(16).toUpperCase().padStart(2, "0")}${name.slice(1)}`;
  return name.length <= TELEGRAM_SESSION_DIRECTORY_MAX_LENGTH
    ? name
    : undefined;
}

/** Inverse of `encodeTelegramSessionDirectoryName`; undefined unless the name is exactly canonical. */
export function decodeTelegramSessionDirectoryName(
  name: string,
): string | undefined {
  if (!/^[a-z0-9_.%A-F-]+$/u.test(name)) return undefined;
  const bytes: number[] = [];
  for (let index = 0; index < name.length; index++) {
    if (name[index] === "%") {
      const hex = name.slice(index + 1, index + 3);
      if (!/^[0-9A-F]{2}$/u.test(hex)) return undefined;
      bytes.push(Number.parseInt(hex, 16));
      index += 2;
    } else bytes.push(name.charCodeAt(index));
  }
  const decoded = Buffer.from(bytes).toString("utf8");
  return encodeTelegramSessionDirectoryName(decoded) === name
    ? decoded
    : undefined;
}

/** Root of every session folder (<agentDir>/tmp/pi-telegram/sessions). */
export function resolveTelegramSessionsDir(
  agentDir = resolveAgentDir(),
): string {
  return join(resolveTelegramTempDir(agentDir), "sessions");
}

/** One session's folder (<agentDir>/tmp/pi-telegram/sessions/<session id>). */
export function resolveTelegramSessionDir(
  sessionId: string,
  agentDir = resolveAgentDir(),
): string {
  const name = encodeTelegramSessionDirectoryName(sessionId);
  if (!name)
    throw new Error("Telegram session directory requires a usable session id.");
  return join(resolveTelegramSessionsDir(agentDir), name);
}

/** Hash naming one recipient incarnation inside its session folder; unchanged from the former follower inbox. */
export function getTelegramRecipientJournalHash(
  recipientBindingKey: string,
): string {
  if (!recipientBindingKey)
    throw new Error("Telegram session journal binding key is required.");
  return createHash("sha256")
    .update(recipientBindingKey)
    .digest("hex")
    .slice(0, 16);
}

/** Durable per-session journal (`sessions/<session id>/journal.<hash>[.<profile>].json`); the name never reveals a role. */
export function resolveTelegramSessionJournalPath(
  sessionId: string,
  recipientBindingKey: string,
  agentDir = resolveAgentDir(),
  profileName?: string,
): string {
  return join(
    resolveTelegramSessionDir(sessionId, agentDir),
    `journal.${getTelegramRecipientJournalHash(recipientBindingKey)}${getTelegramProfilePathSuffix(profileName)}.json`,
  );
}

/** Leader polling journal hosted by the session that first led (`sessions/<id>/inbox[.<profile>].json`); owners.json names it. */
export function resolveTelegramSessionPollingJournalPath(
  sessionId: string,
  agentDir = resolveAgentDir(),
  profileName?: string,
): string {
  return join(
    resolveTelegramSessionDir(sessionId, agentDir),
    `inbox${getTelegramProfilePathSuffix(profileName)}.json`,
  );
}

/** Accept only a canonical `sessions/<id>/inbox[.<profile>].json` beneath this agent's runtime. */
export function isTelegramSessionPollingJournalPath(
  path: string,
  agentDir = resolveAgentDir(),
): boolean {
  const parts = relative(resolveTelegramSessionsDir(agentDir), path).split(sep);
  return (
    isAbsolute(path) &&
    parts.length === 2 &&
    decodeTelegramSessionDirectoryName(parts[0]!) !== undefined &&
    /^inbox(?:\.[a-zA-Z0-9._-]+)?\.json$/u.test(parts[1]!)
  );
}

/** Runtime event log (<agentDir>/tmp/pi-telegram/logs.jsonl). */
export function resolveTelegramRuntimeLogPath(
  agentDir = resolveAgentDir(),
): string {
  return resolveTelegramProfileTempFilePath("logs", "jsonl", agentDir);
}

/** Bounded previous shared log segment below `logs/`. */
export function resolveTelegramPreviousSharedRuntimeLogPath(
  agentDir = resolveAgentDir(),
): string {
  return join(resolveTelegramRuntimeLogsDir(agentDir), "logs._prev.jsonl");
}
