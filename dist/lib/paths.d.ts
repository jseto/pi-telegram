/**
 * Telegram bridge path resolution for Pi-compatible runtimes
 * Zones: telemetry paths, filesystem, runtime identity
 * Owns agent-dir detection and extension-local path derivation
 */
export declare const TELEGRAM_DEFAULT_PROFILE_NAME = "default";
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
export declare function resolveAgentDir(input?: TelegramAgentDirResolutionInput): string;
/**
 * Pure reference preflight against an independently approved resource path.
 * Reject aliases; never repair relative historical references or redirect storage.
 * Equality proves spelling only, not file identity, consumer closure or migration readiness.
 */
export declare function requireTelegramStoragePathReference(path: string, expectedPath: string): string;
/** Telegram bridge configuration file (<agentDir>/telegram.json). */
export declare function resolveTelegramConfigPath(): string;
/** Telegram bridge temporary directory (<agentDir>/tmp/pi-telegram); releases before 0.52.0 used `tmp/telegram`. */
export declare function resolveTelegramTempDir(agentDir?: string): string;
/** Consolidated-root service artifacts (transaction guards, staging and IPC), never session custody. */
export declare function resolveTelegramRuntimeDir(agentDir?: string): string;
/** Rotated shared runtime event segments, outside the persistent root-file census. */
export declare function resolveTelegramRuntimeLogsDir(agentDir?: string): string;
/** Consolidated runtime state for every logical profile (`tmp/pi-telegram/state.json`). */
export declare function resolveTelegramStatePath(agentDir?: string): string;
/** Flat attachment scratch directory (<agentDir>/tmp/pi-telegram/attachments). */
export declare function resolveTelegramAttachmentsDir(agentDir?: string): string;
/** Read-only pre-0.52.0 ownership file, consulted only to refuse a second poller beside a live older leader. */
export declare function resolveLegacyTelegramOwnersPath(agentDir?: string): string;
/** Telegram transport ownership store (<agentDir>/tmp/pi-telegram/owners.json). */
export declare function resolveTelegramOwnersPath(): string;
export declare function getTelegramProfilePathSuffix(profileName?: string): string;
export declare function resolveTelegramProfileTempFilePath(baseName: string, extension: string, agentDir?: string, profileName?: string): string;
export declare function getTelegramDiagnosticsDisplayPaths(_profileName?: string): {
    state: string;
    logs: string;
};
/** Durable Workspace admission ledger (<agentDir>/tmp/pi-telegram/workspace-admission[.<profile>].json). */
export declare function resolveTelegramWorkspaceAdmissionPath(agentDir?: string, profileName?: string): string;
/** Profile-only callback shape; binds storage to the configured agent directory. */
export declare function resolveTelegramWorkspaceAdmissionPathForProfile(profileName?: string): string;
/** Consolidated non-session service journal (`journals/<kind>.<profile sha256>.json`) and its runtime namespace. */
export declare function resolveTelegramServiceJournalStorage(kind: "thread-cleanup" | "channel-posts", agentDir?: string, profileName?: string): {
    path: string;
    runtimeDir: string;
};
/** Bind one non-session journal to its exact colocated-root service namespace, never a format fallback. */
export declare function getTelegramJournalPublicationPaths(path: string, runtimeDir?: string): {
    transactionPath: string;
    temporaryBasePath: string;
};
/** Durable inactive Thread cleanup work-set journal. */
export declare function resolveTelegramThreadCleanupWorkPath(agentDir?: string, profileName?: string): string;
/** Durable agent-authored channel post journal. */
export declare function resolveTelegramChannelPostJournalPath(agentDir?: string, profileName?: string): string;
/** Durable inbound update journal (<agentDir>/tmp/pi-telegram/inbox[.<profile>].json). */
export declare function resolveTelegramUpdateJournalPath(agentDir?: string, profileName?: string): string;
/** Profile-only callback shape; binds storage to the configured agent directory. */
export declare function resolveTelegramUpdateJournalPathForProfile(profileName?: string): string;
/** Durable follower delivery journal, isolated by stable recipient binding. */
export declare function resolveTelegramFollowerJournalPath(recipientBindingKey: string, agentDir?: string, profileName?: string): string;
/**
 * Directory name for a session id. Lowercase letters, digits, `_`, `-` and inner `.` stay verbatim, so every real
 * (UUID-shaped) id is unchanged; any other UTF-8 byte is percent-encoded. Uppercase is encoded so case-insensitive
 * filesystems never merge two sessions; a leading or trailing dot and Windows reserved names are encoded so the name
 * can never be `.`/`..`, hidden, or a device. The encoding is reversible; an empty or over-long result is refused.
 */
export declare function encodeTelegramSessionDirectoryName(sessionId: string): string | undefined;
/** Inverse of `encodeTelegramSessionDirectoryName`; undefined unless the name is exactly canonical. */
export declare function decodeTelegramSessionDirectoryName(name: string): string | undefined;
/** Root of every session folder (<agentDir>/tmp/pi-telegram/sessions). */
export declare function resolveTelegramSessionsDir(agentDir?: string): string;
/** One session's folder (<agentDir>/tmp/pi-telegram/sessions/<session id>). */
export declare function resolveTelegramSessionDir(sessionId: string, agentDir?: string): string;
/** Hash naming one recipient incarnation inside its session folder; unchanged from the former follower inbox. */
export declare function getTelegramRecipientJournalHash(recipientBindingKey: string): string;
/** Durable per-session journal (`sessions/<session id>/journal.<hash>[.<profile>].json`); the name never reveals a role. */
export declare function resolveTelegramSessionJournalPath(sessionId: string, recipientBindingKey: string, agentDir?: string, profileName?: string): string;
/** Leader polling journal hosted by the session that first led (`sessions/<id>/inbox[.<profile>].json`); owners.json names it. */
export declare function resolveTelegramSessionPollingJournalPath(sessionId: string, agentDir?: string, profileName?: string): string;
/** Accept only a canonical `sessions/<id>/inbox[.<profile>].json` beneath this agent's runtime. */
export declare function isTelegramSessionPollingJournalPath(path: string, agentDir?: string): boolean;
/** Runtime event log (<agentDir>/tmp/pi-telegram/logs.jsonl). */
export declare function resolveTelegramRuntimeLogPath(agentDir?: string): string;
/** Bounded previous shared log segment below `logs/`. */
export declare function resolveTelegramPreviousSharedRuntimeLogPath(agentDir?: string): string;
