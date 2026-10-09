/**
 * Workspace slot rotation
 * Zones: telegram, workspace identity, lifecycle
 * Owns fail-closed protection, demand-driven pressure retirement, exact-intent admission,
 * successor recovery, and fenced one-shot deletion before durable slot reuse.
 */

import { isDeepStrictEqual } from "node:util";

import type {
  TelegramJournalNamespaceInspection,
  TelegramUpdateJournalDeadQueueOwnerRecoveryInput,
  TelegramUpdateJournalDeadQueueOwnerRecoveryResult,
  TelegramUpdateJournalEntry,
  TelegramUpdateJournalQueueOwnerIdentity,
} from "./journal.ts";
import {
  areTelegramTargetsEqual as sameTarget,
  type TelegramTarget,
} from "./target.ts";
import {
  getTelegramApiErrorRequestTarget,
  isTelegramApiRequestRejected,
  type TelegramWorkspaceThreadDeletionTransport,
} from "./telegram-api.ts";
import { isTelegramTopicDeletedErrorMessage } from "./thread-reconciler.ts";
import type {
  TelegramTopicTargetStore,
  TelegramWorkspaceExternalProtectionEvidence,
  TelegramWorkspaceJournalSource,
  TelegramWorkspaceProtectionState,
  TelegramWorkspaceRetirementIntent,
  TelegramWorkspaceThreadBinding,
} from "./threads.ts";
import {
  createTelegramWorkspaceAdmissionOperationId,
  isTelegramWorkspaceRetirementFence,
  runWithTelegramWorkspaceAdmissionsAsync,
  type TelegramWorkspaceAdmissionLedger,
  type TelegramWorkspaceAdmissionScope,
  type TelegramWorkspaceDeletionPermit,
  type TelegramWorkspaceRetirementFence,
} from "./workspace-admission.ts";
import {
  planTelegramWorkspaceSlotAllocation,
  TelegramWorkspaceSlotUnavailableError,
  type TelegramWorkspaceSlotOccupancy,
} from "./workspace-slots.ts";

export interface TelegramWorkspaceOperationGate {
  runExclusive: <T>(operation: () => Promise<T>) => Promise<T>;
}

export function createTelegramWorkspaceOperationGate(): TelegramWorkspaceOperationGate {
  let tail: Promise<void> = Promise.resolve();
  return {
    runExclusive<T>(operation: () => Promise<T>): Promise<T> {
      const run = tail.then(operation);
      tail = run.then(
        () => undefined,
        () => undefined,
      );
      return run;
    },
  };
}

export type TelegramWorkspaceOperationRunner = <T>(
  input: {
    operationId: string;
    operationKind: string;
    scopes: readonly TelegramWorkspaceAdmissionScope[];
  },
  operation: () => Promise<T>,
) => Promise<T>;

export interface TelegramWorkspaceOperationRuntime extends TelegramWorkspaceOperationGate {
  run: TelegramWorkspaceOperationRunner;
}

export function createTelegramWorkspaceOperationRuntime(
  input: {
    getWorkspaceAdmission?: () =>
      | Pick<
          TelegramWorkspaceAdmissionLedger,
          "acquireAdmission" | "releaseAdmission"
        >
      | undefined;
    onReleaseError?: (error: unknown, operationKind: string) => void;
  } = {},
): TelegramWorkspaceOperationRuntime {
  const gate = createTelegramWorkspaceOperationGate();
  const run: TelegramWorkspaceOperationRunner = (metadata, operation) => {
    const gated = () => gate.runExclusive(operation);
    if (!input.getWorkspaceAdmission) return gated();
    const admission = input.getWorkspaceAdmission();
    if (!admission) {
      throw new Error("Telegram Workspace admission authority is unavailable.");
    }
    return runWithTelegramWorkspaceAdmissionsAsync({
      ledger: admission,
      ...metadata,
      operation: gated,
      onReleaseError(error) {
        input.onReleaseError?.(error, metadata.operationKind);
      },
    });
  };
  return { run, runExclusive: gate.runExclusive };
}

export interface TelegramWorkspaceJournalProtectionCapture {
  sources: TelegramWorkspaceJournalProtectionSource[];
  complete: boolean;
}

interface TelegramWorkspaceJournalReader {
  recoveryKey?: string;
  journal: {
    read: () => { entries: readonly { update: unknown }[]; exists?: boolean };
  };
  readForProtection?: () => {
    entries: readonly { update: unknown }[];
    exists?: boolean;
  };
}

export function captureTelegramWorkspaceJournalProtectionSources(input: {
  binding: TelegramWorkspaceThreadBinding;
  resolveLeader: () => TelegramWorkspaceJournalReader | undefined;
  createFollowerResolver: (
    journalBindingKey: string,
  ) => () => TelegramWorkspaceJournalReader | undefined;
  createSessionResolver?: (
    recipientBindingKey: string,
    sessionId: string,
  ) => () => TelegramWorkspaceJournalReader | undefined;
  withJournalReference?: <T>(
    binding: TelegramWorkspaceJournalReader,
    operation: () => T,
  ) => T;
  discovery?: {
    paths: readonly string[];
    complete: boolean;
    createResolver: (
      path: string,
    ) => () => TelegramWorkspaceJournalReader | undefined;
  };
}): TelegramWorkspaceJournalProtectionCapture {
  const sources: TelegramWorkspaceJournalProtectionSource[] = [];
  let complete =
    input.binding.journalBindingsComplete === true ||
    input.discovery?.complete === true;
  const capture = (
    scope: TelegramWorkspaceJournalProtectionSource["scope"],
    resolve: () => TelegramWorkspaceJournalReader | undefined,
  ): void => {
    try {
      const binding = resolve();
      if (!binding) {
        complete = false;
        sources.push({ kind: "unknown", scope });
        return;
      }
      const read = binding.readForProtection ?? binding.journal.read;
      const snapshot = input.withJournalReference
        ? input.withJournalReference(binding, read)
        : read();
      sources.push({
        kind: "available",
        scope,
        entries: snapshot.entries,
        ...(snapshot.exists === undefined ? {} : { exists: snapshot.exists }),
      });
    } catch {
      complete = false;
      sources.push({ kind: "unknown", scope });
    }
  };
  capture({ kind: "shared" }, input.resolveLeader);
  for (const journalBindingKey of input.binding.journalBindingKeys ?? []) {
    capture(
      {
        kind: "binding",
        bindingKey: input.binding.bindingKey,
        journalBindingKey,
      },
      input.createFollowerResolver(journalBindingKey),
    );
  }
  for (const source of input.binding.journalSources ?? []) {
    capture(
      {
        kind: "binding",
        bindingKey: input.binding.bindingKey,
        journalBindingKey: source.recipientBindingKey,
        sessionId: source.sessionId,
      },
      input.createSessionResolver?.(
        source.recipientBindingKey,
        source.sessionId,
      ) ?? (() => undefined),
    );
  }
  for (const path of input.discovery?.paths ?? []) {
    capture(
      { kind: "discovered", path },
      input.discovery!.createResolver(path),
    );
  }
  return { sources, complete };
}

export type TelegramWorkspaceJournalProtectionSource =
  | {
      kind: "available";
      scope:
        | { kind: "shared" }
        | {
            kind: "binding";
            bindingKey: string;
            journalBindingKey: string;
            sessionId?: string;
          }
        | { kind: "discovered"; path: string };
      entries: readonly { update: unknown }[];
      /** Positive filesystem presence is distinct from a complete namespace with a missing historical family. */
      exists?: boolean;
    }
  | {
      kind: "unknown";
      scope:
        | { kind: "shared" }
        | {
            kind: "binding";
            bindingKey: string;
            journalBindingKey: string;
            sessionId?: string;
          }
        | { kind: "discovered"; path: string };
    };

function getJournalUpdateTarget(
  update: unknown,
): { chatId: number; threadId?: number } | undefined {
  if (!update || typeof update !== "object" || Array.isArray(update))
    return undefined;
  const record = update as Record<string, unknown>;
  if (record.message_reaction !== undefined) return undefined;
  const direct =
    record.message ?? record.edited_message ?? record.guest_message;
  const callback = record.callback_query;
  const message =
    direct ??
    (callback && typeof callback === "object" && !Array.isArray(callback)
      ? (callback as Record<string, unknown>).message
      : undefined);
  if (!message || typeof message !== "object" || Array.isArray(message))
    return undefined;
  const messageRecord = message as Record<string, unknown>;
  const chat = messageRecord.chat;
  if (!chat || typeof chat !== "object" || Array.isArray(chat))
    return undefined;
  const chatId = (chat as Record<string, unknown>).id;
  if (typeof chatId !== "number") return undefined;
  const threadId = messageRecord.message_thread_id;
  return {
    chatId,
    ...(typeof threadId === "number" ? { threadId } : {}),
  };
}

export function resolveTelegramWorkspaceAcceptedWorkProtection(input: {
  binding: TelegramWorkspaceThreadBinding;
  localAcceptedTargets: readonly { chatId: number; threadId?: number }[];
  journalSources: readonly TelegramWorkspaceJournalProtectionSource[];
  sourcesComplete: boolean;
  /** Relocation cannot infer execution ownership from the original Telegram message target. */
  requireBindingProvenance?: boolean;
}): TelegramWorkspaceProtectionState {
  if (
    input.localAcceptedTargets.some((target) =>
      sameTarget(target, input.binding.target),
    )
  ) {
    return "protected";
  }
  let unknown = !input.sourcesComplete;
  for (const source of input.journalSources) {
    const relevant =
      source.scope.kind !== "binding" ||
      source.scope.bindingKey === input.binding.bindingKey;
    if (!relevant) continue;
    if (source.kind === "unknown") {
      unknown = true;
      continue;
    }
    if (
      input.requireBindingProvenance &&
      source.scope.kind === "binding" &&
      source.exists === false
    ) {
      unknown = true;
      continue;
    }
    if (source.scope.kind === "binding" && source.entries.length > 0) {
      return "protected";
    }
    if (input.requireBindingProvenance && source.entries.length > 0) {
      unknown = true;
      continue;
    }
    for (const entry of source.entries) {
      const target = getJournalUpdateTarget(entry.update);
      if (!target) {
        unknown = true;
        continue;
      }
      if (sameTarget(target, input.binding.target)) return "protected";
    }
  }
  return unknown ? "unknown" : "clear";
}

export type TelegramWorkspaceJournalPruneResult =
  | {
      kind: "committed";
      binding: TelegramWorkspaceThreadBinding;
      removedKeys: string[];
      removedSources?: TelegramWorkspaceJournalSource[];
    }
  | {
      kind: "blocked";
      reason:
        | "incomplete-evidence"
        | "writer-not-quiescent"
        | "state-changed"
        | "publication-refused";
    };

export async function pruneTelegramWorkspaceJournalEvidence(input: {
  store: Pick<
    TelegramTopicTargetStore,
    "commitWorkspaceJournalEvidence" | "persistWorkspaceJournalEvidence"
  >;
  binding: TelegramWorkspaceThreadBinding;
  /** Capture fresh source evidence after admission, not a snapshot taken before the lease. */
  capture: () => TelegramWorkspaceJournalProtectionCapture;
  runExclusive: TelegramWorkspaceOperationGate["runExclusive"];
  getJournalWriterProtection: (
    journalBindingKey: string,
  ) => TelegramWorkspaceProtectionState;
  getLeaderEpoch: () => number | string | undefined;
  getProfileKey: () => string;
  admission: Pick<
    TelegramWorkspaceAdmissionLedger,
    "acquireAdmission" | "releaseAdmission"
  >;
  isCurrent?: () => boolean;
  onAdmissionReleaseError?: (error: unknown) => void;
}): Promise<TelegramWorkspaceJournalPruneResult> {
  const operation = async (): Promise<TelegramWorkspaceJournalPruneResult> => {
    const leaderEpoch = input.getLeaderEpoch();
    const profileKey = input.getProfileKey();
    const isCurrent = () =>
      leaderEpoch !== undefined &&
      input.getLeaderEpoch() === leaderEpoch &&
      input.getProfileKey() === profileKey &&
      input.isCurrent?.() !== false;
    if (!isCurrent()) {
      throw new Error(
        "Telegram Workspace journal pruning requires current leader authority.",
      );
    }
    let capture: TelegramWorkspaceJournalProtectionCapture;
    try {
      capture = input.capture();
    } catch {
      return { kind: "blocked", reason: "incomplete-evidence" };
    }
    const address = (key: string, sessionId?: string) =>
      JSON.stringify([sessionId ?? null, key]);
    const keys = input.binding.journalBindingKeys ?? [];
    const sessions = input.binding.journalSources ?? [];
    const expected = [
      ...keys.map((key) => address(key)),
      ...sessions.map((source) =>
        address(source.recipientBindingKey, source.sessionId),
      ),
    ];
    const shared = capture.sources.filter(
      (source) => source.scope.kind === "shared",
    );
    const bindingSources = capture.sources.filter(
      (source) =>
        source.scope.kind === "binding" &&
        source.scope.bindingKey === input.binding.bindingKey,
    );
    const sourceByAddress = new Map(
      bindingSources.flatMap((source) =>
        source.scope.kind === "binding"
          ? [
              [
                address(source.scope.journalBindingKey, source.scope.sessionId),
                source,
              ] as const,
            ]
          : [],
      ),
    );
    if (
      !capture.complete ||
      shared.length !== 1 ||
      shared[0]?.kind !== "available" ||
      bindingSources.length !== expected.length ||
      sourceByAddress.size !== expected.length ||
      expected.some((key) => sourceByAddress.get(key)?.kind !== "available")
    ) {
      return { kind: "blocked", reason: "incomplete-evidence" };
    }
    const empty = new Set(
      expected.filter((key) => {
        const source = sourceByAddress.get(key);
        return source?.kind === "available" && source.entries.length === 0;
      }),
    );
    const emptyWriterKeys = new Set([
      ...keys.filter((key) => empty.has(address(key))),
      ...sessions
        .filter((source) =>
          empty.has(address(source.recipientBindingKey, source.sessionId)),
        )
        .map((source) => source.recipientBindingKey),
    ]);
    const quiescent = new Set(
      [...emptyWriterKeys].filter(
        (key) => input.getJournalWriterProtection(key) === "clear",
      ),
    );
    const removedKeys = keys.filter(
      (key) => empty.has(address(key)) && quiescent.has(key),
    );
    const removedSources = sessions.filter(
      (source) =>
        empty.has(address(source.recipientBindingKey, source.sessionId)) &&
        quiescent.has(source.recipientBindingKey),
    );
    if (
      empty.size > 0 &&
      removedKeys.length === 0 &&
      removedSources.length === 0
    ) {
      return { kind: "blocked", reason: "writer-not-quiescent" };
    }
    const retainedKeys = keys.filter((key) => !removedKeys.includes(key));
    const retainedSources = sessions.filter(
      (source) => !removedSources.includes(source),
    );
    if (!isCurrent()) {
      throw new Error(
        "Telegram Workspace journal pruning lost leader authority.",
      );
    }
    const binding = input.store.commitWorkspaceJournalEvidence(
      input.binding,
      retainedKeys,
      input.binding.journalBindingsComplete === true,
      retainedSources,
    );
    if (!binding) return { kind: "blocked", reason: "state-changed" };
    if (removedKeys.length > 0 || removedSources.length > 0) {
      const published = await input.store.persistWorkspaceJournalEvidence(
        binding,
        isCurrent,
      );
      if (!isCurrent())
        throw new Error(
          "Telegram Workspace journal pruning lost leader authority.",
        );
      if (!published) return { kind: "blocked", reason: "publication-refused" };
    }
    if (!isCurrent()) {
      throw new Error(
        "Telegram Workspace journal pruning lost leader authority.",
      );
    }
    return {
      kind: "committed",
      binding,
      removedKeys,
      ...(removedSources.length
        ? { removedSources: removedSources.map((source) => ({ ...source })) }
        : {}),
    };
  };
  return runWithTelegramWorkspaceAdmissionsAsync({
    ledger: input.admission,
    operationId: createTelegramWorkspaceAdmissionOperationId(),
    operationKind: "workspace.prune-journal-evidence",
    scopes: [{ kind: "target", target: input.binding.target }],
    operation: () => input.runExclusive(operation),
    onReleaseError(error) {
      input.onAdmissionReleaseError?.(error);
    },
  });
}

export function captureTelegramWorkspaceExternalProtection(input: {
  binding: TelegramWorkspaceThreadBinding;
  requireBindingProvenance?: boolean;
  getLiveOwnerProtection: (
    binding: TelegramWorkspaceThreadBinding,
  ) => TelegramWorkspaceProtectionState;
  getLocalAcceptedTargets: (binding: TelegramWorkspaceThreadBinding) => {
    targets: readonly TelegramTarget[];
    complete: boolean;
  };
  captureJournalSources: (
    binding: TelegramWorkspaceThreadBinding,
  ) => TelegramWorkspaceJournalProtectionCapture;
  getDeliveryAuthorityProtection?: (
    binding: TelegramWorkspaceThreadBinding,
  ) => TelegramWorkspaceProtectionState;
}): TelegramWorkspaceExternalProtectionEvidence {
  let liveOwner: TelegramWorkspaceProtectionState = "unknown";
  let acceptedWork: TelegramWorkspaceProtectionState = "unknown";
  let deliveryAuthority: TelegramWorkspaceProtectionState = "unknown";
  try {
    liveOwner = input.getLiveOwnerProtection(input.binding);
  } catch {
    // Unavailable registry/process evidence must not clear a binding.
  }
  try {
    const journals = input.captureJournalSources(input.binding);
    const local = input.getLocalAcceptedTargets(input.binding);
    acceptedWork = resolveTelegramWorkspaceAcceptedWorkProtection({
      binding: input.binding,
      localAcceptedTargets: local.targets,
      journalSources: journals.sources,
      sourcesComplete: journals.complete && local.complete,
      requireBindingProvenance: input.requireBindingProvenance,
    });
  } catch {
    // Unavailable queue or journal evidence must not clear accepted work.
  }
  try {
    deliveryAuthority =
      input.getDeliveryAuthorityProtection?.(input.binding) ?? "unknown";
  } catch {
    // Unavailable delivery evidence must not clear a binding.
  }
  return { liveOwner, acceptedWork, deliveryAuthority };
}

interface TelegramWorkspaceProtectionObserverDeps {
  listFollowers: () => readonly { target?: TelegramTarget }[];
  getActiveTurnTarget: () => TelegramTarget | undefined;
  getQueuedItems: () => readonly { chatId: number; target?: TelegramTarget }[];
  resolveLeaderJournal: () => TelegramWorkspaceJournalReader | undefined;
  createFollowerJournalResolver: (
    journalBindingKey: string,
  ) => () => TelegramWorkspaceJournalReader | undefined;
  createSessionJournalResolver?: (
    recipientBindingKey: string,
    sessionId: string,
  ) => () => TelegramWorkspaceJournalReader | undefined;
  discoverFollowerJournals?: () => {
    paths: readonly string[];
    complete: boolean;
  };
  /** Strict read-only namespace evidence; it never substitutes for writer closure. */
  inspectJournalNamespace?: () => TelegramJournalNamespaceInspection;
  createJournalPathResolver?: (
    path: string,
  ) => () => TelegramWorkspaceJournalReader | undefined;
  withJournalReference?: <T>(
    binding: TelegramWorkspaceJournalReader,
    operation: () => T,
  ) => T;
  getJournalWriterProtection?: (
    journalBindingKey: string,
  ) => TelegramWorkspaceProtectionState;
  getDeliveryAuthorityProtection?: (
    binding: TelegramWorkspaceThreadBinding,
  ) => TelegramWorkspaceProtectionState;
}

export interface TelegramWorkspaceProtectionObserver {
  capture: (
    binding: TelegramWorkspaceThreadBinding,
    options?: { requireBindingProvenance?: boolean },
  ) => TelegramWorkspaceExternalProtectionEvidence;
  captureJournalSources: (
    binding: TelegramWorkspaceThreadBinding,
  ) => TelegramWorkspaceJournalProtectionCapture;
  getJournalWriterProtection: (
    journalBindingKey: string,
  ) => TelegramWorkspaceProtectionState;
}

/** Shared read-only observer: protection and metadata pruning use the same scoped evidence path. */
export function createTelegramWorkspaceProtectionObserver(
  deps: TelegramWorkspaceProtectionObserverDeps,
): TelegramWorkspaceProtectionObserver {
  const captureJournalSources: TelegramWorkspaceProtectionObserver["captureJournalSources"] =
    (candidate) => {
      const namespace = deps.inspectJournalNamespace?.();
      if (
        deps.inspectJournalNamespace &&
        (!namespace ||
          namespace.sources.filter((source) => source.role === "polling")
            .length !== 1)
      )
        throw new Error(
          "Telegram strict namespace evidence is unavailable or incomplete.",
        );
      if (
        namespace?.retainedInputs?.some(
          (original) => original.state !== "committed",
        )
      )
        throw new Error(
          "Telegram namespace contains uncommitted private retention evidence.",
        );
      const discovery = namespace
        ? {
            paths: namespace.sources
              .filter(
                (source) =>
                  source.role !== "polling" &&
                  source.evidence.kind === "present",
              )
              .map((source) => source.path),
            complete: true,
          }
        : candidate.journalBindingsComplete === true
          ? undefined
          : deps.discoverFollowerJournals?.();
      if (namespace && !deps.createJournalPathResolver)
        throw new Error(
          "Telegram strict namespace protection requires discovered source resolution.",
        );
      return captureTelegramWorkspaceJournalProtectionSources({
        binding: candidate,
        resolveLeader: deps.resolveLeaderJournal,
        createFollowerResolver: deps.createFollowerJournalResolver,
        ...(deps.createSessionJournalResolver
          ? { createSessionResolver: deps.createSessionJournalResolver }
          : {}),
        ...(deps.withJournalReference
          ? { withJournalReference: deps.withJournalReference }
          : {}),
        ...(discovery && deps.createJournalPathResolver
          ? {
              discovery: {
                ...discovery,
                createResolver: deps.createJournalPathResolver,
              },
            }
          : {}),
      });
    };
  const capture: TelegramWorkspaceProtectionObserver["capture"] = function (
    binding,
    options,
  ) {
    return captureTelegramWorkspaceExternalProtection({
      binding,
      requireBindingProvenance: options?.requireBindingProvenance,
      getLiveOwnerProtection(candidate) {
        if (
          deps
            .listFollowers()
            .some(
              (follower) =>
                !!follower.target &&
                sameTarget(follower.target, candidate.target),
            )
        )
          return "protected";
        if (!deps.getJournalWriterProtection) return "unknown";
        let unknown = candidate.journalBindingsComplete !== true;
        const writerKeys = new Set([
          ...(candidate.journalBindingKeys ?? []),
          ...(candidate.journalSources ?? []).map(
            (source) => source.recipientBindingKey,
          ),
        ]);
        for (const journalBindingKey of writerKeys) {
          const protection = deps.getJournalWriterProtection(journalBindingKey);
          if (protection === "protected") return "protected";
          if (protection === "unknown") unknown = true;
        }
        return unknown ? "unknown" : "clear";
      },
      getLocalAcceptedTargets(candidate) {
        const items = deps.getQueuedItems();
        const targets: TelegramTarget[] = [];
        const activeTarget = deps.getActiveTurnTarget();
        if (activeTarget) targets.push(activeTarget);
        let complete = true;
        for (const item of items) {
          if (item.target) targets.push(item.target);
          else if (item.chatId === candidate.target.chatId) complete = false;
        }
        return { targets, complete };
      },
      captureJournalSources,
      ...(deps.getDeliveryAuthorityProtection
        ? {
            getDeliveryAuthorityProtection: deps.getDeliveryAuthorityProtection,
          }
        : {}),
    });
  };
  return {
    capture,
    captureJournalSources,
    getJournalWriterProtection:
      deps.getJournalWriterProtection ?? (() => "unknown"),
  };
}

/** Callable protection view retained for callers that need no metadata observation ports. */
export function createTelegramWorkspaceExternalProtectionCapture(
  deps: TelegramWorkspaceProtectionObserverDeps,
): TelegramWorkspaceProtectionObserver["capture"] {
  return createTelegramWorkspaceProtectionObserver(deps).capture;
}

export function createTelegramWorkspaceJournalEvidencePruner(
  deps: TelegramWorkspaceOperationGate & {
    store: TelegramTopicTargetStore;
    getAdmission: () => TelegramWorkspaceAdmissionLedger | undefined;
    getLeaderEpoch: () => number | string | undefined;
    protection: Pick<
      TelegramWorkspaceProtectionObserver,
      "captureJournalSources" | "getJournalWriterProtection"
    >;
  },
): (
  binding: TelegramWorkspaceThreadBinding,
  isCurrent: () => boolean,
) => Promise<TelegramWorkspaceJournalPruneResult> {
  return async (binding, isCurrent) => {
    const admission = deps.getAdmission();
    if (!admission) return { kind: "blocked", reason: "state-changed" };
    const profileKey = admission.getProfileKey();
    return pruneTelegramWorkspaceJournalEvidence({
      store: deps.store,
      binding,
      admission,
      runExclusive: deps.runExclusive,
      capture: () => deps.protection.captureJournalSources(binding),
      getJournalWriterProtection: deps.protection.getJournalWriterProtection,
      getLeaderEpoch: deps.getLeaderEpoch,
      getProfileKey: () => deps.getAdmission()?.getProfileKey() ?? "",
      isCurrent: () =>
        isCurrent() && deps.getAdmission()?.getProfileKey() === profileKey,
    });
  };
}

export function isCurrentTelegramWorkspaceBinding(
  store: Pick<TelegramTopicTargetStore, "listWorkspaceBindings">,
  expected: TelegramWorkspaceThreadBinding,
): boolean {
  return store
    .listWorkspaceBindings()
    .some((binding) => isDeepStrictEqual(binding, expected));
}

export interface TelegramWorkspaceDeadQueueJournalBinding {
  recoveryKey?: string;
  readForProtection?: () => { entries: readonly TelegramUpdateJournalEntry[] };
  journal: {
    recoverDeadQueueOwner: (
      input: TelegramUpdateJournalDeadQueueOwnerRecoveryInput,
    ) => TelegramUpdateJournalDeadQueueOwnerRecoveryResult;
  };
}

export type TelegramWorkspaceDeadQueueReclamation =
  | { kind: "not-needed" }
  | { kind: "recovered"; receipts: number; updateIds: number[] }
  | {
      kind: "blocked";
      reason:
        | "authority-changed"
        | "live-owner"
        | "local-work"
        | "incomplete-source"
        | "unsupported-custody"
        | "owner-alive"
        | "owner-unverifiable"
        | "mutation-refused"
        | "protection-retained";
    };

/**
 * Demand-only preparation for pressure retirement. Every removed group is still
 * owned by the journal's exact dead-owner CAS; this function never clears local
 * queue memory or turns unknown evidence into deletion authority.
 */
export function createTelegramWorkspaceDeadOwnerQueueReclaimer(deps: {
  getExternalProtection: (
    binding: TelegramWorkspaceThreadBinding,
  ) => TelegramWorkspaceExternalProtectionEvidence;
  getActiveTurnTarget: () => TelegramTarget | undefined;
  getQueuedItems: () => readonly { chatId: number; target?: TelegramTarget }[];
  resolveLeaderJournal: () =>
    TelegramWorkspaceDeadQueueJournalBinding | undefined;
  createFollowerJournalResolver: (
    journalBindingKey: string,
  ) => () => TelegramWorkspaceDeadQueueJournalBinding | undefined;
  createSessionJournalResolver?: (
    recipientBindingKey: string,
    sessionId: string,
  ) => () => TelegramWorkspaceDeadQueueJournalBinding | undefined;
  discoverFollowerJournals?: () => {
    paths: readonly string[];
    complete: boolean;
  };
  createJournalPathResolver?: (
    path: string,
  ) => () => TelegramWorkspaceDeadQueueJournalBinding | undefined;
  withJournalReference?: <T>(
    binding: TelegramWorkspaceDeadQueueJournalBinding,
    operation: () => T,
  ) => T;
  getRecoveryOwner: () => TelegramUpdateJournalQueueOwnerIdentity;
  getQueueOwnerLiveness: (owner: {
    processId: number;
    processBirthId: string;
  }) => "alive" | "dead" | "unverifiable";
  isBindingCurrent: (binding: TelegramWorkspaceThreadBinding) => boolean;
  onMutationError?: (error: unknown) => void;
}): (
  binding: TelegramWorkspaceThreadBinding,
  isCurrent: () => boolean,
) => Promise<TelegramWorkspaceDeadQueueReclamation> {
  const localProtection = (
    binding: TelegramWorkspaceThreadBinding,
  ): "clear" | "protected" | "unknown" => {
    const active = deps.getActiveTurnTarget();
    if (active && sameTarget(active, binding.target)) return "protected";
    let unknown = false;
    for (const item of deps.getQueuedItems()) {
      if (item.target && sameTarget(item.target, binding.target))
        return "protected";
      if (!item.target && item.chatId === binding.target.chatId) unknown = true;
    }
    return unknown ? "unknown" : "clear";
  };
  return async (binding, isCurrent) => {
    if (!isCurrent() || !deps.isBindingCurrent(binding)) {
      return { kind: "blocked", reason: "authority-changed" };
    }
    // Session addresses require exact resolution; never substitute a flat same-hash journal.
    if (binding.journalSources?.length && !deps.createSessionJournalResolver)
      return { kind: "blocked", reason: "incomplete-source" };
    const initial = deps.getExternalProtection(binding);
    if (
      initial.liveOwner !== "clear" ||
      initial.deliveryAuthority !== "clear"
    ) {
      return { kind: "blocked", reason: "live-owner" };
    }
    if (localProtection(binding) !== "clear") {
      return { kind: "blocked", reason: "local-work" };
    }
    if (initial.acceptedWork === "clear") return { kind: "not-needed" };

    const discovery =
      binding.journalBindingsComplete === true
        ? undefined
        : deps.discoverFollowerJournals?.();
    let complete =
      binding.journalBindingsComplete === true || discovery?.complete === true;
    const sources: Array<{
      scope: "shared" | "binding" | "discovered";
      binding: TelegramWorkspaceDeadQueueJournalBinding;
      entries: readonly TelegramUpdateJournalEntry[];
    }> = [];
    const recoveryKeys = new Set<string>();
    const capture = (
      scope: "shared" | "binding" | "discovered",
      resolve: () => TelegramWorkspaceDeadQueueJournalBinding | undefined,
    ): void => {
      try {
        const source = resolve();
        if (!source?.recoveryKey || !source.readForProtection) {
          complete = false;
          return;
        }
        if (recoveryKeys.has(source.recoveryKey)) return;
        const snapshot = deps.withJournalReference
          ? deps.withJournalReference(source, source.readForProtection)
          : source.readForProtection();
        recoveryKeys.add(source.recoveryKey);
        sources.push({ scope, binding: source, entries: snapshot.entries });
      } catch {
        complete = false;
      }
    };
    capture("shared", deps.resolveLeaderJournal);
    for (const key of binding.journalBindingKeys ?? []) {
      capture("binding", deps.createFollowerJournalResolver(key));
    }
    for (const source of binding.journalSources ?? []) {
      capture(
        "binding",
        deps.createSessionJournalResolver!(
          source.recipientBindingKey,
          source.sessionId,
        ),
      );
    }
    for (const path of discovery?.paths ?? []) {
      if (!deps.createJournalPathResolver) {
        complete = false;
        break;
      }
      capture("discovered", deps.createJournalPathResolver(path));
    }
    if (!complete) return { kind: "blocked", reason: "incomplete-source" };

    const plans: Array<{
      source: TelegramWorkspaceDeadQueueJournalBinding;
      recovery: TelegramUpdateJournalDeadQueueOwnerRecoveryInput;
    }> = [];
    for (const source of sources) {
      const relevant =
        source.scope === "binding"
          ? [...source.entries]
          : source.entries.filter((entry) => {
              const target = getJournalUpdateTarget(entry.update);
              return !!target && sameTarget(target, binding.target);
            });
      if (
        source.scope === "binding" &&
        relevant.some((entry) => {
          const target = getJournalUpdateTarget(entry.update);
          return !target || !sameTarget(target, binding.target);
        })
      )
        return { kind: "blocked", reason: "unsupported-custody" };
      const seen = new Set<string>();
      for (const entry of relevant) {
        if (
          entry.state !== "queued" ||
          (entry.queueKind !== "prompt" && entry.queueKind !== "control") ||
          !entry.queueReceiptId ||
          !entry.queueOwner ||
          entry.queueHandoff
        ) {
          return { kind: "blocked", reason: "unsupported-custody" };
        }
        if (seen.has(entry.queueReceiptId)) continue;
        const receiptEntries = source.entries.filter(
          (candidate) => candidate.queueReceiptId === entry.queueReceiptId,
        );
        if (
          !receiptEntries.length ||
          receiptEntries.some((candidate) => {
            const target = getJournalUpdateTarget(candidate.update);
            return (
              candidate.state !== "queued" ||
              candidate.queueKind !== entry.queueKind ||
              !candidate.queueOwner ||
              candidate.queueHandoff !== undefined ||
              !isDeepStrictEqual(candidate.queueOwner, entry.queueOwner) ||
              !target ||
              !sameTarget(target, binding.target)
            );
          })
        )
          return { kind: "blocked", reason: "unsupported-custody" };
        seen.add(entry.queueReceiptId);
        plans.push({
          source: source.binding,
          recovery: {
            queueKind: entry.queueKind,
            receiptId: entry.queueReceiptId,
            sourceUpdateIds: receiptEntries
              .map((candidate) => candidate.updateId)
              .sort((a, b) => a - b),
            deadOwner: entry.queueOwner,
            recoveryOwner: deps.getRecoveryOwner(),
          },
        });
      }
    }
    if (!plans.length)
      return { kind: "blocked", reason: "protection-retained" };
    for (const plan of plans) {
      let liveness: "alive" | "dead" | "unverifiable" = "unverifiable";
      try {
        liveness = deps.getQueueOwnerLiveness(plan.recovery.deadOwner);
      } catch {
        /* Unknown process evidence is never destructive authority. */
      }
      if (liveness === "alive")
        return { kind: "blocked", reason: "owner-alive" };
      if (liveness !== "dead")
        return { kind: "blocked", reason: "owner-unverifiable" };
    }

    const recoveredIds: number[] = [];
    for (const plan of plans) {
      if (!isCurrent() || !deps.isBindingCurrent(binding)) {
        return { kind: "blocked", reason: "authority-changed" };
      }
      const current = deps.getExternalProtection(binding);
      if (
        current.liveOwner !== "clear" ||
        current.deliveryAuthority !== "clear"
      ) {
        return { kind: "blocked", reason: "live-owner" };
      }
      if (localProtection(binding) !== "clear") {
        return { kind: "blocked", reason: "local-work" };
      }
      let result: TelegramUpdateJournalDeadQueueOwnerRecoveryResult;
      try {
        result = deps.withJournalReference
          ? deps.withJournalReference(plan.source, () =>
              plan.source.journal.recoverDeadQueueOwner(plan.recovery),
            )
          : plan.source.journal.recoverDeadQueueOwner(plan.recovery);
      } catch (error) {
        try {
          deps.onMutationError?.(error);
        } catch {
          /* Diagnostics are fail-open. */
        }
        return { kind: "blocked", reason: "mutation-refused" };
      }
      if (result.status === "owner-alive") {
        return { kind: "blocked", reason: "owner-alive" };
      }
      if (result.status === "owner-unverifiable") {
        return { kind: "blocked", reason: "owner-unverifiable" };
      }
      recoveredIds.push(...result.recoveredUpdateIds);
    }
    if (!isCurrent() || !deps.isBindingCurrent(binding)) {
      return { kind: "blocked", reason: "authority-changed" };
    }
    const after = deps.getExternalProtection(binding);
    if (
      after.liveOwner !== "clear" ||
      after.acceptedWork !== "clear" ||
      after.deliveryAuthority !== "clear"
    ) {
      return { kind: "blocked", reason: "protection-retained" };
    }
    return {
      kind: "recovered",
      receipts: plans.length,
      updateIds: recoveredIds.sort((a, b) => a - b),
    };
  };
}

export type TelegramWorkspaceRetirementAdoption =
  | { kind: "adopted"; intent: TelegramWorkspaceRetirementIntent }
  | {
      kind: "blocked";
      reason:
        | "intent-conflict"
        | "profile-changed"
        | "binding-changed"
        | "protection-changed"
        | "commit-rejected";
    };

export async function adoptTelegramWorkspaceRetirementIntent(input: {
  store: Pick<
    TelegramTopicTargetStore,
    | "captureWorkspaceSlotOccupancy"
    | "listWorkspaceBindings"
    | "listWorkspaceRetirementIntents"
    | "replaceWorkspaceRetirementIntent"
  >;
  intent: TelegramWorkspaceRetirementIntent;
  getExternalProtection: (
    binding: TelegramWorkspaceThreadBinding,
  ) => TelegramWorkspaceExternalProtectionEvidence;
  getLeaderEpoch: () => number | string | undefined;
  getProfileKey: () => string;
  isCurrent?: () => boolean;
  runExclusive: <T>(operation: () => Promise<T>) => Promise<T>;
}): Promise<TelegramWorkspaceRetirementAdoption> {
  return input.runExclusive(async () => {
    const leaderEpoch = input.getLeaderEpoch();
    const profileKey = input.getProfileKey();
    const isCurrent = () =>
      leaderEpoch !== undefined &&
      input.getLeaderEpoch() === leaderEpoch &&
      input.getProfileKey() === profileKey &&
      input.isCurrent?.() !== false;
    if (!isCurrent())
      throw new Error(
        "Telegram Workspace retirement adoption requires current leader authority.",
      );
    const intents = input.store.listWorkspaceRetirementIntents();
    if (intents.length !== 1 || !isDeepStrictEqual(intents[0], input.intent)) {
      return { kind: "blocked", reason: "intent-conflict" };
    }
    if (input.intent.profileKey !== profileKey) {
      return { kind: "blocked", reason: "profile-changed" };
    }
    const binding = input.store
      .listWorkspaceBindings()
      .find(
        (candidate) => candidate.bindingKey === input.intent.binding.bindingKey,
      );
    if (!binding || !isDeepStrictEqual(binding, input.intent.binding)) {
      return { kind: "blocked", reason: "binding-changed" };
    }
    const eligible =
      input.store
        .captureWorkspaceSlotOccupancy(input.getExternalProtection, {
          expectedRetirement: input.intent,
        })
        .bindings.find(
          (candidate) => candidate.bindingKey === binding.bindingKey,
        )?.protection === "eligible";
    if (!eligible) return { kind: "blocked", reason: "protection-changed" };
    const replacement = { ...input.intent, leaderEpoch: leaderEpoch! };
    if (!isCurrent())
      throw new Error(
        "Telegram Workspace retirement adoption lost leader authority.",
      );
    if (
      !(await input.store.replaceWorkspaceRetirementIntent(
        input.intent,
        replacement,
        isCurrent,
      ))
    )
      return { kind: "blocked", reason: "commit-rejected" };
    return { kind: "adopted", intent: replacement };
  });
}

function isTelegramWorkspaceDeletionConfirmedAbsent(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const status =
    "status" in error && typeof error.status === "number"
      ? error.status
      : undefined;
  return status === 400 && isTelegramTopicDeletedErrorMessage(error.message);
}

export type TelegramWorkspaceRetirementExecution =
  | { kind: "retired"; bindingKey: string; slot: string }
  | { kind: "cancelled"; reason: "delete-rejected" }
  | {
      kind: "retained";
      reason:
        | "stale-intent"
        | "protection-changed"
        | "admission-active"
        | "fence-conflict"
        | "delete-unconfirmed"
        | "delete-rejected"
        | "authority-changed"
        | "commit-rejected"
        | "fence-release-unconfirmed";
    };

export type TelegramWorkspaceRetirementAbsence =
  "absent" | "present" | "unknown";

function matchesTelegramWorkspaceRetirementFence(
  fence: TelegramWorkspaceRetirementFence,
  intent: TelegramWorkspaceRetirementIntent,
): boolean {
  return (
    fence.retirementIntentId === intent.id &&
    fence.profileKey === intent.profileKey &&
    fence.bindingKey === intent.binding.bindingKey &&
    fence.slot === intent.binding.slot &&
    fence.target.chatId === intent.binding.target.chatId &&
    fence.target.threadId === intent.binding.target.threadId &&
    fence.retirementRequestedAtMs === intent.requestedAtMs
  );
}

async function cancelRejectedTelegramWorkspaceRetirement(
  input: Pick<
    Parameters<typeof executeTelegramWorkspaceRetirement>[0],
    "store" | "admission" | "getLeaderEpoch" | "getProfileKey" | "isCurrent"
  >,
  retained: TelegramWorkspaceRetirementFence,
): Promise<Exclude<TelegramWorkspaceRetirementExecution, { kind: "retired" }>> {
  const epoch = input.getLeaderEpoch();
  const isCurrent = () =>
    epoch !== undefined &&
    input.getLeaderEpoch() === epoch &&
    input.getProfileKey() === retained.profileKey &&
    input.isCurrent?.() !== false;
  if (!isCurrent()) return { kind: "retained", reason: "authority-changed" };
  if (
    retained.phase !== "deletion-rejected" ||
    (retained.destructiveKind ?? "pressure-retirement") !==
      "pressure-retirement"
  ) {
    return { kind: "retained", reason: "fence-conflict" };
  }
  const owner = input.admission.getOwner();
  const fence =
    retained.leaderEpoch === epoch && isDeepStrictEqual(retained.owner, owner)
      ? retained
      : input.admission.adoptRetirementFence(retained, {
          owner,
          leaderEpoch: epoch!,
        });
  if (fence.phase !== "deletion-rejected")
    return { kind: "retained", reason: "fence-conflict" };
  const bindingRetained = () =>
    input.store
      .listWorkspaceBindings()
      .some(
        (binding) =>
          binding.bindingKey === fence.bindingKey &&
          binding.slot === fence.slot &&
          sameTarget(binding.target, fence.target),
      );
  const intents = input.store.listWorkspaceRetirementIntents();
  if (
    !bindingRetained() ||
    intents.length > 1 ||
    (intents[0] && !matchesTelegramWorkspaceRetirementFence(fence, intents[0]))
  ) {
    return { kind: "retained", reason: "stale-intent" };
  }
  if (intents[0] && !input.store.removeWorkspaceRetirementIntent(intents[0])) {
    return { kind: "retained", reason: "commit-rejected" };
  }
  // Publish even after a same-store retry whose failed write left only a dirty
  // in-memory withdrawal. The rejection fence protects both commit prefixes.
  await input.store.persist();
  if (!isCurrent()) return { kind: "retained", reason: "authority-changed" };
  if (
    input.store.listWorkspaceRetirementIntents().length ||
    !bindingRetained()
  ) {
    return { kind: "retained", reason: "commit-rejected" };
  }
  try {
    input.admission.completeRejectedRetirementFence(fence);
  } catch {
    if (input.admission.read().fence)
      return { kind: "retained", reason: "fence-release-unconfirmed" };
  }
  return { kind: "cancelled", reason: "delete-rejected" };
}

export async function executeTelegramWorkspaceRetirement(input: {
  store: Pick<
    TelegramTopicTargetStore,
    | "captureWorkspaceSlotOccupancy"
    | "listWorkspaceBindings"
    | "listWorkspaceRetirementIntents"
    | "commitWorkspaceRetirement"
    | "removeWorkspaceRetirementIntent"
    | "markStaleByTarget"
    | "persist"
  >;
  admission: Pick<
    TelegramWorkspaceAdmissionLedger,
    | "getOwner"
    | "read"
    | "acquireRetirementFence"
    | "adoptRetirementFence"
    | "issueDeletionPermit"
    | "confirmRetirementAbsence"
    | "confirmRetirementRejection"
    | "completeRejectedRetirementFence"
    | "releaseUnissuedRetirementFence"
    | "completeRetirementFence"
  >;
  intent: TelegramWorkspaceRetirementIntent;
  getExternalProtection: (
    binding: TelegramWorkspaceThreadBinding,
  ) => TelegramWorkspaceExternalProtectionEvidence;
  getLeaderEpoch: () => number | string | undefined;
  getProfileKey: () => string;
  isCurrent?: () => boolean;
  runExclusive: <T>(operation: () => Promise<T>) => Promise<T>;
  deleteForumTopic: (
    permit: TelegramWorkspaceDeletionPermit,
    body: { chat_id: number; message_thread_id: number },
    options: { maxAttempts: 1 },
  ) => Promise<unknown>;
  confirmTargetAbsent?: (
    target: TelegramTarget & { threadId: number },
  ) => Promise<TelegramWorkspaceRetirementAbsence>;
}): Promise<TelegramWorkspaceRetirementExecution> {
  return input.runExclusive(async () => {
    const isCurrent = () =>
      input.getLeaderEpoch() === input.intent.leaderEpoch &&
      input.getProfileKey() === input.intent.profileKey &&
      input.isCurrent?.() !== false;
    if (!isCurrent()) return { kind: "retained", reason: "authority-changed" };
    if (
      !input.intent.binding.slot ||
      !/^[A-Z]$/u.test(input.intent.binding.slot)
    ) {
      return { kind: "retained", reason: "stale-intent" };
    }
    const owner = input.admission.getOwner();
    const storedFence = input.admission.read().fence;
    if (storedFence && !isTelegramWorkspaceRetirementFence(storedFence)) {
      return { kind: "retained", reason: "fence-conflict" };
    }
    let fence: TelegramWorkspaceRetirementFence | undefined = storedFence;
    if (
      fence &&
      !matchesTelegramWorkspaceRetirementFence(fence, input.intent)
    ) {
      return { kind: "retained", reason: "fence-conflict" };
    }
    if (
      fence &&
      (fence.leaderEpoch !== input.intent.leaderEpoch ||
        fence.owner.processId !== owner.processId ||
        fence.owner.processBirthId !== owner.processBirthId)
    ) {
      fence = input.admission.adoptRetirementFence(fence, {
        owner,
        leaderEpoch: input.intent.leaderEpoch,
      });
    }
    if (fence?.phase === "deletion-rejected") {
      return cancelRejectedTelegramWorkspaceRetirement(
        { ...input, isCurrent },
        fence,
      );
    }
    const intent = input.store
      .listWorkspaceRetirementIntents()
      .find((candidate) => isDeepStrictEqual(candidate, input.intent));
    const binding = input.store
      .listWorkspaceBindings()
      .find(
        (candidate) => candidate.bindingKey === input.intent.binding.bindingKey,
      );
    if (
      !intent ||
      !binding ||
      !isDeepStrictEqual(binding, input.intent.binding)
    ) {
      if (!intent && !binding && fence?.phase === "commit-ready") {
        try {
          input.admission.completeRetirementFence(fence);
        } catch {
          if (input.admission.read().fence) {
            return { kind: "retained", reason: "fence-release-unconfirmed" };
          }
        }
        return {
          kind: "retired",
          bindingKey: input.intent.binding.bindingKey,
          slot: input.intent.binding.slot,
        };
      }
      return { kind: "retained", reason: "stale-intent" };
    }
    if (fence?.phase === "commit-ready") {
      // Exact confirmed absence retires any same-target routing record that
      // survived the deletion attempt. The fence prevents replacement target
      // publication while this stale local projection is removed.
      input.store.markStaleByTarget(binding.target, "deleted");
    }
    const eligible = () =>
      input.store
        .captureWorkspaceSlotOccupancy(input.getExternalProtection, {
          expectedRetirement: input.intent,
        })
        .bindings.find(
          (candidate) =>
            candidate.bindingKey === input.intent.binding.bindingKey,
        )?.protection === "eligible";
    if (!eligible()) return { kind: "retained", reason: "protection-changed" };
    if (!fence) {
      const acquired = input.admission.acquireRetirementFence({
        // A fresh attempt after proven rejection must not revive an old permit.
        operationId: createTelegramWorkspaceAdmissionOperationId(),
        retirementIntentId: input.intent.id,
        bindingKey: binding.bindingKey,
        slot: binding.slot!,
        target: binding.target,
        leaderEpoch: input.intent.leaderEpoch,
        retirementRequestedAtMs: input.intent.requestedAtMs,
      });
      if (acquired.kind === "blocked") {
        return {
          kind: "retained",
          reason:
            acquired.reason === "admission-active"
              ? "admission-active"
              : "fence-conflict",
        };
      }
      fence = acquired.fence;
    }
    if (!isCurrent()) return { kind: "retained", reason: "authority-changed" };
    if (fence.phase === "fenced" && !eligible()) {
      input.admission.releaseUnissuedRetirementFence(fence);
      return { kind: "retained", reason: "protection-changed" };
    }
    if (fence.phase === "fenced") {
      const issued = input.admission.issueDeletionPermit(fence);
      if (issued.kind !== "issued") {
        return { kind: "retained", reason: "delete-unconfirmed" };
      }
      fence = issued.fence;
      try {
        await input.deleteForumTopic(
          issued.permit,
          {
            chat_id: binding.target.chatId,
            message_thread_id: binding.target.threadId,
          },
          { maxAttempts: 1 },
        );
      } catch (error) {
        if (!isTelegramWorkspaceDeletionConfirmedAbsent(error)) {
          const target = getTelegramApiErrorRequestTarget(error);
          if (
            target &&
            sameTarget(target, binding.target) &&
            isTelegramApiRequestRejected(error, "deleteForumTopic")
          ) {
            fence = input.admission.confirmRetirementRejection(fence);
            const cancellation =
              await cancelRejectedTelegramWorkspaceRetirement(
                { ...input, isCurrent },
                fence,
              );
            return cancellation.kind === "cancelled"
              ? { kind: "retained", reason: "delete-rejected" }
              : cancellation;
          }
          return { kind: "retained", reason: "delete-unconfirmed" };
        }
      }
      fence = input.admission.confirmRetirementAbsence(fence);
    } else if (fence.phase === "deletion-issued") {
      const absence = await input.confirmTargetAbsent?.(binding.target);
      if (absence !== "absent") {
        return { kind: "retained", reason: "delete-unconfirmed" };
      }
      fence = input.admission.confirmRetirementAbsence(fence);
    }
    if (!isCurrent()) return { kind: "retained", reason: "authority-changed" };
    if (fence.phase === "commit-ready") {
      input.store.markStaleByTarget(binding.target, "deleted");
    }
    if (!eligible()) return { kind: "retained", reason: "protection-changed" };
    if (
      !(await input.store.commitWorkspaceRetirement(input.intent, isCurrent))
    ) {
      return { kind: "retained", reason: "commit-rejected" };
    }
    try {
      input.admission.completeRetirementFence(fence);
    } catch {
      if (input.admission.read().fence) {
        return { kind: "retained", reason: "fence-release-unconfirmed" };
      }
    }
    return {
      kind: "retired",
      bindingKey: binding.bindingKey,
      slot: binding.slot!,
    };
  });
}

export type TelegramWorkspaceRetirementPreparation =
  | { kind: "ready"; intent: TelegramWorkspaceRetirementIntent }
  | { kind: "not-needed"; reason: "free-capacity" }
  | {
      kind: "blocked";
      reason:
        | "invalid-state"
        | "protected-capacity"
        | "state-changed"
        | "existing-intent-conflict"
        | "stale-intent";
    };

export interface TelegramWorkspaceRetirementPreparationDeps {
  store: Pick<
    TelegramTopicTargetStore,
    | "captureWorkspaceSlotOccupancy"
    | "listWorkspaceBindings"
    | "listWorkspaceRetirementIntents"
    | "upsertWorkspaceRetirementIntent"
    | "removeWorkspaceRetirementIntent"
    | "persist"
  >;
  getExternalProtection: (
    binding: TelegramWorkspaceThreadBinding,
  ) => TelegramWorkspaceExternalProtectionEvidence;
  getLeaderEpoch: () => number | string | undefined;
  getProfileKey: () => string;
  isCurrent?: () => boolean;
  getNowMs?: () => number;
}

function findEligibleCandidate(
  occupancy: readonly TelegramWorkspaceSlotOccupancy[],
  reservedSlots: readonly string[],
  nowMs: number,
):
  | { kind: "candidate"; candidate: TelegramWorkspaceSlotOccupancy }
  | Exclude<TelegramWorkspaceRetirementPreparation, { kind: "ready" }> {
  const allocation = planTelegramWorkspaceSlotAllocation({
    bindings: occupancy,
    reservedSlots,
    nowMs,
  });
  if (allocation.kind === "free") {
    return { kind: "not-needed", reason: "free-capacity" };
  }
  if (allocation.kind === "blocked") return allocation;
  return { kind: "candidate", candidate: allocation.candidate };
}

export async function prepareTelegramWorkspaceRetirement(
  deps: TelegramWorkspaceRetirementPreparationDeps,
): Promise<TelegramWorkspaceRetirementPreparation> {
  const getNowMs = deps.getNowMs ?? Date.now;
  const leaderEpoch = deps.getLeaderEpoch();
  const profileKey = deps.getProfileKey();
  const isCurrent = () =>
    leaderEpoch !== undefined &&
    deps.getLeaderEpoch() === leaderEpoch &&
    deps.getProfileKey() === profileKey &&
    deps.isCurrent?.() !== false;
  if (!isCurrent())
    throw new Error(
      "Telegram Workspace retirement requires current leader authority.",
    );

  const existing = deps.store.listWorkspaceRetirementIntents();
  if (existing.length > 1) {
    return { kind: "blocked", reason: "existing-intent-conflict" };
  }
  if (existing.length === 1) {
    const intent = existing[0]!;
    const binding = deps.store
      .listWorkspaceBindings()
      .find((candidate) => candidate.bindingKey === intent.binding.bindingKey);
    const snapshot = deps.store.captureWorkspaceSlotOccupancy(
      deps.getExternalProtection,
      { expectedRetirement: intent },
    );
    const candidate = snapshot.bindings.find(
      (entry) => entry.bindingKey === intent.binding.bindingKey,
    );
    if (
      intent.profileKey !== profileKey ||
      intent.leaderEpoch !== leaderEpoch ||
      !binding ||
      !isDeepStrictEqual(binding, intent.binding) ||
      candidate?.protection !== "eligible"
    ) {
      return { kind: "blocked", reason: "stale-intent" };
    }
    if (!isCurrent())
      throw new Error("Telegram Workspace retirement lost leader authority.");
    await deps.store.persist();
    if (!isCurrent())
      throw new Error("Telegram Workspace retirement lost leader authority.");
    return { kind: "ready", intent };
  }

  const nowMs = getNowMs();
  const snapshot = deps.store.captureWorkspaceSlotOccupancy(
    deps.getExternalProtection,
  );
  const selection = findEligibleCandidate(
    snapshot.bindings,
    snapshot.reservedSlots,
    nowMs,
  );
  if (selection.kind !== "candidate") return selection;
  const selected = selection.candidate;
  const binding = deps.store
    .listWorkspaceBindings()
    .find((candidate) => candidate.bindingKey === selected.bindingKey);
  if (
    !binding?.slot ||
    binding.slot.toLowerCase() !== selected.slot ||
    binding.inactiveSinceMs !== selected.inactiveSinceMs
  ) {
    return { kind: "blocked", reason: "state-changed" };
  }
  const intent: TelegramWorkspaceRetirementIntent = {
    id: `workspace-retirement:pressure:${binding.bindingKey}:${binding.slot}:${binding.inactiveSinceMs}`,
    reason: "pressure",
    profileKey,
    binding,
    leaderEpoch: leaderEpoch!,
    requestedAtMs: nowMs,
  };
  if (!isCurrent())
    throw new Error("Telegram Workspace retirement lost leader authority.");
  if (!deps.store.upsertWorkspaceRetirementIntent(intent)) {
    return { kind: "blocked", reason: "state-changed" };
  }
  const rechecked = deps.store
    .captureWorkspaceSlotOccupancy(deps.getExternalProtection, {
      expectedRetirement: intent,
    })
    .bindings.find((candidate) => candidate.bindingKey === binding.bindingKey);
  if (rechecked?.protection !== "eligible" || !isCurrent()) {
    deps.store.removeWorkspaceRetirementIntent(intent);
    if (!isCurrent())
      throw new Error("Telegram Workspace retirement lost leader authority.");
    return { kind: "blocked", reason: "state-changed" };
  }
  await deps.store.persist();
  if (!isCurrent())
    throw new Error("Telegram Workspace retirement lost leader authority.");
  return { kind: "ready", intent };
}

export type TelegramWorkspaceRetirementLifecycleResult =
  | TelegramWorkspaceRetirementExecution
  | Extract<TelegramWorkspaceRetirementPreparation, { kind: "not-needed" }>
  | {
      kind: "blocked";
      stage: "preparation" | "adoption";
      reason: string;
    };

export async function runTelegramWorkspaceRetirementLifecycle(input: {
  store: TelegramWorkspaceRetirementPreparationDeps["store"] &
    Pick<
      TelegramTopicTargetStore,
      | "replaceWorkspaceRetirementIntent"
      | "commitWorkspaceRetirement"
      | "markStaleByTarget"
    >;
  getExternalProtection: (
    binding: TelegramWorkspaceThreadBinding,
  ) => TelegramWorkspaceExternalProtectionEvidence;
  getLeaderEpoch: () => number | string | undefined;
  getProfileKey: () => string;
  isCurrent?: () => boolean;
  getNowMs?: () => number;
  runExclusive: <T>(operation: () => Promise<T>) => Promise<T>;
  admission: Parameters<
    typeof executeTelegramWorkspaceRetirement
  >[0]["admission"];
  deleteForumTopic: Parameters<
    typeof executeTelegramWorkspaceRetirement
  >[0]["deleteForumTopic"];
  confirmTargetAbsent?: Parameters<
    typeof executeTelegramWorkspaceRetirement
  >[0]["confirmTargetAbsent"];
}): Promise<TelegramWorkspaceRetirementLifecycleResult> {
  let intent = input.store.listWorkspaceRetirementIntents()[0];
  const retainedFence = input.admission.read().fence;
  if (
    retainedFence &&
    isTelegramWorkspaceRetirementFence(retainedFence) &&
    retainedFence.phase === "deletion-rejected"
  ) {
    return input.runExclusive(() =>
      cancelRejectedTelegramWorkspaceRetirement(input, retainedFence),
    );
  }
  if (
    !intent &&
    retainedFence &&
    isTelegramWorkspaceRetirementFence(retainedFence) &&
    (retainedFence.destructiveKind ?? "pressure-retirement") ===
      "pressure-retirement" &&
    retainedFence.phase === "commit-ready"
  ) {
    return input.runExclusive(async () => {
      const epoch = input.getLeaderEpoch();
      if (
        epoch === undefined ||
        input.getProfileKey() !== retainedFence.profileKey ||
        input.isCurrent?.() === false
      )
        return { kind: "retained", reason: "authority-changed" };
      if (
        input.store.listWorkspaceRetirementIntents().length ||
        input.store
          .listWorkspaceBindings()
          .some((binding) => binding.bindingKey === retainedFence.bindingKey)
      ) {
        return { kind: "retained", reason: "stale-intent" };
      }
      const owner = input.admission.getOwner();
      const fence =
        retainedFence.leaderEpoch === epoch &&
        isDeepStrictEqual(retainedFence.owner, owner)
          ? retainedFence
          : input.admission.adoptRetirementFence(retainedFence, {
              owner,
              leaderEpoch: epoch,
            });
      input.admission.completeRetirementFence(fence);
      return {
        kind: "retired",
        bindingKey: fence.bindingKey,
        slot: fence.slot,
      };
    });
  }
  if (intent) {
    if (
      retainedFence &&
      isTelegramWorkspaceRetirementFence(retainedFence) &&
      retainedFence.phase === "commit-ready" &&
      matchesTelegramWorkspaceRetirementFence(retainedFence, intent)
    ) {
      const resumed = await input.runExclusive(async () => {
        const epoch = input.getLeaderEpoch();
        const profileKey = input.getProfileKey();
        const isCurrent = () =>
          epoch !== undefined &&
          input.getLeaderEpoch() === epoch &&
          input.getProfileKey() === profileKey &&
          input.isCurrent?.() !== false;
        if (!isCurrent() || intent!.profileKey !== profileKey) return undefined;
        const intents = input.store.listWorkspaceRetirementIntents();
        const binding = input.store
          .listWorkspaceBindings()
          .find(
            (candidate) => candidate.bindingKey === intent!.binding.bindingKey,
          );
        if (
          intents.length !== 1 ||
          !isDeepStrictEqual(intents[0], intent) ||
          !binding ||
          !isDeepStrictEqual(binding, intent.binding)
        )
          return undefined;
        if (intent!.leaderEpoch === epoch) return intent;
        const replacement = { ...intent!, leaderEpoch: epoch! };
        return (await input.store.replaceWorkspaceRetirementIntent(
          intent!,
          replacement,
          isCurrent,
        ))
          ? replacement
          : undefined;
      });
      if (!resumed)
        return {
          kind: "blocked",
          stage: "adoption",
          reason: "commit-rejected",
        };
      intent = resumed;
    } else {
      const adoption = await adoptTelegramWorkspaceRetirementIntent({
        store: input.store,
        intent,
        getExternalProtection: input.getExternalProtection,
        getLeaderEpoch: input.getLeaderEpoch,
        getProfileKey: input.getProfileKey,
        isCurrent: input.isCurrent,
        runExclusive: input.runExclusive,
      });
      if (adoption.kind !== "adopted") {
        return { kind: "blocked", stage: "adoption", reason: adoption.reason };
      }
      intent = adoption.intent;
    }
  } else {
    const preparation = await prepareTelegramWorkspaceRetirement({
      store: input.store,
      getExternalProtection: input.getExternalProtection,
      getLeaderEpoch: input.getLeaderEpoch,
      getProfileKey: input.getProfileKey,
      isCurrent: input.isCurrent,
      getNowMs: input.getNowMs,
    });
    if (preparation.kind === "not-needed") return preparation;
    if (preparation.kind === "blocked") {
      return {
        kind: "blocked",
        stage: "preparation",
        reason: preparation.reason,
      };
    }
    intent = preparation.intent;
  }
  return executeTelegramWorkspaceRetirement({
    store: input.store,
    intent,
    getExternalProtection: input.getExternalProtection,
    getLeaderEpoch: input.getLeaderEpoch,
    getProfileKey: input.getProfileKey,
    isCurrent: input.isCurrent,
    runExclusive: input.runExclusive,
    admission: input.admission,
    deleteForumTopic: input.deleteForumTopic,
    confirmTargetAbsent: input.confirmTargetAbsent,
  });
}

export type TelegramWorkspaceCapacityRunner = <T>(
  operation: () => Promise<T>,
) => Promise<T>;

export interface TelegramWorkspaceSlotRotationPorts extends TelegramWorkspaceOperationGate {
  getAdmission: () => TelegramWorkspaceAdmissionLedger | undefined;
  deleteThread: TelegramWorkspaceThreadDeletionTransport;
  pruneJournalEvidence?: (
    binding: TelegramWorkspaceThreadBinding,
    isCurrent: () => boolean,
  ) => Promise<TelegramWorkspaceJournalPruneResult>;
  reclaimDeadOwnerQueuedWork?: (
    binding: TelegramWorkspaceThreadBinding,
    isCurrent: () => boolean,
  ) => Promise<TelegramWorkspaceDeadQueueReclamation>;
}

/** Retry allocation once, only after the failed operation released all ordinary leases. */
export function createTelegramWorkspaceSlotRotation(
  input: TelegramWorkspaceSlotRotationPorts & {
    store: TelegramTopicTargetStore;
    getLeaderEpoch: () => number | string | undefined;
    getExternalProtection: (
      binding: TelegramWorkspaceThreadBinding,
    ) => TelegramWorkspaceExternalProtectionEvidence;
    recordEvent: (message: string, details: Record<string, unknown>) => void;
  },
): TelegramWorkspaceCapacityRunner {
  const requests = createTelegramWorkspaceOperationGate();
  return async (operation) => {
    const admission = input.getAdmission();
    const epoch = input.getLeaderEpoch();
    if (!admission || epoch === undefined) return operation();
    const profileKey = admission.getProfileKey();
    const isCurrent = () =>
      input.getLeaderEpoch() === epoch &&
      input.getAdmission()?.getProfileKey() === profileKey;
    return requests.runExclusive(async () => {
      if (!isCurrent())
        throw new Error("Telegram Workspace allocation lost leader authority.");
      const rotate = async () => {
        const result = await input.runExclusive(async () => {
          if (!isCurrent())
            throw new Error(
              "Telegram Workspace rotation lost leader authority.",
            );
          await input.store.load();
          if (!isCurrent())
            throw new Error(
              "Telegram Workspace rotation lost leader authority.",
            );
          return runTelegramWorkspaceRetirementLifecycle({
            store: input.store,
            admission,
            getExternalProtection: input.getExternalProtection,
            getLeaderEpoch: input.getLeaderEpoch,
            getProfileKey: () => profileKey,
            isCurrent,
            // This entire lifecycle already owns the shared gate, without an admission lease.
            runExclusive: async (action) => action(),
            deleteForumTopic(permit, body) {
              return input.deleteThread(() => {
                const fence = admission.read().fence;
                if (
                  !isCurrent() ||
                  !fence ||
                  !isTelegramWorkspaceRetirementFence(fence) ||
                  (fence.destructiveKind ?? "pressure-retirement") !==
                    "pressure-retirement" ||
                  (permit.destructiveKind ?? "pressure-retirement") !==
                    "pressure-retirement" ||
                  fence.phase !== "deletion-issued" ||
                  !isDeepStrictEqual(fence.owner, admission.getOwner()) ||
                  fence.profileKey !== permit.profileKey ||
                  fence.leaderEpoch !== permit.leaderEpoch ||
                  fence.operationId !== permit.operationId ||
                  fence.retirementIntentId !== permit.retirementIntentId ||
                  fence.bindingKey !== permit.bindingKey ||
                  fence.slot !== permit.slot ||
                  fence.deletionIssuedAtMs !== permit.issuedAtMs ||
                  !sameTarget(fence.target, permit.target) ||
                  body.chat_id !== permit.target.chatId ||
                  body.message_thread_id !== permit.target.threadId
                ) {
                  throw new Error(
                    "Telegram Workspace deletion permit is stale.",
                  );
                }
                return { ...permit.target };
              });
            },
          });
        });
        if (
          result.kind !== "retired" &&
          result.kind !== "not-needed" &&
          result.kind !== "cancelled"
        ) {
          throw new Error(
            `Telegram Workspace slots A-Z are unavailable; rotation blocked (${result.reason}).`,
          );
        }
        if (result.kind === "retired") {
          try {
            input.recordEvent("Telegram Workspace slot rotated.", {
              slot: result.slot,
            });
          } catch {
            /* Diagnostics cannot turn completed retirement into another attempt. */
          }
        }
        if (!isCurrent())
          throw new Error("Telegram Workspace rotation lost leader authority.");
        return result;
      };
      const pending = await input.runExclusive(async () => {
        await input.store.load();
        if (!isCurrent())
          throw new Error(
            "Telegram Workspace allocation lost leader authority.",
          );
        return input.store.listWorkspaceRetirementIntents().length > 0;
      });
      const fence = admission.read().fence;
      if (
        pending ||
        (fence &&
          isTelegramWorkspaceRetirementFence(fence) &&
          (fence.destructiveKind ?? "pressure-retirement") ===
            "pressure-retirement")
      ) {
        const recovered = await rotate();
        if (recovered.kind !== "cancelled") return operation();
      }
      try {
        return await operation();
      } catch (error) {
        if (!(error instanceof TelegramWorkspaceSlotUnavailableError))
          throw error;
        if (input.pruneJournalEvidence || input.reclaimDeadOwnerQueuedWork) {
          const candidates = await input.runExclusive(async () => {
            await input.store.load();
            if (!isCurrent())
              throw new Error(
                "Telegram Workspace reclamation lost leader authority.",
              );
            const snapshot = input.store.captureWorkspaceSlotOccupancy(
              input.getExternalProtection,
            );
            const allocation = planTelegramWorkspaceSlotAllocation({
              ...snapshot,
              nowMs: Date.now(),
            });
            if (
              allocation.kind === "free" ||
              (allocation.kind === "blocked" &&
                allocation.reason !== "protected-capacity")
            )
              return [];
            if (allocation.kind === "reclaim" && !input.pruneJournalEvidence)
              return [];
            return input.store
              .listWorkspaceBindings()
              .filter(
                (binding) =>
                  (allocation.kind !== "reclaim" ||
                    binding.bindingKey === allocation.candidate.bindingKey) &&
                  typeof binding.inactiveSinceMs === "number" &&
                  Number.isFinite(binding.inactiveSinceMs) &&
                  binding.inactiveSinceMs >= 0,
              )
              .sort(
                (left, right) =>
                  left.inactiveSinceMs! - right.inactiveSinceMs! ||
                  (left.slot ?? "").localeCompare(right.slot ?? ""),
              );
          });
          for (const original of candidates) {
            let binding = original;
            if (
              input.pruneJournalEvidence &&
              (binding.journalBindingKeys?.length ?? 0) +
                (binding.journalSources?.length ?? 0) >
                0
            ) {
              const pruned = await input.pruneJournalEvidence(
                binding,
                isCurrent,
              );
              if (!isCurrent())
                throw new Error(
                  "Telegram Workspace pruning lost leader authority.",
                );
              if (pruned.kind !== "committed") continue;
              binding = pruned.binding;
              const removed =
                pruned.removedKeys.length +
                (pruned.removedSources?.length ?? 0);
              if (removed > 0) {
                try {
                  input.recordEvent(
                    "Telegram Workspace journal evidence pruned.",
                    { slot: binding.slot, removed },
                  );
                } catch {
                  /* Diagnostics cannot revoke a published metadata subset. */
                }
              }
            }
            if (!input.reclaimDeadOwnerQueuedWork) continue;
            const evidence = input.getExternalProtection(binding);
            if (
              evidence.liveOwner !== "clear" ||
              evidence.acceptedWork !== "protected" ||
              evidence.deliveryAuthority !== "clear"
            )
              continue;
            const reclaimed = await input.reclaimDeadOwnerQueuedWork(
              binding,
              isCurrent,
            );
            if (reclaimed.kind === "recovered") {
              try {
                input.recordEvent(
                  "Telegram Workspace dead-owner queue reclaimed.",
                  {
                    slot: binding.slot,
                    receipts: reclaimed.receipts,
                    updateCount: reclaimed.updateIds.length,
                  },
                );
              } catch {
                /* Diagnostics cannot revoke journal-owned recovery. */
              }
              break;
            }
          }
        }
        await rotate();
        return operation();
      }
    });
  };
}
