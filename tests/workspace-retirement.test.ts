/**
 * Workspace retirement preparation tests
 * Zones: telegram, workspace identity, lifecycle
 * Mirrors lib/workspace-retirement.ts and excludes live Telegram deletion.
 */

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, mkdir, realpath, rm, readdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, basename, join } from "node:path";
import test from "node:test";

import {
  createTelegramTopicTargetStore,
  createTelegramWorkspaceBindingIdentity,
  type TelegramWorkspaceExternalProtectionEvidence,
  type TelegramWorkspaceThreadBinding,
} from "../lib/threads.ts";
import {
  adoptTelegramWorkspaceRetirementIntent,
  captureTelegramWorkspaceExternalProtection,
  captureTelegramWorkspaceJournalProtectionSources,
  createTelegramWorkspaceDeadOwnerQueueReclaimer,
  createTelegramWorkspaceExternalProtectionCapture,
  createTelegramWorkspaceProtectionObserver,
  createTelegramWorkspaceJournalEvidencePruner,
  isCurrentTelegramWorkspaceBinding,
  createTelegramWorkspaceOperationGate,
  createTelegramWorkspaceOperationRuntime,
  createTelegramWorkspaceSlotRotation,
  executeTelegramWorkspaceRetirement,
  prepareTelegramWorkspaceRetirement,
  pruneTelegramWorkspaceJournalEvidence,
  resolveTelegramWorkspaceAcceptedWorkProtection,
  runTelegramWorkspaceRetirementLifecycle,
} from "../lib/workspace-retirement.ts";
import {
  createDefaultTelegramBridgeApiRuntime,
  setTelegramApiHttpsFetchForTesting,
  TelegramApiStaleTargetError,
} from "../lib/telegram-api.ts";
import { TelegramWorkspaceSlotUnavailableError } from "../lib/workspace-slots.ts";
import { resolveTelegramSessionJournalPath, resolveTelegramFollowerJournalPath, resolveTelegramSessionPollingJournalPath, resolveTelegramTempDir } from "../lib/paths.ts";
import { createTelegramWorkspaceAdmissionLedger } from "../lib/workspace-admission.ts";
import {
  createTelegramUpdateJournalBotIdentity,
  createTelegramUpdateJournalBindingKey,
  createTelegramUpdateJournalBindingRuntime,
  createTelegramUpdateJournalStore,
  createTelegramUpdateJournalReferenceRegistry,
  inspectTelegramUpdateJournalFamily,
  inspectTelegramSessionJournalNamespace,
  type TelegramUpdateJournalEntry,
} from "../lib/journal.ts";

for (const state of ["empty", "untracked-pending", "committed-original", "uncommitted-original", "missing-original", "corrupt-session", "retained-only", "missing-resolver"] as const) {
  test(`Production protection requires fresh strict session namespace evidence (${state})`, { skip: process.platform === "win32" }, async () => {
    const agentDir = await realpath(await mkdtemp(join(tmpdir(), "pi-namespace-protection-")));
    const directory = join(agentDir, "tmp", "pi-telegram");
    const sessionDir = join(directory, "sessions", "session");
    const path = join(sessionDir, "journal.aaaaaaaaaaaaaaaa.json");
    const botIdentity = createTelegramUpdateJournalBotIdentity({ botToken: "fixture:namespace-protection", botId: 7 });
    const binding: TelegramWorkspaceThreadBinding = { ...createTelegramWorkspaceBindingIdentity("/recipient", 0, "session")!,
      target: { chatId: 7, threadId: 42 }, slot: "A", updatedAtMs: 1, journalBindingsComplete: true };
    const limits = { maxDirectoryEntries: 100, maxFiles: 100, maxBytes: 1_000_000, maxEntries: 100, maxWork: 10_000 };
    try {
      await mkdir(sessionDir, { recursive: true });
      const journal = createTelegramUpdateJournalStore({ path, botIdentity });
      journal.appendBatch([{ update_id: 1, message: { message_id: 1, message_thread_id: 42,
        chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "Untracked accepted work" } }]);
      if (state.endsWith("-original")) {
        const { exists: _exists, serializedBytes: _bytes, ...beforeCancel } = journal.read();
        const cancellation = journal.abandonPending({ entry: beforeCancel.entries[0]!, operatorAuthorityId: "owner:7", isCurrent: () => true,
          journalBindingKey: createTelegramUpdateJournalBindingKey({ path, botIdentity }) });
        if (state === "uncommitted-original") {
          await writeFile(path, JSON.stringify(beforeCancel));
          await rm(`${path}.segments`, { recursive: true, force: true });
          assert.deepEqual(inspectTelegramSessionJournalNamespace({ directory, profile: "default", botIdentity, limits })
            .retainedInputs?.map(original => original.state), ["uncommitted"], "Protection must refuse a valid uncommitted original, not a malformed fixture");
        } else if (state === "missing-original") await rm(cancellation.retainedPath);
      } else if (state !== "untracked-pending") journal.removeCompleted([1]);
      if (state === "corrupt-session") await writeFile(path, "{broken");
      if (state === "retained-only") {
        await mkdir(`${path}.retained`);
        await rm(path);
      }
      const bytes = async () => Promise.all((await readdir(directory, { recursive: true, withFileTypes: true }))
        .sort((left, right) => join(left.parentPath, left.name).localeCompare(join(right.parentPath, right.name)))
        .map(async entry => [join(entry.parentPath, entry.name), entry.isFile() ? await readFile(join(entry.parentPath, entry.name)) : "directory"]));
      const before = await bytes();
      const references = createTelegramUpdateJournalReferenceRegistry();
      let inspections = 0;
      let resolutions = 0;
      const capture = createTelegramWorkspaceExternalProtectionCapture({
        listFollowers: () => [], getActiveTurnTarget: () => undefined, getQueuedItems: () => [],
        getJournalWriterProtection: () => "clear", getDeliveryAuthorityProtection: () => "clear",
        resolveLeaderJournal: () => ({ journal: { read: () => ({ entries: [] }) } }),
        createFollowerJournalResolver: () => () => { assert.fail("No legacy key is registered"); },
        discoverFollowerJournals() { assert.fail("Strict projection must not fall back to unbounded legacy discovery"); },
        inspectJournalNamespace() {
          inspections++;
          return inspectTelegramSessionJournalNamespace({ directory, profile: "default", botIdentity, limits });
        },
        ...(state === "missing-resolver" ? {} : { createJournalPathResolver(discovered: string) {
          resolutions++;
          assert.equal(discovered, path);
          return () => ({ recoveryKey: path,
            journal: { read() { assert.fail("Protection must never repair or replay"); } },
            readForProtection() {
              assert.deepEqual(references.list(), [{ referenceClass: "workspace-retirement", recoveryKey: path }]);
              const evidence = inspectTelegramUpdateJournalFamily({ directory, path, profile: "default", botIdentity, limits });
              if (evidence.kind !== "present") throw new Error("Missing discovered evidence");
              return { entries: evidence.file.entries };
            } });
        } }),
        withJournalReference(reader, operation) {
          return reader.recoveryKey ? references.withReference({ referenceClass: "workspace-retirement", recoveryKey: reader.recoveryKey }, operation) : operation();
        },
      });
      assert.equal(capture(binding).acceptedWork, state === "empty" || state === "committed-original" ? "clear" : state === "untracked-pending" ? "protected" : "unknown");
      assert.equal(inspections, 1, "Even complete binding addresses must not bypass namespace validation");
      assert.equal(resolutions, state === "empty" || state === "untracked-pending" || state === "committed-original" ? 1 : 0);
      assert.deepEqual(references.list(), []);
      assert.deepEqual(await bytes(), before, "No settlement, repair, pruning or filesystem cleanup");
      if (state === "empty") {
        await writeFile(path, "{broken later");
        assert.equal(capture(binding).acceptedWork, "unknown");
        assert.equal(inspections, 2, "Namespace success is not cached across captures");
        assert.equal(await readFile(path, "utf8"), "{broken later");
      }
    } finally { await rm(agentDir, { recursive: true, force: true }); }
  });
}

test("Strict namespace protection refuses a missing result or omitted polling evidence", () => {
  const binding: TelegramWorkspaceThreadBinding = { ...createTelegramWorkspaceBindingIdentity("/recipient", 0, "session")!,
    target: { chatId: 7, threadId: 42 }, slot: "A", updatedAtMs: 1, journalBindingsComplete: true };
  for (const invalid of [undefined, { sources: [], accounting: { directoryEntries: 0, files: 0, bytes: 0, work: 0 } }]) {
    const capture = createTelegramWorkspaceExternalProtectionCapture({
      listFollowers: () => [], getActiveTurnTarget: () => undefined, getQueuedItems: () => [],
      resolveLeaderJournal: () => ({ journal: { read: () => ({ entries: [] }) } }),
      createFollowerJournalResolver: () => () => undefined,
      inspectJournalNamespace: () => invalid as ReturnType<typeof inspectTelegramSessionJournalNamespace>,
      discoverFollowerJournals() { assert.fail("Invalid strict evidence cannot fall back to weaker discovery"); },
      createJournalPathResolver: () => () => undefined,
    });
    assert.equal(capture(binding).acceptedWork, "unknown");
  }
});

for (const scope of ["binding", "shared", "discovered"] as const) for (const availability of ["queued", "empty", "empty-incomplete", "unreadable"] as const) {
  test(`Strict journal capture preserves rerouted accepted work (${scope}, ${availability})`, async () => {
    const directory = await mkdtemp(join(tmpdir(), "pi-restore-provenance-"));
    const path = join(directory, "inbox.json");
    const botIdentity = createTelegramUpdateJournalBotIdentity({ botToken: "fixture:provenance", botId: 7 });
    const owner = { instanceId: "recipient", processId: process.pid, processBirthId: `${process.pid}:recipient`, sessionGeneration: 2 };
    const journal = createTelegramUpdateJournalStore({ path, profileName: "default", botIdentity,
      queueRuntimeIdentity: owner, getQueueProcessLiveness: () => "alive" });
    try {
      journal.appendBatch([{ update_id: 100, message: { message_id: 100, message_thread_id: 77,
        chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "Rerouted accepted work" } }]);
      if (availability === "empty" || availability === "empty-incomplete") journal.removeCompleted([100]);
      else journal.markQueued({ queueKind: "prompt", receiptId: "accepted", sourceUpdateIds: [100], owner });
      assert.deepEqual(journal.read().entries.map(entry => entry.state),
        availability === "empty" || availability === "empty-incomplete" ? [] : ["queued"]);
      if (availability === "unreadable") await writeFile(path, "{broken", { mode: 0o600 });
      const bytes = async () => Promise.all((await readdir(directory, { recursive: true, withFileTypes: true }))
        .sort((left, right) => join(left.parentPath, left.name).localeCompare(join(right.parentPath, right.name)))
        .map(async entry => { const name = join(entry.parentPath, entry.name); return [name, entry.isFile() ? await readFile(name) : "directory"]; }));
      const before = await bytes();
      const references = createTelegramUpdateJournalReferenceRegistry();
      const source = { recoveryKey: path,
        journal: { read() { assert.fail("Protection must not invoke a repairing reader"); } },
        readForProtection() {
          assert.deepEqual(references.list(), [{ referenceClass: "workspace-retirement", recoveryKey: path }]);
          const evidence = inspectTelegramUpdateJournalFamily({ directory, path, profile: "default", botIdentity,
            limits: { maxFiles: 128, maxBytes: 1_000_000, maxEntries: 128, maxWork: 10_000 } });
          if (evidence.kind !== "present") throw new Error("Journal protection evidence unavailable");
          return { entries: evidence.file.entries };
        } };
      const empty = { recoveryKey: "fixture:empty-shared", journal: { read: () => ({ entries: [] }) } };
      const binding: TelegramWorkspaceThreadBinding = { ...createTelegramWorkspaceBindingIdentity("/recipient", 0, "session")!,
        target: { chatId: 7, threadId: 42 }, slot: "A", updatedAtMs: 1,
        journalBindingKeys: scope === "binding" ? ["manual:recipient"] : [], journalBindingsComplete: scope === "discovered" || availability === "empty-incomplete" ? undefined : true };
      const capture = createTelegramWorkspaceExternalProtectionCapture({ listFollowers: () => [],
        getActiveTurnTarget: () => undefined, getQueuedItems: () => [],
        resolveLeaderJournal: () => scope === "shared" ? source : empty,
        createFollowerJournalResolver(key) { assert.equal(key, "manual:recipient"); return () => source; },
        discoverFollowerJournals: () => ({ paths: scope === "discovered" ? [path] : [], complete: availability !== "empty-incomplete" }),
        createJournalPathResolver(discovered) { assert.equal(discovered, path); return () => source; },
        withJournalReference(reader, operation) {
          return references.withReference({ referenceClass: "workspace-retirement", recoveryKey: reader.recoveryKey! }, operation);
        },
      });
      const legacy = capture(binding).acceptedWork;
      const strict = capture(binding, { requireBindingProvenance: true }).acceptedWork;
      assert.equal(legacy, availability === "unreadable" || availability === "empty-incomplete" ? "unknown" : availability === "queued" && scope === "binding" ? "protected" : "clear");
      assert.equal(strict, availability === "empty" ? "clear" : availability === "queued" && scope === "binding" ? "protected" : "unknown",
        "a foreign original message target is not execution-target evidence");
      assert.deepEqual(references.list(), []);
      assert.deepEqual(await bytes(), before, "protection cannot settle, prune, repair or rewrite the source family");
    } finally { await rm(directory, { recursive: true, force: true }); }
  });
}

const clearExternalProtection = () => ({
  liveOwner: "clear" as const,
  acceptedWork: "clear" as const,
  deliveryAuthority: "clear" as const,
});

function createRetirementAdmission(path: string, owner = "executor") {
  return createTelegramWorkspaceAdmissionLedger({
    path,
    profileKey: "default",
    owner: {
      processId: process.pid,
      processBirthId: `${process.pid}:retirement-${owner}`,
    },
    getProcessLiveness: () => "alive",
  });
}

function addBinding(
  store: ReturnType<typeof createTelegramTopicTargetStore>,
  index: number,
  inactiveSinceMs: number,
  sessionId?: string,
): void {
  const slot = String.fromCharCode("A".charCodeAt(0) + index);
  store.upsertWorkspaceBinding({
    ...createTelegramWorkspaceBindingIdentity(
      `/repo/${index}`, 0, sessionId)!,
    target: { chatId: 7, threadId: 40 + index },
    slot,
    threadName: `Workspace${index}`,
    inactiveSinceMs,
    updatedAtMs: 100,
  });
}

test("Workspace operation gate serializes effects and recovers after failure", async () => {
  const gate = createTelegramWorkspaceOperationGate();
  const events: string[] = [];
  let release: (() => void) | undefined;
  const blocker = new Promise<void>((resolve) => { release = resolve; });
  const first = gate.runExclusive(async () => {
    events.push("first:start");
    await blocker;
    events.push("first:end");
  });
  const second = gate.runExclusive(async () => { events.push("second"); throw new Error("fixture"); });
  const third = gate.runExclusive(async () => { events.push("third"); return 3; });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.deepEqual(events, ["first:start"]);
  release?.();
  await first;
  await assert.rejects(second, /fixture/);
  assert.equal(await third, 3);
  assert.deepEqual(events, ["first:start", "first:end", "second", "third"]);
});

test("Journal evidence enumeration reads shared and historical follower journals and preserves incomplete legacy state", () => {
  const binding = { ...createTelegramWorkspaceBindingIdentity("/repo")!,
    target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2,
    journalBindingKeys: ["manual:old", "manual:new"], journalBindingsComplete: true as const };
  const entries = new Map<string, Array<{ update: unknown }>>([
    ["leader", [{ update: { message: { chat: { id: 7 }, message_thread_id: 41 } } }]],
    ["manual:old", []],
    ["manual:new", [{ update: { undecodable: true } }]],
  ]);
  const references: string[] = [];
  const capture = (candidate: TelegramWorkspaceThreadBinding) => captureTelegramWorkspaceJournalProtectionSources({
    binding: candidate,
    resolveLeader: () => ({ recoveryKey: "leader",
      journal: { read: () => ({ entries: entries.get("leader")! }) } }),
    createFollowerResolver: (key) => () => ({ recoveryKey: key,
      journal: { read: () => ({ entries: entries.get(key)! }) } }),
    withJournalReference(binding, operation) {
      references.push(`acquire:${binding.recoveryKey}`);
      try { return operation(); } finally { references.push(`release:${binding.recoveryKey}`); }
    },
  });
  const complete = capture(binding);
  assert.deepEqual(references, ["acquire:leader", "release:leader",
    "acquire:manual:old", "release:manual:old",
    "acquire:manual:new", "release:manual:new"]);
  assert.equal(complete.complete, true);
  assert.deepEqual(complete.sources.map((source) => source.scope), [
    { kind: "shared" },
    { kind: "binding", bindingKey: binding.bindingKey, journalBindingKey: "manual:old" },
    { kind: "binding", bindingKey: binding.bindingKey, journalBindingKey: "manual:new" },
  ]);
  assert.equal(resolveTelegramWorkspaceAcceptedWorkProtection({ binding,
    localAcceptedTargets: [], journalSources: complete.sources,
    sourcesComplete: complete.complete }), "protected");
  assert.equal(capture({ ...binding, journalBindingsComplete: undefined }).complete, false);
  const legacy = captureTelegramWorkspaceJournalProtectionSources({
    binding: { ...binding, journalBindingsComplete: undefined },
    resolveLeader: () => ({ journal: { read: () => ({ entries: [] }) } }),
    createFollowerResolver: () => () => ({ journal: { read: () => ({ entries: [] }) } }),
    discovery: {
      paths: ["/journals/follower-inbox-legacy.json"], complete: true,
      createResolver: () => () => ({ journal: { read: () => ({ entries: [{ update: {
        message: { chat: { id: 7 }, message_thread_id: 42 },
      } }] }) } }),
    },
  });
  assert.equal(legacy.complete, true);
  assert.deepEqual(legacy.sources.at(-1)?.scope,
    { kind: "discovered", path: "/journals/follower-inbox-legacy.json" });
  assert.equal(resolveTelegramWorkspaceAcceptedWorkProtection({ binding,
    localAcceptedTargets: [], journalSources: legacy.sources,
    sourcesComplete: legacy.complete }), "protected");
  const unreadable = captureTelegramWorkspaceJournalProtectionSources({
    binding,
    resolveLeader: () => { throw new Error("journal unavailable"); },
    createFollowerResolver: () => () => undefined,
  });
  assert.equal(unreadable.complete, false);
  assert.ok(unreadable.sources.every((source) => source.kind === "unknown"));
});

test("Session-qualified journal protection reads both session addresses and refuses unavailable resolution", () => {
  const binding = { ...createTelegramWorkspaceBindingIdentity("/repo", 0, "new-session")!,
    target: { chatId: 7, threadId: 42 }, slot: "A", updatedAtMs: 1,
    journalSources: [{ sessionId: "old-session", recipientBindingKey: "manual:same" },
      { sessionId: "new-session", recipientBindingKey: "manual:same" }], journalBindingsComplete: true as const };
  const base = { binding,
    resolveLeader: () => ({ journal: { read: () => ({ entries: [] }) } }),
    createFollowerResolver: () => () => { throw new Error("Session custody must not use a key-only resolver"); },
    discovery: { paths: [], complete: true, createResolver: () => () => undefined },
  };
  const unavailable = captureTelegramWorkspaceJournalProtectionSources(base);
  assert.equal(unavailable.complete, false, "An empty flat discovery cannot certify exact session sources");
  assert.equal(resolveTelegramWorkspaceAcceptedWorkProtection({ binding, localAcceptedTargets: [],
    journalSources: unavailable.sources, sourcesComplete: unavailable.complete }), "unknown");
  const reads: string[] = [];
  const resolved = captureTelegramWorkspaceJournalProtectionSources({ ...base,
    createSessionResolver(key, sessionId) {
      assert.equal(key, "manual:same");
      return () => ({ journal: { read() {
        reads.push(sessionId);
        return { entries: sessionId === "old-session" ? [{ update: { opaqueRetainedCommand: true } }] : [] };
      } } });
    },
  });
  assert.deepEqual(reads, ["old-session", "new-session"]);
  assert.equal(resolved.complete, true);
  assert.deepEqual(resolved.sources.slice(1).map(source => source.scope), binding.journalSources.map(source => ({
    kind: "binding", bindingKey: binding.bindingKey, journalBindingKey: source.recipientBindingKey, sessionId: source.sessionId,
  })));
  assert.equal(resolveTelegramWorkspaceAcceptedWorkProtection({ binding, localAcceptedTargets: [],
    journalSources: resolved.sources, sourcesComplete: resolved.complete }), "protected",
    "Exact binding custody remains protected even when its input cannot be projected to the new target");
});

test("External protection includes exact session sources and their process writers without repairing reads", () => {
  const binding = { ...createTelegramWorkspaceBindingIdentity("/repo", 0, "session-new")!,
    target: { chatId: 7, threadId: 42 }, slot: "A", updatedAtMs: 1,
    journalSources: [{ sessionId: "session-old", recipientBindingKey: "manual:writer" },
      { sessionId: "session-new", recipientBindingKey: "manual:writer" }], journalBindingsComplete: true as const };
  let retainedWork = true;
  let available = true;
  let writer: "clear" | "protected" | "unknown" = "clear";
  const writerKeys: string[] = [];
  const reads: string[] = [];
  const references = createTelegramUpdateJournalReferenceRegistry();
  const deps = {
    listFollowers: () => [], getActiveTurnTarget: () => undefined, getQueuedItems: () => [],
    resolveLeaderJournal: () => ({ journal: { read: () => ({ entries: [] }) } }),
    createFollowerJournalResolver: () => () => { throw new Error("No legacy address was supplied"); },
    getJournalWriterProtection(key: string) { writerKeys.push(key); return writer; },
    getDeliveryAuthorityProtection: () => "clear" as const,
  };
  const capture = createTelegramWorkspaceExternalProtectionCapture({ ...deps,
    createSessionJournalResolver(key, sessionId) {
      assert.equal(key, "manual:writer");
      return () => available ? { recoveryKey: sessionId,
        journal: { read() { assert.fail("Protection must use strict non-repairing inspection"); } },
        readForProtection() {
          assert.deepEqual(references.list(), [{ referenceClass: "workspace-retirement", recoveryKey: sessionId }]);
          reads.push(sessionId);
          return { entries: retainedWork && sessionId === "session-old" ? [{ update: { opaque: true } }] : [] };
        },
      } : undefined;
    },
    withJournalReference(source, operation) {
      return source.recoveryKey ? references.withReference({ referenceClass: "workspace-retirement",
        recoveryKey: source.recoveryKey }, operation) : operation();
    },
  });
  assert.deepEqual(capture(binding), { liveOwner: "clear", acceptedWork: "protected", deliveryAuthority: "clear" });
  assert.deepEqual(writerKeys, ["manual:writer"], "One process key is checked even when it owns multiple session families");
  assert.deepEqual(reads, ["session-old", "session-new"]);
  assert.deepEqual(references.list(), []);
  retainedWork = false;
  assert.equal(capture(binding).acceptedWork, "clear");
  writer = "protected";
  assert.equal(capture(binding).liveOwner, "protected", "An empty journal does not certify writer absence");
  writer = "unknown";
  assert.equal(capture(binding).liveOwner, "unknown");
  available = false;
  assert.equal(capture(binding).acceptedWork, "unknown");
  assert.equal(createTelegramWorkspaceExternalProtectionCapture(deps)(binding).acceptedWork, "unknown",
    "Missing session composition cannot silently use the flat recipient resolver");
});

test("Journal pruning removes only proven-empty known keys under exact profile and leader authority", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-journal-prune-"));
  const path = join(dir, "state.json");
  try {
    const store = createTelegramTopicTargetStore({ path, getNowMs: () => 500 });
    const admission = createRetirementAdmission(
      join(dir, "workspace-admission.json"),
      "journal-prune",
    );
    const binding = { ...createTelegramWorkspaceBindingIdentity("/repo")!,
      target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2,
      journalBindingKeys: ["manual:empty", "manual:busy"], journalBindingsComplete: true as const };
    store.upsertWorkspaceBinding(binding);
    const capture = captureTelegramWorkspaceJournalProtectionSources({
      binding,
      resolveLeader: () => ({ journal: { read: () => ({ entries: [] }) } }),
      createFollowerResolver: (key) => () => ({ journal: { read: () => ({
        entries: key === "manual:busy" ? [{ update: { undecodable: true } }] : [],
      }) } }),
    });
    const fence = admission.acquireRetirementFence({
      operationId: "journal-prune-fence",
      retirementIntentId: "journal-prune-intent",
      bindingKey: binding.bindingKey,
      slot: "A",
      target: binding.target,
      leaderEpoch: 1,
      retirementRequestedAtMs: 1,
    });
    assert.equal(fence.kind, "acquired");
    await assert.rejects(
      pruneTelegramWorkspaceJournalEvidence({ runExclusive: async operation => operation(),
        store, binding, capture: () => capture, admission,
        getJournalWriterProtection: () => "clear",
        getLeaderEpoch: () => 1, getProfileKey: () => "default",
      }),
      /blocked by retirement/u,
    );
    assert.deepEqual(
      store.getWorkspaceBinding("/repo")?.journalBindingKeys,
      ["manual:empty", "manual:busy"],
    );
    if (fence.kind === "acquired") {
      admission.releaseUnissuedRetirementFence(fence.fence);
    }
    assert.deepEqual(await pruneTelegramWorkspaceJournalEvidence({ runExclusive: async operation => operation(),
      store, binding, capture: () => capture, admission,
      getJournalWriterProtection: () => "unknown",
      getLeaderEpoch: () => 1, getProfileKey: () => "default",
    }), { kind: "blocked", reason: "writer-not-quiescent" });
    assert.deepEqual(store.getWorkspaceBinding("/repo")?.journalBindingKeys,
      ["manual:empty", "manual:busy"]);
    const result = await pruneTelegramWorkspaceJournalEvidence({ runExclusive: async operation => operation(),
      store, binding, capture: () => capture, admission,
      getJournalWriterProtection: () => "clear",
      getLeaderEpoch: () => 1, getProfileKey: () => "default",
    });
    assert.equal(result.kind, "committed");
    if (result.kind !== "committed") return;
    assert.deepEqual(result.removedKeys, ["manual:empty"]);
    assert.deepEqual(result.binding.journalBindingKeys, ["manual:busy"]);
    assert.equal(result.binding.updatedAtMs, 500);
    const restored = createTelegramTopicTargetStore({ path });
    await restored.load();
    assert.deepEqual(restored.getWorkspaceBinding("/repo")?.journalBindingKeys, ["manual:busy"]);
    const legacy = { ...createTelegramWorkspaceBindingIdentity("/legacy")!,
      target: { chatId: 7, threadId: 43 }, slot: "B", inactiveSinceMs: 1, updatedAtMs: 2,
      journalBindingKeys: ["manual:known"] };
    store.upsertWorkspaceBinding(legacy);
    const incomplete = captureTelegramWorkspaceJournalProtectionSources({
      binding: legacy,
      resolveLeader: () => ({ journal: { read: () => ({ entries: [] }) } }),
      createFollowerResolver: () => () => ({ journal: { read: () => ({ entries: [] }) } }),
    });
    assert.deepEqual(await pruneTelegramWorkspaceJournalEvidence({ runExclusive: async operation => operation(),
      store, binding: legacy, capture: () => incomplete, admission,
      getJournalWriterProtection: () => "clear",
      getLeaderEpoch: () => 1, getProfileKey: () => "default",
    }), { kind: "blocked", reason: "incomplete-evidence" });
    const discovered = captureTelegramWorkspaceJournalProtectionSources({
      binding: legacy,
      resolveLeader: () => ({ journal: { read: () => ({ entries: [] }) } }),
      createFollowerResolver: () => () => ({ journal: { read: () => ({ entries: [] }) } }),
      discovery: { paths: ["/legacy/follower.json"], complete: true,
        createResolver: () => () => ({ journal: { read: () => ({ entries: [] }) } }) },
    });
    const legacyPrune = await pruneTelegramWorkspaceJournalEvidence({ runExclusive: async operation => operation(),
      store, binding: legacy, capture: () => discovered, admission,
      getJournalWriterProtection: () => "clear",
      getLeaderEpoch: () => 1, getProfileKey: () => "default",
    });
    assert.equal(legacyPrune.kind, "committed");
    assert.equal(store.getWorkspaceBinding("/legacy")?.journalBindingKeys, undefined);
    assert.equal(store.getWorkspaceBinding("/legacy")?.journalBindingsComplete, undefined);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

for (const scenario of ["subset", "late-pending", "unavailable", "corrupt", "wrong-session", "unknown-writer", "authority-loss", "state-changed", "publication-refused", "post-publication-loss", "lost-publication-result", "pre-commit-authority-loss", "pre-commit-path-drift"] as const) {
  test(`Session-qualified pruning commits only a fresh exact empty subset (${scenario})`, { skip: process.platform === "win32" }, async () => {
    const dir = await realpath(await mkdtemp(join(tmpdir(), "pi-session-journal-prune-")));
    try {
      const key = "manual:same", liveKey = "manual:live";
      const journalSources = [{ sessionId: "old", recipientBindingKey: key },
        { sessionId: "new", recipientBindingKey: key }, { sessionId: "live", recipientBindingKey: liveKey }];
      const binding: TelegramWorkspaceThreadBinding = { ...createTelegramWorkspaceBindingIdentity("/repo/prune", 0, "new")!,
        target: { chatId: 7, threadId: 42 }, slot: "A", updatedAtMs: 1,
        journalBindingKeys: [key], journalSources, journalBindingsComplete: true };
      const botIdentity = createTelegramUpdateJournalBotIdentity({ botToken: "7:session-prune" });
      const paths = [resolveTelegramFollowerJournalPath(key, dir, "work"),
        ...journalSources.map(source => resolveTelegramSessionJournalPath(source.sessionId, source.recipientBindingKey, dir, "work"))];
      const journals = paths.map((path, index) => {
        const journal = createTelegramUpdateJournalStore({ path, profileName: "work", botIdentity });
        journal.appendBatch([{ update_id: index + 1, message: { chat: { id: 7 }, text: `source-${index}` } }]);
        if (index !== 2) journal.removeCompleted([index + 1]);
        return journal;
      });
      if (scenario === "corrupt") await writeFile(paths[2]!, "{broken");
      const sourceImage = async () => Promise.all((await readdir(join(dir, "tmp", "pi-telegram"), { recursive: true, withFileTypes: true }))
        .filter(entry => entry.isFile()).sort((left, right) => join(left.parentPath, left.name).localeCompare(join(right.parentPath, right.name)))
        .map(async entry => [join(entry.parentPath, entry.name), await readFile(join(entry.parentPath, entry.name))]));
      let expectedImage = sourceImage();
      let permitPublication = true, interruptCommit = false, changedPath = false;
      const store = createTelegramTopicTargetStore({
        path: () => join(dir, changedPath ? "new-profile-state.json" : "state.json"),
        getNowMs: () => 500, canPersist: () => permitPublication,
        commitPersist(commit) {
          if (interruptCommit) {
            if (scenario === "pre-commit-authority-loss") permitPublication = false;
            else changedPath = true;
          }
          commit(); return true;
        },
      });
      store.upsertWorkspaceBinding(binding); await store.persist();
      const stateBefore = await readFile(join(dir, "state.json"));
      const admission = createRetirementAdmission(join(dir, "admission.json"), "session-prune");
      let epoch = 1, captures = 0, publications = 0;
      if (scenario === "post-publication-loss" || scenario === "lost-publication-result") {
        const publish = store.persistWorkspaceJournalEvidence;
        store.persistWorkspaceJournalEvidence = async (expected, isCurrent) => {
          publications++;
          const published = await publish(expected, isCurrent);
          assert.equal(published, true, "native snapshot was published before the injected result/authority loss");
          if (scenario === "lost-publication-result") throw new Error("fixture publication result lost");
          epoch = 2; return published;
        };
      }
      const readSource = (index: number) => ({ readForProtection() {
        const path = paths[index]!;
        const evidence = inspectTelegramUpdateJournalFamily({ directory: dirname(path), path, profile: "work", botIdentity,
          limits: { maxFiles: 128, maxBytes: 1_000_000, maxEntries: 128, maxWork: 10_000 } });
        if (evidence.kind !== "present") throw new Error("fixture source unavailable");
        return { entries: evidence.file.entries };
      }, journal: { read() { assert.fail("pruning must use strict non-repairing source reads"); } } });
      const invoke = () => pruneTelegramWorkspaceJournalEvidence({ runExclusive: async operation => operation(),
        store, binding,
        admission: { releaseAdmission: admission.releaseAdmission,
          acquireAdmission(input) {
            const lease = admission.acquireAdmission(input);
            if (lease.kind === "acquired" && scenario === "late-pending") {
              // Fault seam: data arrives after the caller's old view, before the protected capture.
              journals[1]!.appendBatch([{ update_id: 99, message: { chat: { id: 7 }, text: "late source" } }]);
              expectedImage = sourceImage();
            }
            return lease;
          } },
        capture() {
          captures++;
          assert.equal(admission.read().leases.length, 1, "capture is evaluated inside admission");
          const capture = captureTelegramWorkspaceJournalProtectionSources({ binding,
            resolveLeader: () => ({ journal: { read: () => ({ entries: [] }) } }),
            createFollowerResolver: observed => { assert.equal(observed, key); return () => readSource(0); },
            createSessionResolver(observed, sessionId) {
              const index = journalSources.findIndex(source => source.sessionId === sessionId && source.recipientBindingKey === observed);
              assert.notEqual(index, -1);
              return () => scenario === "unavailable" && sessionId === "new" ? undefined : readSource(index + 1);
            } });
          if (scenario === "wrong-session") {
            const source = capture.sources.find(source => source.scope.kind === "binding" && source.scope.sessionId === "old")!;
            if (source.scope.kind === "binding") source.scope.sessionId = "foreign";
          }
          if (scenario === "state-changed") store.upsertWorkspaceBinding({ ...binding, updatedAtMs: 2 });
          if (scenario === "publication-refused") permitPublication = false;
          if (scenario === "pre-commit-authority-loss" || scenario === "pre-commit-path-drift") interruptCommit = true;
          return capture;
        },
        getJournalWriterProtection(observed) {
          if (scenario === "authority-loss") epoch = 2;
          return scenario === "unknown-writer" ? "unknown" : observed === liveKey ? "protected" : "clear";
        }, getLeaderEpoch: () => epoch, getProfileKey: () => "default",
      });
      if (scenario === "authority-loss" || scenario === "post-publication-loss") await assert.rejects(invoke(), /lost leader authority/);
      else if (scenario === "pre-commit-authority-loss" || scenario === "pre-commit-path-drift") {
        await assert.rejects(invoke(), /lost its exact frame or authority/);
        assert.deepEqual(await readFile(join(dir, "state.json")), stateBefore, "commit-time drift refuses the actual rename");
        const cold = createTelegramTopicTargetStore({ path: join(dir, "state.json") }); await cold.load();
        assert.deepEqual(cold.getWorkspaceBinding("/repo/prune", "a", "new")?.journalSources, journalSources);
        await assert.rejects(readFile(join(dir, "new-profile-state.json")), { code: "ENOENT" });
      } else if (scenario === "lost-publication-result") {
        await assert.rejects(invoke(), /fixture publication result lost/);
        assert.deepEqual(await invoke(), { kind: "blocked", reason: "state-changed" });
        assert.equal(publications, 1, "a stale-frame retry never reissues publication");
      } else {
        const outcome = await invoke();
        if (scenario === "subset" || scenario === "late-pending") {
          assert.equal(outcome.kind, "committed");
          if (outcome.kind !== "committed") return;
          assert.deepEqual(outcome.removedKeys, [key], "legacy address is separate from its same-key session addresses");
          assert.deepEqual(outcome.removedSources ?? [], scenario === "subset" ? [journalSources[0]] : []);
          const retained = scenario === "subset" ? journalSources.slice(1) : journalSources;
          assert.deepEqual(outcome.binding.journalSources, retained, "busy and live-writer addresses stay intact");
          const cold = createTelegramTopicTargetStore({ path: join(dir, "state.json") }); await cold.load();
          assert.deepEqual(cold.getWorkspaceBinding("/repo/prune", "a", "new")?.journalSources, retained);
          assert.deepEqual(cold.getWorkspaceBinding("/repo/prune", "a", "new")?.journalBindingKeys, [], "Complete empty evidence stays explicit");
        } else {
          const reason = scenario === "state-changed" ? "state-changed" : scenario === "unknown-writer" ? "writer-not-quiescent"
            : scenario === "publication-refused" ? "publication-refused" : "incomplete-evidence";
          assert.deepEqual(outcome, { kind: "blocked", reason });
        }
      }
      if (scenario === "post-publication-loss" || scenario === "lost-publication-result") {
        const cold = createTelegramTopicTargetStore({ path: join(dir, "state.json") }); await cold.load();
        assert.deepEqual(cold.getWorkspaceBinding("/repo/prune", "a", "new")?.journalSources, journalSources.slice(1),
          "a published subset is retained, never compensated after authority loss");
        assert.deepEqual(cold.getWorkspaceBinding("/repo/prune", "a", "new")?.journalBindingKeys, [], "Published complete empty keys survive cold reload");
      } else if (scenario !== "subset" && scenario !== "late-pending" &&
          scenario !== "pre-commit-authority-loss" && scenario !== "pre-commit-path-drift") {
        assert.deepEqual(store.getWorkspaceBinding("/repo/prune", "a", "new")?.journalSources, journalSources);
        assert.deepEqual(store.getWorkspaceBinding("/repo/prune", "a", "new")?.journalBindingKeys, [key]);
        assert.deepEqual(await readFile(join(dir, "state.json")), stateBefore, "refusal publishes no metadata subset");
      }
      assert.equal(captures, scenario === "lost-publication-result" ? 2 : 1);
      assert.deepEqual(await sourceImage(), await expectedImage, "metadata pruning never mutates or repairs source files");
      assert.deepEqual(admission.read().leases, []);
    } finally { await rm(dir, { recursive: true, force: true }); }
  });
}

test("Journal pruning admission spans durable binding publication", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-journal-prune-admission-"));
  const admission = createRetirementAdmission(
    join(dir, "workspace-admission.json"),
    "journal-prune-publication",
  );
  const binding = {
    ...createTelegramWorkspaceBindingIdentity("/repo")!,
    target: { chatId: 7, threadId: 42 },
    slot: "A",
    inactiveSinceMs: 1,
    updatedAtMs: 2,
    journalBindingKeys: ["manual:empty"],
    journalBindingsComplete: true as const,
  };
  let release: (() => void) | undefined;
  let entered: (() => void) | undefined;
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const pruning = pruneTelegramWorkspaceJournalEvidence({ runExclusive: async operation => operation(),
    store: {
      commitWorkspaceJournalEvidence(expected, journalBindingKeys, complete) {
        return {
          ...expected,
          journalBindingKeys: [...journalBindingKeys],
          journalBindingsComplete: complete ? true : undefined,
        };
      },
      async persistWorkspaceJournalEvidence() {
        entered?.();
        await held;
        return true;
      },
    },
    binding,
    capture: () => ({
      complete: true,
      sources: [
        { kind: "available", scope: { kind: "shared" }, entries: [] },
        {
          kind: "available",
          scope: {
            kind: "binding",
            bindingKey: binding.bindingKey,
            journalBindingKey: "manual:empty",
          },
          entries: [],
        },
      ],
    }),
    admission,
    getJournalWriterProtection: () => "clear",
    getLeaderEpoch: () => 1,
    getProfileKey: () => "default",
  });
  try {
    await started;
    assert.deepEqual(
      admission.acquireRetirementFence({
        operationId: "journal-prune-racing-fence",
        retirementIntentId: "journal-prune-racing-intent",
        bindingKey: binding.bindingKey,
        slot: "A",
        target: binding.target,
        leaderEpoch: 1,
        retirementRequestedAtMs: 1,
      }),
      { kind: "blocked", reason: "admission-active" },
    );
    release?.();
    const result = await pruning;
    assert.equal(result.kind, "committed");
    assert.deepEqual(admission.read().leases, []);
  } finally {
    release?.();
    await pruning.catch(() => undefined);
    await rm(dir, { recursive: true, force: true });
  }
});

test("Accepted-work evidence protects exact local and journal targets and fails closed on incomplete sources", () => {
  const binding = { ...createTelegramWorkspaceBindingIdentity("/repo")!,
    target: { chatId: 7, threadId: 42 }, slot: "A", threadName: "Anchor",
    inactiveSinceMs: 1, updatedAtMs: 2 };
  const resolve = (overrides: Partial<Parameters<typeof resolveTelegramWorkspaceAcceptedWorkProtection>[0]> = {}) =>
    resolveTelegramWorkspaceAcceptedWorkProtection({
      binding, localAcceptedTargets: [], journalSources: [], sourcesComplete: true,
      ...overrides,
    });
  assert.equal(resolve(), "clear");
  assert.equal(resolve({ localAcceptedTargets: [{ chatId: 7, threadId: 42 }] }), "protected");
  assert.equal(resolve({ localAcceptedTargets: [{ chatId: 7, threadId: 43 }] }), "clear");
  assert.equal(resolve({ journalSources: [{ kind: "available",
    scope: { kind: "binding", bindingKey: binding.bindingKey, journalBindingKey: "manual:current" },
    entries: [{ update: { undecodable: true } }] }] }), "protected");
  assert.equal(resolve({ journalSources: [{ kind: "available", scope: { kind: "shared" },
    entries: [{ update: { message: { chat: { id: 7 }, message_thread_id: 42 } } }] }] }), "protected");
  assert.equal(resolve({ journalSources: [{ kind: "available", scope: { kind: "shared" },
    entries: [{ update: { callback_query: { message: { chat: { id: 7 }, message_thread_id: 43 } } } }] }] }), "clear");
  assert.equal(resolve({ journalSources: [{ kind: "available", scope: { kind: "shared" },
    entries: [{ update: { message_reaction: { chat: { id: 7 }, message_id: 5 } } }] }] }), "unknown");
  assert.equal(resolve({ journalSources: [{ kind: "unknown", scope: { kind: "shared" } }] }), "unknown");
  assert.equal(resolve({ sourcesComplete: false }), "unknown");
  assert.equal(resolve({ journalSources: [{ kind: "unknown",
    scope: { kind: "binding", bindingKey: "other", journalBindingKey: "manual:other" } }] }), "clear");
});

test("External protection composition keeps incomplete queues, journals, registries, and delivery unknown", () => {
  const binding = { ...createTelegramWorkspaceBindingIdentity("/repo")!,
    target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2,
    journalBindingKeys: [], journalBindingsComplete: true as const };
  const capture = (overrides: Partial<Parameters<typeof captureTelegramWorkspaceExternalProtection>[0]> = {}) =>
    captureTelegramWorkspaceExternalProtection({
      binding,
      getLiveOwnerProtection: () => "clear",
      getLocalAcceptedTargets: () => ({ targets: [], complete: true }),
      captureJournalSources: () => ({ sources: [], complete: true }),
      ...overrides,
    });
  assert.deepEqual(capture(), { liveOwner: "clear", acceptedWork: "clear",
    deliveryAuthority: "unknown" });
  assert.equal(capture({ getDeliveryAuthorityProtection: () => "clear" })
    .deliveryAuthority, "clear");
  assert.equal(capture({ getDeliveryAuthorityProtection: () => "protected" })
    .deliveryAuthority, "protected");
  assert.equal(capture({ getLocalAcceptedTargets: () => ({
    targets: [binding.target], complete: false,
  }) }).acceptedWork, "protected");
  assert.equal(capture({ getLocalAcceptedTargets: () => ({
    targets: [], complete: false,
  }) }).acceptedWork, "unknown");
  assert.deepEqual(capture({
    getLiveOwnerProtection: () => { throw new Error("registry unavailable"); },
    captureJournalSources: () => { throw new Error("journal unavailable"); },
    getDeliveryAuthorityProtection: () => { throw new Error("delivery unavailable"); },
  }), { liveOwner: "unknown", acceptedWork: "unknown", deliveryAuthority: "unknown" });
  let discoveries = 0;
  const assembled = createTelegramWorkspaceExternalProtectionCapture({
    listFollowers: () => [{ target: binding.target }],
    getActiveTurnTarget: () => undefined,
    getQueuedItems: () => [{ chatId: 7 }],
    resolveLeaderJournal: () => ({ journal: { read: () => ({ entries: [] }) } }),
    createFollowerJournalResolver: () => () => ({ journal: { read: () => ({ entries: [] }) } }),
    discoverFollowerJournals: () => { discoveries++; return { paths: [], complete: true }; },
    createJournalPathResolver: () => () => ({ journal: { read: () => ({ entries: [] }) } }),
  });
  assert.deepEqual(assembled(binding), { liveOwner: "protected", acceptedWork: "unknown",
    deliveryAuthority: "unknown" });
  assert.equal(discoveries, 0);
  assembled({ ...binding, journalBindingsComplete: undefined });
  assert.equal(discoveries, 1);
  const writerAware = createTelegramWorkspaceExternalProtectionCapture({
    listFollowers: () => [], getActiveTurnTarget: () => undefined,
    getQueuedItems: () => [],
    resolveLeaderJournal: () => ({ journal: { read: () => ({ entries: [] }) } }),
    createFollowerJournalResolver: () => () => ({ journal: { read: () => ({ entries: [] }) } }),
    getJournalWriterProtection: (key) => key === "live" ? "protected" : "clear",
  });
  assert.equal(writerAware({ ...binding, journalBindingKeys: ["dead"] }).liveOwner, "clear");
  assert.equal(writerAware({ ...binding, journalBindingKeys: ["live"] }).liveOwner, "protected");
  assert.equal(writerAware({ ...binding, journalBindingsComplete: undefined }).liveOwner, "unknown");
});

test("Successor leader adopts only the exact protected intent and persistence failure retains the old epoch", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-adoption-"));
  const path = join(dir, "state.json");
  const admission = createRetirementAdmission(join(dir, "admission.json"), "successor");
  let canCommit = true;
  try {
    const store = createTelegramTopicTargetStore({ path, commitPersist(commit) {
      if (!canCommit) return false;
      commit();
      return true;
    } });
    const binding = { ...createTelegramWorkspaceBindingIdentity("/repo")!,
      target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2 };
    store.upsertWorkspaceBinding(binding);
    const intent = { id: "retire:a", reason: "pressure" as const, profileKey: "default",
      binding, leaderEpoch: 1, requestedAtMs: 3 };
    store.upsertWorkspaceRetirementIntent(intent);
    await store.persist();
    const base = {
      store, intent, getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => 2, getProfileKey: () => "default",
      runExclusive: async <T>(operation: () => Promise<T>) => operation(),
    };
    canCommit = false;
    await assert.rejects(adoptTelegramWorkspaceRetirementIntent(base),
      /lost exact transport ownership/);
    assert.deepEqual(store.listWorkspaceRetirementIntents(), [intent]);
    canCommit = true;
    const adopted = await adoptTelegramWorkspaceRetirementIntent(base);
    assert.equal(adopted.kind, "adopted");
    if (adopted.kind !== "adopted") return;
    assert.equal(adopted.intent.leaderEpoch, 2);
    assert.equal(adopted.intent.requestedAtMs, 3);
    assert.deepEqual(await adoptTelegramWorkspaceRetirementIntent({ ...base,
      intent: adopted.intent, getProfileKey: () => "other",
    }), { kind: "blocked", reason: "profile-changed" });
    assert.deepEqual(await executeTelegramWorkspaceRetirement({
      store, intent, getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => 2, getProfileKey: () => "default", admission,
      runExclusive: base.runExclusive, async deleteForumTopic() { throw new Error("must not delete old epoch"); },
    }), { kind: "retained", reason: "authority-changed" });
    const restored = createTelegramTopicTargetStore({ path });
    await restored.load();
    assert.deepEqual(restored.listWorkspaceRetirementIntents(), [adopted.intent]);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Explicit lifecycle prepares and executes one pressure retirement", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-lifecycle-"));
  try {
    const store = createTelegramTopicTargetStore({ path: join(dir, "state.json") });
    const admission = createRetirementAdmission(join(dir, "admission.json"));
    for (let index = 0; index < 26; index++) addBinding(store, index, index + 1);
    const binding = store.getWorkspaceBinding("/repo/0")!;
    let gates = 0;
    let deletions = 0;
    const run = () => runTelegramWorkspaceRetirementLifecycle({
      store, getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => 1, getProfileKey: () => "default",
      getNowMs: () => 1000,
      runExclusive: async <T>(operation: () => Promise<T>) => { gates++; return operation(); },
      admission,
      async deleteForumTopic(permit, body, options) {
        deletions++;
        assert.equal(permit.slot, "A");
        assert.deepEqual(body, { chat_id: 7, message_thread_id: 40 });
        assert.deepEqual(options, { maxAttempts: 1 });
      },
    });
    assert.deepEqual(await run(), { kind: "retired", bindingKey: binding.bindingKey, slot: "A" });
    assert.equal(gates, 1);
    assert.equal(deletions, 1);
    assert.deepEqual(await run(), { kind: "not-needed", reason: "free-capacity" });
    assert.equal(gates, 1);
    assert.equal(deletions, 1);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Pressure rotation reclaims exact queued custody only after native dead-owner proof", async (t) => {
  if (process.platform === "win32") {
    t.skip("Strict no-follow journal-family inspection is unavailable on Windows.");
    return;
  }
  const dir = await realpath(await mkdtemp(join(tmpdir(), "pi-telegram-dead-owner-reclamation-")));
  try {
    const admission = createRetirementAdmission(join(dir, "admission.json"));
    const store = createTelegramTopicTargetStore({ path: join(dir, "state.json"),
      getExternalReservedSlots: admission.listReservedSlots });
    for (let index = 0; index < 26; index++) addBinding(store, index, index + 1);
    const initial = store.getWorkspaceBinding("/repo/0")!;
    const journalBindingKey = "manual-follower:dead-fixture";
    assert.ok(store.commitWorkspaceJournalEvidence(initial, [journalBindingKey], true));
    await store.persist();

    const botIdentity = createTelegramUpdateJournalBotIdentity({ botToken: "7:fixture" });
    const recoveryOwner = { instanceId: "leader", processId: process.pid,
      processBirthId: `${process.pid}:leader`, sessionGeneration: 2 };
    const deadOwner = { instanceId: "dead-follower",
      processId: process.pid + 1_000_000, processBirthId: `${process.pid + 1_000_000}:dead`,
      sessionGeneration: 1 };
    const createJournal = (name: string, queueRuntimeIdentity = recoveryOwner) => {
      const path = join(dir, name);
      const journal = createTelegramUpdateJournalStore({ path, profileName: "default", botIdentity,
        queueRuntimeIdentity, workspaceAdmission: admission,
        getQueueProcessLiveness: owner => owner.processId === deadOwner.processId ? "dead" : "alive" });
      return { recoveryKey: path, journal, readForProtection: () => {
        const evidence = inspectTelegramUpdateJournalFamily({ directory: dir, path,
          profile: "default", botIdentity,
          limits: { maxFiles: 128, maxBytes: 1_000_000, maxEntries: 128, maxWork: 10_000 } });
        if (evidence.kind !== "present") throw new Error("fixture journal disappeared");
        return { entries: evidence.file.entries };
      } };
    };
    const leader = createJournal("inbox.json");
    leader.journal.appendBatch([{ update_id: 99, message: { message_id: 99,
      chat: { id: 999, type: "private" }, from: { id: 7, is_bot: false }, text: "seed" } }], 99);
    const seeded = leader.journal.markQueued({ queueKind: "prompt", receiptId: "seed-receipt",
      sourceUpdateIds: [99], owner: recoveryOwner });
    assert.ok(seeded.queueOwner);
    leader.journal.completeQueued([{ queueKind: "prompt", receiptId: "seed-receipt",
      sourceUpdateIds: [99], queueOwner: seeded.queueOwner! }]);
    const deadWriter = createJournal("follower-inbox-dead000000000000.json", deadOwner);
    deadWriter.journal.appendBatch([{ update_id: 1, message: { message_id: 10,
      message_thread_id: initial.target.threadId, chat: { id: initial.target.chatId, type: "private" },
      from: { id: 7, is_bot: false }, text: "retained dead-owner work" } }], 1);
    deadWriter.journal.markQueued({ queueKind: "prompt", receiptId: "dead-receipt",
      sourceUpdateIds: [1], owner: deadOwner });
    const follower = createJournal("follower-inbox-dead000000000000.json");

    const capture = createTelegramWorkspaceExternalProtectionCapture({
      listFollowers: () => [], getActiveTurnTarget: () => undefined, getQueuedItems: () => [],
      resolveLeaderJournal: () => leader,
      createFollowerJournalResolver: key => () => key === journalBindingKey ? follower : undefined,
      getJournalWriterProtection: () => "clear", getDeliveryAuthorityProtection: () => "clear",
    });
    const getProtection = (binding: TelegramWorkspaceThreadBinding): TelegramWorkspaceExternalProtectionEvidence =>
      binding.bindingKey === initial.bindingKey ? capture(binding) : {
        liveOwner: "protected", acceptedWork: "clear", deliveryAuthority: "clear",
      };
    const reclaim = createTelegramWorkspaceDeadOwnerQueueReclaimer({
      getExternalProtection: getProtection, getActiveTurnTarget: () => undefined,
      getQueuedItems: () => [], resolveLeaderJournal: () => leader,
      createFollowerJournalResolver: key => () => key === journalBindingKey ? follower : undefined,
      getRecoveryOwner: () => recoveryOwner,
      getQueueOwnerLiveness: candidate => candidate.processId === deadOwner.processId ? "dead" : "alive",
      isBindingCurrent: binding => store.listWorkspaceBindings().some(candidate =>
        candidate.bindingKey === binding.bindingKey && candidate.updatedAtMs === binding.updatedAtMs),
    });
    assert.deepEqual(getProtection(store.getWorkspaceBinding("/repo/0")!), {
      liveOwner: "clear", acceptedWork: "protected", deliveryAuthority: "clear",
    });
    let deletions = 0;
    let effects = 0;
    const reclamations: unknown[] = [];
    const operations = createTelegramWorkspaceOperationRuntime({ getWorkspaceAdmission: () => admission });
    const rotation = createTelegramWorkspaceSlotRotation({
      store, getAdmission: () => admission, getLeaderEpoch: () => 1,
      runExclusive: operations.runExclusive,
      getExternalProtection: getProtection, recordEvent() {},
      reclaimDeadOwnerQueuedWork(binding, isCurrent) {
        return operations.run({ operationId: "dead-owner-reclamation-fixture",
          operationKind: "workspace.reclaim-dead-owner-queue",
          scopes: [{ kind: "target", target: binding.target }] }, async () => {
          const result = await reclaim(binding, isCurrent);
          reclamations.push(result);
          return result;
        });
      },
      async deleteThread(authorize) { authorize(); deletions++; },
    });
    const slot = await rotation(async () => {
      const claimed = store.claimWorkspaceIdentity("/fresh", "fresh");
      if (!claimed) throw new TelegramWorkspaceSlotUnavailableError();
      effects++;
      return claimed.slot;
    });
    assert.equal(slot, "A", JSON.stringify(reclamations));
    assert.equal(deletions, 1);
    assert.equal(effects, 1, "The failed allocation must not execute its effect before reclamation");
    assert.deepEqual(reclamations, [{ kind: "recovered", receipts: 1, updateIds: [1] }]);
    assert.deepEqual(follower.journal.read().entries, []);
    assert.equal(store.listWorkspaceBindings().length, 25);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

for (const scenario of ["queued", "pending", "delivery-protected", "corrupt", "epoch-loss", "no-pressure"] as const) {
  test(`Pressure metadata pruning uses the shared native observer without deletion grants (${scenario})`, { skip: process.platform === "win32", timeout: 15_000 }, async () => {
    const dir = await realpath(await mkdtemp(join(tmpdir(), "pi-pressure-metadata-")));
    try {
      const admission = createRetirementAdmission(join(dir, "admission.json"));
      const store = createTelegramTopicTargetStore({ path: join(dir, "state.json"), getExternalReservedSlots: admission.listReservedSlots });
      for (let i = 0; i < (scenario === "no-pressure" ? 25 : 26); i++) addBinding(store, i, i + 1);
      const initial = store.getWorkspaceBinding("/repo/0")!, key = "manual:metadata-fixture";
      const addresses = ["old", "new"].map(sessionId => ({ sessionId, recipientBindingKey: key }));
      assert.ok(store.commitWorkspaceJournalEvidence(initial, [], true, []));
      store.upsertWorkspaceBinding({ ...initial, journalSources: addresses }); await store.persist();
      const token = "7:metadata-fixture", botIdentity = createTelegramUpdateJournalBotIdentity({ botToken: token });
      const recoveryOwner = { instanceId: "leader", processId: process.pid, processBirthId: `${process.pid}:leader`, sessionGeneration: 2 };
      const deadOwner = { instanceId: "dead", processId: process.pid + 1_000_000, processBirthId: `${process.pid + 1_000_000}:dead`, sessionGeneration: 1 };
      const pollingPath = resolveTelegramSessionPollingJournalPath("host", dir, "work");
      const paths = [pollingPath, ...addresses.map(source => resolveTelegramSessionJournalPath(source.sessionId, key, dir, "work"))];
      const journals = paths.map((path, i) => {
        const journal = createTelegramUpdateJournalStore({ path, profileName: "work", botIdentity, queueRuntimeIdentity: deadOwner });
        journal.appendBatch([{ update_id: i === 2 ? 44 : i + 1, message: { message_id: i + 1, chat: { id: 7, type: "private" },
          from: { id: 7, is_bot: false }, message_thread_id: initial.target.threadId, text: "isolated metadata fixture" } }]);
        if (i !== 2 || scenario === "delivery-protected") journal.removeCompleted([i === 2 ? 44 : i + 1]);
        else if (scenario !== "pending" && scenario !== "epoch-loss") journal.markQueued({ queueKind: "prompt", receiptId: "held", sourceUpdateIds: [44], owner: deadOwner });
        return journal;
      });
      if (scenario === "corrupt") await writeFile(paths[2]!, "{broken");
      const oldImage = await readFile(paths[1]!);
      const references = createTelegramUpdateJournalReferenceRegistry();
      const bindings = createTelegramUpdateJournalBindingRuntime({
        base: { getProfileName: () => "work", getBotToken: () => token, getBotId: () => 7,
          getQueueRuntimeIdentity: () => recoveryOwner, getWorkspaceAdmission: () => admission,
          withSourceSerialization(operation) { return operation(); } },
        getLeaderJournalPath: () => pollingPath, getRuntimeDir: () => resolveTelegramTempDir(dir),
        getFollowerJournalPath: (recipient, profile, sessionId) => sessionId
          ? resolveTelegramSessionJournalPath(sessionId, recipient, dir, profile)
          : resolveTelegramFollowerJournalPath(recipient, dir, profile),
        getActiveFollowerBindingKey: () => key, getActiveFollowerSessionId: () => "new", isFollowerRegistered: () => false,
      });
      let epoch = 1, inGate = false, observingPrune = false, captures = 0, prunes = 0, reclaimed = 0, deletions = 0, allocations = 0;
      const operations = createTelegramWorkspaceOperationRuntime({ getWorkspaceAdmission: () => admission });
      const gate = async <T>(operation: () => Promise<T>): Promise<T> => {
        assert.equal(inGate, false, "no nested non-reentrant gate");
        if (observingPrune) assert.equal(admission.read().leases.length, 1, "admission precedes the shared gate");
        return operations.runExclusive(async () => { inGate = true; try { return await operation(); } finally { inGate = false; } });
      };
      const observer = createTelegramWorkspaceProtectionObserver({
        listFollowers: () => store.listWorkspaceBindings().filter(binding => binding.bindingKey !== initial.bindingKey).map(binding => ({ target: binding.target })),
        getActiveTurnTarget: () => undefined, getQueuedItems: () => [],
        resolveLeaderJournal: bindings.resolveLeader, createFollowerJournalResolver: bindings.createLegacyRecipientResolver,
        createSessionJournalResolver: bindings.createRecipientResolver, createJournalPathResolver: bindings.createPathResolver,
        getJournalWriterProtection: () => "clear", getDeliveryAuthorityProtection: () => scenario === "delivery-protected" ? "protected" : "clear",
        inspectJournalNamespace() {
          if (observingPrune) { captures++; assert.equal(inGate, true); assert.equal(admission.read().leases.length, 1); }
          return inspectTelegramSessionJournalNamespace({ directory: resolveTelegramTempDir(dir), profile: "work", pollingPath, botIdentity,
            limits: { maxDirectoryEntries: 1000, maxFiles: 256, maxBytes: 1_000_000, maxEntries: 1000, maxWork: 10_000 } });
        },
        withJournalReference(binding, operation) {
          assert.ok(binding.recoveryKey);
          return references.withReference({ referenceClass: "workspace-retirement", recoveryKey: binding.recoveryKey }, () => {
            assert.ok(references.list().some(reference => reference.recoveryKey === binding.recoveryKey));
            return operation();
          });
        },
      });
      if (scenario === "epoch-loss") {
        const publish = store.persistWorkspaceJournalEvidence;
        store.persistWorkspaceJournalEvidence = async (expected, isCurrent) => {
          assert.equal(await publish(expected, isCurrent), true); epoch = 2; return true;
        };
      }
      const prune = createTelegramWorkspaceJournalEvidencePruner({ store, getAdmission: () => admission, getLeaderEpoch: () => epoch,
        runExclusive: gate, protection: observer });
      const reclaim = createTelegramWorkspaceDeadOwnerQueueReclaimer({ getExternalProtection: observer.capture,
        getActiveTurnTarget: () => undefined, getQueuedItems: () => [], resolveLeaderJournal: bindings.resolveLeader,
        createFollowerJournalResolver: bindings.createLegacyRecipientResolver, createSessionJournalResolver: bindings.createRecipientResolver,
        getRecoveryOwner: () => recoveryOwner, getQueueOwnerLiveness: () => "dead",
        isBindingCurrent: binding => isCurrentTelegramWorkspaceBinding(store, binding),
      });
      const rotation = createTelegramWorkspaceSlotRotation({ store, getAdmission: () => admission, getLeaderEpoch: () => epoch,
        runExclusive: gate, getExternalProtection: observer.capture, recordEvent() {},
        async pruneJournalEvidence(binding, isCurrent) {
          prunes++; observingPrune = true; try { return await prune(binding, isCurrent); } finally { observingPrune = false; }
        },
        async reclaimDeadOwnerQueuedWork(binding, isCurrent) {
          reclaimed++;
          assert.deepEqual(binding.journalSources, [addresses[1]], "reclamation receives the acknowledged current metadata frame");
          return operations.run({ operationId: "metadata-reclamation", operationKind: "workspace.reclaim-dead-owner-queue",
            scopes: [{ kind: "target", target: binding.target }] }, () => reclaim(binding, isCurrent));
        },
        async deleteThread(authorize) { assert.deepEqual(authorize(), initial.target); deletions++; },
      });
      const attempt = () => rotation(async () => {
        const claim = store.claimWorkspaceIdentity("/fresh", "fresh");
        if (!claim) throw new TelegramWorkspaceSlotUnavailableError();
        allocations++; return claim.slot;
      });
      if (scenario === "queued" || scenario === "no-pressure") assert.equal(await attempt(), scenario === "queued" ? "A" : "Z");
      else await assert.rejects(attempt(), scenario === "epoch-loss" ? /lost leader authority/ : /rotation blocked/);
      assert.equal(prunes, scenario === "no-pressure" ? 0 : 1); assert.equal(captures, prunes);
      assert.equal(deletions, scenario === "queued" ? 1 : 0);
      assert.equal(allocations, scenario === "queued" || scenario === "no-pressure" ? 1 : 0);
      assert.equal(reclaimed, scenario === "queued" || scenario === "pending" ? 1 : 0);
      const cold = createTelegramTopicTargetStore({ path: join(dir, "state.json") }); await cold.load();
      if (scenario === "queued") assert.equal(cold.getWorkspaceBinding("/repo/0"), undefined);
      else assert.deepEqual(cold.getWorkspaceBinding("/repo/0")?.journalSources,
        scenario === "corrupt" || scenario === "no-pressure" ? addresses : scenario === "delivery-protected" ? undefined : [addresses[1]]);
      assert.deepEqual(await readFile(paths[1]!), oldImage, "the dropped metadata address's physical source remains untouched");
      if (scenario !== "corrupt") assert.deepEqual(journals[2]!.read().entries.map(entry => entry.updateId),
        scenario === "queued" || scenario === "delivery-protected" ? [] : [44]);
      else assert.equal(await readFile(paths[2]!, "utf8"), "{broken", "namespace protection never repairs corruption");
      assert.deepEqual(references.list(), []); assert.deepEqual(admission.read().leases, []);
    } finally { await rm(dir, { recursive: true, force: true }); }
  });
}

test("Dead-owner reclamation refuses replacement and partial custody, then resumes interrupted exact groups", async (t) => {
  const binding: TelegramWorkspaceThreadBinding = {
    ...createTelegramWorkspaceBindingIdentity("/repo/dead-owner")!,
    target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2,
    journalBindingsComplete: true,
  };
  const owner = { instanceId: "dead", processId: 444, processBirthId: "444:start:dead",
    sessionGeneration: 1, acquisitionId: "acquire-dead", acquiredAtMs: 1 };
  const recoveryOwner = { instanceId: "leader", processId: 555, processBirthId: "555:start:leader",
    sessionGeneration: 2 };
  const entry = (updateId: number, threadId: number, receiptId: string): TelegramUpdateJournalEntry => ({
    updateId, admittedAtMs: 1, state: "queued", queueKind: "prompt", queueReceiptId: receiptId,
    queueOwner: owner, update: { update_id: updateId, message: { message_id: updateId,
      message_thread_id: threadId, chat: { id: 7, type: "private" },
      from: { id: 7, is_bot: false }, text: `update-${updateId}` } },
  });
  const createHarness = (input: {
    initialEntries: TelegramUpdateJournalEntry[];
    getLiveOwner?: (call: number) => "clear" | "protected" | "unknown";
    liveness?: "alive" | "dead" | "unverifiable";
    recover?: (call: number) => "recovered" | "owner-alive" | "owner-unverifiable" | "throw";
  }) => {
    let entries = [...input.initialEntries];
    let protectionCalls = 0;
    let recoveryCalls = 0;
    const source = {
      recoveryKey: "fixture-source",
      readForProtection: () => ({ entries }),
      journal: { recoverDeadQueueOwner(recovery: Parameters<ReturnType<
        typeof createTelegramUpdateJournalStore>["recoverDeadQueueOwner"]>[0]) {
        recoveryCalls++;
        const outcome = input.recover?.(recoveryCalls) ?? "recovered";
        if (outcome === "throw") throw new Error("interrupted publication");
        if (outcome !== "recovered") return { status: outcome, previousOwner: owner,
          recoveredUpdateIds: [] as [], entryCount: entries.length, serializedBytes: 1 };
        entries = entries.filter((candidate) =>
          !recovery.sourceUpdateIds.includes(candidate.updateId));
        return { status: "recovered" as const, previousOwner: owner,
          recoveredUpdateIds: [...recovery.sourceUpdateIds], entryCount: entries.length, serializedBytes: 1 };
      } },
    };
    const getExternalProtection = (): TelegramWorkspaceExternalProtectionEvidence => {
      protectionCalls++;
      const acceptedWork = entries.some((candidate) =>
        (candidate.update as { message?: { message_thread_id?: number } }).message
          ?.message_thread_id === binding.target.threadId)
        ? "protected" as const : "clear" as const;
      return { liveOwner: input.getLiveOwner?.(protectionCalls) ?? "clear",
        acceptedWork, deliveryAuthority: "clear" };
    };
    const reclaim = createTelegramWorkspaceDeadOwnerQueueReclaimer({
      getExternalProtection, getActiveTurnTarget: () => undefined, getQueuedItems: () => [],
      resolveLeaderJournal: () => source, createFollowerJournalResolver: () => () => undefined,
      getRecoveryOwner: () => recoveryOwner,
      getQueueOwnerLiveness: () => input.liveness ?? "dead",
      isBindingCurrent: () => true,
    });
    return { reclaim, get entries() { return entries; }, get recoveryCalls() { return recoveryCalls; } };
  };

  await t.test("session-qualified evidence refuses legacy disposal", async () => {
    const harness = createHarness({ initialEntries: [entry(1, 42, "receipt")] });
    assert.deepEqual(await harness.reclaim({ ...binding,
      journalSources: [{ sessionId: "retained-session", recipientBindingKey: "manual:retained" }] }, () => true),
      { kind: "blocked", reason: "incomplete-source" });
    assert.equal(harness.recoveryCalls, 0);
    assert.deepEqual(harness.entries.map(candidate => candidate.updateId), [1]);
  });
  await t.test("replacement owner", async () => {
    const harness = createHarness({ initialEntries: [entry(1, 42, "receipt")],
      getLiveOwner: call => call >= 2 ? "protected" : "clear" });
    assert.deepEqual(await harness.reclaim(binding, () => true),
      { kind: "blocked", reason: "live-owner" });
    assert.equal(harness.recoveryCalls, 0);
    assert.deepEqual(harness.entries.map(candidate => candidate.updateId), [1]);
  });
  await t.test("partial grouped receipt", async () => {
    const harness = createHarness({ initialEntries: [
      entry(1, 42, "shared-receipt"), entry(2, 43, "shared-receipt"),
    ] });
    assert.deepEqual(await harness.reclaim(binding, () => true),
      { kind: "blocked", reason: "unsupported-custody" });
    assert.equal(harness.recoveryCalls, 0);
    assert.deepEqual(harness.entries.map(candidate => candidate.updateId), [1, 2]);
  });
  for (const status of ["owner-alive", "owner-unverifiable"] as const) {
    await t.test(status, async () => {
      const harness = createHarness({ initialEntries: [entry(1, 42, "receipt")],
        liveness: status === "owner-alive" ? "alive" : "unverifiable" });
      assert.deepEqual(await harness.reclaim(binding, () => true),
        { kind: "blocked", reason: status });
      assert.deepEqual(harness.entries.map(candidate => candidate.updateId), [1]);
    });
  }
  await t.test("interrupted mutation and exact retry", async () => {
    const harness = createHarness({ initialEntries: [
      entry(1, 42, "dead-receipt"), entry(2, 43, "unrelated-receipt"),
    ], recover: call => call === 1 ? "throw" : "recovered" });
    assert.deepEqual(await harness.reclaim(binding, () => true),
      { kind: "blocked", reason: "mutation-refused" });
    assert.deepEqual(harness.entries.map(candidate => candidate.updateId), [1, 2]);
    assert.deepEqual(await harness.reclaim(binding, () => true),
      { kind: "recovered", receipts: 1, updateIds: [1] });
    assert.deepEqual(harness.entries.map(candidate => candidate.updateId), [2]);
  });
});

for (const scenario of ["dead", "unavailable", "corrupt", "pending", "alive", "authority-loss", "partial-publication"] as const) {
  test(`Session-qualified dead-owner reclamation resolves exact native journal families (${scenario})`, { skip: process.platform === "win32" }, async () => {
    const dir = await realpath(await mkdtemp(join(tmpdir(), "pi-session-dead-owner-")));
    const botIdentity = createTelegramUpdateJournalBotIdentity({ botToken: "7:session-pressure" });
    const recoveryOwner = { instanceId: "leader", processId: 555, processBirthId: "555:leader", sessionGeneration: 2 };
    const recipientBindingKey = "manual:retained";
    const journalSources = ["session-old", "session-new"].map(sessionId => ({ sessionId, recipientBindingKey }));
    const binding: TelegramWorkspaceThreadBinding = { ...createTelegramWorkspaceBindingIdentity("/repo/session-pressure", 0, "session-new")!,
      target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2,
      journalBindingsComplete: true, journalSources };
    let current = true, interrupt = false;
    const mutations: number[] = [], resolved: string[] = [];
    try {
      const sources = journalSources.map(({ sessionId }, index) => {
        const path = resolveTelegramSessionJournalPath(sessionId, recipientBindingKey, dir, "work");
        const owner = { instanceId: `dead-${index}`, processId: 1001 + index,
          processBirthId: `${1001 + index}:dead`, sessionGeneration: 1 };
        const writer = createTelegramUpdateJournalStore({ path, profileName: "work", botIdentity,
          queueRuntimeIdentity: owner, getQueueProcessLiveness: () => "dead" });
        writer.appendBatch([{ update_id: index + 1, message: { message_id: index + 1, message_thread_id: 42,
          chat: { id: 7, type: "private" }, text: `retained-${sessionId}` } }]);
        if (scenario !== "pending" || index === 0) writer.markQueued({ queueKind: "prompt", receiptId: "same-receipt-name",
          sourceUpdateIds: [index + 1], owner });
        const journal = createTelegramUpdateJournalStore({ path, profileName: "work", botIdentity,
          queueRuntimeIdentity: recoveryOwner, getQueueProcessLiveness: () => "dead",
          onPublicationBoundary(point) {
            if (index === 1 && interrupt && point === "after-write-before-rename") {
              interrupt = false; throw new Error("fixture session publication interrupted");
            }
          } });
        return { path, recoveryKey: createTelegramUpdateJournalBindingKey({ path, profileName: "work", botIdentity }),
          readForProtection() {
            const evidence = inspectTelegramUpdateJournalFamily({ directory: dirname(path), path, profile: "work", botIdentity,
              limits: { maxFiles: 128, maxBytes: 1_000_000, maxEntries: 128, maxWork: 10_000 } });
            if (evidence.kind !== "present") throw new Error("fixture exact session source unavailable");
            return { entries: evidence.file.entries };
          }, journal: { recoverDeadQueueOwner(input: Parameters<typeof journal.recoverDeadQueueOwner>[0]) {
            mutations.push(index); return journal.recoverDeadQueueOwner(input);
          } } };
      });
      assert.equal(basename(sources[0]!.path), basename(sources[1]!.path), "same recipient hash, independent session custody");
      assert.notEqual(sources[0]!.recoveryKey, sources[1]!.recoveryKey);
      if (scenario === "corrupt") await writeFile(sources[1]!.path, "{broken");
      const before = await Promise.all(sources.map(source => readFile(source.path, "utf8")));
      interrupt = scenario === "partial-publication";
      const reclaim = createTelegramWorkspaceDeadOwnerQueueReclaimer({
        getExternalProtection() {
          let acceptedWork: TelegramWorkspaceExternalProtectionEvidence["acceptedWork"];
          try { acceptedWork = sources.some(source => source.readForProtection().entries.length > 0) ? "protected" : "clear"; }
          catch { acceptedWork = "unknown"; }
          return { liveOwner: "clear", acceptedWork, deliveryAuthority: "clear" };
        },
        getActiveTurnTarget: () => undefined, getQueuedItems: () => [],
        resolveLeaderJournal: () => ({ recoveryKey: "fixture-empty-polling", readForProtection: () => ({ entries: [] }),
          journal: { recoverDeadQueueOwner() { assert.fail("empty polling source has no receipt to dispose"); } } }),
        createFollowerJournalResolver: () => () => { assert.fail("a session address must never select the flat same-hash source"); },
        createSessionJournalResolver(key, sessionId) {
          assert.equal(key, recipientBindingKey); resolved.push(sessionId);
          return () => scenario === "unavailable" && sessionId === "session-new" ? undefined
            : sources[journalSources.findIndex(source => source.sessionId === sessionId)];
        },
        getRecoveryOwner: () => recoveryOwner,
        getQueueOwnerLiveness(owner) {
          if (scenario === "authority-loss") current = false;
          return scenario === "alive" && owner.processId === 1002 ? "alive" : "dead";
        }, isBindingCurrent: () => current,
      });
      const outcome = await reclaim(binding, () => current);
      assert.deepEqual(resolved, ["session-old", "session-new"]);
      if (scenario === "dead") {
        assert.deepEqual(outcome, { kind: "recovered", receipts: 2, updateIds: [1, 2] });
        assert.deepEqual(mutations, [0, 1]);
        assert.deepEqual(sources.map(source => source.readForProtection().entries), [[], []]);
      } else if (scenario === "partial-publication") {
        assert.deepEqual(outcome, { kind: "blocked", reason: "mutation-refused" });
        assert.deepEqual(sources.map(source => source.readForProtection().entries.map(entry => entry.updateId)), [[], [2]],
          "partial terminal disposal is not rolled back or replayed");
        assert.deepEqual(await reclaim(binding, () => current), { kind: "recovered", receipts: 1, updateIds: [2] });
        assert.deepEqual(mutations, [0, 1, 1], "retry visits only the still-owned receipt");
      } else {
        const reason = scenario === "pending" ? "unsupported-custody" : scenario === "alive" ? "owner-alive"
          : scenario === "authority-loss" ? "authority-changed" : "incomplete-source";
        assert.deepEqual(outcome, { kind: "blocked", reason });
        assert.deepEqual(mutations, [], "whole-source preflight refuses before any terminal CAS");
        assert.deepEqual(await Promise.all(sources.map(source => readFile(source.path, "utf8"))), before,
          "refused source inspection never repairs or overwrites even corrupt snapshots");
      }
    } finally { await rm(dir, { recursive: true, force: true }); }
  });
}

test("Dead-owner pressure reclamation retries an interrupted native journal publication without replay", async (t) => {
  if (process.platform === "win32") {
    t.skip("Strict no-follow journal-family inspection is unavailable on Windows.");
    return;
  }
  const dir = await realpath(await mkdtemp(join(tmpdir(), "pi-telegram-dead-owner-publication-")));
  try {
    const path = join(dir, "inbox.json");
    const botIdentity = createTelegramUpdateJournalBotIdentity({ botToken: "7:publication" });
    const deadOwner = { instanceId: "dead", processId: 444,
      processBirthId: "444:start:dead", sessionGeneration: 1 };
    const recoveryOwner = { instanceId: "leader", processId: 555,
      processBirthId: "555:start:leader", sessionGeneration: 2 };
    const writer = createTelegramUpdateJournalStore({ path, profileName: "default", botIdentity,
      queueRuntimeIdentity: deadOwner, getQueueProcessLiveness: () => "dead" });
    writer.appendBatch([{ update_id: 1, message: { message_id: 1, message_thread_id: 42,
      chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "once" } }], 1);
    writer.markQueued({ queueKind: "prompt", receiptId: "receipt", sourceUpdateIds: [1], owner: deadOwner });
    let interrupt = true;
    let effects = 0;
    const journal = createTelegramUpdateJournalStore({ path, profileName: "default", botIdentity,
      queueRuntimeIdentity: recoveryOwner, getQueueProcessLiveness: () => "dead",
      onPublicationBoundary(boundary) {
        if (interrupt && boundary === "after-write-before-rename") {
          interrupt = false;
          throw new Error("interrupted publication");
        }
      } });
    const inspect = () => {
      const evidence = inspectTelegramUpdateJournalFamily({ directory: dir, path,
        profile: "default", botIdentity,
        limits: { maxFiles: 128, maxBytes: 1_000_000, maxEntries: 128, maxWork: 10_000 } });
      if (evidence.kind !== "present") throw new Error("fixture journal disappeared");
      return { entries: evidence.file.entries };
    };
    const source = { recoveryKey: path, journal, readForProtection: inspect };
    const binding: TelegramWorkspaceThreadBinding = {
      ...createTelegramWorkspaceBindingIdentity("/repo/publication")!,
      target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2,
      journalBindingsComplete: true,
    };
    const getExternalProtection = (): TelegramWorkspaceExternalProtectionEvidence => ({
      liveOwner: "clear",
      acceptedWork: inspect().entries.length ? "protected" : "clear",
      deliveryAuthority: "clear",
    });
    const reclaim = createTelegramWorkspaceDeadOwnerQueueReclaimer({
      getExternalProtection, getActiveTurnTarget: () => undefined, getQueuedItems: () => [],
      resolveLeaderJournal: () => source, createFollowerJournalResolver: () => () => undefined,
      getRecoveryOwner: () => recoveryOwner, getQueueOwnerLiveness: () => "dead",
      isBindingCurrent: () => true,
    });
    const invoke = async () => {
      const result = await reclaim(binding, () => true);
      if (result.kind === "recovered") effects += result.updateIds.length;
      return result;
    };
    assert.deepEqual(await invoke(), { kind: "blocked", reason: "mutation-refused" });
    assert.deepEqual(inspect().entries.map(entry => entry.updateId), [1]);
    assert.deepEqual(await invoke(), { kind: "recovered", receipts: 1, updateIds: [1] });
    assert.equal(effects, 1);
    assert.deepEqual(inspect().entries, []);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Slot rotation cancels proven Telegram rejection but retains ambiguous deletion without replay", async (t) => {
  for (const outcome of ["rejected", "malformed", "forged", "server", "network", "false"] as const) {
    await t.test(outcome, async (t) => {
      const dir = await mkdtemp(join(tmpdir(), "pi-telegram-rotation-rejection-"));
      const attempts: string[] = [];
      t.mock.method(Date, "now", () => 1000);
      let requests = 0;
      let success = false;
      const fetch = async () => {
        requests++;
        if (success) return new Response(JSON.stringify({ ok: true, result: true }));
        if (outcome === "network") throw Object.assign(new Error("connection reset"), { code: "ECONNRESET" });
        if (outcome === "forged") throw Object.assign(new Error("Too Many Requests"), { status: 429 });
        if (outcome === "false") return new Response(JSON.stringify({ ok: true, result: false }));
        const status = outcome === "server" ? 500 : 429;
        return new Response(outcome === "malformed" ? "invalid JSON" : JSON.stringify({
          ok: false, error_code: status, description: "fixture rejection",
        }), { status });
      };
      t.mock.method(globalThis, "fetch", fetch);
      const restoreFetch = setTelegramApiHttpsFetchForTesting(fetch);
      try {
        const admission = createTelegramWorkspaceAdmissionLedger({ path: join(dir, "admission.json"),
          profileKey: "default", owner: { processId: process.pid, processBirthId: `${process.pid}:rejection` },
          getNowMs: () => 1000, getProcessLiveness: () => "alive" });
        const store = createTelegramTopicTargetStore({ path: join(dir, "state.json"), getNowMs: () => 1000,
          getExternalReservedSlots: admission.listReservedSlots });
        for (let index = 0; index < 26; index++) addBinding(store, index, index + 1);
        await store.persist();
        const api = createDefaultTelegramBridgeApiRuntime({
          getBotToken: () => "123:fixture", recordRuntimeEvent() {}, workspaceAdmission: admission,
        });
        const rotation = createTelegramWorkspaceSlotRotation({
          store, getAdmission: () => admission, getLeaderEpoch: () => 1,
          runExclusive: createTelegramWorkspaceOperationGate().runExclusive,
          getExternalProtection: clearExternalProtection, recordEvent() {},
          deleteThread(authorize) {
            attempts.push(admission.read().fence!.operationId);
            return api.deleteWorkspaceThread(authorize);
          },
        });
        await assert.rejects(rotation(async () => { throw new TelegramWorkspaceSlotUnavailableError(); }),
          outcome === "rejected" ? /delete-rejected/ : /delete-unconfirmed/);
        assert.equal(requests, 1);
        assert.equal(store.listWorkspaceBindings().length, 26);
        if (outcome === "rejected") {
          assert.equal(admission.read().fence, undefined);
          assert.deepEqual(store.listWorkspaceRetirementIntents(), []);
          const lease = admission.acquireAdmission({ operationId: "after-rejection",
            operationKind: "workspace.register-follower", scope: { kind: "profile" } });
          assert.equal(lease.kind, "acquired");
          if (lease.kind === "acquired") admission.releaseAdmission(lease.lease);
          assert.equal(await rotation(async () => "existing binding"), "existing binding");
          assert.equal(requests, 1);
          success = true;
          assert.equal(await rotation(async () => {
            const claim = store.claimWorkspaceIdentity("/fresh", "fresh");
            if (!claim) throw new TelegramWorkspaceSlotUnavailableError();
            return claim.slot;
          }), "A");
          assert.equal(requests, 2);
          assert.equal(new Set(attempts).size, 2, "Fresh attempts need distinct authority even with the same clock tick");
        } else {
          assert.equal(admission.read().fence?.phase, "deletion-issued");
          await assert.rejects(rotation(async () => "existing binding"), /delete-unconfirmed/);
          assert.equal(requests, 1);
        }
      } finally {
        restoreFetch();
        await rm(dir, { recursive: true, force: true });
      }
    });
  }
});

test("Rejected rotation recovery finishes each durable cancellation prefix without another delete", async (t) => {
  for (const fault of ["rejection", "withdrawal", "withdrawal-same-store", "withdrawal-ack", "authority", "release", "release-ack"] as const) {
    await t.test(fault, async (t) => {
      const dir = await mkdtemp(join(tmpdir(), "pi-telegram-rejection-recovery-"));
      const path = join(dir, "state.json");
      const admissionPath = join(dir, "admission.json");
      let requests = 0;
      let fail = true;
      let epoch = 1;
      const fetch = async () => {
        requests++;
        return new Response(JSON.stringify({ ok: false, error_code: 429, description: "Too Many Requests" }), { status: 429 });
      };
      t.mock.method(globalThis, "fetch", fetch);
      const restoreFetch = setTelegramApiHttpsFetchForTesting(fetch);
      try {
        const ledger = createRetirementAdmission(admissionPath);
        const admission = { ...ledger,
          confirmRetirementRejection(expected: Parameters<typeof ledger.confirmRetirementRejection>[0]) {
            const result = ledger.confirmRetirementRejection(expected);
            if (fail && fault === "rejection") throw new Error("interrupted rejection ACK");
            return result;
          },
          completeRejectedRetirementFence(expected: Parameters<typeof ledger.completeRejectedRetirementFence>[0]) {
            if (fail && fault === "release") throw new Error("interrupted release");
            const result = ledger.completeRejectedRetirementFence(expected);
            if (fail && fault === "release-ack") throw new Error("interrupted release ACK");
            return result;
          },
        };
        const backing = createTelegramTopicTargetStore({ path, getExternalReservedSlots: ledger.listReservedSlots });
        const store = { ...backing, async persist() {
          const cancelling = ledger.read().fence?.phase === "deletion-rejected";
          if (fail && cancelling && (fault === "withdrawal" || fault === "withdrawal-same-store")) {
            throw new Error("interrupted withdrawal");
          }
          await backing.persist();
          if (fail && cancelling && fault === "withdrawal-ack") throw new Error("interrupted withdrawal ACK");
          if (fail && cancelling && fault === "authority") epoch = 2;
        } };
        for (let index = 0; index < 26; index++) addBinding(store, index, index + 1);
        await store.persist();
        const bindings = store.listWorkspaceBindings();
        const api = createDefaultTelegramBridgeApiRuntime({
          getBotToken: () => "123:fixture", recordRuntimeEvent() {}, workspaceAdmission: admission,
        });
        const rotation = createTelegramWorkspaceSlotRotation({
          store, getAdmission: () => admission, getLeaderEpoch: () => epoch,
          runExclusive: createTelegramWorkspaceOperationGate().runExclusive,
          getExternalProtection: clearExternalProtection, recordEvent() {}, deleteThread: api.deleteWorkspaceThread,
        });
        await assert.rejects(rotation(async () => { throw new TelegramWorkspaceSlotUnavailableError(); }),
          /interrupted|fence-release-unconfirmed|delete-rejected|authority-changed/);
        assert.equal(requests, 1);
        assert.equal(ledger.read().fence?.phase, fault === "release-ack" ? undefined : "deletion-rejected");
        fail = false;
        const successorAdmission = createRetirementAdmission(admissionPath, "successor");
        const successorStore = fault === "withdrawal-same-store" ? store : createTelegramTopicTargetStore({ path,
          getExternalReservedSlots: successorAdmission.listReservedSlots });
        const successor = createTelegramWorkspaceSlotRotation({
          store: successorStore, getAdmission: () => successorAdmission, getLeaderEpoch: () => 2,
          runExclusive: createTelegramWorkspaceOperationGate().runExclusive,
          getExternalProtection() { throw new Error("Cancellation must not require deletion eligibility"); },
          recordEvent() {}, async deleteThread() { throw new Error("Deletion must not replay during cancellation"); },
        });
        assert.equal(await successor(async () => "restored binding"), "restored binding");
        assert.equal(successorAdmission.read().fence, undefined);
        const restored = createTelegramTopicTargetStore({ path });
        await restored.load();
        assert.deepEqual(restored.listWorkspaceRetirementIntents(), []);
        assert.deepEqual(restored.listWorkspaceBindings(), bindings);
        assert.equal(requests, 1);
      } finally {
        restoreFetch();
        await rm(dir, { recursive: true, force: true });
      }
    });
  }
});

test("Slot rotation recovers a committed binding removal before fence completion without deleting again", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-rotation-commit-ready-"));
  try {
    const path = join(dir, "state.json");
    const admission = createRetirementAdmission(join(dir, "admission.json"));
    const operations = createTelegramWorkspaceOperationRuntime({ getWorkspaceAdmission: () => admission });
    const store = createTelegramTopicTargetStore({ path, getExternalReservedSlots: admission.listReservedSlots });
    for (let index = 0; index < 26; index++) addBinding(store, index, index + 1);
    await store.persist();
    let deletions = 0;
    const rotation = createTelegramWorkspaceSlotRotation({
      store, getAdmission: () => ({ ...admission, completeRetirementFence() { throw new Error("interrupted completion"); } }),
      runExclusive: operations.runExclusive, getLeaderEpoch: () => 1,
      getExternalProtection: clearExternalProtection, recordEvent() {},
      async deleteThread(authorize) { authorize(); deletions++; },
    });
    await assert.rejects(rotation(async () => { throw new TelegramWorkspaceSlotUnavailableError(); }), /fence-release-unconfirmed/);
    assert.equal(deletions, 1);
    assert.equal(store.listWorkspaceBindings().length, 25);
    assert.deepEqual(store.listWorkspaceRetirementIntents(), []);
    assert.equal(admission.read().fence?.phase, "commit-ready");
    const reopened = createTelegramTopicTargetStore({ path, getExternalReservedSlots: admission.listReservedSlots });
    const successor = createTelegramWorkspaceSlotRotation({
      store: reopened, getAdmission: () => admission, runExclusive: operations.runExclusive,
      getLeaderEpoch: () => 2, getExternalProtection: clearExternalProtection, recordEvent() {},
      async deleteThread() { throw new Error("must not delete twice"); },
    });
    const restoredSlot = await successor(async () => reopened.claimWorkspaceIdentity("/fresh", "new")?.slot);
    assert.equal(restoredSlot, "A");
    assert.equal(admission.read().fence, undefined);
    assert.equal(deletions, 1);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Slot rotation retires an exact stale active target after confirmed deletion", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-rotation-stale-active-"));
  try {
    const path = join(dir, "state.json");
    const admissionPath = join(dir, "admission.json");
    const admission = createRetirementAdmission(admissionPath);
    const store = createTelegramTopicTargetStore({ path, getExternalReservedSlots: admission.listReservedSlots });
    for (let index = 0; index < 26; index++) addBinding(store, index, index + 1);
    await store.persist();
    let deletions = 0;
    const first = createTelegramWorkspaceSlotRotation({
      store, getAdmission: () => admission,
      runExclusive: createTelegramWorkspaceOperationGate().runExclusive,
      getLeaderEpoch: () => 1, getExternalProtection: clearExternalProtection, recordEvent() {},
      async deleteThread(authorize) {
        const target = authorize();
        deletions++;
        store.upsert({ profileKey: "stale:retired", instanceId: "stale-runtime", target,
          status: "active", slot: "A", createdAtMs: 1, updatedAtMs: 2 });
      },
    });
    assert.equal(await first(async () => {
      const claim = store.claimWorkspaceIdentity("/fresh", "new");
      if (!claim) throw new TelegramWorkspaceSlotUnavailableError();
      return claim.slot;
    }), "A");
    assert.equal(deletions, 1, "confirmed deletion must not replay");
    assert.equal(store.list().some((record) => record.target.threadId === 40), false);
    assert.equal(store.listWorkspaceBindings().some((binding) => binding.slot === "A"), false);
    assert.deepEqual(store.listWorkspaceRetirementIntents(), []);
    assert.equal(admission.read().fence, undefined);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Slot rotation recovers a retained commit-ready target projection under successor authority", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-rotation-stale-recovery-"));
  try {
    const path = join(dir, "state.json");
    const admissionPath = join(dir, "admission.json");
    const originalAdmission = createRetirementAdmission(admissionPath);
    const store = createTelegramTopicTargetStore({ path,
      getExternalReservedSlots: originalAdmission.listReservedSlots });
    for (let index = 0; index < 26; index++) addBinding(store, index, index + 1);
    const binding = store.listWorkspaceBindings().find((candidate) => candidate.slot === "A")!;
    const intent = { id: "retire:stale-active", reason: "pressure" as const,
      profileKey: "default", binding, leaderEpoch: 1, requestedAtMs: 3 };
    store.upsertWorkspaceRetirementIntent(intent);
    await store.persist();
    const acquired = originalAdmission.acquireRetirementFence({ operationId: "retire-stale-active",
      retirementIntentId: intent.id, bindingKey: binding.bindingKey, slot: binding.slot!,
      target: binding.target, leaderEpoch: 1, retirementRequestedAtMs: intent.requestedAtMs });
    assert.equal(acquired.kind, "acquired");
    if (acquired.kind !== "acquired") return;
    const issued = originalAdmission.issueDeletionPermit(acquired.fence);
    assert.equal(issued.kind, "issued");
    if (issued.kind !== "issued") return;
    originalAdmission.confirmRetirementAbsence(issued.fence);
    store.upsert({ profileKey: "stale:retired", instanceId: "stale-runtime", target: binding.target,
      status: "active", slot: "A", createdAtMs: 1, updatedAtMs: 2 });

    const successorAdmission = createRetirementAdmission(admissionPath, "successor");
    const successor = createTelegramWorkspaceSlotRotation({
      store, getAdmission: () => successorAdmission,
      runExclusive: createTelegramWorkspaceOperationGate().runExclusive,
      getLeaderEpoch: () => 2, getExternalProtection: clearExternalProtection, recordEvent() {},
      async deleteThread() { throw new Error("confirmed deletion must not replay"); },
    });
    assert.equal(await successor(async () => store.claimWorkspaceIdentity("/fresh", "new")?.slot), "A");
    assert.equal(store.list().some((record) => record.target.threadId === binding.target.threadId), false);
    assert.deepEqual(store.listWorkspaceRetirementIntents(), []);
    assert.equal(successorAdmission.read().fence, undefined);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Slot rotation validates leader authority again at deletion issuance", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-rotation-epoch-"));
  try {
    const admission = createRetirementAdmission(join(dir, "admission.json"));
    const store = createTelegramTopicTargetStore({ path: join(dir, "state.json") });
    for (let index = 0; index < 26; index++) addBinding(store, index, index + 1);
    await store.persist();
    let epoch = 1;
    let issued = 0;
    const rotate = createTelegramWorkspaceSlotRotation({
      store, getAdmission: () => admission, runExclusive: createTelegramWorkspaceOperationGate().runExclusive,
      getLeaderEpoch: () => epoch, getExternalProtection: clearExternalProtection, recordEvent() {},
      async deleteThread(authorize) { epoch = 2; authorize(); issued++; },
    });
    await assert.rejects(rotate(async () => { throw new Error("unrelated failure"); }), /unrelated failure/);
    assert.equal(admission.read().fence, undefined);
    await assert.rejects(rotate(async () => { throw new TelegramWorkspaceSlotUnavailableError(); }), /delete-unconfirmed/);
    assert.equal(issued, 0);
    assert.equal(store.listWorkspaceBindings().length, 26);
    assert.equal(admission.read().fence?.phase, "deletion-issued");
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("Retirement waits for active admission and releases an unissued fence on late protection", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-fence-race-"));
  try {
    const store = createTelegramTopicTargetStore({ path: join(dir, "state.json") });
    const admission = createRetirementAdmission(join(dir, "admission.json"));
    const binding = { ...createTelegramWorkspaceBindingIdentity("/repo")!,
      target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2 };
    store.upsertWorkspaceBinding(binding);
    const intent = { id: "retire:race", reason: "pressure" as const,
      profileKey: "default", binding, leaderEpoch: 1, requestedAtMs: 3 };
    store.upsertWorkspaceRetirementIntent(intent);
    const leaseResult = admission.acquireAdmission({
      operationId: "active-journal",
      operationKind: "journal.append",
      scope: { kind: "target", target: binding.target },
    });
    assert.equal(leaseResult.kind, "acquired");
    let deletions = 0;
    const execute = (
      getExternalProtection: () => TelegramWorkspaceExternalProtectionEvidence =
        clearExternalProtection,
    ) =>
      executeTelegramWorkspaceRetirement({
        store, intent, admission, getExternalProtection,
        getLeaderEpoch: () => 1, getProfileKey: () => "default",
        runExclusive: async <T>(operation: () => Promise<T>) => operation(),
        async deleteForumTopic() { deletions++; },
      });
    assert.deepEqual(await execute(), { kind: "retained", reason: "admission-active" });
    assert.equal(deletions, 0);
    assert.equal(admission.read().fence, undefined);
    if (leaseResult.kind === "acquired") {
      assert.equal(admission.releaseAdmission(leaseResult.lease), true);
    }
    let protectionReads = 0;
    assert.deepEqual(await execute(() => {
      protectionReads++;
      return protectionReads === 1
        ? clearExternalProtection()
        : { ...clearExternalProtection(), liveOwner: "protected" as const };
    }), { kind: "retained", reason: "protection-changed" });
    assert.equal(protectionReads, 2);
    assert.equal(deletions, 0);
    assert.equal(admission.read().fence, undefined);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Retirement execution retains unknown deletion without issuing a second request", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-retained-"));
  try {
    const store = createTelegramTopicTargetStore({ path: join(dir, "state.json") });
    const admission = createRetirementAdmission(join(dir, "admission.json"));
    const binding = { ...createTelegramWorkspaceBindingIdentity("/repo")!,
      target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2 };
    store.upsertWorkspaceBinding(binding);
    const intent = { id: "retire:a", reason: "pressure" as const, profileKey: "default",
      binding, leaderEpoch: 1, requestedAtMs: 3 };
    store.upsertWorkspaceRetirementIntent(intent);
    const base = {
      store, intent, admission, getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => 1, getProfileKey: () => "default",
      runExclusive: async <T>(operation: () => Promise<T>) => operation(),
    };
    let deletionCalls = 0;
    assert.deepEqual(await executeTelegramWorkspaceRetirement({ ...base,
      async deleteForumTopic(permit) {
        deletionCalls++;
        assert.equal(permit.retirementIntentId, intent.id);
        assert.equal(admission.read().fence?.phase, "deletion-issued");
        assert.deepEqual(admission.acquireAdmission({
          operationId: "late-api",
          operationKind: "api.sendMessage",
          scope: { kind: "target", target: binding.target },
        }), { kind: "blocked", reason: "retirement-fenced" });
        throw new Error("ACK unknown");
      },
    }), { kind: "retained", reason: "delete-unconfirmed" });
    assert.equal(deletionCalls, 1);
    assert.equal(admission.read().fence?.phase, "deletion-issued");
    assert.equal(store.getWorkspaceBinding("/repo")?.slot, "A");
    assert.deepEqual(store.listWorkspaceRetirementIntents(), [intent]);
    assert.deepEqual(await executeTelegramWorkspaceRetirement({ ...base,
      async deleteForumTopic() { throw new Error("must not issue twice"); },
      confirmTargetAbsent: async () => "unknown",
    }), { kind: "retained", reason: "delete-unconfirmed" });
    assert.equal(deletionCalls, 1);
    assert.equal(store.claimWorkspaceIdentity("/fresh", "fresh")?.slot, "B");
    store.releaseWorkspaceClaim("fresh");
    assert.deepEqual(await executeTelegramWorkspaceRetirement({ ...base,
      async deleteForumTopic() { throw new Error("must not issue twice"); },
      async confirmTargetAbsent() {
        assert.equal(store.claimWorkspaceIdentity("/repo", "racer"), undefined);
        return "absent";
      },
    }), { kind: "retired", bindingKey: binding.bindingKey, slot: "A" });
    assert.equal(deletionCalls, 1);
    assert.equal(store.getWorkspaceBinding("/repo"), undefined);
    assert.deepEqual(store.listWorkspaceRetirementIntents(), []);
    assert.equal(admission.read().fence, undefined);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Closed-but-existing topic evidence retains an issued retirement fence", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-closed-"));
  try {
    const store = createTelegramTopicTargetStore({ path: join(dir, "state.json") });
    const admission = createRetirementAdmission(join(dir, "admission.json"));
    const binding = { ...createTelegramWorkspaceBindingIdentity("/repo")!,
      target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2 };
    store.upsertWorkspaceBinding(binding);
    const intent = { id: "retire:closed", reason: "pressure" as const,
      profileKey: "default", binding, leaderEpoch: 1, requestedAtMs: 3 };
    store.upsertWorkspaceRetirementIntent(intent);
    assert.deepEqual(await executeTelegramWorkspaceRetirement({
      store, intent, admission, getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => 1, getProfileKey: () => "default",
      runExclusive: async <T>(operation: () => Promise<T>) => operation(),
      async deleteForumTopic() {
        throw new TelegramApiStaleTargetError("forum topic closed",
          { chatId: 7, threadId: 42 });
      },
    }), { kind: "retained", reason: "delete-unconfirmed" });
    assert.equal(admission.read().fence?.phase, "deletion-issued");
    assert.equal(store.getWorkspaceBinding("/repo")?.slot, "A");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Killed executor leaves durable intent for successor already-absence recovery", {
  skip: process.platform === "win32",
}, async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-kill-"));
  const path = join(dir, "state.json");
  const admissionPath = join(dir, "admission.json");
  try {
    const store = createTelegramTopicTargetStore({ path });
    const binding = { ...createTelegramWorkspaceBindingIdentity("/repo")!,
      target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2 };
    store.upsertWorkspaceBinding(binding);
    const intent = { id: "retire:a", reason: "pressure" as const, profileKey: "default",
      binding, leaderEpoch: 1, requestedAtMs: 3 };
    store.upsertWorkspaceRetirementIntent(intent);
    await store.persist();
    const script = `
      import { createTelegramTopicTargetStore } from './lib/threads.ts';
      import { executeTelegramWorkspaceRetirement } from './lib/workspace-retirement.ts';
      import { createTelegramWorkspaceAdmissionLedger } from './lib/workspace-admission.ts';
      const store = createTelegramTopicTargetStore({ path: process.argv[1] });
      const admission = createTelegramWorkspaceAdmissionLedger({
        path: process.argv[2], profileKey: 'default',
        owner: { processId: process.pid, processBirthId: process.pid + ':retirement-child' },
        getProcessLiveness: () => 'alive',
      });
      await store.load();
      const intent = store.listWorkspaceRetirementIntents()[0];
      await executeTelegramWorkspaceRetirement({
        store, intent, admission,
        getExternalProtection: () => ({ liveOwner: 'clear', acceptedWork: 'clear', deliveryAuthority: 'clear' }),
        getLeaderEpoch: () => 1, getProfileKey: () => 'default',
        runExclusive: async (operation) => operation(),
        deleteForumTopic: async () => {
          console.log('remote-deleted');
          setInterval(() => {}, 1000);
          await new Promise(() => {});
        },
      });
    `;
    const child = spawn(process.execPath, ["--experimental-strip-types", "--input-type=module",
      "--eval", script, path, admissionPath], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    child.stderr.on("data", (chunk) => { stderr += String(chunk); });
    await new Promise<void>((resolve, reject) => {
      child.stdout.on("data", (chunk) => {
        if (String(chunk).includes("remote-deleted")) resolve();
      });
      child.once("exit", (code) => reject(new Error(
        `retirement fixture exited before deletion marker (${code}): ${stderr}`,
      )));
    });
    const exited = once(child, "exit");
    child.kill("SIGKILL");
    await exited;
    const restored = createTelegramTopicTargetStore({ path });
    const successorAdmission = createRetirementAdmission(admissionPath, "successor");
    await restored.load();
    assert.equal(restored.getWorkspaceBinding("/repo")?.slot, "A");
    assert.deepEqual(restored.listWorkspaceRetirementIntents(), [intent]);
    const runExclusive = async <T>(operation: () => Promise<T>) => operation();
    const adoption = await adoptTelegramWorkspaceRetirementIntent({
      store: restored, intent, getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => 2, getProfileKey: () => "default", runExclusive,
    });
    assert.equal(adoption.kind, "adopted");
    if (adoption.kind !== "adopted") return;
    assert.deepEqual(await executeTelegramWorkspaceRetirement({
      store: restored, intent: adoption.intent, admission: successorAdmission,
      getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => 2, getProfileKey: () => "default", runExclusive,
      async deleteForumTopic() { throw new Error("must not issue after successor adoption"); },
      confirmTargetAbsent: async () => "absent",
    }), { kind: "retired", bindingKey: binding.bindingKey, slot: "A" });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Policy disable after deletion retains intent for successor adoption and already-absence retry", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-leadership-"));
  try {
    const store = createTelegramTopicTargetStore({ path: join(dir, "state.json") });
    const admission = createRetirementAdmission(join(dir, "admission.json"));
    const binding = { ...createTelegramWorkspaceBindingIdentity("/repo")!,
      target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2 };
    store.upsertWorkspaceBinding(binding);
    const intent = { id: "retire:a", reason: "pressure" as const, profileKey: "default",
      binding, leaderEpoch: 1, requestedAtMs: 3 };
    store.upsertWorkspaceRetirementIntent(intent);
    let epoch = 1;
    let cleanupEnabled = true;
    const runExclusive = async <T>(operation: () => Promise<T>) => operation();
    assert.deepEqual(await executeTelegramWorkspaceRetirement({
      store, intent, admission, getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => epoch, getProfileKey: () => "default",
      isCurrent: () => cleanupEnabled, runExclusive,
      async deleteForumTopic() { cleanupEnabled = false; },
    }), { kind: "retained", reason: "authority-changed" });
    assert.equal(store.getWorkspaceBinding("/repo")?.slot, "A");
    assert.deepEqual(store.listWorkspaceRetirementIntents(), [intent]);
    cleanupEnabled = true;
    epoch = 2;
    const adoption = await adoptTelegramWorkspaceRetirementIntent({
      store, intent, getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => epoch, getProfileKey: () => "default", runExclusive,
    });
    assert.equal(adoption.kind, "adopted");
    if (adoption.kind !== "adopted") return;
    assert.deepEqual(await executeTelegramWorkspaceRetirement({
      store, intent: adoption.intent, admission, getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => epoch, getProfileKey: () => "default", runExclusive,
      async deleteForumTopic() { throw new Error("must not reissue committed deletion"); },
    }), { kind: "retired", bindingKey: binding.bindingKey, slot: "A" });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Ambiguous persistence after durable rename reloads committed retirement without replay", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-ambiguous-"));
  const path = join(dir, "state.json");
  const admission = createRetirementAdmission(join(dir, "admission.json"));
  let ambiguous = false;
  try {
    const store = createTelegramTopicTargetStore({ path, commitPersist(commit) {
      commit();
      if (ambiguous) throw new Error("publication acknowledgement lost");
      return true;
    } });
    const binding = { ...createTelegramWorkspaceBindingIdentity("/repo")!,
      target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2 };
    store.upsertWorkspaceBinding(binding);
    const intent = { id: "retire:a", reason: "pressure" as const, profileKey: "default",
      binding, leaderEpoch: 1, requestedAtMs: 3 };
    store.upsertWorkspaceRetirementIntent(intent);
    await store.persist();
    ambiguous = true;
    let calls = 0;
    assert.deepEqual(await executeTelegramWorkspaceRetirement({
      store, intent, admission, getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => 1, getProfileKey: () => "default",
      runExclusive: async <T>(operation: () => Promise<T>) => operation(),
      async deleteForumTopic() { calls++; },
    }), { kind: "retired", bindingKey: binding.bindingKey, slot: "A" });
    assert.equal(calls, 1);
    assert.equal(store.getWorkspaceBinding("/repo"), undefined);
    assert.deepEqual(store.listWorkspaceRetirementIntents(), []);
    const restored = createTelegramTopicTargetStore({ path });
    await restored.load();
    assert.equal(restored.getWorkspaceBinding("/repo"), undefined);
    assert.deepEqual(restored.listWorkspaceRetirementIntents(), []);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Confirmed deletion releases a slot only after durable exact retirement and retries failed persistence", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-commit-"));
  const path = join(dir, "state.json");
  const admission = createRetirementAdmission(join(dir, "admission.json"));
  let canCommit = true;
  try {
    const store = createTelegramTopicTargetStore({ path, commitPersist(commit) {
      if (!canCommit) return false;
      commit();
      return true;
    } });
    const binding = { ...createTelegramWorkspaceBindingIdentity("/repo")!,
      target: { chatId: 7, threadId: 42 }, slot: "A", inactiveSinceMs: 1, updatedAtMs: 2 };
    store.upsertWorkspaceBinding(binding);
    const intent = { id: "retire:a", reason: "pressure" as const, profileKey: "default",
      binding, leaderEpoch: 1, requestedAtMs: 3 };
    store.upsertWorkspaceRetirementIntent(intent);
    await store.persist();
    canCommit = false;
    let calls = 0;
    const execute = () => executeTelegramWorkspaceRetirement({
      store, intent, admission, getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => 1, getProfileKey: () => "default",
      runExclusive: async <T>(operation: () => Promise<T>) => operation(),
      async deleteForumTopic() {
        calls++;
        if (calls > 1) throw new Error("must not issue deletion twice");
      },
    });
    await assert.rejects(execute(), /lost exact transport ownership/);
    assert.equal(store.getWorkspaceBinding("/repo")?.slot, "A");
    assert.deepEqual(store.listWorkspaceRetirementIntents(), [intent]);
    assert.equal(store.claimWorkspaceIdentity("/fresh", "fresh")?.slot, "B");
    store.releaseWorkspaceClaim("fresh");
    canCommit = true;
    assert.deepEqual(await execute(), { kind: "retired",
      bindingKey: binding.bindingKey, slot: "A" });
    assert.equal(store.getWorkspaceBinding("/repo"), undefined);
    assert.deepEqual(store.listWorkspaceRetirementIntents(), []);
    assert.equal(store.claimWorkspaceIdentity("/fresh", "fresh")?.slot, "A");
    assert.equal(calls, 1);
    assert.equal(admission.read().fence, undefined);
    const restored = createTelegramTopicTargetStore({ path });
    await restored.load();
    assert.equal(restored.getWorkspaceBinding("/repo"), undefined);
    assert.deepEqual(restored.listWorkspaceRetirementIntents(), []);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Pressure preparation persists the oldest eligible exact intent and resumes it idempotently", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-pressure-"));
  const path = join(dir, "state.json");
  try {
    const store = createTelegramTopicTargetStore({ path, getNowMs: () => 1000 });
    for (let index = 0; index < 26; index++) {
      addBinding(store, index, index === 5 ? 10 : 20 + index,
        index === 5 ? "session-a" : undefined);
    }
    await store.persist();
    const deps = {
      store,
      getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => "leader:1",
      getProfileKey: () => "default",
      getNowMs: () => 1000,
    };
    const prepared = await prepareTelegramWorkspaceRetirement(deps);
    assert.equal(prepared.kind, "ready");
    if (prepared.kind !== "ready") return;
    assert.equal(prepared.intent.binding.cwd, "/repo/5");
    assert.equal(prepared.intent.binding.slot, "F");
    assert.equal(prepared.intent.binding.inactiveSinceMs, 10);
    assert.equal(prepared.intent.binding.sessionId, "session-a");
    assert.equal(prepared.intent.profileKey, "default");
    assert.equal(prepared.intent.leaderEpoch, "leader:1");
    assert.deepEqual(await prepareTelegramWorkspaceRetirement(deps), prepared);
    assert.equal(store.listWorkspaceRetirementIntents().length, 1);
    const restored = createTelegramTopicTargetStore({ path });
    await restored.load();
    assert.deepEqual(restored.listWorkspaceRetirementIntents(), [prepared.intent]);
    assert.equal(restored.getWorkspaceBinding("/repo/5"), undefined);
    assert.equal(restored.getWorkspaceBinding(
      "/repo/5", "a", "session-a")?.slot, "F");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Pressure preparation counts standalone reservations", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-pressure-reservation-"));
  try {
    const capacity = createTelegramTopicTargetStore({ path: join(dir, "capacity.json"), getNowMs: () => 1000 });
    for (let index = 0; index < 25; index++) addBinding(capacity, index, index + 1);
    capacity.reserveThread({ target: { chatId: 7, threadId: 99 }, slot: "Z",
      reason: "leader-reload", createdAtMs: 1, updatedAtMs: 1 });
    const pressure = await prepareTelegramWorkspaceRetirement({
      store: capacity,
      getExternalProtection: clearExternalProtection,
      getLeaderEpoch: () => 1,
      getProfileKey: () => "default",
      getNowMs: () => 1000,
    });
    assert.equal(pressure.kind, "ready");
    if (pressure.kind === "ready") assert.equal(pressure.intent.binding.slot, "A");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Preparation removes an unpersisted intent when protection or leader authority changes", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-retirement-fence-"));
  try {
    const store = createTelegramTopicTargetStore({ path: join(dir, "state.json"), getNowMs: () => 1000 });
    for (let index = 0; index < 26; index++) addBinding(store, index, index + 1);
    let epoch = 1;
    let observations = 0;
    await assert.rejects(prepareTelegramWorkspaceRetirement({
      store,
      getExternalProtection() {
        observations++;
        if (observations === 27) epoch = 2;
        return clearExternalProtection();
      },
      getLeaderEpoch: () => epoch,
      getProfileKey: () => "default",
      getNowMs: () => 1000,
    }), /lost leader authority/);
    assert.equal(store.listWorkspaceRetirementIntents().length, 0);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
