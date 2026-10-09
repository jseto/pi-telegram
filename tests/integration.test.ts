/**
 * Cross-domain integration tests for the Telegram extension
 * Exercises extension-level polling, queue/lifecycle wiring, previews, reactions, compaction, and model switching
 */

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  readlink,
  realpath,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { setTimeout as waitForTimeout } from "node:timers/promises";
import testRoot, { after, mock, type TestContext } from "node:test";
import { fileURLToPath } from "node:url";

import { registerTelegramActivityHandler } from "../api/activity.ts";
import { runNodeEval } from "./fixtures/node-eval.ts";
import * as RoutingFixture from "./fixtures/routing.ts";
import { createTelegramActivityVerbosityRuntime } from "../lib/activity-verbosity.ts";
import * as AgentMessages from "../lib/agent-messages.ts";
import * as Bindings from "../lib/bindings.ts";
import * as BusApi from "../lib/bus-api.ts";
import * as BusFollower from "../lib/bus-follower.ts";
import * as BusLeader from "../lib/bus-leader.ts";
import * as Bus from "../lib/bus.ts";
import * as BusTransport from "../lib/bus-transport.ts";
import * as Media from "../lib/media.ts";
import * as Config from "../lib/config.ts";
import * as Commands from "../lib/commands.ts";
import * as ProcessIdentity from "../lib/process-identity.ts";
import * as WorkspaceAdmission from "../lib/workspace-admission.ts";
import * as Delivery from "../lib/delivery.ts";
import * as Routing from "../lib/routing.ts";
import * as Threads from "../lib/threads.ts";
import * as ThreadCleanupManager from "../lib/thread-cleanup-manager.ts";
import * as Turns from "../lib/turns.ts";
import * as Journal from "../lib/journal.ts";
import * as Locks from "../lib/locks.ts";
import * as Lifecycle from "../lib/lifecycle.ts";
import * as Logging from "../lib/logging.ts";
import type { ExtensionContext } from "../lib/pi.ts";
import * as Ownership from "../lib/ownership.ts";
import * as Queue from "../lib/queue.ts";
import * as Polling from "../lib/polling.ts";
import * as Paths from "../lib/paths.ts";
import * as Sync from "../lib/sync.ts";
import * as Updates from "../lib/updates.ts";
import {
  createTelegramBridgeApiRuntime,
  type TelegramApiClient,
  type TelegramBridgeApiRuntime,
} from "../lib/telegram-api.ts";

// Production timers are unref'd so they never hold Pi open; a live Pi process keeps the loop alive. Mirror that so
// Node 22's runner does not abort runtime tests that await those timers.
const eventLoopKeepAlive = setInterval(() => {}, 60_000);
after(() => { clearInterval(eventLoopKeepAlive); });

type RuntimeTestHandler = (context: TestContext) => void | Promise<void>;
type RuntimeTelegramExtension = (typeof import("../index.ts"))["default"];

function test(
  name: string,
  fn: RuntimeTestHandler,
  timeoutMs = 5_000,
): void {
  void testRoot(name, { concurrency: false, timeout: timeoutMs }, fn);
}

function strictFileTest(name: string, fn: RuntimeTestHandler): void {
  void testRoot(name, { concurrency: false, skip: process.platform === "win32", timeout: 5_000 }, fn);
}

for (const removedFamily of ["sessions", "snapshot-with-segments"] as const) test(
  `Shutdown diagnostics cannot resurrect a removed journal family (${removedFamily})`, async () => {
    const dir = await mkdtemp(join(tmpdir(), "pi-diagnostics-shutdown-"));
    const sessions = join(dir, "sessions"), path = removedFamily === "sessions"
      ? join(sessions, "old-session", "inbox.json") : join(dir, "inbox.json");
    const callbacks: Array<() => void> = [], cleared: unknown[] = [];
    const session = Lifecycle.createTelegramSessionContextStore<ExtensionContext>();
    const ctx = { cwd: "/fixture" } as ExtensionContext;
    session.set(ctx);
    const journalOptions = { path, botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "fixture" }) };
    const journal = Journal.createTelegramUpdateJournalStore(journalOptions);
    const update = (id: number) => ({ update_id: id, message: { message_id: id, text: "fixture" } });
    let projections = 0, publications = 0;
    const diagnostics = Logging.createTelegramRuntimeDiagnosticsRuntime<ExtensionContext>({
      snapshotTimer: {
        setTimer(callback) { callbacks.push(callback); return { unref() {} }; },
        clearTimer(handle) { cleared.push(handle); },
      },
    });
    diagnostics.bindStatus({
      instanceId: "fixture", session, updateStatus() {},
      getStatusState() {
        projections += 1;
        const cursor = journal.read().acceptedThroughUpdateId;
        return { pollingActive: false, pendingDispatch: false, compactionInProgress: false,
          activeToolExecutions: 0, pendingModelSwitch: false, queuedItems: [], recentRuntimeEvents: [], lastUpdateId: cursor };
      },
      async persistSnapshot() { publications += 1; },
    });
    try {
      journal.appendBatch([update(100)], 100);
      journal.appendBatch([update(102)], 102);
      journal.removeCompleted([100, 102]);
      assert.equal(journal.read().acceptedThroughUpdateId, 102);
      diagnostics.onSessionStart();
      diagnostics.scheduleSnapshotPersist();
      const staleCallback = callbacks.shift()!;
      await diagnostics.onSessionShutdown();
      assert.equal(cleared.length, 1);
      if (removedFamily === "sessions") await rm(sessions, { recursive: true, force: true });
      else await rm(path);
      // Simulate an already-dequeued timer at the exact cleanup/reseed boundary; no sleeps or removal retries.
      staleCallback();
      await new Promise(resolve => setImmediate(resolve));
      assert.equal(projections, 0, "Retired status projection must not call the recovering journal reader");
      assert.equal(publications, 0);
      if (removedFamily === "sessions") assert.equal(fs.existsSync(sessions), false, "No directory recreated behind recursive rm");
      else {
        assert.equal(fs.existsSync(path), false, "No old cursor restored from remaining empty segments");
        await rm(path + ".segments", { recursive: true, force: true });
        const next = Journal.createTelegramUpdateJournalStore(journalOptions);
        next.appendBatch([update(100)], 100);
        assert.equal(next.read().acceptedThroughUpdateId, 100, "Fresh fixture has no predecessor cursor");
      }
    } finally {
      await diagnostics.onSessionShutdown();
      await rm(dir, { recursive: true, force: true });
    }
  },
);

let runtimeTelegramExtension: RuntimeTelegramExtension | undefined;
let runtimeAgentDir: string | undefined;

const queueOwnerWorkerPath = fileURLToPath(
  new URL("./fixtures/queue-owner-worker.ts", import.meta.url),
);

interface QueueOwnerTransportHandoffInput {
  journalPath: string;
  recipientJournalPath: string;
  ownersPath: string;
  socketPath: string;
  authSecret: string;
  donorInstanceId: string;
  donorCwd: string;
  recipientInstanceId: string;
  recipientProfileKey: string;
  recipientRegistrationGeneration: string;
  target: { chatId: number; threadId: number };
  dropHandoffAck?: boolean;
}

interface QueueOwnerTransportHandoffProcess {
  child: ReturnType<typeof spawn>;
  ready: Promise<{
    phase: "ready";
    pid: number;
    processBirthId: string;
    transportOwned: boolean;
    executionCount: number;
    foreignQueuedCount: number;
    recipientJournalBindingKey: string;
  }>;
  stop: (command?: "stop" | "execute-control") => Promise<{
    phase: "stopped";
    executionCount: number;
    foreignQueuedCount: number;
    donorEntryCount: number;
    recipientEntryCount: number;
    recipientQueueCount: number;
    handoffCount: number;
    controlExecutions: string[];
    droppedHandoffAck: boolean;
  }>;
}

function spawnQueueOwnerTransportHandoffProcess(
  input: QueueOwnerTransportHandoffInput,
): QueueOwnerTransportHandoffProcess {
  const child = spawn(
    process.execPath,
    [
      "--experimental-strip-types",
      queueOwnerWorkerPath,
      input.journalPath,
      "transport-handoff",
      JSON.stringify(input),
    ],
    { stdio: ["pipe", "pipe", "pipe"] },
  );
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  let buffer = "";
  let stderr = "";
  const lines: unknown[] = [];
  let wake: (() => void) | undefined;
  child.stdout.on("data", (chunk) => {
    buffer += chunk;
    for (;;) {
      const newlineIndex = buffer.indexOf("\n");
      if (newlineIndex < 0) break;
      const line = buffer.slice(0, newlineIndex);
      buffer = buffer.slice(newlineIndex + 1);
      if (line.trim()) lines.push(JSON.parse(line));
      wake?.();
      wake = undefined;
    }
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  let closed: { exitCode: number | null } | undefined;
  child.once("close", (exitCode) => {
    closed = { exitCode };
    wake?.();
    wake = undefined;
  });
  const nextLine = async <T>(): Promise<T> => {
    while (lines.length === 0) {
      if (closed) {
        throw new Error(
          `queue owner transport worker exited ${closed.exitCode}: ${stderr}`,
        );
      }
      await new Promise<void>((resolve) => {
        wake = resolve;
      });
    }
    return lines.shift() as T;
  };
  return {
    child,
    ready: nextLine(),
    async stop(command = "stop") {
      child.stdin.write(`${command}\n`);
      const result = await nextLine<{
        phase: "stopped";
        executionCount: number;
        foreignQueuedCount: number;
        donorEntryCount: number;
        recipientEntryCount: number;
        recipientQueueCount: number;
        handoffCount: number;
        controlExecutions: string[];
        droppedHandoffAck: boolean;
      }>();
      child.kill("SIGKILL");
      await new Promise((resolve) => child.once("close", resolve));
      return result;
    },
  };
}

interface RegistrationRecoveryRaceResult {
  phase: "result";
  registrationOk: boolean;
  recoveryStatus: string;
  registeredPid: number;
  registeredProcessBirthId: string;
  ownerAlive: boolean;
  journalState: string;
  journalOwnerPid: number;
}

function runRegistrationRecoveryRaceProcess(input: {
  journalPath: string;
  socketPath: string;
  startPath: string;
  instanceId: string;
  profileKey: string;
  registrationGeneration: string;
  target: { chatId: number; threadId: number; slot: string };
}): Promise<RegistrationRecoveryRaceResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [
        "--experimental-strip-types",
        queueOwnerWorkerPath,
        input.journalPath,
        "registration-recovery-race",
        JSON.stringify(input),
      ],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    let buffer = "";
    let stderr = "";
    let started = false;
    let result: RegistrationRecoveryRaceResult | undefined;
    child.stdout.on("data", (chunk) => {
      buffer += chunk;
      for (;;) {
        const newlineIndex = buffer.indexOf("\n");
        if (newlineIndex < 0) break;
        const line = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);
        if (!line.trim()) continue;
        const message = JSON.parse(line) as { phase?: string };
        if (message.phase === "ready" && !started) {
          started = true;
          void writeFile(input.startPath, "start", "utf8").catch(reject);
        } else if (message.phase === "result") {
          result = message as RegistrationRecoveryRaceResult;
          child.kill("SIGKILL");
        }
      }
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.once("error", reject);
    child.once("close", (exitCode, signal) => {
      if (result) {
        resolve(result);
        return;
      }
      reject(
        new Error(
          `registration recovery worker exited ${exitCode ?? signal}: ${stderr}`,
        ),
      );
    });
  });
}

function runQueueOwnerReplacementProcess(
  path: string,
  mode: "observe" | "recover" = "observe",
): Promise<{
  executionCount: number;
  foreignQueuedCount: number;
  queuedClaimCount: number;
  entryCount: number;
  directCompletionError?: string;
  recoveryStatus?: string;
}> {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      ["--experimental-strip-types", queueOwnerWorkerPath, path, mode],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.once("error", reject);
    child.once("close", (exitCode) => {
      if (exitCode !== 0) {
        reject(
          new Error(
            `queue owner replacement exited ${exitCode}: ${stderr}`,
          ),
        );
        return;
      }
      try {
        resolve(JSON.parse(stdout.trim()) as {
          executionCount: number;
          foreignQueuedCount: number;
          queuedClaimCount: number;
          entryCount: number;
          directCompletionError?: string;
          recoveryStatus?: string;
        });
      } catch (error) {
        reject(
          new Error(`queue owner replacement returned invalid JSON: ${stdout}`, {
            cause: error,
          }),
        );
      }
    });
  });
}

async function ensureRuntimeAgentDir(): Promise<string> {
  if (!runtimeAgentDir) {
    runtimeAgentDir = await mkdtemp(
      join(tmpdir(), "pi-telegram-runtime-agent-"),
    );
    process.env.PI_CODING_AGENT_DIR = runtimeAgentDir;
  }
  return runtimeAgentDir;
}

async function getRuntimeTelegramExtension(): Promise<RuntimeTelegramExtension> {
  if (runtimeTelegramExtension) return runtimeTelegramExtension;
  await ensureRuntimeAgentDir();
  runtimeTelegramExtension = (await import("../index.ts")).default;
  return runtimeTelegramExtension;
}

for (const role of ["leader", "follower"] as const) {
  for (const remembered of [false, true]) {
    test(`Resume ${role} selects ${remembered ? "remembered" : "fresh"} destination binding without consuming source`, async () => {
      const resume = { reason: "resume", cwd: "/repo", sessionFile: "/sessions/destination.jsonl" };
      const shutdown = { reason: "resume", cwd: "/repo", targetSessionFile: resume.sessionFile, connected: false };
      const dir = await mkdtemp(join(tmpdir(), "pi-connection-resume-"));
      const backgroundSettled = Promise.withResolvers<void>();
      let followerProvisioned = false;
      const keepAlive = setInterval(() => {}, 1000);
      const runWorkspaceOperation: NonNullable<BusLeader.TelegramBusFollowerTargetProvisionerDeps["runWorkspaceOperation"]> = (input, operation) => {
        const run = Promise.resolve().then(operation);
        if (input.operationKind === "workspace.reconcile-follower-provision") {
          void run.then(() => backgroundSettled.resolve(), (error) => backgroundSettled.reject(error));
        }
        return run;
      };
      try {
        const store = Threads.createTelegramTopicTargetStore({ path: join(dir, "state.json"), getNowMs: () => 1000 });
        const sourceBinding = { ...Threads.createTelegramWorkspaceBindingIdentity("/repo", 0, "source")!,
          slot: "A", threadName: "Atlas", target: { chatId: 7, threadId: 41 }, updatedAtMs: 1 };
        store.upsertWorkspaceBinding(sourceBinding);
        const destinationBinding = { ...Threads.createTelegramWorkspaceBindingIdentity("/repo", 0, "destination")!,
          slot: "B", threadName: "Birch", target: { chatId: 7, threadId: 42 }, updatedAtMs: 1 };
        if (remembered) store.upsertWorkspaceBinding(destinationBinding);
        store.upsert({ profileKey: role === "leader" ? "cwd:/repo" : "manual:old",
          owner: role === "leader" ? { kind: "leader", cwd: "/repo", instanceId: "77:1" }
            : { kind: "manual-follower", instanceId: "77:1" },
          instanceId: "77:1", target: sourceBinding.target, slot: "A", threadName: "Atlas",
          status: "active", createdAtMs: 1, updatedAtMs: 1 });
        await store.persist();
        const methods: string[] = [];
        const callApi = async <T>(method: string): Promise<T> => {
          methods.push(method);
          assert.ok(!["deleteForumTopic", "closeForumTopic"].includes(method));
          return { message_thread_id: 43 } as T;
        };
        const handoffs: Lifecycle.TelegramConnectionHandoffStore = {};
        const source = Lifecycle.createTelegramConnectionIntentRuntime({ store: handoffs });
        source.begin("/repo");
        source.suspend(shutdown);
        let starts = 0;
        let finished!: () => void;
        const completed = new Promise<void>((resolve) => { finished = resolve; });
        const failures: unknown[] = [];
        const intent = Lifecycle.createTelegramConnectionIntentRuntime({ store: handoffs });
        const ctx = { cwd: "/repo", sessionManager: {
          getSessionFile: () => resume.sessionFile, getSessionId: () => "destination",
        }, ui: { notify: (text: string) => failures.push(text) } } as unknown as ExtensionContext;
        const lifecycle = Lifecycle.createTelegramConnectionLifecycle({ intent,
          getGeneration: () => 1, isCurrent: () => true,
          getProfileName: () => undefined, isConnected: () => false,
          activateProfile: async () => true,
          async start(current) {
            starts += 1;
            assert.equal(current, ctx);
            try {
              const ports = { getAllowedUserId: () => 7, topicTargetStore: store, callApi,
                runWorkspaceOperation, recordEvent() {},
                recordRuntimeEvent(_category: string, error: unknown) {
                  if (error instanceof Error) failures.push(error);
                }, getNowMs: () => 1000 };
              const result = role === "leader"
                ? await Sync.ensureTelegramLeaderThreadBinding({ ...ports, instanceId: "77:2",
                    cwd: current.cwd, sessionId: current.sessionManager.getSessionId(),
                    probeWorkspaceBinding: async () => {} })
                : await BusLeader.createTelegramBusFollowerTargetProvisioner({ ...ports,
                    getSyncState: () => ({}), setSyncState() {} })({ instanceId: "77:2",
                    profileKey: "manual:new", cwd: current.cwd,
                    sessionId: current.sessionManager.getSessionId(), connectedAtMs: 1000 });
              followerProvisioned = role === "follower" && Boolean(result);
              assert.ok(result);
              const target = "target" in result ? result.target : result;
              assert.equal(target.chatId, 7);
              assert.equal(target.threadId, remembered ? 42 : 43);
              assert.equal(store.getWorkspaceBinding("/repo", "a", "destination")?.slot, "B");
              const retainedSource = store.getWorkspaceBinding("/repo", "a", "source")!;
              assert.equal(retainedSource.bindingKey, sourceBinding.bindingKey);
              assert.equal(retainedSource.slot, "A");
              assert.deepEqual(retainedSource.target, sourceBinding.target);
              assert.equal(methods.filter((m) => m === "createForumTopic").length, remembered ? 0 : 1);
              return { ok: true };
            } catch (error) { failures.push(error); throw error; }
            finally { finished(); }
          }, recordError(error) { failures.push(error); },
        });
        const launch = lifecycle.prepare({ type: "session_start", reason: "resume" }, ctx);
        assert.ok(launch);
        launch();
        await completed;
        if (followerProvisioned) await backgroundSettled.promise;
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(starts, 1);
        assert.deepEqual(failures, []);
      } finally {
        try {
          // Provisioning returns before its real background publisher; join it before deleting storage.
          if (followerProvisioned) await backgroundSettled.promise;
        } finally {
          clearInterval(keepAlive);
          await rm(dir, { recursive: true, force: true });
        }
      }
    });
  }
}

test("Storage reference preparation rejects historical aliases without redirecting accepted work or leases", async () => {
  // A child isolates cwd/env; only explicitly created fixtures may be discovered.
  const result = await runNodeEval(`
    import assert from "node:assert/strict";
    import * as fs from "node:fs";
    import { tmpdir } from "node:os";
    import { join } from "node:path";
    import * as Paths from ${JSON.stringify(new URL("../lib/paths.ts", import.meta.url).href)};
    import * as Config from ${JSON.stringify(new URL("../lib/config.ts", import.meta.url).href)};
    import * as Journal from ${JSON.stringify(new URL("../lib/journal.ts", import.meta.url).href)};
    import * as Admission from ${JSON.stringify(new URL("../lib/workspace-admission.ts", import.meta.url).href)};
    const originalCwd = process.cwd();
    const root = fs.realpathSync(fs.mkdtempSync(join(tmpdir(), "pi-telegram-reference-")));
    const snapshot = (directory) => fs.readdirSync(directory, { withFileTypes: true })
      .sort((a, b) => a.name.localeCompare(b.name))
      .map(entry => [entry.name, entry.isDirectory() ? snapshot(join(directory, entry.name))
        : fs.readFileSync(join(directory, entry.name)).toString("hex")]);
    try {
      const agentDir = join(root, "agent");
      process.env.PI_CODING_AGENT_DIR = agentDir;
      const config = Config.createTelegramConfigStore({ agentDir, initialConfig: { profiles: {
        default: { botToken: "7:fixture-default", allowedUserId: 7 },
        work: { botToken: "7:fixture-work", allowedUserId: 7 },
      } } });
      await config.persist();
      const owner = { processId: process.pid, processBirthId: process.pid + ":fixture-birth" };
      const queueOwner = { ...owner, instanceId: "fixture-instance", sessionGeneration: 1 };
      const leaseInput = { operationId: "retained", operationKind: "fixture", scope: { kind: "profile" } };
      const compose = (checked) => {
        const admission = Admission.createTelegramWorkspaceAdmissionRuntimeBinding({
          getProfileName: config.getActiveProfileName, getBotToken: config.getBotToken,
          owner, getProcessLiveness: () => "alive",
          getPath(profileName) {
            // Reconstruct the historical defective callback shape without changing its fixture bytes.
            const selected = Paths.resolveTelegramWorkspaceAdmissionPath(profileName);
            return checked ? Paths.requireTelegramStoragePathReference(selected,
              Paths.resolveTelegramWorkspaceAdmissionPath(undefined, profileName)) : selected;
          },
        });
        const journals = Journal.createTelegramUpdateJournalBindingRuntime({
          base: { getProfileName: config.getActiveProfileName, getBotToken: config.getBotToken,
            getBotId: () => 7, getWorkspaceAdmission: admission.resolve,
            getQueueRuntimeIdentity: () => ({ ...owner, instanceId: queueOwner.instanceId }),
            onRecovery() { assert.fail("Reference preparation must not recover journals"); } },
          getLeaderJournalPath(profileName) {
            const selected = Paths.resolveTelegramUpdateJournalPath(profileName);
            return checked ? Paths.requireTelegramStoragePathReference(selected,
              Paths.resolveTelegramUpdateJournalPath(undefined, profileName)) : selected;
          },
          getFollowerJournalPath(bindingKey, profileName) {
            const selected = Paths.resolveTelegramFollowerJournalPath(bindingKey, undefined, profileName);
            return checked ? Paths.requireTelegramStoragePathReference(selected,
              Paths.resolveTelegramFollowerJournalPath(bindingKey, agentDir, profileName)) : selected;
          },
          getActiveFollowerBindingKey: () => "fixture-recipient", isFollowerRegistered: () => false,
        });
        return { admission, journals };
      };
      const legacy = compose(false);
      const checked = compose(true);
      // The unchanged default wiring remains usable under the reference preflight.
      checked.journals.resolveLeader().journal.appendBatch([{ update_id: 90 }]);
      assert.equal(checked.admission.resolve().acquireAdmission(leaseInput).kind, "acquired");
      assert.equal(config.activateProfile("work"), true);
      const references = [];
      for (const [index, cwdName] of ["cwd-a", "cwd-b"].entries()) {
        const cwd = join(root, cwdName);
        fs.mkdirSync(cwd);
        process.chdir(cwd);
        const binding = legacy.journals.resolveLeader();
        const updateId = index + 1;
        binding.journal.appendBatch([{ update_id: updateId }]);
        binding.journal.markQueued({ queueKind: "prompt", receiptId: "retained-queue",
          sourceUpdateIds: [updateId], owner: queueOwner });
        const ledger = legacy.admission.resolve();
        assert.equal(ledger.acquireAdmission(leaseInput).kind, "acquired");
        assert.equal(binding.journal.read().entries[0].state, "queued");
        assert.equal(ledger.read().leases.length, 1);
        references.push({ runtimeKey: binding.runtimeKey, recoveryKey: binding.recoveryKey });
        assert.equal(JSON.parse(binding.runtimeKey).path, join("work", "tmp", "pi-telegram", "inbox.json"));
        assert.ok(fs.existsSync(join(cwd, "work", "tmp", "pi-telegram", "inbox.json")));
        assert.ok(fs.existsSync(join(cwd, "work", "tmp", "pi-telegram", "workspace-admission.json")));
        const before = snapshot(root);
        const denied = /Telegram storage reference does not match its approved absolute path/;
        assert.throws(() => checked.admission.resolve().acquireAdmission({ ...leaseInput,
          operationId: "must-not-write" }), denied);
        assert.throws(() => checked.journals.resolveLeader().journal.appendBatch([{ update_id: 99 }]), denied);
        // A correctly placed follower file cannot bypass the mismatched admission reference.
        assert.throws(() => checked.journals.resolveFollower().journal.appendBatch([{ update_id: 99 }]), denied);
        assert.throws(() => checked.journals.createRecipientResolver("other-recipient")()
          .journal.appendBatch([{ update_id: 99 }]), denied);
        assert.deepEqual(snapshot(root), before, "No file, segment, staging directory, receipt or lease changed");
      }
      // Opaque raw-relative keys alone cannot distinguish two physical source locations.
      assert.deepEqual(references[0], references[1]);
      assert.equal(fs.existsSync(Paths.resolveTelegramUpdateJournalPath(undefined, "work")), false);
      assert.equal(fs.existsSync(Paths.resolveTelegramWorkspaceAdmissionPath(undefined, "work")), false);
      {
        const before = snapshot(root);
        const census = Journal.inspectTelegramProfileJournalNamespace({
          directory: Paths.resolveTelegramTempDir(), profile: "work",
          botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: config.getBotToken(), botId: 7 }),
          limits: { maxDirectoryEntries: 32, maxFiles: 32, maxBytes: 100000, maxEntries: 32, maxWork: 1000 },
        });
        assert.equal(census.sources.length, 1);
        assert.equal(census.sources[0].evidence.kind, "absent");
        assert.deepEqual(snapshot(root), before, "Canonical absence is not historical reference closure");
      }
      console.log("reference-preparation-ok");
    } finally {
      process.chdir(originalCwd);
      fs.rmSync(root, { recursive: true, force: true });
    }
  `);
  assert.equal(result.code, 0, result.stderr || result.stdout);
  assert.equal(result.stdout.trim(), "reference-preparation-ok");
}, 10_000);

test("Cross-instance agent turns route in both leader and follower directions", async () => {
  const followerRegistry = Bus.createTelegramBusFollowerRegistry();
  followerRegistry.register({
    instanceId: "follower",
    connectedAtMs: 1,
    registrationGeneration: "follower-generation",
    target: { chatId: 7, threadId: 99 },
    threadName: "Birch",
  });
  const events: string[] = [];
  const handleUpdate = async (update: Updates.TelegramUpdateFlow) => {
    await Updates.executeTelegramUpdate(update, 7, {
      ctx: "ctx",
      getCurrentInstanceId: () => "leader",
      getMessageOwnership: () => ({ instanceId: "source-instance" }),
      getTargetOwnership: (target) =>
        target.threadId === 99
          ? {
              instanceId: "follower",
              ownerGeneration: "follower-generation",
            }
          : { instanceId: "leader" },
      recordMessageOwnership: ({ instanceId, messageId }) => {
        events.push(`record:${instanceId}:${messageId}`);
      },
      foreignOwnedUpdateForwarder: {
        forwardMessage: async ({ message, ownership }) => {
          events.push(
            `forward:${ownership.instanceId}:${ownership.ownerGeneration}:${message.message_thread_id}:${message.pi_telegram_agent_source_thread}`,
          );
          return {
            status: "accepted",
            delivery: {
              deliveryId: "test-delivery",
              sourceUpdateId: 1,
              recipientBindingKey: "test-recipient",
            },
          };
        },
      },
      removePendingMediaGroupMessages: () => {},
      removeQueuedTelegramTurnsByMessageIds: () => 0,
      handleAuthorizedTelegramReactionUpdate: async () => {},
      pairTelegramUserIfNeeded: async () => false,
      answerCallbackQuery: async () => {},
      answerGuestQuery: async () => {},
      handleAuthorizedTelegramCallbackQuery: async () => {},
      sendTextReply: async () => undefined,
      handleAuthorizedTelegramMessage: async (message) => {
        events.push(
          `local:${message.message_thread_id}:${message.pi_telegram_agent_source_thread}`,
        );
      },
      handleAuthorizedTelegramEditedMessage: async () => {},
    });
  };
  const runtime = AgentMessages.createTelegramAgentMessageRuntime({
    instanceId: "leader",
    getAllowedChatId: () => 7,
    getLeaderTarget: () => ({ chatId: 7, threadId: 42 }),
    getLeaderThreadName: () => "Aster",
    followerRegistry,
    getContext: () => "ctx",
    handleUpdate,
  });

  await runtime.route({
    sourceTarget: { chatId: 7, threadId: 42 },
    sourceThreadName: "Aster",
    message: {
      target: { chatId: 7, threadId: 99 },
      messageId: 101,
      text: "Leader to follower",
    },
  });
  await runtime.route({
    sourceTarget: { chatId: 7, threadId: 99 },
    sourceThreadName: "Birch",
    message: {
      target: { chatId: 7, threadId: 42 },
      messageId: 102,
      text: "Follower to leader",
    },
  });

  assert.deepEqual(events, [
    "record:follower:101",
    "forward:follower:follower-generation:99:Aster",
    "local:42:Birch",
  ]);
});

for (const [scenario, keepRouter, recoverCopy] of [["cold-router", false, false], ["retained-router", true, false], ["protected-restart", true, true]] as const)
  test(`Retired historical UI protects uncertain delivery and preserves protected-attempt recovery (${scenario})`, async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-history-ui-"));
  const socketPath = join(dir, "recipient.sock");
  const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:historical-ui" });
  const sourceOptions = { path: join(dir, "source.json"), botIdentity };
  let interruptCommit = false;
  const source = Journal.createTelegramUpdateJournalStore({ ...sourceOptions, onPublicationBoundary(boundary, target) {
    if (interruptCommit && boundary === "before-write" && !target.startsWith(`${sourceOptions.path}.retained`)) throw new Error("fixture interrupted source commit");
  } });
  const recipientJournal = Journal.createTelegramUpdateJournalStore({ path: join(dir, "recipient.json"), botIdentity });
  const binding = Journal.createTelegramUpdateJournalBindingKey(sourceOptions);
  const threadStore = Threads.createTelegramTopicTargetStore({ path: join(dir, "threads.json") });
  await threadStore.load();
  for (const [cwd, threadId, instanceId, slot] of [["/repo", 42, "leader-a", "A"], ["/other", 43, "recipient", "B"]] as const) {
    threadStore.upsert({ profileKey: `cwd:${cwd}`, target: { chatId: 7, threadId }, status: "active",
      instanceId, slot, createdAtMs: 1, updatedAtMs: 1 });
  }
  await threadStore.persist();
  const original = { update_id: 100, message: { message_id: 12, message_thread_id: 99,
    chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text: "original accepted elsewhere" } };
  let acknowledge!: () => void;
  const ackGate = new Promise<void>(resolve => { acknowledge = resolve; });
  let admitted!: () => void;
  const admissionGate = new Promise<void>(resolve => { admitted = resolve; });
  let forwards = 0;
  const admission = BusFollower.createTelegramBusFollowerDurableAdmissionRuntime<string>({ journal: recipientJournal, signalWorker: () => {} });
  const server = Bus.createTelegramBusLocalServer({ socketPath, async handleEnvelope(envelope) {
    assert.equal(envelope.kind, "leader.forwardMessage");
    if (envelope.kind !== "leader.forwardMessage") throw new Error("Unexpected fixture envelope");
    forwards++;
    const receipt = await admission.admit(envelope, "recipient-ctx");
    admitted(); await ackGate;
    return { kind: "bus.ack", requestId: envelope.requestId, ok: true, result: receipt };
  } });
  const forwarder = Bus.createTelegramBusForeignOwnedUpdateForwarder({ socketPath, createRequestId: () => "history-ui", timeoutMs: 5_000 });
  const surfaces: Array<{ id: number; text: string; markup: unknown }> = [];
  let copies = 0;
  let reviewing = false;
  const routeOptions: RoutingFixture.RouteHarnessOptions = { threadStore,
    getAdmissionJournalBinding: () => binding, getCurrentLeaderEpoch: () => 1, isContextActive: () => true,
    async runWorkspaceOperation(_input, operation) { return operation(); },
    getTargetOwnership: target => target.chatId === 7 && target.threadId === 43 ? {
      instanceId: "recipient", ownerGeneration: "recipient-generation", recipientBindingKey: "manual:recipient",
      protocolIdentity: Bus.createTelegramBusProtocolIdentity({ runtimeBuild: "fixture",
        capabilities: [Bus.TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION] }),
    } : undefined,
    foreignOwnedUpdateForwarder: forwarder,
    async editInteractiveMessage(_chat, id, text, _mode, markup) { surfaces.push({ id, text, markup }); },
    async callApi() { assert.equal(reviewing, false, "Stopping the source must not probe/create/delete a Thread"); return true as never; },
    async deleteMessage() { assert.fail("Stopping the source must not delete Telegram messages"); },
  };
  let harness = RoutingFixture.createRouteHarness(routeOptions);
  const completed: number[] = [];
  const executed: number[] = [];
  const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<RoutingFixture.TestUpdate & Journal.TelegramJournaledUpdate, RoutingFixture.TestContext>({
    journal: { ...source, abandonPending(input) {
      copies++; return source.abandonPending(input);
    } },
    getJournalBindingKey: () => binding, hasAuthority: () => true,
    shouldReviewHistoricalInput: (entry, ctx, signal) => harness.routeRuntime.shouldReviewHistoricalInput(entry, ctx, signal),
    async defaultHandle(update, ctx, execution) { executed.push(update.update_id); await harness.routeRuntime.handleUpdate(update, ctx, execution); },
    onUpdateCompleted: id => { completed.push(id); },
  });
  let nextId = 101;
  const appendClick = (data: string, messageId = 500, threadId = 42) => {
    source.appendBatch([{ update_id: nextId++, callback_query: { id: `history-${nextId}`, from: { id: 7, is_bot: false }, data,
      message: { message_id: messageId, message_thread_id: threadId, chat: { id: 7, type: "private" } } } }]);
    worker.signal();
  };
  const click = async (data: string, messageId = 500, threadId = 42) => { appendClick(data, messageId, threadId); await worker.waitForDrain(); };
  const action = (suffix: string) => {
    const data = JSON.stringify(surfaces.at(-1)?.markup).match(new RegExp(`reroutecancel:(?:history|review):[a-z0-9]+:${suffix}`))?.[0];
    assert.ok(data, `Expected ${suffix}: ${surfaces.at(-1)?.text}`); return data;
  };
  try {
    await server.start();
    worker.start({ cwd: "/repo" }); await worker.waitForDrain();
    source.appendBatch([original]); worker.signal(); await worker.waitForDrain();
    const entry = structuredClone(source.read().entries[0]!);
    assert.ok(harness.events.some(event => event.includes("reroutecancel:1")),
      `Fresh original starts with an ordinary chooser: ${JSON.stringify({ events: harness.events, state: worker.getState(), records: threadStore.list() })}`);
    appendClick("reroute:1:43", 99, 99);
    await Promise.race([admissionGate, worker.waitForDrain().then(() => { assert.fail("Selection did not reach durable follower admission"); })]);
    assert.deepEqual(source.read().entries.find(item => item.updateId === 100), entry);
    const acceptedRecipient = structuredClone(recipientJournal.read());
    assert.equal(acceptedRecipient.entries.length, 1);
    const stopping = worker.stop(); acknowledge(); await stopping;
    assert.deepEqual(source.read().entries.find(item => item.updateId === 100), entry, "Late ACK cannot complete a revoked source");
    if (!keepRouter) harness = RoutingFixture.createRouteHarness(routeOptions);
    if (recoverCopy) {
      // Supplied precondition: an interrupted retention attempt exists before replacement startup; no retired UI action grants it.
      interruptCommit = true;
      try { assert.throws(() => source.abandonPending({ journalBindingKey: binding, entry,
        operatorAuthorityId: "telegram-owner:7", isCurrent: () => true }), /journal mutation failed/); }
      finally { interruptCommit = false; }
      assert.ok(source.inspectPendingRetention(entry));
    }
    reviewing = true;
    worker.start({ cwd: "/repo" }); await worker.waitForDrain();
    assert.equal(worker.getState().historicalClaimCount ?? 0, recoverCopy ? 0 : 1);
    assert.equal(worker.getState().abandoningClaimCount ?? 0, recoverCopy ? 1 : 0);
    assert.equal(forwards, 1, "Startup and stale queued chooser callbacks cannot forward again");
    await click("reroutecancel:1", 99, 99);
    assert.equal(copies, 0, "The stronger ordinary cancellation contract stays unavailable");
    const priorSurfaceCount = surfaces.length;
    for (const suffix of ["open", "nonce:choose:0", "nonce:confirm:0", "nonce:retry:0", "nonce:text", "nonce:list", "nonce:refresh", "nonce:more"]) {
      await click(`reroutecancel:history:${suffix}`);
      assert.match(harness.events.filter(value => value.startsWith("answer:")).at(-1)!, /no longer available/);
      assert.deepEqual(source.read().entries.find(item => item.updateId === 100), entry);
      assert.equal(copies, 0);
      assert.equal(surfaces.length, priorSurfaceCount, "Retired callbacks cannot recreate the submenu");
    }
    if (recoverCopy) {
      await click("reroutecancel:review:open");
      assert.match(surfaces.at(-1)!.text, /previously accepted work may continue/);
      await click(action("retry:0"));
      assert.equal(copies, 1);
      const retained = source.inspectPendingRetention(entry)!;
      assert.deepEqual(JSON.parse(await readFile(retained.retainedPath, "utf8")).entry, entry);
    }
    assert.equal(source.read().entries.some(item => item.updateId === 100), !recoverCopy);
    assert.deepEqual(recipientJournal.read(), acceptedRecipient, "Retired UI and protected recovery cannot change independently accepted custody");
    assert.equal(completed.includes(100), false, "Source abandonment must never fabricate task completion");
    assert.equal(executed.filter(id => id === 100).length, 1);
    assert.equal(harness.telegramQueueStore.getQueuedItems().length, 0);
    assert.deepEqual(source.appendBatch([original]).duplicateUpdateIds, [100]);
    if (recoverCopy) assert.ok(surfaces.some(surface => surface.id === 99 && surface.text.includes("Routing cancelled")), "Protected recovery retires the obsolete chooser");
    await click("reroute:1:43", 99, 99); assert.equal(forwards, 1);
    const recipientExecutions: number[] = [];
    const recipient = Updates.createTelegramUpdateWorkerRuntime<string>({ journal: recipientJournal, hasAuthority: () => true,
      executeUpdate(update) { recipientExecutions.push(update.update_id); return { kind: "complete" }; } });
    try {
      recipient.start("recipient-ctx"); await recipient.waitForDrain();
      assert.deepEqual(recipientExecutions, [100], "Already accepted work still executes independently from source UI");
    } finally { await recipient.stop(); }
  } finally {
    acknowledge(); await worker.stop(); await server.stop(); await rm(dir, { recursive: true, force: true });
  }
}, 15_000);

for (const source of ["fresh", "adopted"] as const) {
  void testRoot(`Generic follower delivery acknowledges repeated pending or completed input without re-execution (${source})`,
    { concurrency: false, timeout: 5_000 }, async () => {
    const dir = await mkdtemp(join(tmpdir(), "pi-telegram-completed-delivery-"));
    const recipientBindingKey = "manual:recipient", sessionId = "session-b", updateId = 44;
    const journalSerialization = Journal.createTelegramJournalSourceSerialization(() => join(dir, "journals.transaction"));
    const bindingRuntime = Journal.createTelegramUpdateJournalBindingRuntime({
      base: { getProfileName: () => "work", getBotToken: () => "fixture:completed-delivery", getBotId: () => 7,
        withSourceSerialization: journalSerialization },
      getRuntimeDir: () => Paths.resolveTelegramTempDir(dir),
      getLeaderJournalPath: () => Paths.resolveTelegramSessionPollingJournalPath(sessionId, dir, "work"),
      getFollowerJournalPath: (key, profile, id) => Paths.resolveTelegramSessionJournalPath(id!, key, dir, profile),
      getActiveFollowerBindingKey: () => recipientBindingKey, getActiveFollowerSessionId: () => sessionId,
      isFollowerRegistered: () => true,
    });
    // Registration/readiness are supplied preconditions; this witness does not drive the Pi session gateway.
    const binding = bindingRuntime.resolveActive()!;
    const message = { message_id: 12, chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false },
      text: "completed-delivery fixture", pi_telegram_source_update_id: updateId };
    const predecessor = source === "adopted" ? bindingRuntime.createRecipientResolver(recipientBindingKey, "session-a")()! : undefined;
    // Production admission resolves the active recipient journal on each call; the adopted case starts in its predecessor.
    let admissionJournal = predecessor?.journal ?? binding.journal;
    const executed: number[] = [];
    const worker = Updates.createTelegramUpdateAdmissionWorkerRuntime<Updates.TelegramUpdateFlow & Journal.TelegramJournaledUpdate, string>({
      journal: binding.journal, getJournalBindingKey: () => binding.recoveryKey, hasAuthority: () => true,
      isContextCurrent: ctx => ctx === "recipient-ctx",
      async defaultHandle(update) { executed.push(update.update_id); },
    });
    const admissions: Array<ReturnType<typeof binding.journal.appendBatch>> = [];
    const socketPath = Bus.getTelegramBusFollowerSocketPath("recipient", dir);
    const receiver = BusFollower.createTelegramBusForwardedUpdateReceiverRuntime({
      socketPath, instanceId: "recipient", getAuthSecret: () => "fixture-secret",
      getRegistrationGeneration: () => "registration-b", getRecipientBindingKey: () => recipientBindingKey,
      getContext: () => "recipient-ctx",
      durableAdmission: BusFollower.createTelegramBusFollowerDurableAdmissionRuntime({
        journal: { appendBatch(updates) {
          const result = admissionJournal.appendBatch(updates); admissions.push(result); return result;
        } }, signalWorker: () => worker.signal(),
      }),
    });
    const delivery = Bus.createTelegramBusFollowerDeliveryIdentity({ kind: "leader.forwardMessage", recipientBindingKey, sourceUpdateId: updateId });
    const send = async (requestId: string) => {
      const ack = await Bus.sendTelegramBusLocalEnvelope({ socketPath, envelope: {
        kind: "leader.forwardMessage", requestId, auth: "fixture-secret", recipientInstanceId: "recipient",
        recipientRegistrationGeneration: "registration-b", delivery, message, sentAtMs: 1,
      } });
      assert.equal(ack?.kind, "bus.ack");
      if (ack?.kind !== "bus.ack") throw new Error("missing fixture delivery ACK");
      assert.equal(ack.ok, true, ack.message);
      assert.deepEqual(ack.result, { deliveryId: delivery.deliveryId, sourceUpdateId: updateId });
    };
    try {
      await receiver.start();
      await send("initial-request");
      if (predecessor) {
        assert.deepEqual(bindingRuntime.adoptPredecessorPending({ recipientBindingKey,
          predecessorSessionId: "session-a", successorSessionId: sessionId, isCurrent: () => true }),
        { adoptedUpdateIds: [updateId], retainedUpdateIds: [] });
        assert.ok(predecessor.journal.inspectAbandonedPending(updateId), "adoption privately commits away its predecessor");
        assert.deepEqual(predecessor.journal.read().entries, []);
        admissionJournal = binding.journal;
      }
      await send("pending-retry");
      assert.deepEqual(binding.journal.read().entries.map(entry => [entry.updateId, entry.state]), [[updateId, "pending"]]);
      assert.deepEqual(admissions.map(result => result.addedUpdateIds), [[updateId]], "a pending retry is acknowledged without another append");
      assert.deepEqual(executed, [], "durable admission ACKs before the paused worker executes");
      worker.start("recipient-ctx"); await worker.waitForDrain();
      assert.deepEqual(executed, [updateId]);
      assert.deepEqual(binding.journal.read().entries, [], "real worker completion removes recipient custody");
      // A fresh RPC request carries the same delivery/source identity, bypassing only the transport request cache.
      await send("completed-retry"); await worker.waitForDrain();
      assert.equal(admissions.length, 1, "a completed retry is acknowledged from the bounded delivery window");
      assert.deepEqual(executed, [updateId], "the handler does not run again after completion");
      assert.deepEqual(binding.journal.read().entries, []);
    } finally { await worker.stop(); await receiver.stop(); await rm(dir, { recursive: true, force: true }); }
  });
}

test("Historical pending source alone cannot prove no follower acceptance", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-historical-pending-"));
  const socketPath = join(dir, "recipient.sock");
  const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:historical-fixture" });
  const sourcePath = join(dir, "source.json");
  const sourceJournal = Journal.createTelegramUpdateJournalStore({ path: sourcePath, botIdentity });
  const recipientJournal = Journal.createTelegramUpdateJournalStore({ path: join(dir, "recipient.json"), botIdentity });
  const original = { update_id: 100, message: {
    message_id: 9, message_thread_id: 11, chat: { id: 7, type: "private" },
    from: { id: 7, is_bot: false }, text: "historical original",
    reply_to_message: { message_id: 11, forum_topic_created: { name: "Fixture", icon_color: 0 } },
  } };
  sourceJournal.appendBatch([original]);
  const beforeForward = structuredClone(sourceJournal.read());
  let acknowledge!: () => void;
  const acknowledgementGate = new Promise<void>((resolve) => { acknowledge = resolve; });
  let admitted!: () => void;
  const admittedGate = new Promise<void>((resolve) => { admitted = resolve; });
  const durableAdmission = BusFollower.createTelegramBusFollowerDurableAdmissionRuntime<string>({
    journal: recipientJournal, signalWorker: () => {},
  });
  const server = Bus.createTelegramBusLocalServer({
    socketPath,
    async handleEnvelope(envelope) {
      if (envelope.kind !== "leader.forwardMessage") throw new Error("Unexpected fixture envelope");
      const receipt = await durableAdmission.admit(envelope, "recipient-ctx");
      admitted();
      await acknowledgementGate;
      return { kind: "bus.ack", requestId: envelope.requestId, ok: true, result: receipt };
    },
  });
  const forwarder = Bus.createTelegramBusForeignOwnedUpdateForwarder({
    socketPath, createRequestId: () => "historical:1", timeoutMs: 10_000,
  });
  const worker = Updates.createTelegramUpdateWorkerRuntime<string>({
    journal: sourceJournal, hasAuthority: () => true,
    async executeUpdate(update) {
      // Legacy selection/forwarding intent is not published in the source entry.
      const settlement = await forwarder.forwardMessage({
        message: { ...original.message, pi_telegram_source_update_id: update.update_id },
        ownership: { instanceId: "recipient", ownerGeneration: "generation-1", recipientBindingKey: "manual:recipient" },
        ctx: "source-ctx",
      });
      assert.equal(settlement.status, "accepted");
      return { kind: "complete" };
    },
  });
  try {
    await server.start();
    worker.start("source-ctx");
    await Promise.race([admittedGate, worker.waitForDrain().then(() => {
      assert.fail("Forwarding ended before the recipient admission gate");
    })]);
    assert.equal(recipientJournal.read().entries.length, 1, "Recipient already holds durable accepted input");
    assert.deepEqual(sourceJournal.read(), beforeForward,
      "Unforwarded and accepted-without-ACK histories have identical source journal evidence");
    const stopping = worker.stop();
    acknowledge();
    await stopping;
    const reopened = Journal.createTelegramUpdateJournalStore({ path: sourcePath, botIdentity }).read();
    assert.deepEqual(reopened, beforeForward, "Revoked source generation cannot publish late completion");
    const entry = reopened.entries[0]!;
    assert.equal(entry.state, "pending");
    assert.equal(entry.queueReceiptId, undefined);
    assert.equal(entry.queueOwner, undefined);
    assert.equal(entry.inputClaim, undefined);
    assert.equal(entry.inputProvenance, undefined);
    assert.equal(recipientJournal.read().entries.length, 1, "Recipient authority survives source shutdown");
    const acceptedRecipient = structuredClone(recipientJournal.read());
    const journalBindingKey = Journal.createTelegramUpdateJournalBindingKey({ path: sourcePath, botIdentity });
    const completed: number[] = [];
    let carrier: unknown;
    const recovery = Updates.createTelegramUpdateAdmissionWorkerRuntime<Updates.TelegramUpdateFlow & Journal.TelegramJournaledUpdate, string>({
      journal: sourceJournal, getJournalBindingKey: () => journalBindingKey, hasAuthority: () => true,
      shouldReviewHistoricalInput: candidate => candidate.updateId === original.update_id,
      onUpdateCompleted: id => { completed.push(id); },
      async defaultHandle(update) {
        assert.equal(update.update_id, 101, "Historical input must not be delivered a second time");
        carrier = update.callback_query;
      },
    });
    try {
      sourceJournal.appendBatch([{ update_id: 101, callback_query: { id: "owner-review", from: { id: 7, is_bot: false },
        message: { message_id: 20, chat: { id: 7, type: "private" } } } }]);
      recovery.start("recovery-ctx");
      await recovery.waitForDrain();
      const page = Updates.inspectTelegramHistoricalInputs(carrier, { journalBindingKey, isCurrent: () => true });
      assert.equal(page?.sources.length, 1);
      const result = page!.sources[0]!.retry({ operatorAuthorityId: "owner:7", isCurrent: () => true });
      assert.ok(result);
      assert.deepEqual(JSON.parse(await readFile(result.retainedPath, "utf8")).entry, entry);
      assert.deepEqual(sourceJournal.read().entries, []);
      assert.deepEqual(recipientJournal.read(), acceptedRecipient, "Source-only stopping does not revoke independently accepted recipient custody");
      assert.deepEqual(completed, [101], "Stopping the original is not task completion");
      assert.deepEqual(sourceJournal.appendBatch([original]).duplicateUpdateIds, [100]);
      const recipientExecutions: number[] = [];
      const recipient = Updates.createTelegramUpdateWorkerRuntime<string>({
        journal: recipientJournal, hasAuthority: () => true,
        executeUpdate(update) { recipientExecutions.push(update.update_id); return { kind: "complete" }; },
      });
      try {
        recipient.start("recipient-ctx");
        await recipient.waitForDrain();
        assert.deepEqual(recipientExecutions, [100], "Already accepted recipient work can still execute independently");
      } finally { await recipient.stop(); }
    } finally { await recovery.stop(); }
  } finally {
    acknowledge();
    await worker.stop();
    await server.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

for (const scenario of [
  { carrier: "message", cached: false, replace: true, threaded: true },
  { carrier: "edited_message", cached: false, replace: false, threaded: true },
  { carrier: "callback_query", cached: true, replace: false, threaded: true },
  { carrier: "callback_query", cached: true, replace: true, threaded: false },
  { carrier: "message_reaction", cached: true, replace: true, threaded: false },
] as const) {
  test(`Cached forwarding retries ${scenario.carrier} (${scenario.threaded ? "threaded" : "message-only"}) through durable admission`, async () => {
    const dir = await mkdtemp(join(tmpdir(), "pi-telegram-cached-forward-"));
    const socketPath = join(dir, "receiver.sock");
    const target = { chatId: 7, threadId: 11 };
    const registry = Bus.createTelegramBusFollowerRegistry();
    const registration = {
      instanceId: "follower",
      profileKey: "manual:recipient",
      registrationGeneration: "g1",
      connectedAtMs: 1,
      target,
      protocol: Bus.createTelegramBusProtocolIdentity({
        runtimeBuild: "fixture",
        capabilities: [Bus.TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION],
      }),
    };
    registry.register(registration);
    const ownership = Ownership.createTelegramBusMessageOwnershipRuntime({
      instanceId: "leader",
      getProfileKey: () => "default",
      listFollowers: registry.list,
    });
    if (scenario.cached) ownership.recordFollower({
      chatId: target.chatId, messageId: 9, target, follower: registration,
    });
    const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({
      botToken: "123:cached-forward-fixture",
    });
    let now = 1_000;
    const leaderJournal = Journal.createTelegramUpdateJournalStore({
      path: join(dir, "leader.json"), botIdentity, getNowMs: () => now,
    });
    const followerJournal = Journal.createTelegramUpdateJournalStore({
      path: join(dir, "follower.json"), botIdentity, getNowMs: () => now,
    });
    const message = {
      message_id: 9, chat: { id: 7, type: "private" },
      from: { id: 7, is_bot: false },
      ...(scenario.threaded ? { message_thread_id: 11 } : {}),
      voice: { file_id: "fixture-voice", file_unique_id: "fixture-voice", duration: 1 },
    };
    const source: Updates.TelegramUpdateFlow & Journal.TelegramJournaledUpdate = {
      update_id: 100,
      ...(scenario.carrier === "callback_query"
        ? { callback_query: { id: "fixture-callback", from: message.from, message } }
        : scenario.carrier === "message_reaction"
          ? { message_reaction: { chat: message.chat, user: message.from,
              message_id: 9, old_reaction: [], new_reaction: [{ type: "emoji", emoji: "👍" }] } }
          : { [scenario.carrier]: message }),
    };
    const received: Bus.TelegramBusEnvelope[] = [];
    const durableAdmission = BusFollower.createTelegramBusFollowerDurableAdmissionRuntime<string>({
      journal: followerJournal,
      signalWorker: () => {},
    });
    let admissionFailures = scenario.replace ? 1 : 2;
    const receiver = BusFollower.createTelegramBusForwardedUpdateReceiverRuntime({
      socketPath, instanceId: registration.instanceId,
      getAuthSecret: () => "fixture-auth",
      getRegistrationGeneration: () => scenario.replace ? "g2" : "g1",
      getRecipientBindingKey: () => registration.profileKey,
      getContext: () => "follower-ctx",
      durableAdmission: {
        async admit(envelope, ctx) {
          received.push(envelope);
          if (admissionFailures-- > 0) throw new Error("Fixture admission unavailable.");
          return durableAdmission.admit(envelope, ctx);
        },
      },
    });
    let requests = 0;
    const attempts: Bus.TelegramBusForwardOwnership[] = [];
    const rejectedDeliveryIds: string[] = [];
    const validate = Bus.createTelegramBusForwardOwnershipValidator(registry);
    const forwarder = Bus.createTelegramBusForeignOwnedUpdateForwarder<
      string, Updates.TelegramMessageReactionUpdated, Updates.TelegramCallbackQuery, Updates.TelegramUpdateMessage
    >({
      socketPath, createRequestId: () => `fixture:${++requests}`,
      getAuthSecret: () => "fixture-auth",
      recordRuntimeEvent(_category, _error, details) {
        if (typeof details?.deliveryId === "string") rejectedDeliveryIds.push(details.deliveryId);
      },
      validateForwardOwnership(snapshot) {
        attempts.push(snapshot);
        return validate(snapshot);
      },
    });
    const noLocalExecution = () => assert.fail("Foreign input must not execute on the leader");
    const runtime = Updates.createTelegramUpdateRuntime<string>({
      getAllowedUserId: () => 7,
      getCurrentInstanceId: () => "leader",
      getMessageOwnership: ownership.getForwardOwnership,
      getTargetOwnership: (requested) => Bus.getTelegramFollowerTargetOwnership({
        target: requested, followers: registry.list(),
      }),
      recordMessageOwnership: ownership.recordRouted,
      foreignOwnedUpdateForwarder: forwarder,
      removePendingMediaGroupMessages: noLocalExecution,
      removeQueuedTelegramTurnsByMessageIds: noLocalExecution,
      applyQueuedTelegramTurnReactionByMessageId: noLocalExecution,
      pairTelegramUserIfNeeded: noLocalExecution,
      answerCallbackQuery: async () => {},
      answerGuestQuery: noLocalExecution,
      handleAuthorizedTelegramCallbackQuery: noLocalExecution,
      sendTextReply: noLocalExecution,
      handleAuthorizedTelegramMessage: noLocalExecution,
      handleAuthorizedTelegramEditedMessage: noLocalExecution,
      handleUnboundTelegramTopicMessage: noLocalExecution,
    });
    const worker = Updates.createTelegramUpdateWorkerRuntime<string>({
      journal: leaderJournal, hasAuthority: () => true, getNowMs: () => now,
      scheduleRetry: () => 0, cancelRetry: () => {},
      async executeUpdate(update, ctx) {
        await runtime.handleUpdate(Updates.bindTelegramUpdateAdmissionSource(
          update as Updates.TelegramUpdateFlow & Journal.TelegramJournaledUpdate, noLocalExecution,
        ), ctx);
        return { kind: "complete" };
      },
    });
    try {
      await receiver.start();
      leaderJournal.appendBatch([source]);
      worker.start("leader-ctx");
      for (let failureCount = 1; failureCount <= 2; failureCount++) {
        await worker.waitForDrain();
        const entry = leaderJournal.read().entries[0]!;
        assert.equal(entry.state, "retry-wait");
        assert.equal(entry.failure?.attemptCount, failureCount);
        assert.equal(entry.failure?.failureClass, "acknowledgement-rejected");
        assert.deepEqual(entry.update, source);
        assert.deepEqual(followerJournal.read().entries, []);
        assert.equal(ownership.store.get(7, 9)?.recipientBindingKey, registration.profileKey);
        if (failureCount === 1 && scenario.replace) {
          assert.match(entry.failure!.summary, /Stale Telegram bus follower registration generation/);
          registry.register({ ...registration, registrationGeneration: "g2", connectedAtMs: 2 });
        }
        now = entry.nextRetryAtMs!;
        worker.signal();
      }
      await worker.waitForDrain();
      assert.deepEqual(leaderJournal.read().entries, []);
      assert.equal(attempts.length, 3);
      assert.deepEqual(attempts.map((attempt) => attempt.ownerGeneration),
        scenario.replace ? ["g1", "g2", "g2"] : ["g1", "g1", "g1"]);
      assert.equal(attempts.every((attempt) =>
        JSON.stringify(attempt.protocolIdentity) === JSON.stringify(registration.protocol)), true);
      const admitted = followerJournal.read().entries;
      assert.deepEqual(admitted.map((entry) => entry.updateId), [source.update_id]);
      assert.deepEqual(admitted[0]!.update[scenario.carrier], {
        ...source[scenario.carrier],
        ...(scenario.carrier === "callback_query" ? { message: {
          ...message, pi_telegram_source_update_id: source.update_id,
        } } : {}),
        pi_telegram_source_update_id: source.update_id,
      });
      const kind = scenario.carrier === "message" ? "leader.forwardMessage"
        : scenario.carrier === "edited_message" ? "leader.forwardEditedMessage"
          : scenario.carrier === "callback_query" ? "leader.forwardCallback" : "leader.forwardReaction";
      const delivery = Bus.createTelegramBusFollowerDeliveryIdentity({
        kind, recipientBindingKey: registration.profileKey, sourceUpdateId: source.update_id,
      });
      assert.deepEqual(rejectedDeliveryIds, [delivery.deliveryId, delivery.deliveryId]);
      assert.equal(received.length, scenario.replace ? 2 : 3);
      for (const envelope of received) {
        assert.equal(envelope.kind, kind);
        assert.ok("delivery" in envelope);
        assert.deepEqual(envelope.delivery, delivery);
      }
    } finally {
      await worker.stop();
      await receiver.stop();
      await rm(dir, { recursive: true, force: true });
    }
  });
}

async function flushMicrotasks(iterations = 10): Promise<void> {
  for (let i = 0; i < iterations; i++) {
    await Promise.resolve();
  }
}

async function waitForEventLoopCondition(
  predicate: () => boolean,
  iterations = 1_000,
): Promise<void> {
  for (let i = 0; i < iterations; i++) {
    if (predicate()) return;
    await new Promise((resolve) => setImmediate(resolve));
  }
  throw new Error("Timed out waiting for event-loop condition");
}

async function waitForCondition(
  predicate: () => boolean,
  timeoutMs = 2000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  if (predicate()) return;
  throw new Error("Timed out waiting for condition");
}

async function waitForAsyncCondition(
  predicate: () => Promise<boolean>,
  timeoutMs = 2000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  if (await predicate()) return;
  throw new Error("Timed out waiting for async condition");
}

function parseJsonRequestBody(
  init: RequestInit | undefined,
): Record<string, unknown> | undefined {
  if (typeof init?.body !== "string") return undefined;
  return JSON.parse(init.body) as Record<string, unknown>;
}

function getRuntimeTelegramApiMethod(input: string | URL | Request): string {
  const url = typeof input === "string" ? input : input.toString();
  return url.split("/").at(-1) ?? "";
}

async function getRuntimeIntegrationDiagnostics(
  methods: Array<{ method: string; body?: Record<string, unknown> }>,
): Promise<string> {
  const agentDir = await ensureRuntimeAgentDir();
  const runtimeDir = join(agentDir, "tmp", "pi-telegram");
  const readOptional = async (path: string) => {
    try {
      return await readFile(path, "utf8");
    } catch (error) {
      return `<unavailable: ${error instanceof Error ? error.message : String(error)}>`;
    }
  };
  const [state, logs] = await Promise.all([
    readOptional(join(runtimeDir, "state.json")),
    readOptional(join(runtimeDir, "logs.jsonl")),
  ]);
  const ownersText = await readRuntimeTelegramLocks().then(value => JSON.stringify(value), error => String(error));
  let owner: Record<string, unknown> | string = ownersText;
  try {
    const parsed = JSON.parse(ownersText) as Record<string, Record<string, unknown>>;
    const current = parsed.default;
    owner = current
      ? {
          pid: current.pid,
          cwd: current.cwd,
          acquiredAtMs: current.acquiredAtMs,
          heartbeatAtMs: current.heartbeatAtMs,
          leaderEpoch: current.leaderEpoch,
        }
      : {};
  } catch {
    /* preserve unreadable owner evidence */
  }
  return JSON.stringify({ methods, owner, state, logs }, null, 2);
}

function getRuntimeTelegramApiText(
  body: Record<string, unknown> | undefined,
): string {
  const richMessage = body?.rich_message as
    | { html?: string; markdown?: string }
    | undefined;
  return String(body?.text ?? richMessage?.html ?? richMessage?.markdown ?? "");
}

function setRuntimeTestFetch(fetchImpl: typeof fetch): () => void {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = fetchImpl;
  return () => {
    globalThis.fetch = originalFetch;
  };
}

async function createRuntimeTelegramConfigFixture() {
  const agentDir = await ensureRuntimeAgentDir();
  const configPath = join(agentDir, "telegram.json");
  const previousConfig = await readFile(configPath, "utf8").catch(
    () => undefined,
  );
  const isolated = process.env.PI_CODING_AGENT_DIR === agentDir;
  return {
    write: async (config: Record<string, unknown>) => {
      await mkdir(agentDir, { recursive: true });
      const telegramTempDir = join(agentDir, "tmp", "pi-telegram");
      const tempEntries = await readdir(telegramTempDir).catch(() => []);
      await Promise.all(
        tempEntries
          .filter((entry) => entry.startsWith("inbox") || entry === "sessions")
          .map((entry) =>
            rm(join(telegramTempDir, entry), {
              recursive: true,
              force: true,
            }),
          ),
      );
      const assistant =
        typeof config.assistant === "object" && config.assistant !== null
          ? (config.assistant as Record<string, unknown>)
          : {};
      await writeFile(
        configPath,
        JSON.stringify(
          {
            ...config,
            assistant: {
              activity: "quiet",
              timeInjection: "hidden",
              ...assistant,
            },
          },
          null,
          "\t",
        ) + "\n",
        "utf8",
      );
    },
    restore: async () => {
      if (isolated) return;
      if (previousConfig === undefined) {
        await rm(configPath, { force: true });
        return;
      }
      await writeFile(configPath, previousConfig, "utf8");
    },
  };
}

/** Fixture-only replacement of every profile's consolidated transport section; sibling sections stay intact. */
async function writeRuntimeTelegramLocks(
  locks: Record<string, unknown>,
): Promise<void> {
  const agentDir = await ensureRuntimeAgentDir();
  const telegramRuntimeDir = join(agentDir, "tmp", "pi-telegram");
  await mkdir(telegramRuntimeDir, { recursive: true });
  const path = join(telegramRuntimeDir, "state.json");
  const state = Locks.readTelegramRuntimeState(path);
  const profiles: Record<string, Record<string, unknown>> = {};
  for (const [profile, sections] of Object.entries(state.profiles)) {
    const { transport: _transport, ...rest } = sections as Record<string, unknown>;
    if (Object.keys(rest).length) profiles[profile] = rest;
  }
  for (const [profile, transport] of Object.entries(locks)) profiles[profile] = { ...profiles[profile], transport };
  await writeFile(path, JSON.stringify({ version: 2, profiles }, null, "\t") + "\n", { encoding: "utf8", mode: 0o600 });
  await chmod(path, 0o600);
}

/** Fixture Workspace writer: a temporary exact transport owner publishes, then releases before the extension starts. */
async function createRuntimeSeedWorkspaceStore(): Promise<{ threads: Threads.TelegramTopicTargetStore; release: () => Promise<void> }> {
  const path = join(await ensureRuntimeAgentDir(), "tmp", "pi-telegram", "state.json");
  const transports = await readRuntimeTelegramLocks();
  const owner = Locks.createTelegramLockRuntime<{ cwd: string }>({ statePath: path, key: () => "default", pid: process.pid,
    instanceId: "fixture-workspace-seed", runtimeGeneration: 1, isProcessAlive: () => true });
  assert.equal(owner.acquire({ cwd: "/fixture-workspace-seed" }).ok, true);
  let released = false;
  const threads = Threads.createTelegramTopicTargetStore({ path, canPersist: () => !released && owner.owns(),
    consolidated: {
      captureAuthority() {
        const epoch = released ? undefined : owner.getOwnedLeaderEpoch();
        return epoch === undefined ? undefined : () => !released && owner.owns() && owner.getOwnedLeaderEpoch() === epoch;
      },
      publishIfOwned: owner.publishStateSectionIfOwned!,
    } });
  await threads.load();
  // Restore the exact prior transport sections so the seed owner leaves no previous-leader evidence.
  return { threads, async release() { if (!released) { released = true; owner.release(); await writeRuntimeTelegramLocks(transports); } } };
}

/** Read-only consolidated Workspace view for assertions; it can never publish. */
function createRuntimeWorkspaceReader(path: string): Threads.TelegramTopicTargetStore {
  return Threads.createTelegramTopicTargetStore({ path,
    consolidated: { captureAuthority: () => undefined, publishIfOwned: () => ({ committed: false }) } });
}

async function readRuntimeTelegramLocks(): Promise<Record<string, Record<string, unknown>>> {
  const state = Locks.readTelegramRuntimeState(join(await ensureRuntimeAgentDir(), "tmp", "pi-telegram", "state.json"));
  return Object.fromEntries(Object.entries(state.profiles).filter(([, sections]) => sections.transport !== undefined)
    .map(([profile, sections]) => [profile, sections.transport as Record<string, unknown>]));
}

async function stageRuntimeV02712Artifacts(): Promise<string | undefined> {
  const agentDir = await ensureRuntimeAgentDir();
  const telegramRuntimeDir = join(agentDir, "tmp", "pi-telegram");
  await mkdir(telegramRuntimeDir, { recursive: true });
  // Start from a clean consolidated state, as a fresh root after upgrading from `tmp/telegram`.
  await rm(join(telegramRuntimeDir, "state.json"), { force: true });
  const seed = await createRuntimeSeedWorkspaceStore();
  seed.threads.setBotState({ threadMode: "enabled" });
  await seed.threads.persist();
  await seed.release();
  if (process.platform === "win32") return undefined;
  const busPath = Bus.resolveTelegramBusSocketPath(
    Bus.getTelegramBusSocketPath(agentDir, process.platform, undefined, "consolidated"),
    process.platform,
  );
  await mkdir(dirname(busPath), { recursive: true, mode: 0o700 });
  await rm(busPath, { force: true });
  await symlink(".pt-v02712-missing.sock", busPath);
  return busPath;
}

function createRuntimeDeferredResponse() {
  let resolve: (value: Response) => void = () => {};
  const promise = new Promise<Response>((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
}

function createRuntimeTelegramApiResponse(result: unknown): Response {
  return { json: async () => ({ ok: true, result }) } as Response;
}

function createRuntimeTelegramApiErrorResponse(
  status: number,
  description: string,
): Response {
  return {
    ok: false,
    status,
    headers: new Headers({ "retry-after": "0" }),
    text: async () => JSON.stringify({ ok: false, description }),
  } as Response;
}

let runtimeExtensionContextSequence = 0;

function createRuntimeExtensionContext(
  overrides: Record<string, unknown> = {},
) {
  const sessionId = `integration-session-${++runtimeExtensionContextSequence}`;
  return {
    hasUI: true,
    cwd: process.cwd(),
    model: undefined,
    signal: undefined,
    sessionManager: { getSessionId: () => sessionId },
    ui: {
      theme: {
        fg: (_token: string, text: string) => text,
      },
      setStatus: () => {},
      notify: () => {},
    },
    isIdle: () => true,
    hasPendingMessages: () => false,
    abort: () => {},
    ...overrides,
  };
}

type RuntimeModelFixture = {
  provider: string;
  id: string;
  reasoning?: boolean;
};

function createRuntimeModel(
  provider: string,
  id: string,
  reasoning?: boolean,
): RuntimeModelFixture {
  return reasoning === undefined
    ? { provider, id }
    : { provider, id, reasoning };
}

type RuntimeModelContextOptions = {
  model?: RuntimeModelFixture;
  availableModels: RuntimeModelFixture[];
  isIdle?: () => boolean;
  abort?: () => void;
  setStatus?: (slot: string, text: string) => void;
};

function createRuntimeModelContext(options: RuntimeModelContextOptions) {
  return createRuntimeExtensionContext({
    cwd: process.cwd(),
    model: options.model,
    ui: {
      theme: {
        fg: (_token: string, text: string) => text,
      },
      setStatus: options.setStatus ?? (() => {}),
      notify: () => {},
    },
    sessionManager: {
      getEntries: () => [],
    },
    modelRegistry: {
      refresh: () => {},
      getAvailable: () => options.availableModels,
      isUsingOAuth: () => false,
    },
    getContextUsage: () => undefined,
    isIdle: options.isIdle ?? (() => true),
    abort: options.abort ?? (() => {}),
  });
}

type RuntimeHarnessTextBlock = { type: string; text?: string };
type RuntimeHarnessMessage = string | RuntimeHarnessTextBlock[];

function getRuntimeHarnessTextBlock(
  content: RuntimeHarnessMessage | undefined,
): RuntimeHarnessTextBlock {
  assert.equal(Array.isArray(content), true);
  if (!Array.isArray(content)) throw new Error("Expected text-block message");
  return content[0] ?? { type: "" };
}

function getRuntimeHarnessMessageText(content: RuntimeHarnessMessage): string {
  if (typeof content === "string") return content;
  return getRuntimeHarnessTextBlock(content).text ?? "";
}

function recordRuntimeDispatchEvent(
  events: string[],
  content: RuntimeHarnessMessage,
): void {
  events.push(`dispatch:${getRuntimeHarnessMessageText(content)}`);
}

type RuntimeHarnessHandler = (event: unknown, ctx: unknown) => Promise<unknown>;
type RuntimeHarnessCommand = {
  handler: (args: string, ctx: unknown) => Promise<void>;
};
type RuntimeHarnessTool = {
  name: string;
  execute: (toolCallId: string, params: Record<string, unknown>) => Promise<unknown>;
};
type RuntimePiHarnessOptions = {
  sendMessage?: (message: unknown, options?: unknown) => void;
  sendUserMessage?: (content: RuntimeHarnessMessage) => void;
  activeTools?: string[];
  getThinkingLevel?: () => string;
  setModel?: (model: { provider: string; id: string }) => Promise<boolean>;
  setThinkingLevel?: (level: string) => void;
  getCommands?: () => unknown[];
};

function createRuntimePiHarness(options: RuntimePiHarnessOptions = {}) {
  const handlers = new Map<string, RuntimeHarnessHandler>();
  const commands = new Map<string, RuntimeHarnessCommand>();
  const tools = new Map<string, RuntimeHarnessTool>();
  let activeTools = [...(options.activeTools ?? ["read", "foreign_tool"])];
  const pi = {
    on: (event: string, handler: RuntimeHarnessHandler) => {
      handlers.set(event, handler);
    },
    registerCommand: (name: string, definition: RuntimeHarnessCommand) => {
      commands.set(name, definition);
    },
    registerTool: (definition: RuntimeHarnessTool) => {
      tools.set(definition.name, definition);
      if (!activeTools.includes(definition.name)) activeTools.push(definition.name);
    },
    getActiveTools: () => [...activeTools],
    setActiveTools: (names: string[]) => {
      activeTools = [...names];
    },
    sendMessage: options.sendMessage ?? (() => {}),
    sendUserMessage: options.sendUserMessage ?? (() => {}),
    getCommands: options.getCommands ?? (() => []),
    getThinkingLevel: options.getThinkingLevel ?? (() => "medium"),
    ...(options.setModel ? { setModel: options.setModel } : {}),
    ...(options.setThinkingLevel
      ? { setThinkingLevel: options.setThinkingLevel }
      : {}),
  };
  return {
    handlers,
    commands,
    tools,
    pi: pi as never,
    getActiveTools: () => [...activeTools],
  };
}

function createIntegrationQueueTurn(
  replyToMessageId: number,
): Queue.PendingTelegramTurn {
  return {
    kind: "prompt",
    chatId: 42,
    target: { chatId: 42, threadId: 7 },
    replyToMessageId,
    queueOrder: replyToMessageId,
    queueLane: "default",
    laneOrder: replyToMessageId,
    statusSummary: `turn ${replyToMessageId}`,
    sourceMessageIds: [replyToMessageId],
    queuedAttachments: [],
    content: [{ type: "text", text: `turn ${replyToMessageId}` }],
    historyText: `turn ${replyToMessageId}`,
  };
}

test("Busy Next continues into the selected prompt when its abort notice fails", async () => {
  const events: string[] = [];
  const activeTurnStore = Queue.createTelegramActiveTurnStore();
  const interrupted = createIntegrationQueueTurn(10);
  const next = createIntegrationQueueTurn(11);
  activeTurnStore.set(interrupted);
  let queuedItems: Queue.TelegramQueueItem<string>[] = [next];
  const dispatch = Queue.createTelegramQueueDispatchController<string>({
    getQueuedItems: () => queuedItems,
    setQueuedItems: (items) => {
      queuedItems = items;
    },
    canDispatch: () => true,
    updateStatus: () => {},
    sendTextReply: async (_chatId, replyToMessageId, text) => {
      events.push(`next-notice:${replyToMessageId}:${text}`);
      return 100;
    },
    onPromptDispatchStart: () => events.push("next-start"),
    sendUserMessage: () => events.push("next-send"),
    onPromptDispatchFailure: () => events.push("next-failure"),
  });
  await Commands.handleTelegramNextCommand({
    hasAbortHandler: () => true,
    isIdle: () => false,
    hasQueuedItems: () => queuedItems.length > 0,
    clearPendingModelSwitch: () => {},
    abortCurrentTurn: () => events.push("abort"),
    dispatchNextQueuedTurn: () => dispatch.dispatchNext("ctx"),
    requestNextDispatchAnnouncement: dispatch.requestNextDispatchAnnouncement,
    markActiveTurnNextAbortAnnouncement:
      activeTurnStore.markNextAbortAnnouncement,
    clearFoldForDispatch: () => {},
    updateStatus: () => {},
    sendTextReply: async () => {},
  });
  const settledTurn = activeTurnStore.get();
  await Queue.handleTelegramAgentEndRuntime({
    turn: settledTurn,
    assistant: { stopReason: "aborted" },
    foldQueuedPromptsIntoHistory: false,
    resetRuntimeState: activeTurnStore.clear,
    updateStatus: () => {},
    dispatchNextQueuedTelegramTurn: () => dispatch.dispatchNext("ctx"),
    clearPreview: async () => {},
    setPreviewPendingText: () => {},
    finalizeMarkdownPreview: async () => false,
    sendMarkdownReply: async () => {},
    sendTextReply: async () => {
      events.push("abort-notice");
      throw new Error("Telegram unavailable");
    },
    sendQueuedAttachments: async () => {},
    recordRuntimeEvent: (_category, _error, details) => {
      events.push(`contained:${details?.phase}`);
    },
  });
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.deepEqual(events, [
    "abort",
    "abort-notice",
    "contained:next-abort-announcement",
    "next-notice:11:<b>⏩ Dispatching next queued turn.</b>",
    "next-start",
    "next-send",
  ]);
});

for (const supersedingCommand of ["abort", "stop"] as const) {
  test(`Busy Next drops transition notices when superseded by /${supersedingCommand}`, async () => {
    const events: string[] = [];
    const activeTurnStore = Queue.createTelegramActiveTurnStore();
    activeTurnStore.set(createIntegrationQueueTurn(20));
    let queuedItems: Queue.TelegramQueueItem<string>[] = [
      createIntegrationQueueTurn(21),
    ];
    let foldQueuedPromptsIntoHistory = false;
    const dispatch = Queue.createTelegramQueueDispatchController<string>({
      getQueuedItems: () => queuedItems,
      setQueuedItems: (items) => {
        queuedItems = items;
      },
      canDispatch: () => true,
      updateStatus: () => {},
      sendTextReply: async (_chatId, _replyToMessageId, text) => {
        events.push(`queue-notice:${text}`);
        return 100;
      },
      onPromptDispatchStart: () => events.push("next-start"),
      sendUserMessage: () => events.push("next-send"),
      onPromptDispatchFailure: () => events.push("next-failure"),
    });
    const cancelNextTransitionAnnouncements = () => {
      activeTurnStore.clearNextAbortAnnouncement();
      dispatch.cancelNextDispatchAnnouncement();
    };
    await Commands.handleTelegramNextCommand({
      hasAbortHandler: () => true,
      isIdle: () => false,
      hasQueuedItems: () => queuedItems.length > 0,
      clearPendingModelSwitch: () => {},
      abortCurrentTurn: () => events.push("next-abort"),
      dispatchNextQueuedTurn: () => dispatch.dispatchNext("ctx"),
      requestNextDispatchAnnouncement: dispatch.requestNextDispatchAnnouncement,
      markActiveTurnNextAbortAnnouncement:
        activeTurnStore.markNextAbortAnnouncement,
      clearFoldForDispatch: () => {
        foldQueuedPromptsIntoHistory = false;
      },
      updateStatus: () => {},
      sendTextReply: async () => {},
    });
    if (supersedingCommand === "abort") {
      await Commands.handleTelegramAbortCommand({
        hasAbortHandler: () => true,
        hasActiveTelegramTurn: activeTurnStore.has,
        clearPendingModelSwitch: () => {},
        cancelNextTransitionAnnouncements,
        abortCurrentTurn: () => events.push("superseding-abort"),
        setFoldQueuedPromptsIntoHistory: (fold) => {
          foldQueuedPromptsIntoHistory = fold;
        },
        updateStatus: () => {},
        sendTextReply: async (text) => {
          events.push(`command-notice:${text}`);
        },
      });
    } else {
      await Commands.handleTelegramStopCommand({
        hasAbortHandler: () => true,
        clearPendingModelSwitch: () => {},
        cancelNextTransitionAnnouncements,
        clearQueuedTelegramItems: () => {
          const count = queuedItems.length;
          queuedItems = [];
          return count;
        },
        setFoldQueuedPromptsIntoHistory: (fold) => {
          foldQueuedPromptsIntoHistory = fold;
        },
        abortCurrentTurn: () => events.push("superseding-abort"),
        updateStatus: () => {},
        sendTextReply: async (text) => {
          events.push(`command-notice:${text}`);
        },
      });
    }
    const settledTurn = activeTurnStore.get();
    await Queue.handleTelegramAgentEndRuntime({
      turn: settledTurn,
      assistant: { stopReason: "aborted" },
      foldQueuedPromptsIntoHistory,
      resetRuntimeState: activeTurnStore.clear,
      updateStatus: () => {},
      dispatchNextQueuedTelegramTurn: () => dispatch.dispatchNext("ctx"),
      clearPreview: async () => {},
      setPreviewPendingText: () => {},
      finalizeMarkdownPreview: async () => false,
      sendMarkdownReply: async () => {},
      sendTextReply: async () => {
        events.push("unexpected-lifecycle-notice");
      },
      sendQueuedAttachments: async () => {},
    });
    if (queuedItems.length === 0) {
      queuedItems = [createIntegrationQueueTurn(22)];
    }
    dispatch.dispatchNext("ctx");
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(
      events.some((event) => event.includes("Dispatching next queued turn")),
      false,
    );
    assert.equal(events.includes("unexpected-lifecycle-notice"), false);
    assert.equal(events.filter((event) => event.startsWith("command-notice:")).length, 1);
    assert.equal(events.includes("next-start"), true);
    assert.equal(events.includes("next-send"), true);
  });
}

test("v0.27.12 artifacts and graceful tab cleanup preserve same-directory auto-connect ownership", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const { handlers, commands, pi } = createRuntimePiHarness();
  const methods: Array<{ method: string; body?: Record<string, unknown> }> = [];
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    methods.push({ method, ...(body ? { body } : {}) });
    if (method === "deleteWebhook" || method === "setMyCommands") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getMe") {
      return createRuntimeTelegramApiResponse({
        id: 123,
        username: "test_bot",
        has_topics_enabled: true,
      });
    }
    if (method === "getUpdates") {
      return await new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          reject(new DOMException("stop", "AbortError"));
        });
      });
    }
    if (method === "createForumTopic") {
      return createRuntimeTelegramApiResponse({
        message_thread_id: 42,
        name: "Atlas",
      });
    }
    if (
      method === "sendMessage" ||
      method === "closeForumTopic" ||
      method === "deleteForumTopic"
    ) {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      botId: 123,
      botUsername: "test_bot",
      allowedUserId: 77,
      lastUpdateId: 0,
      threads: { automaticCleanup: true },
    });
    await writeRuntimeTelegramLocks({});
    const legacyBusPath = await stageRuntimeV02712Artifacts();
    const runtimeAgentPath = await ensureRuntimeAgentDir();
    const journalPath = join(runtimeAgentPath, "tmp", "pi-telegram", "inbox.json");
    const journal = Journal.createTelegramUpdateJournalStore({
      path: journalPath,
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({
        botToken: "123:abc",
        botId: 123,
      }),
    });
    journal.appendBatch([
      {
        update_id: 1,
        message: {
          message_id: 1,
          date: 1,
          chat: { id: 77, type: "private" },
          from: { id: 77, is_bot: false, first_name: "Owner" },
          text: "recover admitted authority",
        },
      },
    ]);
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext({ cwd: "/repo/graceful-leader" });
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForCondition(
      () => methods.some((entry) => entry.method === "createForumTopic"),
      10_000,
    );
    assert.deepEqual(
      journal.read().entries.map((entry) => entry.updateId),
      [1],
      "upgrade startup must preserve pre-existing journal authority",
    );
    if (legacyBusPath) {
      try {
        await waitForAsyncCondition(async () => {
          try {
            return (await readlink(legacyBusPath)) !== ".pt-v02712-missing.sock";
          } catch {
            return true;
          }
        }, 20_000);
      } catch (error) {
        throw new Error(
          `${error instanceof Error ? error.message : String(error)}\n${await getRuntimeIntegrationDiagnostics(methods)}`,
        );
      }
    }

    await handlers.get("session_shutdown")?.(
      { type: "session_shutdown", reason: "quit" },
      ctx,
    );

    const agentDir = await ensureRuntimeAgentDir();
    void agentDir;
    const ownersAfterQuit = await readRuntimeTelegramLocks() as Record<string, { pid?: number; cwd?: string }>;
    assert.equal(ownersAfterQuit.default?.pid, process.pid);
    assert.equal(ownersAfterQuit.default?.cwd, "/repo/graceful-leader");

    const restartedCtx = createRuntimeExtensionContext({
      cwd: "/repo/graceful-leader",
    });
    await handlers.get("session_start")?.({}, restartedCtx);
    try {
      await waitForCondition(
        () =>
          methods.filter((entry) => entry.method === "createForumTopic").length >=
          2,
        40_000,
      );
    } catch (error) {
      throw new Error(
        `${error instanceof Error ? error.message : String(error)}\n${await getRuntimeIntegrationDiagnostics(methods)}`,
      );
    }
    await handlers.get("session_shutdown")?.(
      { type: "session_shutdown", reason: "quit" },
      restartedCtx,
    );

    const deleteCall = methods.find(
      (entry) => entry.method === "deleteForumTopic",
    );
    assert.deepEqual(deleteCall?.body, {
      chat_id: 77,
      message_thread_id: 42,
    }, deleteCall ? undefined : await getRuntimeIntegrationDiagnostics(methods));
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
}, 60_000);

for (const siblingTiming of ["none", "startup", "live", "retry"] as const) test(`Production startup spends previous-process Restore originals without delivery even after their Thread becomes bound (sibling=${siblingTiming})`, async () => {
  const withSibling = siblingTiming !== "none", siblingUpdateId = siblingTiming === "live" ? 103 : 99;
  const config = await createRuntimeTelegramConfigFixture();
  const agentDir = await ensureRuntimeAgentDir();
  const dir = join(agentDir, "tmp", "pi-telegram");
  const dispatched: RuntimeHarnessMessage[] = [];
  const { handlers, commands, pi } = createRuntimePiHarness({ sendUserMessage: content => dispatched.push(content) });
  const ctx = createRuntimeExtensionContext({ cwd: "/repo/restore-held",
    sessionManager: { getSessionId: () => "restore-held-session", getEntries: () => [] },
    modelRegistry: { refresh() {}, getAvailable: () => [], isUsingOAuth: () => false },
    getContextUsage: () => undefined });
  const methods: string[] = [];
  const messages: unknown[] = [];
  let polls = 0, releaseLiveSibling: (() => void) | undefined;
  const liveSiblingGate = new Promise<void>(resolve => { releaseLiveSibling = resolve; });
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    methods.push(method);
    if (method === "getMe") return createRuntimeTelegramApiResponse({ id: 123, has_topics_enabled: true });
    if (method === "sendMessage") {
      messages.push(JSON.parse(String(init?.body)));
      return createRuntimeTelegramApiResponse({ message_id: 500 + messages.length });
    }
    if (method === "getUpdates") {
      if (++polls === 1) return createRuntimeTelegramApiResponse([{ update_id: 102, message: { message_id: 102, date: 1,
        chat: { id: 77, type: "private" }, from: { id: 77, is_bot: false, first_name: "Owner" },
        message_thread_id: 42, text: "/start" } }]);
      if (polls === 2 && siblingTiming === "live") {
        // Wait for a normal command to finish: this source cannot belong to the worker's startup census.
        await liveSiblingGate;
        return createRuntimeTelegramApiResponse([{ update_id: siblingUpdateId, message: { message_id: siblingUpdateId, date: 1,
          chat: { id: 77, type: "private" }, from: { id: 77, is_bot: false, first_name: "Owner" },
          message_thread_id: 42, text: "/start" } },
          { update_id: 104, message: { message_id: 104, date: 1, chat: { id: 77, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Owner" }, message_thread_id: 42, text: "/start" } }]);
      }
      return await new Promise<Response>((_resolve, reject) => {
        if (init?.signal?.aborted) reject(new DOMException("stop", "AbortError"));
        else init?.signal?.addEventListener("abort", () => reject(new DOMException("stop", "AbortError")), { once: true });
      });
    }
    if (["deleteWebhook", "setMyCommands", "sendMessage", "editForumTopic", "sendChatAction"].includes(method)) {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await config.write({ botToken: "123:abc", botId: 123, allowedUserId: 77, threads: { automaticCleanup: false } });
    await writeRuntimeTelegramLocks({});
    await stageRuntimeV02712Artifacts();
    const seed = await createRuntimeSeedWorkspaceStore();
    const threads = seed.threads;
    const binding = { ...Threads.createTelegramWorkspaceBindingIdentity(ctx.cwd, 0, ctx.sessionManager.getSessionId())!,
      slot: "A", threadName: "Atlas", target: { chatId: 77, threadId: 10 }, updatedAtMs: 1 };
    const owner = { profileKey: `cwd:${ctx.cwd}`, slot: "A", threadName: "Atlas", instanceId: "former",
      owner: { kind: "leader" as const, cwd: ctx.cwd, instanceId: "former" }, target: binding.target,
      status: "active" as const, createdAtMs: 1, updatedAtMs: 1 };
    threads.upsert(owner);
    threads.upsertWorkspaceBinding(binding);
    await threads.persist();
    const journalOptions = { path: join(dir, "inbox.json"), botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:abc", botId: 123 }) };
    const journal = Journal.createTelegramUpdateJournalStore(journalOptions);
    journal.appendBatch([...(siblingTiming === "startup" || siblingTiming === "retry" ? [{ update_id: siblingUpdateId, message: { message_id: siblingUpdateId, date: 1,
      chat: { id: 77, type: "private" }, from: { id: 77, is_bot: false, first_name: "Owner" },
      message_thread_id: 42, text: siblingTiming === "retry" ? "/start" : "held temporary-tab sibling" } }] : []), { update_id: 100, message: { message_id: 100, date: 1,
      chat: { id: 77, type: "private" }, from: { id: 77, is_bot: false, first_name: "Owner" },
      message_thread_id: 42, text: "held Restore original" } }], 100);
    if (siblingTiming === "retry") journal.markExecutionFailure({ updateId: siblingUpdateId, expectedAttemptCount: 0,
      failedAtMs: 1, nextRetryAtMs: 2, failureClass: "fixture", summary: "interrupted sibling execution", disposition: "retry-wait" });
    const resolve = Threads.createTelegramWorkspaceRestoreResolver({ agentDir, getProfileName: () => undefined,
      getBotToken: () => "123:abc", threadStore: threads });
    const store = resolve()!;
    await threads.load();
    const request = { operationId: "interrupted-restore", binding: threads.listWorkspaceBindings()[0]!,
      owner: threads.list()[0]!, target: { chatId: 77, threadId: 42 },
      source: { journalBindingKey: Journal.createTelegramUpdateJournalBindingKey(journalOptions), updateIds: [100] } };
    if (withSibling) {
      const temporaryAuthority = { executor: { instanceId: "former", leaderEpoch: "previous" }, operatorUserId: 77, isCurrent: () => true };
      const entry = store.acknowledgeTemporaryThread(store.reserveTemporaryThread({ journalBindingKey: request.source.journalBindingKey, updateId: 100 }, "a".repeat(32), temporaryAuthority)!.entry,
        { chatId: 77, threadId: 42 }, temporaryAuthority)!;
      assert.ok(store.recordTemporaryThreadInput(entry, { journalBindingKey: request.source.journalBindingKey, updateIds: [siblingUpdateId] }, temporaryAuthority));
    }
    const relocated = await store.commit(request, { executor: { instanceId: "former", leaderEpoch: "previous" },
      operatorUserId: 77, isCurrent: () => true });
    assert.equal(relocated?.phase, "relocated", JSON.stringify({ request, intents: store.list(),
      bindings: threads.listWorkspaceBindings(), records: threads.list() }));
    await seed.release();
    (await getRuntimeTelegramExtension())(pi);
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    try {
      await waitForCondition(() => messages.some(message => JSON.stringify(message).includes("menu:model")), 10_000);
    } catch (error) {
      throw new Error(`${String(error)}\n${JSON.stringify({ methods, messages, dispatched })}\n${await readFile(join(dir, "logs.jsonl"), "utf8")}`);
    }
    if (siblingTiming === "live") {
      releaseLiveSibling!();
      await waitForCondition(() => messages.filter(message => JSON.stringify(message).includes("menu:model")).length >= 2, 10_000);
    }
    assert.deepEqual(dispatched, []);
    assert.doesNotMatch(JSON.stringify(messages), /Historical inputs|reroutecancel:history:/);
    assert.equal(journal.read().entries.some(entry => entry.updateId === 100), false, "the new-world restart spends the previous Restore original");
    assert.equal(journal.inspectAbandonedPending(100), undefined, "spending writes no private copy or tombstone");
    if (withSibling) {
      // A startup pending sibling is spent and retry-wait keeps its hold. A live arrival races forgetting: held while the
      // previous temporary entry exists, then an ordinary command of the now-bound tab. Never a private cancellation.
      const siblingState = journal.read().entries.find(entry => entry.updateId === siblingUpdateId)?.state;
      if (siblingTiming === "live") assert.ok(siblingState === undefined || siblingState === "pending", String(siblingState));
      else assert.equal(siblingState, siblingTiming === "startup" ? undefined : "retry-wait");
      assert.equal(journal.inspectAbandonedPending(siblingUpdateId), undefined, "neither spending nor a hold is private cancellation");
    }
    // The new world forgets the previous Restore and temporary entry; the restored tab stays bound and is never deleted.
    await waitForCondition(() => store.list().length === 0 && store.listTemporaryThreads().length === 0, 10_000);
    if (siblingTiming === "live") assert.ok(messages.filter(message => JSON.stringify(message).includes("menu:model")).length >= 2);
    await threads.load();
    assert.equal(threads.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.target.threadId, 42);
    assert.equal(threads.listWorkspaceBindings().find(value => value.bindingKey === binding.bindingKey)?.slot, "A");
    assert.equal(methods.includes("createForumTopic"), false);
    assert.equal(methods.includes("deleteForumTopic"), false);
  } finally {
    releaseLiveSibling?.();
    await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, ctx);
    await rm(join(dir, "state.json"), { force: true });
    restoreFetch();
    await config.restore();
  }
}, 20_000);

test("Graceful preserved leader quit publishes inactivity and restores its intact Workspace", async () => {
  const config = await createRuntimeTelegramConfigFixture();
  const { handlers, commands, pi } = createRuntimePiHarness();
  const methods: string[] = [];
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    methods.push(method);
    if (method === "getMe") return createRuntimeTelegramApiResponse({ id: 123, has_topics_enabled: true });
    if (method === "createForumTopic") return createRuntimeTelegramApiResponse({ message_thread_id: 42 });
    if (method === "getUpdates") return await new Promise<Response>((_resolve, reject) => {
      if (init?.signal?.aborted) reject(new DOMException("stop", "AbortError"));
      else init?.signal?.addEventListener("abort", () => reject(new DOMException("stop", "AbortError")), { once: true });
    });
    if (["deleteWebhook", "setMyCommands", "sendMessage", "editForumTopic"].includes(method)) {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  const ctx = createRuntimeExtensionContext({ cwd: "/repo/preserved-leader" });
  try {
    await config.write({ botToken: "123:abc", botId: 123, allowedUserId: 77,
      threads: { automaticCleanup: false } });
    await writeRuntimeTelegramLocks({});
    await stageRuntimeV02712Artifacts();
    (await getRuntimeTelegramExtension())(pi);
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    const dir = join(await ensureRuntimeAgentDir(), "tmp", "pi-telegram");
    await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "quit" }, ctx);
    const saved = createRuntimeWorkspaceReader(join(dir, "state.json"));
    await saved.load();
    assert.equal(saved.list().length, 0);
    const binding = saved.listWorkspaceBindings()[0]!;
    assert.ok(binding);
    assert.equal(binding.target.threadId, 42);
    assert.equal(typeof binding.inactiveSinceMs, "number");
    assert.equal(saved.listSyncObservations().some((entry) => entry.syncStatus === "deleted"), false);
    const owners = await readRuntimeTelegramLocks();
    assert.equal(owners.default?.pid, process.pid);
    assert.equal(owners.default?.cwd, ctx.cwd);
    assert.equal(methods.includes("deleteForumTopic"), false);
    assert.equal(methods.includes("closeForumTopic"), false);
    await handlers.get("session_start")?.({}, ctx);
    await waitForAsyncCondition(async () => {
      await saved.refresh!();
      return saved.list().length === 1 && saved.listWorkspaceBindings()[0]?.inactiveSinceMs === undefined;
    }, 10_000);
    assert.equal(saved.listWorkspaceBindings()[0]?.target.threadId, 42);
    assert.equal(saved.listWorkspaceBindings()[0]?.slot, binding.slot);
    assert.equal(methods.filter((method) => method === "createForumTopic").length, 1);
  } finally {
    await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "quit" }, ctx);
    restoreFetch();
    await config.restore();
  }
}, 20_000);

test("Native connect failure emits one compact notice and retains redacted debug evidence", async () => {
  const config = await createRuntimeTelegramConfigFixture();
  const { handlers, commands, pi } = createRuntimePiHarness();
  const notices: string[] = [];
  const ctx = createRuntimeExtensionContext({ cwd: "/repo/connect-error" });
  ctx.ui.notify = (text?: string) => { notices.push(text ?? ""); };
  const restoreFetch = setRuntimeTestFetch(async (input) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "getMe") return createRuntimeTelegramApiResponse({ id: 123, has_topics_enabled: true });
    if (method === "createForumTopic") return createRuntimeTelegramApiErrorResponse(401, "Unauthorized token 123:abc");
    if (method === "deleteWebhook") return createRuntimeTelegramApiResponse(true);
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await config.write({ botToken: "123:abc", botId: 123, allowedUserId: 77 });
    await writeRuntimeTelegramLocks({});
    await stageRuntimeV02712Artifacts();
    (await getRuntimeTelegramExtension())(pi);
    await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);
    await commands.get("telegram-connect")!.handler("", ctx);
    assert.deepEqual(notices, ["Telegram token rejected. Run /telegram-setup."]);
    await commands.get("telegram-status")!.handler("--debug", ctx);
    assert.match(notices.at(-1)!, /Unauthorized token/);
    assert.match(notices.at(-1)!, /redacted-token/);
    assert.ok(!notices.at(-1)!.includes("123:abc"));
  } finally {
    await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "quit" }, ctx);
    restoreFetch();
    await config.restore();
  }
}, 20_000);

test("Bare connect followed immediately by resume starts the destination through the native runtime", async () => {
  for (const remembered of [false, true]) {
    const config = await createRuntimeTelegramConfigFixture();
    const source = createRuntimePiHarness();
    const destination = createRuntimePiHarness();
    const methods: string[] = [];
    const notices: string[] = [];
    const cwd = "/repo/connect-resume";
    const sessionFile = "/sessions/destination.jsonl";
    const ctx = createRuntimeExtensionContext({ cwd });
    const next = createRuntimeExtensionContext({ cwd,
      sessionManager: { getSessionId: () => "destination", getSessionFile: () => sessionFile } });
    let sourceClosed = false;
    const sourceManager = ctx.sessionManager;
    Object.defineProperty(ctx, "sessionManager", { get() {
      if (sourceClosed) throw new Error("This extension ctx is stale after session replacement");
      return sourceManager;
    } });
    ctx.ui.notify = (text?: string) => { notices.push(text ?? ""); };
    next.ui.notify = (text?: string) => { notices.push(text ?? ""); };
    const restoreFetch = setRuntimeTestFetch(async (input, init) => {
      const method = getRuntimeTelegramApiMethod(input);
      methods.push(method);
      if (method === "getMe") return createRuntimeTelegramApiResponse({ id: 123, has_topics_enabled: true });
      if (method === "createForumTopic") return createRuntimeTelegramApiResponse({ message_thread_id: 43 });
      if (method === "getUpdates") return await new Promise<Response>((_resolve, reject) => {
        if (init?.signal?.aborted) reject(new DOMException("stop", "AbortError"));
        else init?.signal?.addEventListener("abort", () => reject(new DOMException("stop", "AbortError")), { once: true });
      });
      if (["deleteWebhook", "setMyCommands", "sendMessage", "editForumTopic"].includes(method)) {
        return createRuntimeTelegramApiResponse(true);
      }
      throw new Error(`Unexpected Telegram API method: ${method}`);
    });
    try {
      await config.write({ botToken: "123:abc", botId: 123, allowedUserId: 77,
        threads: { automaticCleanup: false } });
      await writeRuntimeTelegramLocks({});
      await stageRuntimeV02712Artifacts();
      const path = join(await ensureRuntimeAgentDir(), "tmp", "pi-telegram", "state.json");
      const seed = await createRuntimeSeedWorkspaceStore();
      const store = seed.threads;
      store.upsertWorkspaceBinding({ ...Threads.createTelegramWorkspaceBindingIdentity(cwd, 0, sourceManager.getSessionId())!,
        slot: "A", threadName: "Atlas", target: { chatId: 77, threadId: 41 }, updatedAtMs: 1 });
      if (remembered) store.upsertWorkspaceBinding({ ...Threads.createTelegramWorkspaceBindingIdentity(cwd, 0, "destination")!,
        slot: "B", threadName: "Birch", target: { chatId: 77, threadId: 42 }, updatedAtMs: 1 });
      await store.persist();
      await seed.release();
      void path;
      const extension = await getRuntimeTelegramExtension();
      extension(source.pi);
      await source.handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);
      const connect = source.commands.get("telegram-connect")!.handler("", ctx);
      await source.handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "resume", targetSessionFile: sessionFile }, ctx);
      sourceClosed = true;
      extension(destination.pi);
      await destination.handlers.get("session_start")?.({ type: "session_start", reason: "resume" }, next);
      await connect;
      try {
        await waitForAsyncCondition(async () => {
          // Binding publication precedes polling; avoid competing Windows file reads during startup.
          if (!methods.includes("getUpdates")) return false;
          await store.refresh!();
          return store.getWorkspaceBinding(cwd, "a", "destination")?.target.threadId === (remembered ? 42 : 43) &&
            store.list().some((entry) => entry.target.threadId === (remembered ? 42 : 43) && entry.status === "active");
        }, 10_000);
      } catch (error) {
        throw new Error(
          `Resume startup (remembered=${remembered}): ${String(error)}; notices=${JSON.stringify(notices)}\n` +
          await getRuntimeIntegrationDiagnostics(methods.map((method) => ({ method }))),
        );
      }
      const binding = store.getWorkspaceBinding(cwd, "a", "destination")!;
      assert.equal(binding.target.threadId, remembered ? 42 : 43);
      assert.equal(binding.slot, "B");
      assert.equal(store.getWorkspaceBinding(cwd, "a", sourceManager.getSessionId())?.target.threadId, 41);
      assert.equal(methods.filter((method) => method === "createForumTopic").length, remembered ? 0 : 1);
      assert.equal(notices.some((text) => /stale|failed/i.test(text)), false, notices.join("\n"));
    } finally {
      await destination.handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "quit" }, next);
      if (!sourceClosed) await source.handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "quit" }, ctx);
      restoreFetch();
      await config.restore();
    }
  }
}, 30_000);

test("Graceful follower disconnect persists intent and deletes through its live leader", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-follower-cleanup-integration-"));
  const socketPath = join(dir, "bus.sock");
  const store = Threads.createTelegramTopicTargetStore({
    path: join(dir, "state.json"),
    getNowMs: () => 2000,
  });
  const registry = Bus.createTelegramBusFollowerRegistry();
  const calls: Array<{ method: string; body: Record<string, unknown> }> = [];
  let syncState = {};
  const disconnectFollower =
    BusLeader.createTelegramBusFollowerDisconnectHandler({
      topicTargetStore: store,
      async callApi<TResponse>(method: string, body: Record<string, unknown>) {
        calls.push({ method, body });
        return { ok: true } as TResponse;
      },
      getCurrentLeaderEpoch: () => 7,
      getSyncState: () => syncState,
      setSyncState: (state) => {
        syncState = state;
      },
      getNowMs: () => 2000,
      recordRuntimeEvent: () => undefined,
    });
  const server = Bus.createTelegramBusLocalServer({
    socketPath,
    handleEnvelope: BusLeader.createTelegramBusLeaderEnvelopeHandler({
      followerRegistry: registry,
      authSecret: "leader-secret",
      protocolIdentity: Bus.createTelegramBusProtocolIdentity({
        runtimeBuild: "test",
      }),
      getNowMs: () => 2000,
      async provisionFollowerTarget(registration) {
        store.upsert({
          profileKey: registration.profileKey ?? "manual:follower-a",
          owner: {
            kind: "manual-follower",
            instanceId: registration.instanceId,
          },
          target: { chatId: 77, threadId: 43 },
          status: "active",
          createdAtMs: 1900,
          updatedAtMs: 1900,
          instanceId: registration.instanceId,
          slot: "B",
          threadName: "Beacon",
        });
        await store.persist();
        return {
          chatId: 77,
          threadId: 43,
          slot: "B",
          threadName: "Beacon",
        };
      },
      onFollowerDisconnected: disconnectFollower,
      getCurrentLeaderEpoch: () => 7,
    }),
  });
  let requestSequence = 0;
  const follower = BusFollower.createTelegramBusFollowerRegistrationRuntime({
    instanceId: "follower-a",
    createRequestId: () => `follower-a:${++requestSequence}`,
    protocolIdentity: Bus.createTelegramBusProtocolIdentity({
      runtimeBuild: "test",
    }),
    getLeaderAuthSecret: (leader) => leader.busSecret,
    getSessionId: () => "session-a",
    getNowMs: () => 2000,
    registrationTimeoutMs: 5_000,
  });
  try {
    await server.start();
    assert.equal(
      await follower.registerWithLeader(
        { cwd: "/repo/follower-a" },
        { busSocketPath: socketPath, busSecret: "leader-secret" },
      ),
      true,
    );
    assert.equal(await follower.disconnectFromLeader?.(), true);

    assert.deepEqual(calls, [
      {
        method: "closeForumTopic",
        body: { chat_id: 77, message_thread_id: 43 },
      },
      {
        method: "deleteForumTopic",
        body: { chat_id: 77, message_thread_id: 43 },
      },
    ]);
    assert.equal(registry.get("follower-a"), undefined);
    assert.equal(store.getActiveByInstanceId("follower-a"), undefined);
    assert.deepEqual(store.listPendingCleanups(), []);
  } finally {
    follower.stop();
    await server.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("Public activity delivery reaches the classic instance without blocking agent start", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const { handlers, commands, pi } = createRuntimePiHarness();
  const activitySend = createRuntimeDeferredResponse();
  const sentBodies: Array<Record<string, unknown>> = [];
  let activityHandledCount = 0;
  let unregisterActivity: (() => void) | undefined;
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook" || method === "setMyCommands") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      return await new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          reject(new DOMException("stop", "AbortError"));
        });
      });
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "sendMessage") {
      sentBodies.push(body ?? {});
      if (body?.text === "Activity from local") return activitySend.promise;
      return createRuntimeTelegramApiResponse({ message_id: 90 });
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      botId: 123,
      botUsername: "test_bot",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    await writeRuntimeTelegramLocks({});
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext();
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    unregisterActivity = registerTelegramActivityHandler({
      id: "integration-classic-activity",
      handle: async (event, activityCtx) => {
        if (event.type !== "agent-start") return;
        await activityCtx.send({ text: "Activity from local" });
        activityHandledCount += 1;
      },
    });

    await handlers.get("input")?.({ source: "interactive" }, ctx);
    await handlers.get("agent_start")?.({}, ctx);
    await waitForEventLoopCondition(() =>
      sentBodies.some((body) => body.text === "Activity from local"),
    );
    assert.equal(
      activityHandledCount,
      0,
      "agent_start must not await extension-owned activity delivery",
    );
    const activityBody = sentBodies.find(
      (body) => body.text === "Activity from local",
    );
    assert.equal(activityBody?.chat_id, 77);
    assert.equal(activityBody?.message_thread_id, undefined);

    activitySend.resolve(
      createRuntimeTelegramApiResponse({ message_id: 91 }),
    );
    await waitForEventLoopCondition(() => activityHandledCount === 1);
    await handlers.get("agent_settled")?.({}, ctx);
    await handlers.get("session_shutdown")?.({}, ctx);

    await handlers.get("session_start")?.({}, ctx);
    await handlers.get("input")?.({ source: "interactive" }, ctx);
    await handlers.get("agent_start")?.({}, ctx);
    await waitForEventLoopCondition(() => activityHandledCount === 2);
    await handlers.get("agent_settled")?.({}, ctx);
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    unregisterActivity?.();
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Verbose activity reaches classic transport before the final assistant answer", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const { handlers, commands, pi } = createRuntimePiHarness();
  const calls: Array<{
    method: string;
    body: Record<string, unknown>;
  }> = [];
  let nextMessageId = 100;
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init) ?? {};
    if (method === "deleteWebhook" || method === "setMyCommands") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      return await new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          reject(new DOMException("stop", "AbortError"));
        });
      });
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    calls.push({ method, body });
    if (method === "sendRichMessageDraft" || method === "editMessageText") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      return createRuntimeTelegramApiResponse({
        message_id: nextMessageId++,
      });
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
      assistant: {
        activity: "verbose",
        proactivePush: true,
      },
    });
    await writeRuntimeTelegramLocks({});
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext({
      cwd: "/repo/verbose-classic",
    });
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await handlers.get("input")?.(
      { source: "interactive", text: "verbose probe" },
      ctx,
    );
    await handlers.get("agent_start")?.({}, ctx);
    await handlers.get("message_update")?.(
      {
        message: {},
        assistantMessageEvent: {
          type: "thinking_delta",
          contentIndex: 0,
          delta: "provider-exposed reasoning",
        },
      },
      ctx,
    );
    await handlers.get("message_update")?.(
      {
        message: {},
        assistantMessageEvent: {
          type: "thinking_end",
          contentIndex: 0,
          content: "provider-exposed reasoning",
        },
      },
      ctx,
    );
    for (const [toolCallId, toolName] of [
      ["one", "read"],
      ["two", "exec"],
    ] as const) {
      await handlers.get("tool_execution_start")?.(
        {
          type: "tool_execution_start",
          toolCallId,
          toolName,
          args: { path: `${toolCallId}.txt` },
        },
        ctx,
      );
      await handlers.get("tool_execution_end")?.(
        {
          type: "tool_execution_end",
          toolCallId,
          toolName,
          result: `${toolCallId} result`,
          isError: false,
        },
        ctx,
      );
    }
    const assistantMessage = {
      role: "assistant",
      content: [{ type: "text", text: "Semantic **answer**" }],
    };
    await handlers.get("message_update")?.(
      {
        message: assistantMessage,
        assistantMessageEvent: {
          type: "text_end",
          contentIndex: 0,
          content: "Semantic **answer**",
          partial: assistantMessage,
        },
      },
      ctx,
    );
    await handlers.get("message_update")?.(
      {
        message: assistantMessage,
        assistantMessageEvent: {
          type: "done",
          reason: "stop",
          message: assistantMessage,
        },
      },
      ctx,
    );
    await handlers.get("agent_end")?.(
      { messages: [assistantMessage] },
      ctx,
    );
    await waitForCondition(() =>
      calls.some((call) => {
        const richMessage = call.body.rich_message as
          | { markdown?: string }
          | undefined;
        return richMessage?.markdown?.includes("Semantic **answer**") ?? false;
      }),
    );

    const thinkingIndex = calls.findIndex(
      (call) =>
        call.method === "sendMessage" &&
        typeof call.body.text === "string" &&
        call.body.text.includes("<blockquote expandable>") &&
        !call.body.text.includes("Thinking:"),
    );
    assert.equal(calls[thinkingIndex]?.body.chat_id, 77);
    const toolSendIndex = calls.findIndex(
      (call) =>
        call.method === "sendRichMessage" &&
        JSON.stringify(call.body.rich_message).includes("Read:") &&
        JSON.stringify(call.body.rich_message).includes("details"),
    );
    const toolEditIndex = calls.findIndex(
      (call) =>
        call.method === "editMessageText" &&
        JSON.stringify(call.body.rich_message).includes("Exec:") &&
        JSON.stringify(call.body.rich_message).includes("details"),
    );
    const finalIndex = calls.findIndex((call) => {
      const richMessage = call.body.rich_message as
        | { markdown?: string }
        | undefined;
      return richMessage?.markdown?.includes("Semantic **answer**") ?? false;
    });
    assert.ok(thinkingIndex >= 0);
    assert.ok(toolSendIndex > thinkingIndex);
    assert.ok(
      toolEditIndex > toolSendIndex,
      JSON.stringify(calls, undefined, 2),
    );
    assert.ok(finalIndex > toolEditIndex);
    const editedRich = JSON.stringify(
      calls[toolEditIndex]?.body.rich_message,
    );
    assert.match(editedRich, /Read/);
    assert.match(editedRich, /Exec/);
    assert.match(editedRich, /arguments/);
    assert.match(editedRich, /result/);
    assert.equal(calls[toolEditIndex]?.body.text, undefined);
    await handlers.get("agent_settled")?.({}, ctx);
    await commands.get("telegram-disconnect")?.handler("", ctx);
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Verbose activity uses follower transport and loses stale registration authority", async () => {
  let followerGeneration = "follower-1";
  const followerCalls: Array<{
    operation: string;
    args: unknown[];
  }> = [];
  const directRuntime = createTelegramBridgeApiRuntime({
    client: {
      call: async <TResponse>() => {
        return Promise.reject(
          new Error("Direct transport must not be used by a follower."),
        ) as Promise<TResponse>;
      },
      callMultipart: async <TResponse>() => {
        return Promise.reject(
          new Error("Multipart transport is not expected."),
        ) as Promise<TResponse>;
      },
      downloadFile: async () => "/tmp/file",
      answerCallbackQuery: async () => {},
    } satisfies TelegramApiClient,
    tempDir: "/tmp",
    maxFileSizeBytes: 1,
    tempFileMaxAgeMs: 1,
    recordRuntimeEvent: () => {},
  });
  const api = BusApi.createTelegramBusAwareApiRuntime({
    directRuntime,
    ownsDirect: () => false,
    getDefaultTarget: () => ({ chatId: 77, threadId: 43 }),
    async callFollowerApi(operation: string, args: unknown[]) {
      followerCalls.push({ operation, args });
      const method = args[0];
      return method === "sendMessage" || method === "sendRichMessage"
        ? { message_id: 501 }
        : true;
    },
  });
  const authority = Routing.createTelegramAssistantOutputAuthorityRuntime({
    getPreferredTarget: () => ({ chatId: 77, threadId: 43 }),
    getFallbackChatId: () => 77,
    getTransportStamp: () => "profile-1",
    isTransportStampActive: (stamp) => stamp === "profile-1",
    ownsDirect: () => false,
    getDirectEpoch: () => undefined,
    isFollowerRegistered: () => followerGeneration !== "",
    getFollowerGeneration: () => followerGeneration || undefined,
  });
  const runtime = createTelegramActivityVerbosityRuntime({
    getActivityMode: () => "verbose",
    resolveTarget: () => ({ chatId: 77, threadId: 43 }),
    captureAuthority: authority.captureAuthority,
    isAuthorityActive: authority.isAuthorityActive,
    sendMessage: api.sendMessage,
    sendRichMessage: api.sendRichMessage,
    editMessageText: api.editMessageText,
  });
  const base = {
    activityId: "follower-activity",
    source: "telegram",
    target: { chatId: 77, threadId: 43 },
    timestamp: 1,
  } as const;
  runtime.accept({ ...base, sequence: 1, type: "agent-start" });
  runtime.accept({
    ...base,
    sequence: 2,
    type: "reasoning-delta",
    contentIndex: 0,
    delta: "follower reasoning",
  });
  runtime.accept({
    ...base,
    sequence: 3,
    type: "reasoning-end",
    contentIndex: 0,
    text: "follower reasoning",
  });
  runtime.accept({
    ...base,
    sequence: 4,
    type: "tool-end",
    toolCallId: "tool-1",
    toolName: "read",
    result: "done",
    isError: false,
  });
  await runtime.waitForIdle();
  assert.deepEqual(
    followerCalls.map((call) => call.args[0]),
    ["sendMessage", "sendRichMessage"],
  );
  const thinkingBody = followerCalls[0]?.args[1] as
    | Record<string, unknown>
    | undefined;
  assert.equal(thinkingBody?.chat_id, 77);
  assert.equal(thinkingBody?.message_thread_id, 43);

  followerGeneration = "follower-2";
  runtime.accept({
    ...base,
    sequence: 5,
    type: "tool-end",
    toolCallId: "tool-2",
    toolName: "exec",
    result: "stale",
    isError: false,
  });
  await runtime.waitForIdle();
  assert.equal(followerCalls.length, 2);
  runtime.stop();
});

test("Follower aggregate delivery crosses the authorized leader transport", async () => {
  const follower = {
    instanceId: "follower-one",
    connectedAtMs: 1,
    lastHeartbeatMs: 1,
    target: { chatId: 77, threadId: 12 },
  };
  const directBodies: Array<Record<string, unknown>> = [];
  const leaderProxy = BusLeader.createTelegramBusLeaderApiProxy({
    async call(method, body) {
      assert.equal(method, "sendMessage");
      directBodies.push(body);
      return { message_id: 301 };
    },
    async callMultipart() {
      throw new Error("Multipart transport is not expected");
    },
    async downloadFile() {
      throw new Error("Download transport is not expected");
    },
  });
  const busAwareApi = BusApi.createTelegramBusAwareApiRuntime({
    directRuntime: {} as TelegramBridgeApiRuntime,
    ownsDirect: () => false,
    getDefaultTarget: () => follower.target,
    async callFollowerApi(method, args) {
      assert.equal(
        Bus.isTelegramFollowerApiCallAllowed({ follower, method, args }),
        true,
      );
      return leaderProxy(method, args);
    },
  });
  const runtime = Delivery.createTelegramBridgeDeliveryRuntime({
    generation: "follower-generation",
    getTargetPolicyView: () => ({
      canDeliver: true,
      ownsDirect: false,
      allowedChatId: 77,
      followerTarget: follower.target,
    }),
    getActiveTurnTarget: () => follower.target,
    api: busAwareApi,
    recordOwnership() {},
  });

  const result = await runtime.sendView(
    { text: "Aggregate activity" },
    { scope: { kind: "aggregate" } },
  );

  assert.equal(result.ok, true);
  assert.deepEqual(directBodies, [
    { chat_id: 77, text: "Aggregate activity" },
  ]);
});

test("Leader transport coalesces matching direct and follower chat actions", async () => {
  let calls = 0;
  let releaseAction: (value: boolean) => void = () => {};
  const pendingAction = new Promise<boolean>((resolve) => {
    releaseAction = resolve;
  });
  const directRuntime = createTelegramBridgeApiRuntime({
    client: {
      call: async <TResponse>() => {
        calls += 1;
        return (await pendingAction) as TResponse;
      },
      callMultipart: async <TResponse>() => true as TResponse,
      downloadFile: async () => "/tmp/file",
      answerCallbackQuery: async () => {},
    } satisfies TelegramApiClient,
    tempDir: "/tmp",
    maxFileSizeBytes: 1,
    tempFileMaxAgeMs: 1,
    recordRuntimeEvent: () => {},
  });
  const leaderProxy = BusLeader.createTelegramBusLeaderApiProxy({
    call: directRuntime.call,
    callMultipart: directRuntime.callMultipart,
    downloadFile: directRuntime.downloadFile,
  });
  const body = { chat_id: 77, action: "typing" };

  const direct = directRuntime.call<boolean>("sendChatAction", body);
  const follower = leaderProxy("call", ["sendChatAction", body]);
  await flushMicrotasks();
  assert.equal(calls, 1);
  releaseAction(true);
  assert.deepEqual(await Promise.all([direct, follower]), [true, true]);
});

for (const drift of ["none", "context", "generation", "epoch", "owner", "profile", "path", "canonical"] as const) {
  test(`Native section owners compose Restore, nonleader admission and queued projection (${drift})`, async () => {
    const dir = await mkdtemp(join(tmpdir(), "pt-section-owners-")), path = Paths.resolveTelegramStatePath(dir);
    const scope = { path, profile: "default" }, ctx = { cwd: "/section-owner" };
    const sessions = Lifecycle.createTelegramSessionContextStore<typeof ctx>();
    sessions.set(ctx);
    const lock = Locks.createTelegramLockRuntime<typeof ctx>({ statePath: path, key: () => scope.profile,
      pid: 10, instanceId: "section-owner", runtimeGeneration: 1, isProcessAlive: () => true });
    const captureAuthority = Locks.createTelegramOwnedStateAuthorityCapture(lock, sessions);
    const open = () => Threads.createTelegramTopicTargetStore({ path: () => scope.path, telegramProfile: () => scope.profile,
      consolidated: { captureAuthority, publishIfOwned: lock.publishStateSectionIfOwned! }, getNowMs: () => 1000 });
    const namedLock = Locks.createTelegramLockRuntime<typeof ctx>({ statePath: path, key: () => "work",
      pid: 20, instanceId: "named-owner", runtimeGeneration: 1, isProcessAlive: () => true });
    const namedSessions = Lifecycle.createTelegramSessionContextStore<typeof ctx>();
    namedSessions.set(ctx);
    const named = Threads.createTelegramTopicTargetStore({ path, telegramProfile: "work", getNowMs: () => 1000,
      consolidated: { captureAuthority: Locks.createTelegramOwnedStateAuthorityCapture(namedLock, namedSessions),
        publishIfOwned: namedLock.publishStateSectionIfOwned! } });
    const processBirthId = ProcessIdentity.getTelegramProcessBirthIdentity(process.pid, "section-admission");
    assert.ok(processBirthId);
    const ledger = (profile: string) => WorkspaceAdmission.createTelegramWorkspaceAdmissionLedger({ path, stateProfile: profile,
      profileKey: WorkspaceAdmission.createTelegramWorkspaceAdmissionProfileKey({ profileName: profile, botToken: `section-${profile}` }),
      owner: { processId: process.pid, processBirthId }, getProcessLiveness: () => "alive", getNowMs: () => 1000 });
    try {
      assert.equal(lock.acquire(ctx).ok, true);
      assert.equal(namedLock.acquire(ctx).ok, true);
      await named.load();
      named.setBotState({ threadMode: "disabled" });
      await named.persist();
      named.setStatusSnapshot({ runtime: { pollingActive: false }, diagnostics: { pendingDispatch: true } });
      await named.persistStatus();
      const namedAdmission = ledger("work");
      assert.equal(namedAdmission.acquireAdmission({ operationId: "named-busy", operationKind: "send", scope: { kind: "profile" } }).kind, "acquired");
      const store = open();
      await store.load();
      const target = { chatId: 7, threadId: 10 };
      store.upsert({ profileKey: "cwd:/section-owner", owner: { kind: "leader", cwd: ctx.cwd, instanceId: "old" },
        instanceId: "old", target, status: "active", slot: "A", threadName: "Atlas", createdAtMs: 1, updatedAtMs: 1 });
      store.upsertWorkspaceBinding({ ...Threads.createTelegramWorkspaceBindingIdentity(ctx.cwd, 0, "session")!,
        target, slot: "A", threadName: "Atlas", journalBindingKeys: [], journalBindingsComplete: true, journalSources: [], updatedAtMs: 1 });
      await store.persist();
      await store.load();
      const admission = ledger("default"), lease = admission.acquireAdmission({ operationId: "nonleader-busy",
        operationKind: "send", scope: { kind: "target", target } });
      assert.equal(lease.kind, "acquired");
      if (lease.kind !== "acquired") return;
      assert.notEqual(lease.lease.owner.processId, (Locks.readTelegramRuntimeState(path).profiles.default!.transport as { pid: number }).pid);
      const view = store.workspaceRestore({ profileName: "default", tokenSha256: "a".repeat(64), getNowMs: () => 1000 });
      const auth = { executor: { instanceId: "section-owner", leaderEpoch: String(lock.getOwnedLeaderEpoch()!) },
        operatorUserId: 7, isCurrent: captureAuthority()! };
      const request = { operationId: "section-restore", binding: store.listWorkspaceBindings()[0]!,
        owner: store.list()[0]!, target: { chatId: 7, threadId: 42 }, source: { journalBindingKey: "source", updateIds: [500] } };
      assert.equal(Threads.isTelegramWorkspaceRestoreRequest(request), true, JSON.stringify(request));
      const relocated = await view.commit(request, auth);
      assert.ok(relocated);
      const recipient = { kind: "leader" as const, instanceId: "old", sessionId: "session", generation: "registration" };
      const issued = view.issueRecipient(relocated, recipient, auth)!;
      assert.equal(issued.issued, true);
      let registrations = 0;
      store.commitWorkspaceRestoreRegistration({ target: { chatId: 7, threadId: 42 },
        bindingKey: relocated.request.binding.bindingKey, slot: "A" }, () => { registrations++; });
      assert.equal(registrations, 1, "Registration observes the existing Workspace frame without nesting a shared transaction");
      const stable = Locks.readTelegramRuntimeState(path);
      const canonicalRestore = Threads.parseTelegramWorkspaceStateSection(stable.profiles.default!.workspace, "default")!.workspaceRestore;
      const epoch = lock.getOwnedLeaderEpoch();
      store.setBotState({ threadMode: "enabled" });
      store.setStatusSnapshot({ runtime: { pollingActive: true }, diagnostics: { pendingDispatch: false,
        recentRuntimeEvents: [{ at: 1, category: "fixture", message: "logged separately" }] } });
      if (drift === "canonical") {
        const competing = open();
        await competing.load();
        competing.setBotState({ lastReconcileAction: "concurrent-section-change" });
        await competing.persist();
      }
      // Both production owner ports capture before their independent queue awaits.
      const workspaceWrite = store.persist(), runtimeWrite = store.persistStatus();
      const workspaceResult = workspaceWrite.then(() => undefined, error => error as Error);
      if (drift === "context") sessions.set({ cwd: ctx.cwd });
      if (drift === "generation") { sessions.clear(ctx); sessions.set(ctx); }
      if (drift === "epoch") { lock.release(); assert.equal(lock.acquire(ctx).ok, true); assert.notEqual(lock.getOwnedLeaderEpoch(), epoch); }
      if (drift === "owner") {
        const replacement = Locks.createTelegramLockRuntime({ statePath: path, pid: 30, instanceId: "replacement", isProcessAlive: () => true });
        const predecessor = lock.getState();
        assert.equal(predecessor.kind, "active-here");
        if (predecessor.kind !== "active-here") return;
        assert.equal(replacement.acquire({ cwd: "/replacement" }, { force: true, expectedOwner: predecessor.lock }).ok, true);
      }
      if (drift === "profile") scope.profile = "work";
      if (drift === "path") scope.path = join(dir, "wrong-state.json");
      const beforeFlush = Locks.readTelegramRuntimeState(path);
      const workspaceError = await workspaceResult;
      await runtimeWrite;
      if (drift === "none") assert.equal(workspaceError, undefined);
      else assert.match(String(workspaceError), drift === "canonical" ? /canonical snapshot changed/ : /captured publication authority/);
      const after = Locks.readTelegramRuntimeState(path);
      assert.deepEqual(after.profiles.work, stable.profiles.work, "Actual named-profile owners remain untouched");
      for (const section of ["transport", "admission"] as const) assert.deepEqual(after.profiles.default?.[section], beforeFlush.profiles.default?.[section]);
      const workspace = Threads.parseTelegramWorkspaceStateSection(after.profiles.default!.workspace, "default")!;
      assert.deepEqual(workspace.workspaceRestore, canonicalRestore, "Issued Restore facts are not rolled back by sibling publication");
      assert.equal(workspace.workspaceBindings?.[0]?.target.threadId, 42);
      assert.deepEqual(workspace.workspaceBindings?.[0]?.journalBindingKeys, []);
      assert.deepEqual(workspace.workspaceBindings?.[0]?.journalSources, []);
      assert.equal(workspace.workspaceBindings?.[0]?.journalBindingsComplete, true);
      if (drift === "none") assert.equal(workspace.bot.threadMode, "enabled");
      else assert.deepEqual(after.profiles.default!.workspace, beforeFlush.profiles.default!.workspace, "Refused Workspace writes preserve the full canonical section");
      if (drift === "none" || drift === "canonical") {
        assert.equal((after.profiles.default!.runtime as { runtime: { pollingActive: boolean } }).runtime.pollingActive, true);
        assert.equal((after.profiles.default!.runtime as { diagnostics: { recentRuntimeEvents?: unknown } }).diagnostics.recentRuntimeEvents, undefined);
      } else assert.deepEqual(after.profiles.default, beforeFlush.profiles.default, "Revoked grants publish neither queued section");
      if (drift === "canonical") assert.equal(workspace.bot.lastReconcileAction, "concurrent-section-change");
      assert.equal(fs.existsSync(join(dir, "wrong-state.json")), false);
      const beforeRelease = Locks.readTelegramRuntimeState(path);
      assert.equal(admission.releaseAdmission(lease.lease), true, "Nonleader process/birth authority survives transport/session changes");
      const released = Locks.readTelegramRuntimeState(path);
      for (const section of ["transport", "workspace", "runtime"] as const) assert.deepEqual(released.profiles.default?.[section], beforeRelease.profiles.default?.[section]);
      assert.deepEqual(released.profiles.work, stable.profiles.work);
      assert.equal(namedAdmission.read().leases.length, 1);
    } finally { await rm(dir, { recursive: true, force: true }); }
  });
}

for (const profile of [undefined, "work"] as const) for (const boundary of ["review-reopen", "authority-race", "session-context", "session-generation"] as const) {
  const interrupted = boundary !== "review-reopen";
  void testRoot(`Fresh production root uses shared files and ignores released storage (${profile ?? "default"}, ${boundary})`,
    { concurrency: false, timeout: 15_000 }, async () => {
    const extension = await getRuntimeTelegramExtension(), previousAgentDir = process.env.PI_CODING_AGENT_DIR;
    const agentDir = await realpath(await mkdtemp(join(tmpdir(), "pt-fresh-"))), directory = Paths.resolveTelegramTempDir(agentDir);
    const oldDir = join(agentDir, "tmp", "telegram"), oldFiles = {
      "owners.json": "{}\n", "state.json": "{\"version\":999,\"sentinel\":\"released-workspace\"}\n",
      "inbox.json": "{\"version\":999,\"sentinel\":\"never-replay\"}\n", "logs.jsonl": "released log bytes\n",
    };
    await mkdir(oldDir, { recursive: true });
    for (const [name, bytes] of Object.entries(oldFiles)) await writeFile(join(oldDir, name), bytes, { mode: 0o600 });
    const bot = { botToken: "123:fresh-root", botId: 123, allowedUserId: 77, threads: { automaticCleanup: false } };
    await writeFile(join(agentDir, "telegram.json"), JSON.stringify({ ...bot, profiles: { work: bot }, assistant: { activity: "quiet", timeInjection: "hidden" } }), { mode: 0o600 });
    const methods: string[] = [], dispatched: unknown[] = [];
    const { handlers, commands, tools, pi } = createRuntimePiHarness({ sendUserMessage: message => { dispatched.push(message); } });
    const modelRegistry = { refresh() {}, getAvailable: () => [], isUsingOAuth: () => false };
    const ctx = createRuntimeExtensionContext({ cwd: "/fresh-root", modelRegistry,
      getContextUsage: () => undefined }), followerCtx = createRuntimeExtensionContext({ cwd: "/fresh-follower" });
    const follower = createRuntimePiHarness({ sendUserMessage: message => { dispatched.push(message); } });
    let followerStarted = false, nextThread = 42, channelSends = 0, polls = 0, reviewUpdateId = 100;
    let releaseReview: (() => void) | undefined;
    const reviewAnswers: string[] = [];
    let resumed: ReturnType<typeof createRuntimePiHarness> | undefined;
    let holdCreation = false, releaseCreation: (() => void) | undefined;
    let followerConnect: Promise<void> | undefined, sessionStart: Promise<unknown> | undefined;
    let activeFollowerCtx = followerCtx;
    let replacementOwner: ReturnType<typeof Locks.createTelegramLockRuntime> | undefined;
    const resumedCtx = createRuntimeExtensionContext({ cwd: ctx.cwd, modelRegistry, getContextUsage: () => undefined,
      sessionManager: { ...ctx.sessionManager, getSessionFile: () => `/sessions/${ctx.sessionManager.getSessionId()}.jsonl` } });
    const restoreFetch = setRuntimeTestFetch(async (input, init) => {
      const method = getRuntimeTelegramApiMethod(input); methods.push(method);
      if (method === "getMe") return createRuntimeTelegramApiResponse({ id: 123, username: "fixture_bot", has_topics_enabled: true });
      if (method === "getChat") return createRuntimeTelegramApiResponse({ id: -100123, type: "channel", username: "fixture_channel" });
      if (method === "sendRichMessage" && parseJsonRequestBody(init)?.chat_id === -100123) {
        channelSends++;
        if ((parseJsonRequestBody(init)?.rich_message as { markdown: string }).markdown === "Unknown outcome") throw new Error("isolated lost channel ACK");
        return createRuntimeTelegramApiResponse({ message_id: 91, chat: { id: -100123, type: "channel" } });
      }
      if (method === "createForumTopic") {
        const threadId = nextThread++;
        if (holdCreation) await new Promise<void>(resolve => { releaseCreation = resolve; });
        return createRuntimeTelegramApiResponse({ message_thread_id: threadId, name: "Fresh" });
      }
      if (method === "getUpdates" && ++polls === 1) return createRuntimeTelegramApiResponse([]);
      if (method === "getUpdates") return await new Promise<Response>((resolve, reject) => {
        releaseReview = () => resolve(createRuntimeTelegramApiResponse([{ update_id: reviewUpdateId++, callback_query: {
          id: `cleanup-placement-review-${reviewUpdateId}`, from: { id: 77, is_bot: false }, data: "settings:review:inactive-threads",
          message: { message_id: 300, message_thread_id: 42, chat: { id: 77, type: "private" } },
        } }]));
        const abort = () => reject(new DOMException("stop", "AbortError"));
        if (init?.signal?.aborted) abort(); else init?.signal?.addEventListener("abort", abort, { once: true });
      });
      if (method === "answerCallbackQuery") {
        reviewAnswers.push(String(parseJsonRequestBody(init)?.text ?? ""));
        return createRuntimeTelegramApiResponse(true);
      }
      if (["deleteWebhook", "setMyCommands", "sendMessage", "sendRichMessage", "editMessageText", "editForumTopic", "sendChatAction"].includes(method)) return createRuntimeTelegramApiResponse(true);
      throw new Error(`Unexpected fresh-root API method: ${method}`);
    });
    try {
      process.env.PI_CODING_AGENT_DIR = agentDir;
      assert.equal(fs.existsSync(directory), false);
      const seedOwner = Locks.createTelegramLockRuntime<{ cwd: string }>({ statePath: Paths.resolveTelegramStatePath(agentDir),
        key: () => profile ?? "default", pid: process.pid, instanceId: "cleanup-placement-seed", runtimeGeneration: 1 });
      assert.equal(seedOwner.acquire({ cwd: "/cleanup-placement-seed" }).ok, true);
      const seed = Threads.createTelegramTopicTargetStore({ path: Paths.resolveTelegramStatePath(agentDir), telegramProfile: profile,
        consolidated: { captureAuthority() {
          const epoch = seedOwner.getOwnedLeaderEpoch();
          return epoch === undefined ? undefined : () => seedOwner.owns() && seedOwner.getOwnedLeaderEpoch() === epoch;
        }, publishIfOwned: seedOwner.publishStateSectionIfOwned! } });
      await seed.load();
      const inactiveBinding = { ...Threads.createTelegramWorkspaceBindingIdentity("/inactive-cleanup", 0, "inactive-cleanup")!,
        slot: "Z", threadName: "Zenith", target: { chatId: 77, threadId: 90 }, inactiveSinceMs: 1, updatedAtMs: 2,
        journalBindingKeys: [], journalBindingsComplete: true as const, journalSources: [] };
      seed.upsertWorkspaceBinding(inactiveBinding);
      await seed.persist();
      seedOwner.release();
      extension(pi);
      await handlers.get("session_start")?.({}, ctx);
      await commands.get("telegram-connect")!.handler(profile ?? "", ctx);
      await waitForCondition(() => !!releaseReview);
      const initialPollerCount = methods.filter(method => method === "getUpdates").length;
      const rootFiles = (await readdir(directory, { withFileTypes: true })).filter(entry => !entry.isDirectory()).map(entry => entry.name).sort();
      assert.deepEqual(rootFiles, ["logs.jsonl", "state.json"], "IPC/staging/rotation never create another root file");
      const state = Locks.readTelegramRuntimeState(Paths.resolveTelegramStatePath(agentDir)), sections = state.profiles[profile ?? "default"]!;
      assert.ok(sections.transport && sections.workspace && sections.admission);
      const workspace = Threads.parseTelegramWorkspaceStateSection(sections.workspace, profile ?? "default")!;
      assert.ok(workspace.threads.some(record => record.target.threadId === 42));
      const logical = Bus.getTelegramBusSocketPath(agentDir, process.platform, profile, "consolidated"), endpoint = Bus.resolveTelegramBusSocketPath(logical);
      // Over-long Unix endpoints (for example deep macOS temp roots) use the bounded private fallback instead.
      if (process.platform !== "win32" && Buffer.byteLength(logical) > BusTransport.TELEGRAM_BUS_MAX_DIRECT_UNIX_ENDPOINT_BYTES) {
        assert.notEqual(dirname(endpoint), Paths.resolveTelegramRuntimeDir(agentDir));
      } else if (process.platform !== "win32") {
        assert.equal(dirname(endpoint), Paths.resolveTelegramRuntimeDir(agentDir));
        const target = await readlink(endpoint);
        assert.equal(dirname(target), ".", "Native publication has a colocated relative private listener");
        assert.equal(fs.lstatSync(join(dirname(endpoint), target)).isSocket(), true);
      }
      const channelTool = tools.get("telegram_message")!;
      await channelTool.execute("fresh-root-channel", { text: "Retained channel post", chat_id: -100123, channel: true });
      const service = Paths.resolveTelegramServiceJournalStorage("channel-posts", agentDir, profile ?? "default");
      assert.equal(fs.existsSync(service.path), true);
      const logPath = Paths.resolveTelegramRuntimeLogPath(agentDir);
      await writeFile(logPath, JSON.stringify({ kind: "event", at: 1, category: "fixture", profile: profile ?? "default", message: "x".repeat(5 * 1024 * 1024) }) + "\n", { mode: 0o600 });
      await assert.rejects(channelTool.execute("fresh-root-unknown", { text: "Unknown outcome", chat_id: -100123, channel: true }), /channel publication failed/u);
      const previousLog = Paths.resolveTelegramPreviousSharedRuntimeLogPath(agentDir);
      await waitForAsyncCondition(async () => fs.existsSync(previousLog));
      assert.equal((await readFile(previousLog, "utf8")).includes('"category":"fixture"'), true);
      assert.equal(fs.existsSync(join(directory, "logs._prev.jsonl")), false);
      assert.equal(fs.existsSync(join(directory, "channel-posts.json")), false);
      const serviceBeforeRace = await readFile(service.path);
      const creationsBefore = methods.filter(method => method === "createForumTopic").length;
      holdCreation = interrupted;
      if (interrupted) {
        const siblingOwner = Locks.createTelegramLockRuntime({ statePath: Paths.resolveTelegramStatePath(agentDir),
          key: () => profile ? "default" : "work", instanceId: "production-race-sibling" });
        assert.equal(siblingOwner.acquire({ cwd: "/sibling-profile" }).ok, true);
        const sibling = Threads.createTelegramTopicTargetStore({ path: Paths.resolveTelegramStatePath(agentDir),
          telegramProfile: profile ? undefined : "work", consolidated: {
            captureAuthority: () => siblingOwner.owns,
            publishIfOwned: siblingOwner.publishStateSectionIfOwned!,
          } });
        try { await sibling.load(); sibling.setBotState({ threadMode: "disabled" }); await sibling.persist(); }
        finally { siblingOwner.release(); }
      }
      extension(follower.pi);
      followerStarted = true;
      followerConnect = (async () => {
        await follower.handlers.get("session_start")?.({}, followerCtx);
        await follower.commands.get("telegram-connect")!.handler(profile ?? "", followerCtx);
      })();
      if (interrupted) {
        // Supersede captured authority while native follower registration holds admission across an API await.
        try { await waitForCondition(() => !!releaseCreation); }
        catch (error) { throw new Error(`${String(error)}\n${JSON.stringify({ methods })}\n${await readFile(Paths.resolveTelegramRuntimeLogPath(agentDir), "utf8")}`); }
        const statePath = Paths.resolveTelegramStatePath(agentDir), stateBeforeRevocation = Locks.readTelegramRuntimeState(statePath);
        const held = stateBeforeRevocation.profiles[profile ?? "default"]!;
        assert.ok((held.admission as { leases: unknown[] }).leases.length > 0, "Production admission spans the actual creation await");
        assert.equal(Object.keys(stateBeforeRevocation.profiles).length, 2, "The preservation assertion has a real sibling-profile section");
        if (boundary !== "authority-race") {
          // The follower's captured attempt, not the leader's independent session, owns local readiness.
          activeFollowerCtx = boundary === "session-generation" ? followerCtx : createRuntimeExtensionContext({
            cwd: followerCtx.cwd, sessionManager: { ...followerCtx.sessionManager },
          });
          sessionStart = follower.handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, activeFollowerCtx);
          await sessionStart;
          releaseCreation!();
          await followerConnect;
          const after = Locks.readTelegramRuntimeState(statePath);
          const retained = Threads.parseTelegramWorkspaceStateSection(after.profiles[profile ?? "default"]!.workspace, profile ?? "default")!;
          const remoteBinding = retained.workspaceBindings?.find(binding => binding.cwd === followerCtx.cwd);
          assert.ok(remoteBinding, "The independently current leader retains acknowledged remote creation; local refusal is not rollback");
          assert.equal(remoteBinding.sessionId, followerCtx.sessionManager.getSessionId());
          assert.equal(remoteBinding.target.threadId, 43);
          assert.equal(remoteBinding.slot, "B");
          assert.deepEqual(retained.workspaceBindings?.filter(binding => binding.cwd !== followerCtx.cwd), workspace.workspaceBindings);
          assert.ok(remoteBinding.journalSources?.length);
          for (const source of remoteBinding.journalSources!) assert.equal(fs.existsSync(Paths.resolveTelegramSessionJournalPath(
            source.sessionId, source.recipientBindingKey, agentDir, profile)), false,
            "Stale creation ACK leaves the recipient family absent");
          const sendsBeforeRefusal = methods.length;
          await assert.rejects(follower.tools.get("telegram_message")!.execute("stale-registration", { text: "Must not send" }),
            /requires this Pi instance to own \/telegram-connect or be registered/u);
          assert.equal(methods.length, sendsBeforeRefusal, "Refused local authority issues no Telegram API call");
          assert.deepEqual((after.profiles[profile ?? "default"]!.admission as { leases: unknown[] }).leases, []);
          for (const [key, value] of Object.entries(stateBeforeRevocation.profiles)) {
            if (key !== (profile ?? "default")) assert.deepEqual(after.profiles[key], value);
          }
          assert.equal(methods.filter(method => method === "createForumTopic").length, creationsBefore + 1);
          assert.deepEqual(await readFile(service.path), serviceBeforeRace);
          assert.deepEqual(dispatched, []);
          assert.equal(methods.includes("deleteForumTopic"), false);
          for (const [name, bytes] of Object.entries(oldFiles)) assert.equal(await readFile(join(oldDir, name), "utf8"), bytes);
          await follower.commands.get("telegram-connect")!.handler(profile ?? "", activeFollowerCtx);
          await follower.tools.get("telegram_message")!.execute("fresh-registration", { text: "Fresh attempt only" });
          assert.equal(methods.filter(method => method === "createForumTopic").length, creationsBefore + 1,
            "Fresh local registration reuses the acknowledged remote binding without repeating creation");
          const freshState = Locks.readTelegramRuntimeState(statePath);
          const fresh = Threads.parseTelegramWorkspaceStateSection(freshState.profiles[profile ?? "default"]!.workspace, profile ?? "default")!;
          assert.deepEqual(fresh.workspaceBindings?.find(binding => binding.cwd === followerCtx.cwd)?.target, remoteBinding.target);
          assert.equal(fresh.workspaceBindings?.find(binding => binding.cwd === followerCtx.cwd)?.slot, remoteBinding.slot);
          for (const [key, value] of Object.entries(stateBeforeRevocation.profiles)) {
            if (key !== (profile ?? "default")) assert.deepEqual(freshState.profiles[key], value);
          }
          assert.deepEqual(await readFile(service.path), serviceBeforeRace);
          assert.deepEqual((freshState.profiles[profile ?? "default"]!.admission as { leases: unknown[] }).leases, []);
          assert.deepEqual(dispatched, []);
          await follower.handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, activeFollowerCtx);
          followerStarted = false;
          return;
        }
        replacementOwner = Locks.createTelegramLockRuntime({ statePath, key: () => profile ?? "default",
          instanceId: "production-race-successor" });
        const oldOwner = replacementOwner.getState();
        assert.equal(oldOwner.kind, "active-here", "The native fixture shares this OS process, not the runtime owner identity");
        if (oldOwner.kind !== "active-here") throw new Error("Missing production owner");
        assert.equal(replacementOwner.acquire({ cwd: ctx.cwd }, { force: true, expectedOwner: oldOwner.lock }).ok, true);
        assert.notEqual(replacementOwner.getOwnedLeaderEpoch(), oldOwner.lock.leaderEpoch);
        const sentinel = { runtime: { pollingActive: false }, diagnostics: { phase: "successor-only" } };
        assert.equal(replacementOwner.publishStateSectionIfOwned!("runtime", () => ({ value: sentinel, result: true }),
          { isCurrent: replacementOwner.owns }).committed, true);
        const successorTransport = Locks.readTelegramRuntimeState(statePath).profiles[profile ?? "default"]!.transport;
        releaseCreation!();
        await followerConnect;
        await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, ctx);
        await follower.handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, followerCtx);
        followerStarted = false;
        const afterRace = Locks.readTelegramRuntimeState(statePath);
        assert.deepEqual(afterRace.profiles[profile ?? "default"]!.workspace, held.workspace,
          "Late creation ACK cannot publish an owner/binding under the revoked production leader");
        assert.deepEqual(afterRace.profiles[profile ?? "default"]!.runtime, sentinel,
          "Late diagnostics and teardown cannot overwrite the successor runtime section");
        assert.deepEqual(afterRace.profiles[profile ?? "default"]!.transport, successorTransport);
        assert.deepEqual((afterRace.profiles[profile ?? "default"]!.admission as { leases: unknown[] }).leases, [],
          "Process/birth-owned admission releases after transport ownership changes");
        for (const [key, value] of Object.entries(stateBeforeRevocation.profiles)) {
          if (key !== (profile ?? "default")) assert.deepEqual(afterRace.profiles[key], value);
        }
        assert.equal(methods.filter(method => method === "createForumTopic").length, creationsBefore + 1);
        assert.deepEqual(await readFile(service.path), serviceBeforeRace);
        assert.deepEqual(dispatched, []);
        assert.equal(methods.includes("deleteForumTopic"), false);
        for (const [name, bytes] of Object.entries(oldFiles)) assert.equal(await readFile(join(oldDir, name), "utf8"), bytes);
        return;
      }
      await followerConnect;
      await waitForAsyncCondition(async () => {
        const raw = Locks.readTelegramRuntimeState(Paths.resolveTelegramStatePath(agentDir)).profiles[profile ?? "default"]?.workspace;
        return Threads.parseTelegramWorkspaceStateSection(raw, profile ?? "default")?.threads.some(record => record.owner?.kind === "manual-follower") === true;
      });
      assert.equal(methods.filter(method => method === "getUpdates").length, initialPollerCount, "The follower joins the native leader, never starts a second poller");
      const joinedWorkspace = Threads.parseTelegramWorkspaceStateSection(Locks.readTelegramRuntimeState(Paths.resolveTelegramStatePath(agentDir)).profiles[profile ?? "default"]!.workspace, profile ?? "default")!;
      const joinedFollower = joinedWorkspace.threads.find(record => record.owner?.kind === "manual-follower")!;
      assert.ok(joinedFollower.instanceId);
      const followerEndpoint = Bus.resolveTelegramBusSocketPath(Bus.getTelegramBusFollowerSocketPath(joinedFollower.instanceId, agentDir, process.platform, profile, "consolidated"));
      if (process.platform !== "win32") {
        assert.equal(dirname(followerEndpoint), Paths.resolveTelegramRuntimeDir(agentDir));
        const target = await readlink(followerEndpoint);
        assert.equal(dirname(target), ".");
        assert.equal(fs.lstatSync(join(dirname(followerEndpoint), target)).isSocket(), true);
      }
      assert.deepEqual((await readdir(directory, { withFileTypes: true })).filter(entry => !entry.isDirectory()).map(entry => entry.name).sort(), ["logs.jsonl", "state.json"]);
      const cleanupStorage = Paths.resolveTelegramServiceJournalStorage("thread-cleanup", agentDir, profile ?? "default");
      assert.equal(fs.existsSync(cleanupStorage.path), false);
      assert.ok(releaseReview);
      releaseReview();
      try {
        await waitForAsyncCondition(async () => fs.existsSync(cleanupStorage.path) && reviewAnswers.includes("Review prepared. No tabs were deleted"));
      } catch (error) {
        throw new Error(`${String(error)}\n${JSON.stringify({ reviewAnswers, methods })}\n${await readFile(Paths.resolveTelegramRuntimeLogPath(agentDir), "utf8")}`);
      }
      const cleanupOptions = { ...cleanupStorage, profileName: profile ?? "default",
        tokenSha256: Journal.createTelegramUpdateJournalBotIdentity({ botToken: bot.botToken }).tokenSha256 };
      const cleanup = ThreadCleanupManager.createTelegramThreadCleanupWorkStore(cleanupOptions);
      const work = cleanup.list();
      assert.equal(work.length, 1);
      assert.equal(work[0]?.entries.length, 1);
      assert.equal(work[0]?.entries[0]?.bindingKey, inactiveBinding.bindingKey);
      assert.equal(work[0]?.entries[0]?.state, "prepared");
      assert.equal(work[0]?.entries[0]?.issuedAtMs, undefined);
      assert.deepEqual(work[0]?.entries[0]?.target, inactiveBinding.target);
      const pollingPath = (Locks.readTelegramRuntimeState(Paths.resolveTelegramStatePath(agentDir)).profiles[profile ?? "default"]!.transport as { journalPath: string }).journalPath;
      const pollingJournal = Journal.createTelegramUpdateJournalStore({ path: pollingPath, profileName: profile ?? "default",
        botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: bot.botToken, botId: bot.botId }) });
      await waitForCondition(() => pollingJournal.read().acceptedThroughUpdateId === 100 && pollingJournal.read().entries.length === 0);
      assert.equal(dirname(service.path), dirname(cleanupStorage.path));
      assert.deepEqual((await readdir(join(directory, "journals"))).sort(), [basename(service.path), basename(cleanupStorage.path)].sort());
      assert.equal((await readdir(Paths.resolveTelegramRuntimeDir(agentDir))).some(name => name.endsWith(".tmp")), false);
      const cleanupBeforeRestart = await readFile(cleanupStorage.path);
      await follower.handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "quit" }, followerCtx);
      followerStarted = false;
      await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "quit" }, ctx);
      const serviceBeforeRestart = await readFile(service.path);
      const workspaceBeforeRestart = Threads.parseTelegramWorkspaceStateSection(Locks.readTelegramRuntimeState(Paths.resolveTelegramStatePath(agentDir)).profiles[profile ?? "default"]!.workspace, profile ?? "default")!;
      const pollerCount = methods.filter(method => method === "getUpdates").length;
      resumed = createRuntimePiHarness();
      extension(resumed.pi);
      await resumed.handlers.get("session_start")?.({ type: "session_start", reason: "resume" }, resumedCtx);
      await resumed.commands.get("telegram-connect")!.handler(profile ?? "", resumedCtx);
      await waitForCondition(() => methods.filter(method => method === "getUpdates").length > pollerCount);
      const resumedTool = resumed.tools.get("telegram_message")!;
      await resumedTool.execute("fresh-root-channel", { text: "Retained channel post", chat_id: -100123, channel: true });
      await assert.rejects(resumedTool.execute("fresh-root-unknown", { text: "Unknown outcome", chat_id: -100123, channel: true }), /channel publication failed/u);
      assert.equal(channelSends, 2, "Success and unknown issuance survive a replacement instance without resending either");
      assert.deepEqual(await readFile(service.path), serviceBeforeRestart);
      assert.deepEqual(await readFile(cleanupStorage.path), cleanupBeforeRestart);
      assert.deepEqual(ThreadCleanupManager.createTelegramThreadCleanupWorkStore(cleanupOptions).list(), work);
      assert.ok(releaseReview);
      releaseReview();
      await waitForCondition(() => reviewAnswers.filter(answer => answer === "Review prepared. No tabs were deleted").length === 2 &&
        pollingJournal.read().acceptedThroughUpdateId === 101 && pollingJournal.read().entries.length === 0);
      assert.deepEqual(await readFile(cleanupStorage.path), cleanupBeforeRestart, "Repeated production review reuses the same work set without issuance or rewriting");
      assert.deepEqual(ThreadCleanupManager.createTelegramThreadCleanupWorkStore(cleanupOptions).list(), work);
      const workspaceAfterRestart = Threads.parseTelegramWorkspaceStateSection(Locks.readTelegramRuntimeState(Paths.resolveTelegramStatePath(agentDir)).profiles[profile ?? "default"]!.workspace, profile ?? "default")!;
      assert.ok(workspaceBeforeRestart.workspaceBindings?.length);
      const retainedInactive = workspaceAfterRestart.workspaceBindings?.find(value => value.bindingKey === inactiveBinding.bindingKey);
      assert.deepEqual(retainedInactive?.journalBindingKeys, []);
      assert.equal(retainedInactive?.journalBindingsComplete, true);
      assert.deepEqual(retainedInactive?.journalSources, []);
      assert.equal(retainedInactive?.inactiveSinceMs, 1);
      for (const binding of workspaceBeforeRestart.workspaceBindings ?? []) assert.ok(workspaceAfterRestart.workspaceBindings?.some(current => current.bindingKey === binding.bindingKey && current.target.threadId === binding.target.threadId));
      assert.deepEqual((await readdir(directory, { withFileTypes: true })).filter(entry => !entry.isDirectory()).map(entry => entry.name).sort(), ["logs.jsonl", "state.json"]);
      assert.deepEqual(dispatched, [], "Released inbox is never parsed, imported or replayed");
      assert.equal(methods.includes("deleteForumTopic"), false);
      for (const [name, bytes] of Object.entries(oldFiles)) assert.equal(await readFile(join(oldDir, name), "utf8"), bytes);
      assert.deepEqual((await readdir(oldDir)).sort(), Object.keys(oldFiles).sort());
    } finally {
      releaseCreation?.();
      await sessionStart?.catch(() => {});
      await followerConnect?.catch(() => {});
      if (followerStarted) await follower.handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, activeFollowerCtx);
      if (resumed) await resumed.handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, resumedCtx);
      await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, ctx);
      replacementOwner?.release();
      restoreFetch();
      if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
      await rm(agentDir, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
    }
  });
}

for (const settlement of ["tool-success", "manual-compaction", "auto-threshold", "auto-overflow"] as const) test(`Real Pi SDK preserves one injection per queued input through a held tool and status (${settlement})`, async () => {
  const { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } = await import("@earendil-works/pi-coding-agent");
  const { fauxProvider, fauxAssistantMessage, fauxToolCall, Type } = await import("@earendil-works/pi-ai");
  await ensureRuntimeAgentDir();
  const extension = (await import("../dist/index.js")).default;
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  const dir = await realpath(await mkdtemp(join(tmpdir(), "pt-real-sdk-")));
  const dispatches: string[] = [], errors: string[] = [], updates: unknown[] = [], methods: string[] = [], notices: string[] = [], compactionReasons: string[] = [];
  let pollReply: ((response: Response) => void) | undefined, callback: string | undefined, buttonMessageId = 0;
  let releaseTool = () => {}, toolStarted = false, toolAborts = 0, compactions = 0, messageId = 100;
  let session: import("@earendil-works/pi-coding-agent").AgentSession | undefined;
  const incoming = (update: unknown) => {
    updates.push(update);
    if (pollReply) { const reply = pollReply; pollReply = undefined; reply(createRuntimeTelegramApiResponse(updates.splice(0))); }
  };
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input), body = init?.body ? JSON.parse(String(init.body)) : {};
    methods.push(String(method));
    if (method === "getMe") return createRuntimeTelegramApiResponse({ id: 123, is_bot: true, username: "fixture_bot" });
    if (method === "getChat") return createRuntimeTelegramApiResponse({ id: 99, type: "private" });
    if (method === "getUpdates") {
      if (updates.length) return createRuntimeTelegramApiResponse(updates.splice(0));
      return new Promise<Response>((resolve, reject) => {
        pollReply = resolve;
        const abort = () => { if (pollReply === resolve) pollReply = undefined; reject(new DOMException("stop", "AbortError")); };
        if (init?.signal?.aborted) abort(); else init?.signal?.addEventListener("abort", abort, { once: true });
      });
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      const id = ++messageId;
      const markup = body.reply_markup ?? body.rich_message?.reply_markup;
      const candidate = markup?.inline_keyboard?.flat().find((cell: { text: string }) => cell.text === "▶️ Queued fixture");
      if (candidate) { callback = candidate.callback_data; buttonMessageId = id; }
      return createRuntimeTelegramApiResponse({ message_id: id, chat: { id: 99, type: "private" } });
    }
    if (["deleteWebhook", "setMyCommands", "sendChatAction", "answerCallbackQuery", "editMessageText", "editMessageReplyMarkup", "sendMessageDraft"].includes(method ?? "")) return createRuntimeTelegramApiResponse(true);
    throw new Error(`Unexpected isolated SDK HTTP operation: ${method}`);
  });
  try {
    process.env.PI_CODING_AGENT_DIR = dir;
    await writeFile(join(dir, "telegram.json"), JSON.stringify({ botToken: "123:fixture-sdk", botId: 123, allowedUserId: 77, lastUpdateId: 0 }), { mode: 0o600 });
    const modelRuntime = await ModelRuntime.create({ authPath: join(dir, "auth.json"), modelsPath: null,
      modelsStorePath: join(dir, "models-cache.json"), allowModelNetwork: false, refreshOnCreate: false });
    const faux = fauxProvider({ provider: "telegram-fixture", api: "telegram-fixture", tokensPerSecond: Infinity,
      ...(settlement === "auto-threshold" ? { models: [{ id: "fixture-small-context", contextWindow: 4096, maxTokens: 512 }] } : {}) });
    modelRuntime.registerNativeProvider(faux.provider);
    faux.setResponses([() => fauxAssistantMessage(fauxToolCall("telegram_message", { text: 'Fixture button\n<!-- telegram_button {▶️ Queued fixture|button fixture} -->' }), { stopReason: "toolUse" }),
      () => fauxAssistantMessage("Fixture button prepared"), () => fauxAssistantMessage(fauxToolCall("hold_fixture", {}, { id: "held-tool" }), { stopReason: "toolUse" }),
      ...(settlement === "auto-overflow" ? [() => fauxAssistantMessage("", { stopReason: "error", errorMessage: "context_length_exceeded" })] : []),
      ...Array.from({ length: 8 }, () => () => fauxAssistantMessage("Fixture finished"))]);
    const settings = SettingsManager.inMemory({ compaction: { enabled: false, keepRecentTokens: 1,
      reserveTokens: settlement === "auto-threshold" ? 4000 : 128 }, retry: { enabled: false } });
    const resources = new DefaultResourceLoader({ cwd: dir, agentDir: dir, settingsManager: settings,
      extensionFactories: [pi => {
        extension({ ...pi, sendUserMessage(content, options) {
          dispatches.push(typeof content === "string" ? content : content.filter(part => part.type === "text").map(part => part.text).join("\n"));
          pi.sendUserMessage(content, options);
        } });
        pi.on("session_before_compact", async event => {
          compactionReasons.push(event.reason);
          if (settlement.startsWith("auto-")) {
            session!.setAutoCompactionEnabled(false);
            const beforeCompaction = dispatches.length;
            await waitForTimeout(1100);
            assert.equal(dispatches.length, beforeCompaction, "Watchdog cannot inject into actual SDK compaction");
          }
          return { compaction: { summary: "Isolated fixture summary", firstKeptEntryId: event.preparation.firstKeptEntryId, tokensBefore: event.preparation.tokensBefore } };
        });
        pi.on("session_compact", () => { compactions++; });
        pi.registerTool({ name: "hold_fixture", label: "Hold", description: "Isolated controllable long tool", parameters: Type.Object({}),
          async execute(_id, _params, signal) {
            toolStarted = true;
            await new Promise<void>(resolve => {
              const abort = () => { toolAborts++; resolve(); };
              releaseTool = () => { signal?.removeEventListener("abort", abort); resolve(); };
              if (signal?.aborted) abort(); else signal?.addEventListener("abort", abort, { once: true });
            });
            return { content: [{ type: "text", text: "held fixture completed" }], details: undefined };
          } });
      }] });
    await resources.reload();
    assert.deepEqual(resources.getExtensions().errors, []);
    ({ session } = await createAgentSession({ cwd: dir, agentDir: dir, modelRuntime, model: faux.getModel(), thinkingLevel: "off", resourceLoader: resources,
      settingsManager: settings, sessionManager: SessionManager.create(dir, join(dir, "sessions")), tools: ["hold_fixture", "telegram_message"] }));
    await session.bindExtensions({ mode: "rpc", uiContext: { notify(message) { notices.push(message); } } as import("@earendil-works/pi-coding-agent").ExtensionUIContext,
      onError: error => { errors.push(JSON.stringify(error)); } });
    assert.ok(session.extensionRunner.getCommand("telegram-connect"));
    await session.prompt("/telegram-connect");
    await session.prompt("Send the isolated fixture button to the paired Telegram chat");
    incoming({ update_id: 1, message: { message_id: 10, chat: { id: 99, type: "private" }, from: { id: 77, is_bot: false }, text: "held fixture" } });
    try { await waitForCondition(() => toolStarted && callback !== undefined); }
    catch (error) {
      await session.prompt("/telegram-status --debug");
      const logs = await readFile(Paths.resolveTelegramRuntimeLogPath(dir), "utf8").catch(() => "missing log");
      throw new Error(JSON.stringify({ dispatches, errors, methods, notices, toolStarted, callback, messages: session.messages, logs }), { cause: error });
    }
    for (const [id, text] of [[2, "queued fixture A"], [3, "queued fixture B"]] as const) incoming({ update_id: id, message: { message_id: id + 10,
      chat: { id: 99, type: "private" }, from: { id: 77, is_bot: false }, text } });
    incoming({ update_id: 4, callback_query: { id: "fixture-callback", from: { id: 77, is_bot: false }, data: callback,
      message: { message_id: buttonMessageId, chat: { id: 99, type: "private" } } } });
    await waitForTimeout(2200);
    assert.equal(dispatches.length, 1, "Multiple actual watchdog ticks cannot inject while a tool is active");
    assert.equal(toolAborts, 0, "Queued Telegram input must not abort the held tool");
    const logPath = Paths.resolveTelegramRuntimeLogPath(dir), before = await readFile(logPath, "utf8");
    await session.prompt("/telegram-status --debug");
    assert.ok((await readFile(logPath, "utf8")).startsWith(before), "The real SDK diagnostic command cannot truncate existing evidence");
    if (settlement === "manual-compaction") {
      await session.compact();
      assert.equal(compactions, 1, "Actual SDK compaction completes, not a supplied bridge event");
      assert.equal(toolAborts, 1, "Only explicit manual SDK compaction aborts the held tool");
    } else {
      if (settlement.startsWith("auto-")) session.setAutoCompactionEnabled(true);
      releaseTool();
      if (settlement.startsWith("auto-")) {
        await waitForCondition(() => compactions === 1);
        assert.deepEqual(compactionReasons, [settlement === "auto-threshold" ? "threshold" : "overflow"], "SDK itself chooses automatic compaction, not manual compact()");
      }
    }
    try { await waitForCondition(() => dispatches.length === 4); }
    catch (error) { throw new Error(JSON.stringify({ dispatches, errors, compactions, compactionReasons, toolAborts }), { cause: error }); }
    await session.waitForIdle(); await waitForTimeout(2200);
    assert.equal(dispatches.length, 4, "One first prompt and exactly three queued inputs, without a redispatch cascade");
    assert.equal(toolAborts, settlement === "manual-compaction" ? 1 : 0);
    for (const text of ["queued fixture A", "queued fixture B", "button fixture"]) {
      assert.equal(dispatches.filter(content => content.includes(text)).length, 1);
      const entries: import("@earendil-works/pi-coding-agent").SessionEntry[] = session.sessionManager.getEntries();
      assert.equal(entries.filter(entry => entry.type === "message" && entry.message.role === "user" && JSON.stringify(entry.message.content).includes(text)).length, 1, "The actual SDK persists each input exactly once");
    }
    assert.deepEqual(errors, []);
  } finally {
    releaseTool();
    if (session) { await session.abort(); await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" }); session.dispose(); }
    restoreFetch();
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    await rm(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  }
}, 30000);

for (const peer of ["legacy", "capable", "status", "abort", "stop", "next", "continue"] as const) test(`Production follower root advertises unified live-rebind capabilities but preserves recipient gates (${peer})`, async () => {
  const extension = await getRuntimeTelegramExtension(), previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  const agentDir = await realpath(await mkdtemp(join(tmpdir(), "pt-live-ports-")));
  const protocol = Bus.createTelegramBusProtocolIdentity({ runtimeBuild: "prepared-peer", capabilities: [
    Bus.TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION, Bus.TELEGRAM_BUS_CAPABILITY_QUEUE_HANDOFF,
    ...(peer !== "legacy" ? [Bus.TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_SAVE, Bus.TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_APPLY,
      Bus.TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_SETTLE, Bus.TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_COMMAND_SET,
      Bus.TELEGRAM_BUS_CAPABILITY_SELECTED_MENU_DELIVERY] : [])] });
  const leaderSocket = Bus.getTelegramBusFollowerSocketPath("fixture-leader", agentDir);
  let leaderChild: ReturnType<typeof spawn> | undefined, leaderExit: Promise<number | null> | undefined;
  const registry = Bus.createTelegramBusFollowerRegistry(), { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage() { assert.fail("Held save or unprepared release cannot dispatch"); },
  });
  const ctx = createRuntimeExtensionContext({ cwd: "/prepared-follower" });
  const notices: string[] = []; ctx.ui.notify = (text?: string) => { notices.push(text ?? ""); };
  const target = { chatId: 77, threadId: 43 };
  const server = Bus.createTelegramBusLocalServer({ socketPath: leaderSocket,
    handleEnvelope: BusLeader.createTelegramBusLeaderEnvelopeHandler({ followerRegistry: registry, protocolIdentity: protocol,
      authSecret: "fixture-secret", provisionFollowerTarget: () => ({ ...target, slot: "A" }),
      callApi(method) { return method === "getMe" ? { id: 123, has_topics_enabled: true } : true; },
    }) });
  const restoreFetch = setRuntimeTestFetch(async input => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "getMe") return createRuntimeTelegramApiResponse({ id: 123, has_topics_enabled: true });
    if (method === "getUpdates" || method === "deleteForumTopic") throw new Error(`Follower must not issue ${method}`);
    return createRuntimeTelegramApiResponse(true);
  });
  try {
    process.env.PI_CODING_AGENT_DIR = agentDir;
    await writeFile(join(agentDir, "telegram.json"), JSON.stringify({ botToken: "123:prepared-ports", botId: 123, allowedUserId: 77,
      threads: { automaticCleanup: false }, assistant: { activity: "quiet" } }), { mode: 0o600 });
    const leaderSource = `
      import * as Locks from ${JSON.stringify(new URL("../lib/locks.ts", import.meta.url).href)};
      import * as Threads from ${JSON.stringify(new URL("../lib/threads.ts", import.meta.url).href)};
      const owner = Locks.createTelegramLockRuntime({ statePath: ${JSON.stringify(Paths.resolveTelegramStatePath(agentDir))},
        instanceId: "fixture-leader", busSocketPath: ${JSON.stringify(leaderSocket)}, busSecret: "fixture-secret" });
      if (!owner.acquire({ cwd: "/fixture-leader" }).ok) throw new Error("Leader fixture acquisition failed");
      const store = Threads.createTelegramTopicTargetStore({ path: ${JSON.stringify(Paths.resolveTelegramStatePath(agentDir))}, consolidated: {
        captureAuthority: () => owner.owns, publishIfOwned: owner.publishStateSectionIfOwned,
      } });
      await store.load(); store.setBotState({ threadMode: "enabled" }); await store.persist();
      const timer = setInterval(() => owner.refresh(), 1000);
      process.on("SIGTERM", () => { clearInterval(timer); owner.release(); process.exit(0); });
      process.stdout.write("READY\\n");
    `;
    leaderChild = spawn(process.execPath, ["--experimental-strip-types", "--input-type=module", "--eval", leaderSource], { stdio: ["ignore", "pipe", "pipe"] });
    leaderExit = new Promise(resolve => leaderChild!.once("exit", code => resolve(code)));
    await new Promise<void>((resolve, reject) => {
      let output = "", errors = "";
      const timer = setTimeout(() => reject(new Error(`Leader fixture not ready: ${output} ${errors}`)), 5000);
      leaderChild!.stdout!.on("data", chunk => { output += chunk; if (output.includes("READY\n")) { clearTimeout(timer); resolve(); } });
      leaderChild!.stderr!.on("data", chunk => { errors += chunk; });
      leaderChild!.once("error", error => { clearTimeout(timer); reject(error); });
      leaderChild!.once("exit", code => { clearTimeout(timer); reject(new Error(`Leader fixture exited ${code}: ${errors}`)); });
    });
    await server.start();
    extension(pi); await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")!.handler("", ctx);
    const follower = registry.list()[0]; assert.ok(follower?.busSocketPath && follower.profileKey && follower.sessionId && follower.registrationGeneration,
      notices.join("\n") + "\n" + await readFile(Paths.resolveTelegramRuntimeLogPath(agentDir), "utf8"));
    for (const cap of [Bus.TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_SAVE, Bus.TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_APPLY,
      Bus.TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_SETTLE, Bus.TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_COMMAND_SET, Bus.TELEGRAM_BUS_CAPABILITY_SELECTED_MENU_DELIVERY]) {
      assert.equal(Bus.hasTelegramBusCapability(follower.protocol, cap), true, "The actual production registration advertises the unified channel");
    }
    const path = Paths.resolveTelegramSessionJournalPath(follower.sessionId, follower.profileKey, agentDir);
    const bindingKey = Journal.createTelegramUpdateJournalBindingKey({ path, profileName: "default",
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:prepared-ports", botId: 123 }) });
    const journal = Journal.createTelegramUpdateJournalStore({ path, profileName: "default",
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:prepared-ports", botId: 123 }) });
    const before = journal.read();
    const selectedCommand = peer === "legacy" || peer === "capable" ? undefined : { name: peer, target: { chatId: 77, threadId: 42 } };
    const envelope = { requestId: "activated-save", auth: "fixture-secret", recipientInstanceId: follower.instanceId,
      recipientRegistrationGeneration: follower.registrationGeneration, recipientSessionId: follower.sessionId,
      recipientBindingKey: bindingKey, operationId: "unprepared-operation", sentAtMs: 1, ...(selectedCommand ? { selectedCommand } : {}) };
    let preparedSource: Bus.TelegramBusPreparedCommandSource | undefined;
    for (const kind of ["leader.prepareLiveRebind", "leader.applyLiveRebind", "leader.settleLiveRebind"] as const) {
      const ack = await Bus.sendTelegramBusLocalEnvelope({ socketPath: follower.busSocketPath, timeoutMs: 1000,
        envelope: { ...envelope, requestId: kind, ...(preparedSource ? { preparedSource } : {}), ...(kind === "leader.prepareLiveRebind"
          ? { kind, updates: [{ update_id: 100, message: { message_id: 100, chat: { id: 77, type: "private" },
            from: { id: 77, is_bot: false }, text: selectedCommand ? `/${selectedCommand.name}` : "not dispatched" } }] }
          : kind === "leader.applyLiveRebind" ? { kind, mode: "apply", sourceUpdateIds: [100] }
            : { kind, mode: "release", sourceUpdateIds: [100] }) } });
      assert.equal(ack?.kind, "bus.ack");
      const saved = peer !== "legacy" && kind === "leader.prepareLiveRebind";
      assert.equal(ack?.kind === "bus.ack" && ack.ok, saved, JSON.stringify(ack));
      if (!saved) assert.match(ack?.kind === "bus.ack" ? ack.message ?? "" : "", /capability|authority|operation|binding|released|canonical|retained|context|intent/iu);
      if (saved) {
        if (selectedCommand) {
          assert.ok(ack?.kind === "bus.ack" && ack.result && typeof ack.result === "object");
          preparedSource = Reflect.get(ack.result, "preparedSource");
          assert.ok(preparedSource?.sourceSha256 && preparedSource.updateId === 100);
        }
        assert.deepEqual(journal.read().entries.map(entry => entry.updateId), [100], "A negotiated production save retains its genuine held original");
      }
    }
    if (peer === "legacy") assert.deepEqual(journal.read(), before, "Missing peer capability cannot borrow the activated local feature");
    else assert.deepEqual(journal.read().entries.map(entry => entry.updateId), [100], "Unprepared apply/release cannot consume the saved source");
    assert.deepEqual(registry.get(follower.instanceId)?.target, follower.target);
  } finally {
    await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, ctx);
    await server.stop();
    if (leaderChild && leaderChild.exitCode === null && leaderChild.signalCode === null) leaderChild.kill("SIGTERM");
    if (leaderExit) await leaderExit;
    restoreFetch();
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    await rm(agentDir, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  }
}, 15000);

for (const commandName of ["compact", "new"] as const) {
for (const boundary of ["current", "busy", "recipient-response"] as const) {
  test(`Production root selected ${commandName} prepares only guarded confirmation (${boundary})`, async () => {
    const result = await runNodeEval(`
      import assert from "node:assert/strict";
      import { mkdtemp, realpath, writeFile, rm } from "node:fs/promises";
      import { join } from "node:path";
      import { tmpdir } from "node:os";
      import { mock } from "node:test";
      let runtimeExtensionContextSequence = 0;
      const createRuntimeExtensionContext = ${createRuntimeExtensionContext.toString()};
      const createRuntimePiHarness = ${createRuntimePiHarness.toString()};
      const commandName = ${JSON.stringify(commandName)}, boundary = ${JSON.stringify(boundary)}, captured = {}, diagnostics = [];
      const dir = await realpath(await mkdtemp(join(tmpdir(), "pt-root-compact-")));
      process.env.PI_CODING_AGENT_DIR = dir;
      const keepAlive = setInterval(() => {}, 60000);
      async function captureModule(name, factories) {
        const url = new URL("./dist/lib/" + name + ".js", import.meta.url);
        const original = await import(url.href), exports = { ...original };
        for (const [factory, key] of Object.entries(factories)) exports[factory] = (...args) => {
          if (key === "command") {
            const send = args[0].sendInteractiveMessage;
            args[0].sendInteractiveMessage = (...values) => {
              captured.guard = values[4]?.assertAuthority;
              const result = send(...values); captured.delivery = result; void result.catch(() => {}); return result;
            };
            args[0].recordRuntimeEvent = (_category, error, detail) => diagnostics.push([detail?.phase, error]);
          }
          const value = original[factory](...args); captured[key] = value; return value;
        };
        mock.module(url.href, { namedExports: exports });
      }
      await captureModule("commands", { createTelegramCommandHandlerTargetRuntime: "command", createTelegramSessionActionAssembly: "sessionActions" });
      await captureModule("lifecycle", { createTelegramSessionContextStore: "session" });
      await captureModule("threads", { createTelegramLeaderThreadStateRuntime: "leader" });
      const { default: extension } = await import("./dist/index.js");
      const { pi, handlers, commands } = createRuntimePiHarness();
      let compactions = 0, reports = 0, source = true, sends = 0, polls = 0;
      const ctx = createRuntimeExtensionContext({ cwd: "/root-compact", sessionManager: { getSessionId: () => "compact-session" },
        compact() { compactions++; } });
      const entered = Promise.withResolvers(), responseGate = Promise.withResolvers();
      globalThis.fetch = async (input, init) => {
        const method = String(input).split("/").at(-1);
        const response = result => new Response(JSON.stringify({ ok: true, result }), { status: 200, headers: { "content-type": "application/json" } });
        if (method === "getMe") return response({ id: 123, has_topics_enabled: true });
        if (method === "createForumTopic") return response({ message_thread_id: 43, name: "Anchor" });
        if (method === "sendMessage") {
          const body = JSON.parse(String(init.body));
          if (body.text === (commandName === "compact" ? "<b>Compact session?</b>" : "<b>Start a new session?</b>")) {
            assert.equal(reports, 1); assert.equal(source, false);
            assert.deepEqual([body.chat_id, body.message_thread_id], [77, 43]);
            assert.equal(body.reply_parameters, undefined); sends++; entered.resolve(); await responseGate.promise;
            return response({ message_id: 99 });
          }
          return response({ message_id: 100 });
        }
        if (method === "getUpdates") {
          if (++polls === 1) return response([]);
          return await new Promise((_resolve, reject) => {
            const abort = () => reject(new DOMException("stop", "AbortError"));
            if (init?.signal?.aborted) abort(); else init?.signal?.addEventListener("abort", abort, { once: true });
          });
        }
        return response(true);
      };
      try {
        await writeFile(join(dir, "telegram.json"), JSON.stringify({ botToken: "123:root-compact", botId: 123, allowedUserId: 77,
          threads: { automaticCleanup: false }, assistant: { activity: "quiet" } }), { mode: 0o600 });
        extension(pi); await handlers.get("session_start")({}, ctx); await commands.get("telegram-connect").handler("", ctx);
        const target = captured.leader.getTarget(); assert.equal(target.threadId, 43);
        if (boundary === "busy") ctx.isIdle = () => false;
        const message = { message_id: 11, message_thread_id: 43, chat: { id: 77, type: "private" }, from: { id: 77, is_bot: false }, text: "/" + commandName };
        const identity = structuredClone(captured.leader.getIdentity());
        const assertRecipientCurrent = () => { assert.equal(captured.session.get(), ctx); assert.deepEqual(captured.leader.getTarget(), target); };
        const dispatch = captured.command[commandName === "compact" ? "prepareSelectedCompactCommand" : "prepareSelectedNewCommand"]({ name: commandName, args: "" }, [message], ctx, {
          assertSourceCurrent() { assert.ok(source); }, assertRecipientCurrent, reportCompleted() { reports++; source = false; return true; },
        });
        assert.equal(typeof dispatch, "function"); assert.equal(await dispatch(), true); assert.equal(await dispatch(), false);
        await entered.promise; assert.equal(reports, 1, "Completion does not await confirmation HTTP");
        if (boundary === "recipient-response") captured.session.set({ ...ctx });
        assert.equal(captured.guard, assertRecipientCurrent);
        responseGate.resolve();
        if (boundary === "current" || boundary === "busy") await captured.delivery;
        else await assert.rejects(captured.delivery);
        await new Promise(resolve => setImmediate(resolve));
        assert.equal(sends, 1); assert.equal(reports, 1); assert.equal(compactions, 0); assert.equal(captured.sessionActions.action.hasPending(), false, "Confirmation never arms the replacement gateway");
        assert.deepEqual(captured.leader.getIdentity(), identity);
        assert.equal(diagnostics.length, boundary === "recipient-response" ? 1 : 0);
        if (diagnostics.length) assert.equal(diagnostics[0][0], "confirmation-render");
      } finally {
        responseGate.resolve(); if (captured.delivery) await captured.delivery.catch(() => {});
        await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, captured.session.get() ?? ctx);
        clearInterval(keepAlive); await rm(dir, { recursive: true, force: true });
      }
    `, { nodeArgs: ["--experimental-test-module-mocks"], timeoutMs: 15000 });
    assert.equal(result.code, 0, result.stderr || result.stdout);
  }, 20000);
}

}

for (const boundary of ["current", "source-response", "source-publication", "recipient-response", "successor", "expiry", "invalid-id", "consumed", "follower"] as const) {
  test(`Production root bare name completes attributable dialog publication (${boundary})`, async () => {
    const result = await runNodeEval(`
      import assert from "node:assert/strict";
      import { mkdtemp, realpath, writeFile, rm } from "node:fs/promises";
      import { join } from "node:path";
      import { tmpdir } from "node:os";
      import { mock } from "node:test";
      let runtimeExtensionContextSequence = 0;
      const createRuntimeExtensionContext = ${createRuntimeExtensionContext.toString()};
      const createRuntimePiHarness = ${createRuntimePiHarness.toString()};
      const boundary = ${JSON.stringify(boundary)}, captured = {};
      const dir = await realpath(await mkdtemp(join(tmpdir(), "pt-root-bare-name-")));
      process.env.PI_CODING_AGENT_DIR = dir;
      const keepAlive = setInterval(() => {}, 60000);
      let publishing = false;
      async function captureModule(name, factories) {
        const url = new URL("./dist/lib/" + name + ".js", import.meta.url);
        const original = await import(url.href), exports = { ...original };
        for (const [factory, key] of Object.entries(factories)) exports[factory] = (...args) => {
          if (key === "command") {
            const open = args[0].openThreadNameDialog;
            args[0].openThreadNameDialog = (...values) => {
              const result = open(...values); captured.publication = result; return result;
            };
          }
          const value = original[factory](...args); captured[key] = value;
          if (key === "naming") {
            const prepare = value.prepare;
            value.prepare = (...values) => {
              const lifetime = prepare(...values);
              if (!lifetime) return lifetime;
              const publish = lifetime.publish;
              lifetime.publish = (...values) => { publishing = true; try { return publish(...values); } finally { publishing = false; } };
              return lifetime;
            };
          }
          if (key === "routing") {
            const send = args[0].sendInteractiveMessage;
            args[0].sendInteractiveMessage = (...values) => { captured.guard = values[4]?.assertAuthority; return send(...values); };
          }
          return value;
        };
        mock.module(url.href, { namedExports: exports });
      }
      await captureModule("thread-naming", { createTelegramThreadNameDialogRuntime: "naming" });
      await captureModule("commands", { createTelegramCommandHandlerTargetRuntime: "command" });
      await captureModule("routing", { createTelegramInboundRouteRuntime: "routing" });
      await captureModule("lifecycle", { createTelegramSessionContextStore: "session" });
      await captureModule("threads", { createTelegramTopicTargetStore: "store", createTelegramLeaderThreadStateRuntime: "leader" });
      await captureModule("bus-follower", { createTelegramBusFollowerRegistrationState: "follower" });
      const { default: extension } = await import("./dist/index.js");
      const { pi, handlers, commands } = createRuntimePiHarness();
      const ctx = createRuntimeExtensionContext({ cwd: "/root-dialog", sessionManager: { getSessionId: () => "dialog-session" } });
      let sends = 0, polls = 0, reports = 0, source = true, now = Date.now(), target;
      const start = now, originalNow = Date.now;
      const entered = Promise.withResolvers(), responseGate = Promise.withResolvers();
      globalThis.fetch = async (input, init) => {
        const method = String(input).split("/").at(-1);
        const response = result => new Response(JSON.stringify({ ok: true, result }), { status: 200, headers: { "content-type": "application/json" } });
        if (method === "getMe") return response({ id: 123, has_topics_enabled: true });
        if (method === "createForumTopic") return response({ message_thread_id: 43, name: "Anchor" });
        if (method === "sendMessage") {
          const body = JSON.parse(String(init.body));
          if (/Send a (?:new )?Thread name/.test(String(body.text))) {
            sends++; entered.resolve(); await responseGate.promise;
            return response({ message_id: boundary === "invalid-id" ? 0 : 99 });
          }
          return response({ message_id: 100 });
        }
        if (method === "getUpdates") {
          if (++polls === 1) return response([]);
          return await new Promise((_resolve, reject) => {
            const abort = () => reject(new DOMException("stop", "AbortError"));
            if (init?.signal?.aborted) abort(); else init?.signal?.addEventListener("abort", abort, { once: true });
          });
        }
        return response(true);
      };
      let execution;
      try {
        await writeFile(join(dir, "telegram.json"), JSON.stringify({ botToken: "123:root-dialog", botId: 123, allowedUserId: 77,
          threads: { automaticCleanup: false }, assistant: { activity: "quiet" } }), { mode: 0o600 });
        Date.now = () => now;
        extension(pi); await handlers.get("session_start")({}, ctx);
        await commands.get("telegram-connect").handler("", ctx);
        target = captured.leader.getTarget(); assert.equal(target.threadId, 43);
        if (boundary === "follower") captured.follower.setRegistered(true, target, { slot: "A", registrationGeneration: "fixture", sessionId: "dialog-session" });
        const message = { message_id: 11, message_thread_id: target.threadId,
          chat: { id: 77, type: "private" }, from: { id: 77, is_bot: false }, text: "/name" };
        const before = structuredClone(captured.leader.getIdentity());
        const dispatch = captured.command.prepareSelectedNameDialogCommand({ name: "name", args: "" }, [message], ctx, {
          assertSourceCurrent() { if (!source) throw new Error("Source revoked"); },
          assertRecipientCurrent() { if (boundary === "source-publication" && publishing) source = false; },
          reportCompleted() { reports++; source = false; return true; },
        });
        assert.equal(typeof dispatch, "function");
        execution = dispatch().then(() => "fulfilled", () => "rejected");
        if (boundary !== "follower") {
          await entered.promise; assert.equal(reports, 0, "Held HTTP cannot complete semantics");
          if (boundary === "source-response") source = false;
          if (boundary === "recipient-response") captured.session.set({ ...ctx });
          if (boundary === "expiry") now = start + 5 * 60_000;
          if (boundary === "successor") captured.naming.open({ scope: "successor", target, dialogMessageId: 101 });
        }
        responseGate.resolve();
        const success = boundary === "current" || boundary === "consumed";
        assert.equal(await execution, success ? "fulfilled" : "rejected");
        assert.equal(reports, success ? 1 : 0); assert.equal(sends, boundary === "follower" ? 0 : 1);
        assert.equal(await dispatch(), false, "Warm attempts cannot issue again");
        const candidate = captured.naming.inspect(target);
        if (success) {
          assert.equal(candidate.dialogMessageId, 99); assert.equal(candidate.expiresAtMs, start + 5 * 60_000);
          const proof = await captured.publication;
          assert.equal(typeof proof.assertPublished, "function");
          assert.doesNotThrow(proof.assertPublished, "Source completion must not invalidate the dialog");
          if (boundary === "consumed") {
            captured.naming.capture(candidate).finish();
            assert.throws(proof.assertPublished, /publication/);
            assert.doesNotThrow(captured.guard, "Delivery authority must not borrow the ended input or source");
          }
        } else if (boundary === "successor") assert.equal(candidate.dialogMessageId, 101);
        else assert.equal(candidate, undefined, "Unconfirmed/stale publication cannot install input");
        assert.deepEqual(captured.leader.getIdentity(), before, "Opening a dialog cannot mutate titles/bindings");
      } finally {
        responseGate.resolve(); if (execution) await execution;
        Date.now = originalNow; await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, captured.session.get() ?? ctx);
        clearInterval(keepAlive); await rm(dir, { recursive: true, force: true });
      }
    `, { nodeArgs: ["--experimental-test-module-mocks"], timeoutMs: 15000 });
    assert.equal(result.code, 0, result.stderr || result.stdout);
  }, 20000);
}

for (const boundary of ["current", "context", "canonical", "local", "follower", "classic",
  "text-rename", "text-reset", "text-rename-owner-loss", "text-reset-owner-loss", "text-rename-result-loss", "text-reset-result-loss",
  "callback-reset", "callback-reset-owner-loss", "callback-reset-edit-loss", "callback-reset-answer-loss",
  "callback-cancel", "callback-cancel-edit-loss", "callback-cancel-answer-loss"] as const) {
  test(`Production root name-dialog captures independent recipient delivery (${boundary})`, async () => {
    const result = await runNodeEval(`
      import assert from "node:assert/strict";
      import fs from "node:fs";
      import { mkdtemp, realpath, writeFile, rm } from "node:fs/promises";
      import { join, dirname, basename } from "node:path";
      import { tmpdir } from "node:os";
      import { mock } from "node:test";
      let runtimeExtensionContextSequence = 0;
      const createRuntimeExtensionContext = ${createRuntimeExtensionContext.toString()};
      const createRuntimePiHarness = ${createRuntimePiHarness.toString()};
      const boundary = ${JSON.stringify(boundary)}, captured = {};
      const dir = await realpath(await mkdtemp(join(tmpdir(), "pt-root-dialog-")));
      process.env.PI_CODING_AGENT_DIR = dir;
      const keepAlive = setInterval(() => {}, 60000);
      async function captureModule(name, factories) {
        const url = new URL("./dist/lib/" + name + ".js", import.meta.url);
        const original = await import(url.href), exports = { ...original };
        for (const [factory, key] of Object.entries(factories)) exports[factory] = (...args) => {
          const value = original[factory](...args); captured[key] = value;
          if (key === "routing") {
            captured.routingDeps = args[0];
            for (const port of ["renameCurrentThread", "resetCurrentThreadName"]) {
              const owner = args[0][port];
              args[0][port] = (...values) => {
                captured.ownerGuard = values[port === "renameCurrentThread" ? 2 : 1]?.assertAuthority;
                return owner(...values);
              };
            }
            const reply = args[0].sendTextReply;
            args[0].sendTextReply = (...values) => {
              captured.resultGuard = values[3]?.assertAuthority;
              const result = reply(...values); captured.delivery = result; void result.catch(() => {}); return result;
            };
            for (const [port, optionIndex, key] of [["editInteractiveMessage", 5, "editGuard"], ["answerCallbackQuery", 2, "answerGuard"]]) {
              const effect = args[0][port];
              args[0][port] = (...values) => { captured[key] = values[optionIndex]?.assertAuthority; return effect(...values); };
            }
            const send = args[0].sendInteractiveMessage;
            args[0].sendInteractiveMessage = (...values) => {
              captured.guard = values[4]?.assertAuthority;
              return send(...values);
            };
          }
          return value;
        };
        mock.module(url.href, { namedExports: exports });
      }
      await captureModule("thread-naming", { createTelegramThreadNameDialogRuntime: "naming" });
      await captureModule("routing", { createTelegramInboundRouteRuntime: "routing" });
      await captureModule("lifecycle", { createTelegramSessionContextStore: "session" });
      await captureModule("threads", { createTelegramTopicTargetStore: "store", createTelegramLeaderThreadStateRuntime: "leader" });
      await captureModule("bus-follower", { createTelegramBusFollowerRegistrationState: "follower" });
      await captureModule("locks", { createTelegramLockRuntime: "lock" });
      const Locks = await import("./dist/lib/locks.js"), Paths = await import("./dist/lib/paths.js");
      const { default: extension } = await import("./dist/index.js");
      const { pi, handlers, commands } = createRuntimePiHarness();
      const ctx = createRuntimeExtensionContext({ cwd: "/root-dialog", sessionManager: { getSessionId: () => "dialog-session" } });
      let sends = 0, polls = 0, edits = 0, resultSends = 0, callbackEdits = 0, answers = 0, testingInput = false;
      const originalFetch = globalThis.fetch;
      globalThis.fetch = async (input, init) => {
        const method = String(input).split("/").at(-1);
        const response = result => new Response(JSON.stringify({ ok: true, result }), { status: 200, headers: { "content-type": "application/json" } });
        if (method === "getMe") return response({ id: 123, has_topics_enabled: true });
        if (method === "createForumTopic") return response({ message_thread_id: 43, name: "Anchor" });
        if (method === "editForumTopic" && testingInput) {
          edits++; if (boundary.endsWith("owner-loss")) captured.session.set({ ...ctx }); return response(true);
        }
        if (testingInput && method === "editMessageText") {
          callbackEdits++;
          const body = JSON.parse(String(init.body));
          assert.equal(body.chat_id, 77); assert.equal(body.message_id, 99);
          if (boundary.endsWith("edit-loss")) captured.session.set({ ...ctx });
          return response({ message_id: 99 });
        }
        if (testingInput && method === "answerCallbackQuery") {
          answers++;
          assert.equal(JSON.parse(String(init.body)).callback_query_id, "root-callback");
          if (boundary.endsWith("answer-loss")) captured.session.set({ ...ctx });
          return response(true);
        }
        if (method === "sendMessage") {
          const body = JSON.parse(String(init.body));
          if (testingInput) {
            resultSends++; if (boundary.endsWith("result-loss")) captured.session.set({ ...ctx });
          }
          if (/Send a (?:new )?Thread name/.test(String(body.text))) {
            sends++;
            if (boundary === "context") captured.session.set({ ...ctx });
            if (boundary === "local") captured.store.upsert({ ...captured.store.list()[0], status: "offline" });
            if (boundary === "canonical") {
              const path = Paths.resolveTelegramStatePath(dir);
              Locks.withTelegramFileTransaction(join(dirname(path), "runtime", basename(path) + ".transaction"), () => {
                const state = JSON.parse(fs.readFileSync(path, "utf8"));
                state.profiles.default.workspace.workspaceBindings[0].inactiveSinceMs = 2;
                fs.writeFileSync(path, JSON.stringify(state), { mode: 0o600 });
              });
            }
          }
          return response({ message_id: 99 });
        }
        if (method === "getUpdates") {
          if (++polls === 1) return response([]);
          return await new Promise((_resolve, reject) => {
            const abort = () => reject(new DOMException("stop", "AbortError"));
            if (init?.signal?.aborted) abort(); else init?.signal?.addEventListener("abort", abort, { once: true });
          });
        }
        return response(true);
      };
      try {
        await writeFile(join(dir, "telegram.json"), JSON.stringify({ botToken: "123:root-dialog", botId: 123, allowedUserId: 77,
          threads: { automaticCleanup: false }, assistant: { activity: "quiet" } }), { mode: 0o600 });
        extension(pi); await handlers.get("session_start")({}, ctx);
        await commands.get("telegram-connect").handler("", ctx);
        const target = captured.leader.getTarget(); assert.equal(target.threadId, 43);
        if (boundary === "follower") captured.follower.setRegistered(true, target, { slot: "A", registrationGeneration: "fixture", sessionId: "dialog-session" });
        if (boundary === "follower" || boundary === "classic") {
          assert.equal(captured.routingDeps.captureThreadNameRecipientAuthority(boundary === "classic" ? { chatId: 77 } : target, ctx), undefined);
        } else {
          const statePath = Paths.resolveTelegramStatePath(dir);
          Locks.mutateTelegramRuntimeStateSection(statePath, "other", "runtime", () => ({ value: { sentinel: "unrelated-profile" }, result: true }), { isCurrent: () => true });
          const other = Locks.readTelegramRuntimeState(statePath).profiles.other;
          const textBoundary = boundary.startsWith("text-"), callbackBoundary = boundary.startsWith("callback-");
          if ((textBoundary || callbackBoundary) && boundary.includes("reset")) assert.equal((await captured.routingDeps.renameCurrentThread(target, "Azure")).ok, true);
          const before = structuredClone(captured.leader.getIdentity());
          const input = { message: { message_id: 11, message_thread_id: target.threadId,
            chat: { id: 77, type: "private" }, from: { id: 77, is_bot: false }, text: "/name" } };
          const result = await captured.routing.handleUpdate(input, ctx).then(() => "fulfilled", () => "rejected");
          assert.equal(result, boundary === "current" || textBoundary || callbackBoundary ? "fulfilled" : "rejected");
          assert.equal(typeof captured.guard, "function"); assert.equal(sends, 1, "issued dialog never repeats");
          if (textBoundary) {
            testingInput = true;
            assert.ok(captured.naming.inspect(target), "Published input remains current before text consumption");
            const textInput = { message: { ...input.message, message_id: 12,
              text: boundary.includes("reset") ? before.slot : "Azure" } };
            await captured.routing.handleUpdate(textInput, ctx);
            const consumption = captured.routingDeps.textGroupRuntime.flushMessage(textInput.message.message_id);
            if (boundary.endsWith("owner-loss")) await assert.rejects(consumption, /authority/);
            else await consumption;
            assert.equal(typeof captured.ownerGuard, "function", "Routing must forward exact input authority through actual root bindings");
            assert.equal(edits, 1, "Issued rename/reset cannot replay");
            if (boundary.endsWith("owner-loss")) {
              assert.equal(resultSends, 0); assert.deepEqual(captured.leader.getIdentity(), before);
              assert.throws(captured.ownerGuard, /authority/);
            } else {
              await captured.delivery.catch(() => {});
              assert.equal(typeof captured.resultGuard, "function", "Detached result must reach actual root/API delivery");
              assert.equal(resultSends, 1);
              assert.equal(captured.leader.getIdentity().threadName, boundary.includes("reset") ? before.slot : "Azure");
              input.message.text = "changed-source";
              if (boundary.endsWith("result-loss")) assert.throws(captured.resultGuard, /context|authority/);
              else {
                assert.doesNotThrow(captured.resultGuard, "Ended input and changed source cannot revoke result authority");
                captured.session.set({ ...ctx }); assert.throws(captured.resultGuard, /context|authority/);
              }
            }
          } else if (callbackBoundary) {
            testingInput = true;
            const action = boundary.includes("cancel") ? "cancel" : "reset";
            const callback = { callback_query: { id: "root-callback", from: { id: 77, is_bot: false },
              data: "thread-name:" + action, message: { message_id: 99, message_thread_id: target.threadId,
                chat: { id: 77, type: "private" } } } };
            const outcome = await captured.routing.handleUpdate(callback, ctx).then(() => "fulfilled", () => "rejected");
            const lostOwner = boundary.endsWith("owner-loss"), lostEdit = boundary.endsWith("edit-loss");
            assert.equal(edits, action === "reset" ? 1 : 0, "Issued reset never repeats");
            assert.equal(callbackEdits, lostOwner ? 0 : 1);
            assert.equal(answers, lostOwner || lostEdit ? 0 : 1);
            assert.equal(outcome, action === "cancel" && (lostEdit || boundary.endsWith("answer-loss")) ? "rejected" : "fulfilled");
            if (action === "reset") assert.equal(typeof captured.ownerGuard, "function");
            if (!lostOwner) assert.equal(typeof captured.editGuard, "function", "Actual root edit must receive independent recipient authority");
            if (!lostOwner && !lostEdit) assert.equal(typeof captured.answerGuard, "function", "Actual root toast must receive recipient authority");
            if (lostOwner) assert.deepEqual(captured.leader.getIdentity(), before);
            else if (action === "reset") assert.equal(captured.leader.getIdentity().threadName, before.slot);
            input.message.text = "changed-source";
            assert.equal(captured.naming.inspect(target), undefined, "Callback must finish exact input without reopening on authority loss");
            if (!boundary.endsWith("loss")) {
              assert.doesNotThrow(captured.editGuard, "Ended input and changed source cannot revoke effect authority");
              assert.doesNotThrow(captured.answerGuard);
              captured.session.set({ ...ctx });
            }
            assert.throws(captured.guard, /context|authority/);
            if (!lostOwner) assert.throws(captured.editGuard, /context|authority/);
          } else if (boundary === "current") {
            input.message.text = "changed-source";
            await captured.routing.handleUpdate({ callback_query: { id: "end-input", from: { id: 77, is_bot: false },
              data: "thread-name:cancel", message: { message_id: 99, message_thread_id: target.threadId, chat: { id: 77, type: "private" } } } }, ctx);
            assert.doesNotThrow(captured.guard, "Source and ended input cannot revoke independent recipient authority");
            captured.session.set({ ...ctx }); assert.throws(captured.guard, /context|recipient authority/);
          } else assert.throws(captured.guard, /context|recipient authority/);
          assert.deepEqual(Locks.readTelegramRuntimeState(statePath).profiles.other, other);
        }
        console.log("root-dialog-pass:" + boundary);
      } finally {
        await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, captured.session.get() ?? ctx);
        captured.lock?.release(); globalThis.fetch = originalFetch; clearInterval(keepAlive); mock.restoreAll();
        await rm(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
      }
    `, { nodeArgs: ["--experimental-test-module-mocks"], timeoutMs: 15000 });
    assert.equal(result.code, 0, result.stdout + result.stderr);
    assert.match(result.stdout, /root-dialog-pass:/u);
  }, 20000);
}

for (const fault of ["current", "source-held", "session-held", "expiry", "successor", "lost-ack", "consumed"] as const) {
  test(`Production root admitted bare name composes exact publication and worker source ACK (${fault})`, async () => {
    const result = await runNodeEval(`
      import assert from "node:assert/strict";
      import { mkdtemp, realpath, writeFile, rm } from "node:fs/promises";
      import { join } from "node:path";
      import { tmpdir } from "node:os";
      import { mock } from "node:test";
      let runtimeExtensionContextSequence = 0;
      const createRuntimeExtensionContext = ${createRuntimeExtensionContext.toString()};
      const createRuntimePiHarness = ${createRuntimePiHarness.toString()};
      const fault = ${JSON.stringify(fault)}, captured = {};
      const keepAlive = setInterval(() => {}, 60000);
      const dir = await realpath(await mkdtemp(join(tmpdir(), "pt-root-admitted-dialog-")));
      process.env.PI_CODING_AGENT_DIR = dir;
      async function captureModule(name, factories) {
        const url = new URL("./dist/lib/" + name + ".js", import.meta.url);
        const original = await import(url.href), exports = { ...original };
        for (const [factory, key] of Object.entries(factories)) exports[factory] = (...args) => {
          if (key === "routing") {
            args[0] = { ...args[0], hasWorkspaceLiveRebindAuthority: () => true, getAdmissionJournalBinding: () => captured.journalKey };
            captured.routingDeps = args[0];
            const send = args[0].sendInteractiveMessage;
            args[0].sendInteractiveMessage = (...values) => {
              if (values[4]?.assertAuthority) captured.effectGuard = values[4].assertAuthority;
              return send(...values);
            };
          }
          if (key === "command") {
            const open = args[0].openThreadNameDialog;
            args[0].openThreadNameDialog = (...values) => { const result = open(...values); captured.publication = result; return result; };
          }
          const value = original[factory](...args); captured[key] = value; return value;
        };
        mock.module(url.href, { namedExports: exports });
      }
      await captureModule("thread-naming", { createTelegramThreadNameDialogRuntime: "naming" });
      await captureModule("commands", { createTelegramCommandHandlerTargetRuntime: "command" });
      await captureModule("lifecycle", { createTelegramSessionContextStore: "session" });
      await captureModule("threads", { createTelegramTopicTargetStore: "store", createTelegramLeaderThreadStateRuntime: "leader" });
      await captureModule("routing", { createTelegramInboundRouteRuntime: "routing" });
      await captureModule("locks", { createTelegramLockRuntime: "lock" });
      const Updates = await import("./dist/lib/updates.js"), Journal = await import("./dist/lib/journal.js");
      const Locks = await import("./dist/lib/locks.js"), Paths = await import("./dist/lib/paths.js");
      const { default: extension } = await import("./dist/index.js");
      const { pi, handlers, commands } = createRuntimePiHarness();
      const ctx = createRuntimeExtensionContext({ cwd: "/root-admitted-dialog", sessionManager: { getSessionId: () => "admitted-dialog-session" } });
      const responseGate = Promise.withResolvers();
      let now = Date.now(), polls = 0, removals = 0, executions = 0, admissions = 0, selecting = false;
      const originalNow = Date.now, originalFetch = globalThis.fetch, start = now;
      const requests = [], choosers = [], answers = [], originals = new Map();
      const response = result => new Response(JSON.stringify({ ok: true, result }), { status: 200 });
      globalThis.fetch = async (input, init) => {
        const method = String(input).split("/").at(-1), payload = init?.body && JSON.parse(String(init.body));
        if (method === "getMe") return response({ id: 123, has_topics_enabled: true });
        if (method === "createForumTopic") return response({ message_thread_id: 43, name: "Anchor" });
        if (method === "getUpdates") {
          if (++polls === 1) return response([]);
          return await new Promise((_resolve, reject) => {
            const abort = () => reject(new DOMException("stop", "AbortError"));
            if (init?.signal?.aborted) abort(); else init?.signal?.addEventListener("abort", abort, { once: true });
          });
        }
        if (method === "answerCallbackQuery") answers.push(payload.text);
        if (method === "sendMessage") {
          if (selecting && /Send a (?:new )?Thread name/.test(String(payload.text))) {
            requests.push(payload); await responseGate.promise; return response({ message_id: 99 });
          }
          if (payload.reply_markup) choosers.push(JSON.stringify(payload.reply_markup));
          return response({ message_id: 500 });
        }
        return response(true);
      };
      let worker;
      const flush = async () => { for (let i = 0; i < 24; i++) await new Promise(resolve => setImmediate(resolve)); };
      try {
        await writeFile(join(dir, "telegram.json"), JSON.stringify({ botToken: "123:root-admitted-dialog", botId: 123, allowedUserId: 77,
          threads: { automaticCleanup: false }, assistant: { activity: "quiet" } }), { mode: 0o600 });
        Date.now = () => now;
        extension(pi); await handlers.get("session_start")({}, ctx); await commands.get("telegram-connect").handler("", ctx);
        assert.equal(captured.leader.getTarget()?.threadId, 43);
        const statePath = Paths.resolveTelegramStatePath(dir);
        Locks.mutateTelegramRuntimeStateSection(statePath, "other", "runtime", () => ({ value: { sentinel: "other-profile" }, result: true }), { isCurrent: () => true });
        const other = Locks.readTelegramRuntimeState(statePath).profiles.other;
        const options = { path: join(dir, "selected-originals.json"), profileName: "default",
          botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:root-admitted-dialog" }) };
        const journal = Journal.createTelegramUpdateJournalStore(options); captured.journalKey = Journal.createTelegramUpdateJournalBindingKey(options);
        worker = Updates.createTelegramUpdateAdmissionWorkerRuntime({ journal: { ...journal, removeCompletedExact(...args) {
          if (args[0].includes(100)) removals++;
          const result = journal.removeCompletedExact(...args);
          if (args[0].includes(100) && fault === "lost-ack") throw new Error("Source ACK lost");
          return result;
        } }, getJournalBindingKey: () => captured.journalKey, hasAuthority: () => true, isContextCurrent: value => value === ctx,
          getQueueOwnerIdentity: () => ({ instanceId: "fixture-worker", processId: process.pid, processBirthId: process.pid + ":dialog", sessionGeneration: 1 }),
          async defaultHandle(update, value, execution) { if (update.message) { originals.set(update.update_id, update.message); executions++; }
            await captured.routing.handleUpdate(update, value, execution); }, onQueueReceiptCommitted() { admissions++; } });
        worker.start(ctx); await worker.waitForDrain();
        journal.appendBatch([{ update_id: 100, message: { message_id: 100, message_thread_id: 42, chat: { id: 77, type: "private" },
          from: { id: 77, is_bot: false }, text: "/name" } }]);
        worker.signal(); await worker.waitForDrain();
        const id = choosers.at(-1).match(/reroutemenu:([a-z0-9]+)/)[1];
        let callbackId = 200;
        const click = async () => {
          journal.appendBatch([{ update_id: callbackId++, callback_query: { id: "selected-dialog-" + callbackId, from: { id: 77, is_bot: false },
            data: "reroutenew:" + id + ":43", message: { message_id: 500, message_thread_id: 42, chat: { id: 77, type: "private" } } } }]);
          worker.signal(); await worker.waitForDrain(); await flush();
        };
        selecting = true; await click();
        assert.equal(requests.length, 1); assert.equal(removals, 0);
        const target = captured.leader.getTarget(); assert.equal(target.threadId, 42);
        assert.equal(captured.naming.inspect(target), undefined, "Pending delivery hides input");
        assert.equal(typeof captured.effectGuard, "function"); assert.doesNotThrow(captured.effectGuard, "Recipient delivery outlives chooser admission");
        if (fault === "source-held") originals.get(100).text = "/name Azure";
        if (fault === "session-held") captured.session.set({ ...ctx });
        if (fault === "expiry") now = start + 5 * 60_000;
        if (fault === "successor") captured.naming.open({ scope: captured.routingDeps.getCurrentInstanceId(), target, dialogMessageId: 101 });
        responseGate.resolve(); await flush();
        const published = ["current", "lost-ack", "consumed"].includes(fault), ack = published && fault !== "lost-ack";
        assert.equal(removals, published ? 1 : 0); assert.equal(!!Updates.inspectTelegramDeferredSourceCompletion(originals.get(100)), ack);
        assert.equal(journal.read().entries.some(row => row.updateId === 100), !published);
        const candidate = captured.naming.inspect(target);
        if (published) {
          assert.equal(candidate.dialogMessageId, 99); assert.equal(candidate.expiresAtMs, start + 5 * 60_000);
          await click(); assert.equal(answers.some(text => text?.includes("Command source disposal confirmed")), ack);
          originals.get(100).text = "changed after completion";
          const proof = await captured.publication; assert.doesNotThrow(proof.assertPublished, "Source release cannot invalidate published input");
          if (fault === "consumed") {
            captured.naming.capture(candidate).finish(); assert.throws(proof.assertPublished, /publication/);
            assert.doesNotThrow(captured.effectGuard, "Independent effects cannot borrow ended input/source authority");
          }
        } else if (fault === "successor") assert.equal(candidate.dialogMessageId, 101);
        else assert.equal(candidate, undefined);
        assert.equal(requests.length, 1); assert.equal(removals, published ? 1 : 0); assert.equal(executions, 1); assert.equal(admissions, 0);
        const canonical = Locks.readTelegramRuntimeState(statePath).profiles.default.workspace;
        assert.equal(canonical.workspaceBindings[0].target.threadId, 42, "Refusal does not roll back binding");
        assert.equal(canonical.workspaceBindings[0].manualThreadName, undefined, "Dialog completion is not title mutation");
        assert.deepEqual(Locks.readTelegramRuntimeState(statePath).profiles.other, other);
        console.log("root-admitted-dialog-pass:" + fault);
      } finally {
        responseGate.resolve(); await flush(); await worker?.stop(); Date.now = originalNow;
        await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, captured.session.get() ?? ctx);
        captured.lock?.release(); globalThis.fetch = originalFetch; clearInterval(keepAlive); mock.restoreAll();
        await rm(dir, { recursive: true, force: true });
      }
    `, { nodeArgs: ["--experimental-test-module-mocks"], timeoutMs: 15000 });
    assert.equal(result.code, 0, result.stdout + result.stderr);
    assert.match(result.stdout, /root-admitted-dialog-pass:/u);
  }, 20000);
}

for (const commandName of ["compact", "new"] as const) {
for (const fault of ["current", "session-held", "response-session", "lost-ack", "missing-capture", "missing-send", "source-before"] as const) {
  test(`Production root admitted ${commandName} composes handled confirmation and exact source ACK (${fault})`, async () => {
    const result = await runNodeEval(`
      import assert from "node:assert/strict";
      import { mkdtemp, realpath, writeFile, rm } from "node:fs/promises";
      import { join } from "node:path";
      import { tmpdir } from "node:os";
      import { mock } from "node:test";
      let runtimeExtensionContextSequence = 0;
      const createRuntimeExtensionContext = ${createRuntimeExtensionContext.toString()};
      const createRuntimePiHarness = ${createRuntimePiHarness.toString()};
      const fault = ${JSON.stringify(fault)}, commandName = ${JSON.stringify(commandName)}, captured = {};
      const keepAlive = setInterval(() => {}, 60000);
      const dir = await realpath(await mkdtemp(join(tmpdir(), "pt-root-admitted-" + commandName + "-")));
      process.env.PI_CODING_AGENT_DIR = dir;
      async function captureModule(name, factories) {
        const url = new URL("./dist/lib/" + name + ".js", import.meta.url);
        const original = await import(url.href), exports = { ...original };
        for (const [factory, key] of Object.entries(factories)) exports[factory] = (...args) => {
          if (key === "routing") {
            args[0] = { ...args[0], hasWorkspaceLiveRebindAuthority: () => true, getAdmissionJournalBinding: () => captured.journalKey };
            captured.routingDeps = args[0];
            const apply = args[0].setCurrentLeaderIdentity;
            args[0].setCurrentLeaderIdentity = (...values) => {
              const result = apply(...values);
              if (fault === "source-before") originals.get(100).text = "/" + commandName + " changed";
              return result;
            };
            const send = args[0].sendInteractiveMessage;
            args[0].sendInteractiveMessage = (...values) => {
              if (values[4]?.assertAuthority) captured.effectGuard = values[4].assertAuthority;
              return send(...values);
            };
          }
          if (key === "reply") {
            const record = args[0].recordOwnership;
            args[0] = { ...args[0], recordOwnership: value => {
              if (selecting) ownership.push(value.messageId);
              return record(value);
            } };
          }
          const value = original[factory](...args); captured[key] = value; return value;
        };
        mock.module(url.href, { namedExports: exports });
      }
      await captureModule("commands", { createTelegramSessionActionAssembly: "sessionActions" });
      await captureModule("replies", { createTelegramRenderedMessageDeliveryRuntime: "reply" });
      await captureModule("lifecycle", { createTelegramSessionContextStore: "session" });
      await captureModule("threads", { createTelegramTopicTargetStore: "store", createTelegramLeaderThreadStateRuntime: "leader" });
      await captureModule("routing", { createTelegramInboundRouteRuntime: "routing" });
      await captureModule("locks", { createTelegramLockRuntime: "lock" });
      const Updates = await import("./dist/lib/updates.js"), Journal = await import("./dist/lib/journal.js");
      const Locks = await import("./dist/lib/locks.js"), Paths = await import("./dist/lib/paths.js");
      const { default: extension } = await import("./dist/index.js");
      const { pi, handlers, commands } = createRuntimePiHarness({ sendUserMessage: () => assert.fail("Confirmation cannot enter the internal Pi gateway") });
      const ctx = createRuntimeExtensionContext({ cwd: "/root-admitted-compact", compact: () => assert.fail("Selected confirmation cannot compact"), newSession: () => assert.fail("Selected confirmation cannot replace a session"), sessionManager: { getSessionId: () => "admitted-compact-session" } });
      const responseGate = Promise.withResolvers();
      let polls = 0, removals = 0, executions = 0, admissions = 0, selecting = false;
      const originalFetch = globalThis.fetch;
      const requests = [], choosers = [], answers = [], originals = new Map(), ownership = [];
      const response = result => new Response(JSON.stringify({ ok: true, result }), { status: 200 });
      globalThis.fetch = async (input, init) => {
        const method = String(input).split("/").at(-1), payload = init?.body && JSON.parse(String(init.body));
        if (method === "getMe") return response({ id: 123, has_topics_enabled: true });
        if (method === "createForumTopic") return response({ message_thread_id: 43, name: "Anchor" });
        if (method === "getUpdates") {
          if (++polls === 1) return response([]);
          return await new Promise((_resolve, reject) => {
            const abort = () => reject(new DOMException("stop", "AbortError"));
            if (init?.signal?.aborted) abort(); else init?.signal?.addEventListener("abort", abort, { once: true });
          });
        }
        if (method === "answerCallbackQuery") answers.push(payload.text);
        if (method === "sendMessage") {
          if (selecting && String(payload.text).includes(commandName === "compact" ? "Compact session" : "Start a new session")) {
            requests.push(payload); await responseGate.promise;
            if (fault === "response-session") captured.session.set({ ...ctx });
            return response({ message_id: 99 });
          }
          if (payload.reply_markup) choosers.push(JSON.stringify(payload.reply_markup));
          return response({ message_id: 500 });
        }
        return response(true);
      };
      let worker;
      const flush = async () => { for (let i = 0; i < 24; i++) await new Promise(resolve => setImmediate(resolve)); };
      try {
        await writeFile(join(dir, "telegram.json"), JSON.stringify({ botToken: "123:root-admitted-compact", botId: 123, allowedUserId: 77,
          threads: { automaticCleanup: false }, assistant: { activity: "quiet" } }), { mode: 0o600 });
        extension(pi); await handlers.get("session_start")({}, ctx); await commands.get("telegram-connect").handler("", ctx);
        assert.equal(captured.leader.getTarget()?.threadId, 43);
        const statePath = Paths.resolveTelegramStatePath(dir);
        Locks.mutateTelegramRuntimeStateSection(statePath, "other", "runtime", () => ({ value: { sentinel: "other-profile" }, result: true }), { isCurrent: () => true });
        const other = Locks.readTelegramRuntimeState(statePath).profiles.other;
        const options = { path: join(dir, "selected-originals.json"), profileName: "default",
          botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:root-admitted-compact" }) };
        const journal = Journal.createTelegramUpdateJournalStore(options); captured.journalKey = Journal.createTelegramUpdateJournalBindingKey(options);
        worker = Updates.createTelegramUpdateAdmissionWorkerRuntime({ journal: { ...journal, removeCompletedExact(...args) {
          if (args[0].includes(100)) removals++;
          const result = journal.removeCompletedExact(...args);
          if (args[0].includes(100) && fault === "lost-ack") throw new Error("Source ACK lost");
          return result;
        } }, getJournalBindingKey: () => captured.journalKey, hasAuthority: () => true, isContextCurrent: value => value === ctx,
          getQueueOwnerIdentity: () => ({ instanceId: "fixture-worker", processId: process.pid, processBirthId: process.pid + ":dialog", sessionGeneration: 1 }),
          async defaultHandle(update, value, execution) { if (update.message) { originals.set(update.update_id, update.message); executions++; }
            await captured.routing.handleUpdate(update, value, execution); }, onQueueReceiptCommitted() { admissions++; },
          onUpdateCompleted(updateId, value, binding) {
            captured.sessionActions.action.onUpdateCompleted(updateId);
            captured.routing.onUpdateCompleted(updateId, value, binding);
          } });
        worker.start(ctx); await worker.waitForDrain();
        journal.appendBatch([{ update_id: 100, message: { message_id: 100, message_thread_id: 42, chat: { id: 77, type: "private" },
          from: { id: 77, is_bot: false }, text: "/" + commandName + " ignored" } }]);
        worker.signal(); await worker.waitForDrain();
        const id = choosers.at(-1).match(/reroutemenu:([a-z0-9]+)/)[1];
        let callbackId = 200;
        const click = async () => {
          journal.appendBatch([{ update_id: callbackId++, callback_query: { id: "selected-compact-" + callbackId, from: { id: 77, is_bot: false },
            data: "reroutenew:" + id + ":43", message: { message_id: 500, message_thread_id: 42, chat: { id: 77, type: "private" } } } }]);
          worker.signal(); await worker.waitForDrain(); await flush();
        };
        selecting = true;
        if (fault === "missing-capture") captured.routingDeps.captureThreadNameRecipientAuthority = undefined;
        if (fault === "missing-send") captured.routingDeps.sendInteractiveMessage = undefined;
        await click();
        const reported = !["source-before", "missing-capture", "missing-send"].includes(fault);
        const ack = reported && fault !== "lost-ack";
        assert.equal(requests.length, reported ? 1 : 0); assert.equal(removals, reported ? 1 : 0);
        const target = captured.leader.getTarget();
        assert.equal(target.threadId, ["missing-capture", "missing-send"].includes(fault) ? 43 : 42);
        assert.equal(!!Updates.inspectTelegramDeferredSourceCompletion(originals.get(100)), ack);
        assert.equal(journal.read().entries.some(row => row.updateId === 100), !reported);
        await click();
        assert.equal(answers.some(text => text?.includes("Command source disposal confirmed")), ack);
        if (ack) assert.ok(answers.some(text => text?.includes(commandName === "compact" ? "confirmation delivery and actual compaction" : "confirmation delivery and actual session replacement")));
        if (reported) {
          assert.equal(typeof captured.effectGuard, "function"); assert.doesNotThrow(captured.effectGuard);
          originals.get(100).text = "changed after handled completion";
          assert.doesNotThrow(captured.effectGuard, "Source completion and chooser exit are not recipient delivery authority");
          if (fault === "session-held") captured.session.set({ ...ctx });
        }
        assert.equal(captured.sessionActions.action.hasPending(), false, "Handled reporting and source ACK cannot arm replacement");
        assert.deepEqual(ownership, [], "Held confirmation has not published ownership despite source disposal");
        responseGate.resolve(); await flush();
        assert.deepEqual(ownership, reported && !["session-held", "response-session"].includes(fault) ? [99] : [],
          "Actual rendered/root API response refuses ownership after recipient loss");
        if (["session-held", "response-session"].includes(fault)) assert.throws(captured.effectGuard);
        else if (reported) assert.doesNotThrow(captured.effectGuard);
        assert.equal(requests.length, reported ? 1 : 0); assert.equal(removals, reported ? 1 : 0);
        assert.equal(executions, 1); assert.equal(admissions, 0);
        assert.equal(captured.sessionActions.action.hasPending(), false, "Confirmation delivery cannot arm replacement");
        assert.ok(requests.every(row => row.chat_id === 77 && row.message_thread_id === 42 && !row.reply_parameters && !("assertAuthority" in row)));
        const canonical = Locks.readTelegramRuntimeState(statePath).profiles.default.workspace;
        assert.equal(canonical.workspaceBindings[0].target.threadId, ["missing-capture", "missing-send"].includes(fault) ? 43 : 42,
          "Missing ports refuse before binding; issued source or delivery refusal never rolls back binding");
        assert.equal(canonical.workspaceBindings[0].manualThreadName, undefined, "Confirmation completion is not title mutation or compaction");
        assert.deepEqual(Locks.readTelegramRuntimeState(statePath).profiles.other, other);
        console.log("root-admitted-confirmation-pass:" + commandName + ":" + fault);
      } finally {
        responseGate.resolve(); await flush(); await worker?.stop();
        await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, captured.session.get() ?? ctx);
        captured.lock?.release(); globalThis.fetch = originalFetch; clearInterval(keepAlive); mock.restoreAll();
        await rm(dir, { recursive: true, force: true });
      }
    `, { nodeArgs: ["--experimental-test-module-mocks"], timeoutMs: 15000 });
    assert.equal(result.code, 0, result.stdout + result.stderr);
    assert.match(result.stdout, /root-admitted-confirmation-pass:/u);
  }, 20000);
}

}

for (const operation of ["rename", "reset"] as const) {
for (const fault of ["current", "owner-source", "owner-session", "reply-session"] as const) {
  test(`Production root admitted explicit name ${operation} composes held owner and exact source ACK (${fault})`, async () => {
    const result = await runNodeEval(`
      import assert from "node:assert/strict";
      import { mkdtemp, realpath, writeFile, rm } from "node:fs/promises";
      import { join } from "node:path";
      import { tmpdir } from "node:os";
      import { mock } from "node:test";
      let runtimeExtensionContextSequence = 0;
      const createRuntimeExtensionContext = ${createRuntimeExtensionContext.toString()};
      const createRuntimePiHarness = ${createRuntimePiHarness.toString()};
      const operation = ${JSON.stringify(operation)}, fault = ${JSON.stringify(fault)}, captured = {};
      const keepAlive = setInterval(() => {}, 60000);
      const dir = await realpath(await mkdtemp(join(tmpdir(), "pt-root-selected-name-")));
      process.env.PI_CODING_AGENT_DIR = dir;
      async function captureModule(name, factories) {
        const url = new URL("./dist/lib/" + name + ".js", import.meta.url);
        const original = await import(url.href), exports = { ...original };
        for (const [factory, key] of Object.entries(factories)) exports[factory] = (...args) => {
          if (key === "routing") args[0] = { ...args[0], hasWorkspaceLiveRebindAuthority: () => true,
            getAdmissionJournalBinding: () => captured.journalKey };
          const value = original[factory](...args); captured[key] = value;
          if (key === "renameBinding" || key === "resetBinding") {
            const port = key === "renameBinding" ? "rename" : "reset", invoke = value[port];
            value[port] = async (...callArgs) => {
              if (selecting) { ownerCalls++; captured.ownerGuard = callArgs.at(-1)?.assertAuthority; }
              return invoke(...callArgs);
            };
          }
          return value;
        };
        mock.module(url.href, { namedExports: exports });
      }
      await captureModule("commands", { createTelegramThreadDisplayNameRenameBinding: "renameBinding", createTelegramThreadDisplayNameResetBinding: "resetBinding" });
      await captureModule("lifecycle", { createTelegramSessionContextStore: "session" });
      await captureModule("threads", { createTelegramTopicTargetStore: "store", createTelegramLeaderThreadStateRuntime: "leader" });
      await captureModule("routing", { createTelegramInboundRouteRuntime: "routing" });
      await captureModule("locks", { createTelegramLockRuntime: "lock" });
      const Updates = await import("./dist/lib/updates.js"), Journal = await import("./dist/lib/journal.js");
      const Locks = await import("./dist/lib/locks.js"), Paths = await import("./dist/lib/paths.js");
      const { default: extension } = await import("./dist/index.js");
      const { pi, handlers, commands } = createRuntimePiHarness();
      const ctx = createRuntimeExtensionContext({ cwd: "/root-selected-name", sessionManager: { getSessionId: () => "selected-name-session" } });
      const owner = Promise.withResolvers(), reply = Promise.withResolvers();
      let selecting = false, ownerCalls = 0, polls = 0, removals = 0, executions = 0, admissions = 0;
      const requests = [], choosers = [], originals = new Map();
      const originalFetch = globalThis.fetch;
      const response = result => new Response(JSON.stringify({ ok: true, result }), { status: 200 });
      globalThis.fetch = async (input, init) => {
        const method = String(input).split("/").at(-1), payload = init?.body && JSON.parse(String(init.body));
        if (method === "getMe") return response({ id: 123, has_topics_enabled: true });
        if (method === "createForumTopic") return response({ message_thread_id: 43, name: "Anchor" });
        if (method === "getUpdates") {
          if (++polls === 1) return response([]);
          return await new Promise((_resolve, reject) => {
            const abort = () => reject(new DOMException("stop", "AbortError"));
            if (init?.signal?.aborted) abort(); else init?.signal?.addEventListener("abort", abort, { once: true });
          });
        }
        if (method === "editForumTopic" && selecting && ownerCalls) {
          assert.equal(payload.message_thread_id, 42); await owner.promise;
        }
        if (method === "sendMessage" && payload.reply_markup) choosers.push(JSON.stringify(payload.reply_markup));
        else if (method === "sendMessage" && selecting) { requests.push(payload); await reply.promise; }
        return response(method === "sendMessage" ? { message_id: 500 } : true);
      };
      let worker;
      const flush = async () => { for (let i = 0; i < 24; i++) await new Promise(resolve => setImmediate(resolve)); };
      try {
        await writeFile(join(dir, "telegram.json"), JSON.stringify({ botToken: "123:root-selected-name", botId: 123, allowedUserId: 77,
          threads: { automaticCleanup: false }, assistant: { activity: "quiet" } }), { mode: 0o600 });
        extension(pi); await handlers.get("session_start")({}, ctx); await commands.get("telegram-connect").handler("", ctx);
        assert.equal(captured.leader.getTarget()?.threadId, 43);
        if (operation === "reset") assert.equal((await captured.renameBinding.rename(captured.leader.getTarget(), "Azure")).ok, true);
        const statePath = Paths.resolveTelegramStatePath(dir);
        Locks.mutateTelegramRuntimeStateSection(statePath, "other", "runtime", () => ({ value: { sentinel: "other-profile" }, result: true }), { isCurrent: () => true });
        const other = Locks.readTelegramRuntimeState(statePath).profiles.other;
        const options = { path: join(dir, "selected-originals.json"), profileName: "default",
          botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:root-selected-name" }) };
        const journal = Journal.createTelegramUpdateJournalStore(options); captured.journalKey = Journal.createTelegramUpdateJournalBindingKey(options);
        worker = Updates.createTelegramUpdateAdmissionWorkerRuntime({ journal: { ...journal, removeCompletedExact(...args) {
          if (args[0].includes(100)) removals++; return journal.removeCompletedExact(...args);
        } }, getJournalBindingKey: () => captured.journalKey, hasAuthority: () => true, isContextCurrent: value => value === ctx,
          getQueueOwnerIdentity: () => ({ instanceId: "fixture-worker", processId: process.pid, processBirthId: process.pid + ":name", sessionGeneration: 1 }),
          async defaultHandle(update, value, execution) { if (update.message) { originals.set(update.update_id, update.message); executions++; }
            await captured.routing.handleUpdate(update, value, execution); }, onQueueReceiptCommitted() { admissions++; } });
        worker.start(ctx); await worker.waitForDrain();
        journal.appendBatch([{ update_id: 100, message: { message_id: 100, message_thread_id: 42, chat: { id: 77, type: "private" },
          from: { id: 77, is_bot: false }, text: operation === "reset" ? "/name A" : "/name Azure" } }]);
        worker.signal(); await worker.waitForDrain();
        const id = choosers.at(-1).match(/reroutemenu:([a-z0-9]+)/)[1];
        let callbackId = 200;
        const click = async () => {
          journal.appendBatch([{ update_id: callbackId++, callback_query: { id: "selected-" + callbackId, from: { id: 77, is_bot: false },
            data: "reroutenew:" + id + ":43", message: { message_id: 500, message_thread_id: 42, chat: { id: 77, type: "private" } } } }]);
          worker.signal(); await worker.waitForDrain(); await flush();
        };
        selecting = true; await click();
        assert.equal(ownerCalls, 1); assert.equal(typeof captured.ownerGuard, "function");
        assert.doesNotThrow(captured.ownerGuard, "Root owner guard must survive chooser invocation end");
        assert.equal(removals, 0); assert.equal(requests.length, 0);
        if (fault === "owner-source") originals.get(100).text = "/name changed";
        if (fault === "owner-session") captured.session.set({ ...ctx });
        owner.resolve(); await flush();
        const completed = fault === "current" || fault === "reply-session";
        assert.equal(removals, completed ? 1 : 0); assert.equal(requests.length, completed ? 1 : 0);
        assert.equal(!!Updates.inspectTelegramDeferredSourceCompletion(originals.get(100)), completed);
        assert.equal(journal.read().entries.some(row => row.updateId === 100), !completed);
        if (completed) {
          const canonical = Locks.readTelegramRuntimeState(statePath).profiles.default.workspace;
          assert.equal(canonical.workspaceBindings[0].target.threadId, 42);
          assert.equal(canonical.workspaceBindings[0].manualThreadName, operation === "rename" ? "Azure" : undefined, JSON.stringify(requests));
          assert.equal(captured.leader.getIdentity().threadName, operation === "rename" ? "Azure" : "A");
          await click(); originals.get(100).text = "changed after completion";
        } else {
          const canonical = Locks.readTelegramRuntimeState(statePath).profiles.default.workspace;
          assert.equal(canonical.workspaceBindings[0].target.threadId, 42, "Name refusal cannot roll back rebinding");
          assert.equal(canonical.workspaceBindings[0].manualThreadName, operation === "reset" ? "Azure" : undefined);
        }
        if (fault === "reply-session") captured.session.set({ ...ctx });
        reply.resolve(); await flush();
        assert.equal(ownerCalls, 1); assert.equal(executions, 1); assert.equal(admissions, 0);
        assert.equal(requests.length, completed ? 1 : 0, "Current and refused replies never reissue");
        assert.ok(requests.every(value => value.chat_id === 77 && value.message_thread_id === 42));
        assert.deepEqual(Locks.readTelegramRuntimeState(statePath).profiles.other, other);
        console.log("root-selected-name-pass:" + operation + ":" + fault);
      } finally {
        owner.resolve(); reply.resolve(); await flush(); await worker?.stop();
        await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, captured.session.get() ?? ctx);
        captured.lock?.release(); globalThis.fetch = originalFetch; clearInterval(keepAlive); mock.restoreAll();
        await rm(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
      }
    `, { nodeArgs: ["--experimental-test-module-mocks"], timeoutMs: 15000 });
    assert.equal(result.code, 0, result.stdout + result.stderr);
    assert.match(result.stdout, /root-selected-name-pass:/u);
  }, 20000);
}
}

for (const operation of ["rename", "reset"] as const) {
for (const boundary of ["current", "ordinary", "entry-session", "caller", "context", "generation", "session", "operator", "token", "profile", "epoch", "canonical", "local", "leader-target", "late-publication", "late-local-name", "late-canonical-name", "follower", "result-observation-caller", "result-observation-session",
  ...(operation === "reset" ? ["entry-caller", "cwd", "late-local-title", "late-canonical-title", "late-local-owner-name", "late-canonical-owner-name", "state-publication"] : [])]) {
  test(`Production root guarded ${operation} captures recipient lifetime (${boundary})`, async () => {
    const result = await runNodeEval(`
      import assert from "node:assert/strict";
      import fs from "node:fs";
      import { mkdtemp, realpath, writeFile, rm } from "node:fs/promises";
      import { join, dirname, basename } from "node:path";
      import { tmpdir } from "node:os";
      import { mock } from "node:test";
      let runtimeExtensionContextSequence = 0;
      const createRuntimeExtensionContext = ${createRuntimeExtensionContext.toString()};
      const createRuntimePiHarness = ${createRuntimePiHarness.toString()};
      const boundary = ${JSON.stringify(boundary)}, operation = ${JSON.stringify(operation)}, captured = {};
      const keepAlive = setInterval(() => {}, 60000);
      const dir = await realpath(await mkdtemp(join(tmpdir(), "pt-root-rename-")));
      process.env.PI_CODING_AGENT_DIR = dir;
      async function captureModule(name, factories) {
        const url = new URL("./dist/lib/" + name + ".js", import.meta.url);
        const original = await import(url.href), exports = { ...original };
        for (const [factory, key] of Object.entries(factories)) exports[factory] = (...args) => {
          const value = original[factory](...args); captured[key] = value;
          if (key === "store" && boundary.startsWith("result-observation-")) {
            const loseRecipient = () => {
              if (boundary === "result-observation-caller") revoke = true;
              else captured.session.set({ ...ctx });
            };
            const rename = value.captureWorkspaceThreadRenameObservation;
            value.captureWorkspaceThreadRenameObservation = (...args) => {
              const read = rename(...args);
              return name => { const result = read(name); if (name !== undefined) loseRecipient(); return result; };
            };
            const reset = value.captureWorkspaceThreadResetObservation;
            value.captureWorkspaceThreadResetObservation = (...args) => {
              const read = reset(...args);
              return { ...read, isResultCurrent(title) { const result = read.isResultCurrent(title); loseRecipient(); return result; } };
            };
          }
          return value;
        };
        mock.module(url.href, { namedExports: exports });
      }
      await captureModule("commands", { createTelegramThreadDisplayNameRenameBinding: "renameBinding", createTelegramThreadDisplayNameResetBinding: "resetBinding" });
      await captureModule("lifecycle", { createTelegramSessionContextStore: "session" });
      await captureModule("config", { createTelegramConfigStore: "config" });
      await captureModule("threads", { createTelegramTopicTargetStore: "store", createTelegramLeaderThreadStateRuntime: "leader" });
      await captureModule("bus-follower", { createTelegramBusFollowerRegistrationState: "follower" });
      await captureModule("locks", { createTelegramLockRuntime: "lock" });
      const Locks = await import("./dist/lib/locks.js"), Paths = await import("./dist/lib/paths.js");
      const BusLeader = await import("./dist/lib/bus-leader.js");
      let late = () => {}, admittedGuards = [];
      mock.module(new URL("./dist/lib/bus-leader.js", import.meta.url).href, { namedExports: {
        ...BusLeader, createTelegramBusLeaderRuntimeAssembly(...args) {
          const runtime = BusLeader.createTelegramBusLeaderRuntimeAssembly(...args);
          const port = operation === "rename" ? "renameLeaderThreadAdmitted" : "resetLeaderThreadName";
          const invoke = runtime[port];
          runtime[port] = async (...callArgs) => {
            admittedGuards.push(typeof callArgs[operation === "rename" ? 2 : 1]);
            const result = await invoke(...callArgs); late(); return result;
          };
          return runtime;
        },
      } });
      const { default: extension } = await import("./dist/index.js");
      const { pi, handlers, commands } = createRuntimePiHarness();
      let sessionId = "root-recipient-session", revoke = false, onEdit = () => {}, edits = 0, polls = 0;
      const ctx = createRuntimeExtensionContext({ cwd: "/root-recipient", sessionManager: { getSessionId: () => sessionId } });
      const originalFetch = globalThis.fetch;
      globalThis.fetch = async (input, init) => {
        const method = String(input).split("/").at(-1);
        const response = result => new Response(JSON.stringify({ ok: true, result }), { status: 200, headers: { "content-type": "application/json" } });
        if (method === "getMe") return response({ id: 123, has_topics_enabled: true });
        if (method === "createForumTopic") return response({ message_thread_id: 43, name: "Anchor" });
        if (method === "editForumTopic") { edits++; onEdit(); return response(true); }
        if (method === "getUpdates") {
          if (++polls === 1) return response([]);
          return await new Promise((_resolve, reject) => {
            const abort = () => reject(new DOMException("stop", "AbortError"));
            if (init?.signal?.aborted) abort(); else init?.signal?.addEventListener("abort", abort, { once: true });
          });
        }
        return response(true);
      };
      try {
        await writeFile(join(dir, "telegram.json"), JSON.stringify({ botToken: "123:root-rename", botId: 123, allowedUserId: 77,
          threads: { automaticCleanup: false }, assistant: { activity: "quiet" } }), { mode: 0o600 });
        extension(pi); await handlers.get("session_start")({}, ctx);
        await commands.get("telegram-connect").handler("", ctx);
        assert.ok(captured.leader.getTarget()?.threadId, "actual root provisioned target");
        if (operation === "reset") assert.equal((await captured.renameBinding.rename(captured.leader.getTarget(), "Azure")).ok, true);
        const before = structuredClone(captured.leader.getIdentity());
        const statePath = Paths.resolveTelegramStatePath(dir);
        Locks.mutateTelegramRuntimeStateSection(statePath, "other", "runtime", () => ({ value: { sentinel: "unrelated-profile" }, result: true }), { isCurrent: () => true });
        const other = Locks.readTelegramRuntimeState(statePath).profiles.other;
        edits = 0;
        const drift = () => {
          if (boundary === "caller") revoke = true;
          if (boundary === "context") captured.session.set({ ...ctx });
          if (boundary === "generation") captured.session.set(ctx);
          if (boundary === "session") sessionId = "replacement-session";
          if (boundary === "cwd") ctx.cwd = "/replacement-cwd";
          if (boundary === "operator") captured.config.setAllowedUserId(88);
          if (boundary === "token") captured.config.setProfile("default", { ...captured.config.get().profiles.default, botToken: "123:changed" });
          if (boundary === "profile") { captured.config.setProfile("other", { botToken: "123:other", allowedUserId: 77 }); captured.config.activateProfile("other"); }
          if (boundary === "epoch") captured.lock.release();
          if (boundary === "canonical") Locks.withTelegramFileTransaction(join(dirname(statePath), "runtime", basename(statePath) + ".transaction"), () => {
            const state = JSON.parse(fs.readFileSync(statePath, "utf8")); state.profiles.default.workspace.workspaceBindings[0].target.threadId++;
            fs.writeFileSync(statePath, JSON.stringify(state), { mode: 0o600 });
          });
          if (boundary === "local") captured.store.upsert({ ...captured.store.list()[0], status: "inactive" });
          if (boundary === "leader-target") captured.leader.set({ ...before, target: { ...before.target, threadId: 99 } });
          if (boundary === "late-publication") captured.session.set({ ...ctx });
          if (boundary === "late-local-name") captured.store.renameByTarget(before.target, "Apex");
          if (boundary === "late-local-title") assert.equal(captured.store.setWorkspaceDisplayTitle(captured.store.listWorkspaceBindings()[0], "Apex"), true);
          if (boundary === "late-local-owner-name") captured.store.upsert({ ...captured.store.list()[0], manualThreadName: "Apex" });
          if (boundary === "late-canonical-title" || boundary === "late-canonical-owner-name") Locks.withTelegramFileTransaction(join(dirname(statePath), "runtime", basename(statePath) + ".transaction"), () => {
            const state = JSON.parse(fs.readFileSync(statePath, "utf8"));
            if (boundary === "late-canonical-title") state.profiles.default.workspace.workspaceBindings[0].displayTitle = "Apex";
            else state.profiles.default.workspace.threads[0].manualThreadName = "Apex";
            fs.writeFileSync(statePath, JSON.stringify(state), { mode: 0o600 });
          });
          if (boundary === "late-canonical-name") Locks.withTelegramFileTransaction(join(dirname(statePath), "runtime", basename(statePath) + ".transaction"), () => {
            const state = JSON.parse(fs.readFileSync(statePath, "utf8"));
            state.profiles.default.workspace.workspaceBindings[0].manualThreadName = "Apex";
            state.profiles.default.workspace.threads[0].manualThreadName = "Apex";
            fs.writeFileSync(statePath, JSON.stringify(state), { mode: 0o600 });
          });
        };
        if (boundary === "entry-session") sessionId = "unprepared-session";
        if (boundary === "entry-caller") revoke = true;
        if (boundary === "state-publication") {
          const set = captured.leader.set;
          captured.leader.set = input => { set(input); revoke = true; };
        }
        if (boundary === "follower") captured.follower.setRegistered(true, before.target, { slot: before.slot, registrationGeneration: "fixture", sessionId });
        if (boundary.startsWith("late-")) late = drift; else onEdit = drift;
        const options = boundary === "ordinary" ? undefined : {
          assertAuthority() { if (revoke) throw new Error("recipient caller ended"); },
        };
        const renamed = await (operation === "rename"
          ? captured.renameBinding.rename(before.target, "Azure", options)
          : captured.resetBinding.reset(before.target, options)).catch(error => ({ ok: false, message: error.message }));
        const success = boundary === "current" || boundary === "ordinary";
        assert.equal(renamed.ok, success, JSON.stringify(renamed));
        assert.equal(edits, boundary === "entry-session" || boundary === "entry-caller" || boundary === "follower" ? 0 : 1, "no retry or redirected edit");
        if (success) {
          assert.equal(captured.leader.getIdentity().threadName, operation === "rename" ? "Azure" : before.slot);
          assert.equal(renamed.threadName, operation === "rename" ? "Azure" : before.slot);
          assert.equal(Locks.readTelegramRuntimeState(statePath).profiles.default.workspace.workspaceBindings[0].manualThreadName, operation === "rename" ? "Azure" : undefined);
          if (operation === "reset" && boundary === "current") assert.equal(captured.leader.getIdentity().slot, before.slot, "guarded publication preserves captured slot");
        } else {
          if (boundary !== "leader-target" && boundary !== "state-publication") assert.deepEqual(captured.leader.getIdentity(), before, "late result cannot publish leader identity");
          const manualName = Locks.readTelegramRuntimeState(statePath).profiles.default.workspace.workspaceBindings[0].manualThreadName;
          if (!boundary.startsWith("late-") && !boundary.startsWith("result-observation-") && boundary !== "state-publication") assert.equal(manualName === "Azure", operation === "reset");
          else if (boundary === "late-canonical-name") assert.equal(manualName, "Apex", "newer canonical name is preserved");
          else assert.equal(manualName, operation === "rename" ? "Azure" : undefined, "issued canonical publication is not rolled back");
          if (boundary === "late-local-title") assert.equal(captured.store.listWorkspaceBindings()[0].displayTitle, "Apex");
          if (boundary === "late-local-owner-name") assert.equal(captured.store.list()[0].manualThreadName, "Apex");
          if (boundary === "late-canonical-title") assert.equal(Locks.readTelegramRuntimeState(statePath).profiles.default.workspace.workspaceBindings[0].displayTitle, "Apex");
          if (boundary === "late-canonical-owner-name") assert.equal(Locks.readTelegramRuntimeState(statePath).profiles.default.workspace.threads[0].manualThreadName, "Apex");
          if (boundary === "state-publication") assert.equal(captured.leader.getIdentity().threadName, before.slot, "already current publication is not rolled back");
          if (boundary === "late-local-name") assert.equal(captured.store.listWorkspaceBindings()[0].manualThreadName, "Apex", "newer local name is preserved");
          if (boundary === "follower") assert.match(renamed.message, /Guarded follower/u);
        }
        assert.deepEqual(Locks.readTelegramRuntimeState(statePath).profiles.other, other);
        if (boundary === "current") assert.deepEqual(admittedGuards, ["function"], "actual root forwards guard into staged leader publication");
        if (boundary === "ordinary") assert.deepEqual(admittedGuards, ["undefined"], "ordinary naming retains unguarded owner path");
        console.log("root-boundary-pass:" + boundary);
      } finally {
        await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, captured.session.get() ?? ctx);
        captured.lock?.release(); globalThis.fetch = originalFetch; clearInterval(keepAlive); mock.restoreAll();
        await rm(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
      }
    `, { nodeArgs: ["--experimental-test-module-mocks"], timeoutMs: 15000 });
    assert.equal(result.code, 0, result.stdout + result.stderr);
    assert.match(result.stdout, /root-boundary-pass:/u);
  }, 20000);
}

}

for (const damage of ["malformed-json", "nul-envelope", "invalid-workspace", "invalid-admission"] as const) {
  test(`Production connect resets damaged shared runtime state and becomes leader (${damage})`, async () => {
    const extension = await getRuntimeTelegramExtension(), previousAgentDir = process.env.PI_CODING_AGENT_DIR;
    const agentDir = await realpath(await mkdtemp(join(tmpdir(), "pt-reset-")));
    const statePath = Paths.resolveTelegramStatePath(agentDir);
    await mkdir(dirname(statePath), { recursive: true });
    await writeFile(statePath, damage === "malformed-json" ? "{ damaged" : damage === "nul-envelope" ? Buffer.alloc(706)
      : JSON.stringify({ version: 2, profiles: { default: damage === "invalid-admission"
        ? { admission: { version: 1, profileKey: "invalid-fixture-key", leases: [] } }
        : { workspace: { version: 99 } } } }), { mode: 0o600 });
    await writeFile(join(agentDir, "telegram.json"), JSON.stringify({ botToken: "123:reset-root", botId: 123, allowedUserId: 77 }), { mode: 0o600 });
    const { handlers, commands, tools, pi } = createRuntimePiHarness();
    const ctx = createRuntimeExtensionContext({ cwd: "/reset-root" });
    let polls = 0, outbound = 0;
    const restoreFetch = setRuntimeTestFetch(async (input, init) => {
      const method = getRuntimeTelegramApiMethod(input);
      if (method === "getMe") return createRuntimeTelegramApiResponse({ id: 123, username: "fixture_bot" });
      if (method === "sendMessage" || method === "sendRichMessage") { outbound++; return createRuntimeTelegramApiResponse({ message_id: 1 }); }
      if (method === "getUpdates" && ++polls === 1) return createRuntimeTelegramApiResponse([]);
      if (method === "getUpdates") return await new Promise<Response>((_resolve, reject) => {
        const abort = () => reject(new DOMException("stop", "AbortError"));
        if (init?.signal?.aborted) abort(); else init?.signal?.addEventListener("abort", abort, { once: true });
      });
      return createRuntimeTelegramApiResponse(true);
    });
    try {
      process.env.PI_CODING_AGENT_DIR = agentDir;
      extension(pi);
      await handlers.get("session_start")?.({}, ctx);
      await commands.get("telegram-connect")!.handler("", ctx);
      await waitForCondition(() => polls >= 2);
      const state = Locks.readTelegramRuntimeState(statePath);
      assert.ok(state.profiles.default?.transport, "the connecting instance owns the fresh envelope");
      assert.equal(JSON.stringify(state).includes("damaged") || JSON.stringify(state).includes('"version":99'), false);
      WorkspaceAdmission.assertTelegramConsolidatedAdmissionSection(state.profiles.default?.admission, "default");
      await tools.get("telegram_message")!.execute("fixture-recovered-send", { text: "Recovered outbound fixture", chat_id: 77 });
      assert.equal(outbound, 1, "Actual production outbound delivery succeeds through the recovered admission ledger");
      const logs = await readFile(Paths.resolveTelegramRuntimeLogPath(agentDir), "utf8");
      assert.match(logs, /state-reset/u, "the reset leaves one diagnostic event");
    } finally {
      await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "reload" }, ctx);
      restoreFetch();
      if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
      await rm(agentDir, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
    }
  }, 15000);
}

test("Extension runtime polls, pairs, and dispatches an inbound Telegram turn into pi", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const sentMessages: RuntimeHarnessMessage[] = [];
  let resolveDispatch: ((value: RuntimeHarnessMessage) => void) | undefined;
  const dispatched = new Promise<RuntimeHarnessMessage>((resolve) => {
    resolveDispatch = resolve;
  });
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      sentMessages.push(content);
      resolveDispatch?.(content);
    },
  });
  let getUpdatesCalls = 0;
  let sendMessageCalls = 0;
  const apiCalls: string[] = [];
  const restoreFetch = setRuntimeTestFetch(async (input) => {
    const method = getRuntimeTelegramApiMethod(input);
    apiCalls.push(method);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 42,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "hello from telegram",
            },
          },
        ]);
      }
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      sendMessageCalls += 1;
      if (sendMessageCalls === 1) {
        return createRuntimeTelegramApiErrorResponse(
          429,
          "temporary rate limit",
        );
      }
      return createRuntimeTelegramApiResponse({ message_id: 100 });
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({ botToken: "123:abc", lastUpdateId: 0 });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext();
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    const dispatchedContent = await dispatched;
    await flushMicrotasks();
    assert.equal(sentMessages.length, 1);
    assert.equal(Array.isArray(dispatchedContent), true);
    assert.equal(apiCalls.includes("sendMessage"), true);
    assert.equal(sendMessageCalls, 2);
    assert.equal(apiCalls.includes("sendChatAction"), true);
    const promptBlock = getRuntimeHarnessTextBlock(dispatchedContent);
    assert.equal(promptBlock.type, "text");
    assert.match(promptBlock.text ?? "", /^\[telegram\] hello from telegram$/);
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

for (const outcome of ["unchanged", "offered", "transferred", "discarded", "cancelled",
  "offered-without-wake", "observation-failed", "prompt-unchanged", "prompt-race",
  "prompt-completion-lost-ack", "discard-completion-lost-ack"] as const) {
strictFileTest(`Prepared donor receipt projection gates real dispatch (${outcome})`, async () => {
  const dir = await realpath(await mkdtemp(join(tmpdir(), "pi-telegram-donor-projection-")));
  const configPath = join(dir, "telegram.json");
  const botToken = "123:fixture-donor-projection";
  const owner = { instanceId: "donor", processId: process.pid,
    processBirthId: `${process.pid}:fixture-donor`, sessionGeneration: 1 };
  const recipient = { instanceId: "recipient", processId: process.pid + 1,
    processBirthId: `${process.pid + 1}:fixture-recipient`, sessionGeneration: 1 };
  let shutdown: (() => Promise<void>) | undefined;
  try {
    await writeFile(configPath, JSON.stringify({ profiles: { work: { botToken, allowedUserId: 7 } } }));
    const config = Config.createTelegramConfigStore({ agentDir: dir, configPath });
    const journalSerialization = Journal.createTelegramJournalSourceSerialization(() => join(dir, "journals.transaction"));
    await config.load(); config.activateProfile("work");
    const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({ botToken });
    const ledger = WorkspaceAdmission.createTelegramWorkspaceAdmissionLedger({
      path: join(dir, "workspace-admission.json"), profileKey: "fixture:work", owner,
      getProcessLiveness: () => "alive" });
    const options: Journal.TelegramInputJournalStoreOptions = {
      path: join(dir, "inbox.work.json"), profileName: "work", botIdentity,
      queueRuntimeIdentity: owner, workspaceAdmission: ledger,
      sourceAccess: { directory: dir, limits: {
        maxFiles: 1000, maxBytes: 10_000_000, maxEntries: 100, maxWork: 100_000 } },
      withSourceSerialization: journalSerialization,
      withPairingAdmission: publish => config.withPairingAdmission("work", botIdentity.tokenSha256, publish),
      getInputContext: () => ({ owner, recipientBindingKey: "workspace:donor" }),
    };
    const source = Journal.createTelegramInputJournalStore(options);
    const isPrompt = outcome.startsWith("prompt-");
    source.appendBatch([{ update_id: 1 }, { update_id: 2 }, { update_id: 3 }], 3);
    const input = source.acquireInput({ updateId: 1, recipientBindingKey: "workspace:donor" });
    source.startInput(input.receipt);
    const grouped = source.acquireInput({ updateId: 3, recipientBindingKey: "workspace:donor" });
    source.startInput(grouped.receipt);
    const queued = source.queueInputs({ queueKind: isPrompt ? "prompt" : "control", receiptId: "donor-receipt",
      receipts: [grouped.receipt, input.receipt] }).queueReceipt;
    assert.deepEqual(queued.sourceUpdateIds, [1, 3]);
    const independent = source.acquireInput({ updateId: 2, recipientBindingKey: "workspace:donor" });
    source.startInput(independent.receipt);
    const unrelated = source.queueInputs({ queueKind: "control", receiptId: "unrelated",
      receipts: [independent.receipt] }).queueReceipt;
    let readUnavailable = false;
    // Inject only unavailable observation / a lost return after the native durable completion.
    const boundSource: Journal.TelegramInputJournalStore = { ...source,
      read() {
        if (readUnavailable) throw new Error("fixture observation unavailable");
        return source.read();
      },
      completeQueued(receipts) {
        const result = source.completeQueued(receipts);
        if (outcome.endsWith("completion-lost-ack")) throw new Error("fixture completion ACK lost");
        return result;
      },
    };
    const journalBindingKey = Journal.createTelegramUpdateJournalBindingKey(options);
    const handoff = { queueKind: queued.queueKind, receiptId: queued.receiptId,
      sourceUpdateIds: queued.sourceUpdateIds, expectedOwner: queued.queueOwner, recipientOwner: recipient,
      handoffToken: Journal.createTelegramUpdateQueueHandoffToken() };
    const resolveBinding = Updates.createTelegramInputCustodyLifecycleBindingResolver({
      isEnabled: () => true,
      resolveInputJournal: () => ({ runtimeKey: "donor-runtime", recoveryKey: journalBindingKey, journal: boundSource }),
      getRecipientBindingKey: () => "workspace:donor",
    });
    let transport = true;
    const admission = Updates.createTelegramUpdateAdmissionRuntimeBinding<string>({ isFollowerRegistered: () => false });
    const assembly = Updates.createTelegramUpdateAdmissionLifecycleAssembly({ runtimeBinding: admission,
      worker: { getQueueOwnerIdentity: () => owner, isContextCurrent: (ctx: string) => ctx === "ctx",
        async defaultHandle() { assert.fail("Queued input must not replay its raw handler"); } },
      leader: { resolveBinding, hasAuthority: () => transport },
      follower: { resolveBinding: () => undefined, isRegistered: () => false,
        getGeneration: () => undefined, prepareUpdateForExecution: update => update },
    });
    shutdown = admission.onSessionShutdown;
    const store = Queue.createTelegramQueueStore<string>();
    const deferredDispatch = Queue.createTelegramDeferredQueueDispatchRuntime<string>();
    deferredDispatch.bind("ctx");
    const effects: string[] = [];
    const binding = Bindings.createTelegramQueueBindingRuntime({ store, admission, deferredDispatch,
      queue: { allocateItemOrder: () => 1 }, activeTurn: { has: () => false },
      lifecycle: { isCompactionInProgress: () => false, hasDispatchPending: () => false },
      transportStamp: { isActive: () => true }, isIdle: () => true, hasPendingMessages: () => false,
      updateStatus: () => {}, sendTextReply: async () => undefined,
      sendUserMessage: () => { effects.push("prompt"); },
      promptDispatch: { startTypingLoop: () => {}, onPromptDispatchFailure: () => {},
        onPromptDispatchStart: () => {
          if (outcome === "prompt-race") assembly.leader.offerQueueReceiptHandoff(handoff);
        } },
    });
    const base = { chatId: 7, replyToMessageId: 1, queueOrder: 1, laneOrder: 1,
      statusSummary: "retained work", admissionReceipts: [{ ...queued, journalBindingKey }] };
    const item: Queue.TelegramQueueItem<string> = isPrompt
      ? { ...base, kind: "prompt", queueLane: "default", sourceMessageIds: [1], queuedAttachments: [],
        content: [{ type: "text", text: "retained prompt" }], historyText: "" }
      : { ...base, kind: "control", controlType: "status", queueLane: "control",
        async execute() { effects.push("control"); } };
    binding.mutation.append(item, "ctx");
    await assembly.leader.onSessionStart("ctx");
    await new Promise<void>(done => setImmediate(done));
    assert.equal(admission.getSettlement()!.isItemReady(item), true);
    // Transport loss cannot revoke otherwise valid accepted local work.
    transport = false;
    assembly.leader.signal();
    await new Promise<void>(done => setImmediate(done));
    assert.equal(assembly.leader.getState()?.blockedReason, "authority-lost");
    if (outcome === "discarded") assembly.leader.discardQueueReceipt(handoff);
    else if (outcome === "offered-without-wake") {
      Journal.createTelegramInputJournalStore(options).offerQueuedHandoff(handoff);
    } else if (outcome === "offered" || outcome === "cancelled" || outcome === "transferred") {
      assembly.leader.offerQueueReceiptHandoff(handoff);
      if (outcome === "cancelled") assembly.leader.cancelQueueReceiptHandoff(handoff);
      if (outcome === "transferred") {
        const receiver = Journal.createTelegramInputJournalStore({ ...options, queueRuntimeIdentity: recipient,
          getInputContext: () => ({ owner: recipient, recipientBindingKey: "workspace:recipient" }) });
        receiver.acceptQueuedHandoff(handoff);
        // Retain donor memory, as after a lost acceptance acknowledgement.
      }
    }
    const settlement = admission.getSettlement()!;
    if (outcome === "observation-failed") {
      const before = source.read();
      readUnavailable = true;
      assert.equal(settlement.isItemReady(item), false);
      assert.equal(settlement.getQueueReceiptOwner(item.admissionReceipts![0]!), undefined);
      assert.throws(() => binding.dispatchNext("ctx"), /observation unavailable/);
      assert.deepEqual(effects, []);
      assert.equal(store.getQueuedItems().length, 1);
      assert.deepEqual(source.read(), before);
      readUnavailable = false;
      // A fresh observation restores unchanged accepted work without transport reacquisition or raw drain.
      assert.equal(settlement.isItemReady(item), true);
    }
    const before = source.read();
    if (outcome === "discard-completion-lost-ack") {
      assert.throws(() => binding.mutation.clear("ctx"), /could not be discarded durably/);
    } else binding.dispatchNext("ctx");
    await new Promise<void>(done => setImmediate(done));
    const executable = outcome === "unchanged" || outcome === "cancelled" ||
      outcome === "observation-failed" || outcome === "prompt-unchanged";
    assert.deepEqual(effects, executable ? [isPrompt ? "prompt" : "control"] : [],
      "Durable revocation or missing completion ACK must precede effects");
    if (!executable) {
      assert.equal(store.getQueuedItems().length, 1);
      assert.equal(settlement.isItemReady(item), false);
      assert.equal(settlement.getQueueReceiptOwner(item.admissionReceipts![0]!), undefined);
      if (outcome === "prompt-race") assert.ok(source.read().entries[0]!.queueHandoff);
      else if (outcome.endsWith("completion-lost-ack")) {
        assert.deepEqual(source.read().entries.map(entry => entry.updateId), [2]);
      } else assert.deepEqual(source.read(), before);
      const retained = source.read();
      assert.throws(() => binding.mutation.clear("ctx"), /could not be discarded durably/);
      assert.equal(store.getQueuedItems().length, 1);
      assert.deepEqual(source.read(), retained);
    } else assert.deepEqual(source.read().entries.map(entry => entry.updateId), [2]);
    assert.equal(settlement.isItemReady({ admissionReceipts: [{ ...unrelated, journalBindingKey }] }), true);
    if (outcome === "prompt-unchanged") {
      const receipt = item.admissionReceipts![0]!;
      const ids = receipt.sourceUpdateIds;
      receipt.sourceUpdateIds = [1];
      assert.throws(() => binding.mutation.clear("ctx"), /could not be discarded durably/);
      receipt.sourceUpdateIds = ids;
      item.admissionReceipts = [{ ...receipt }];
      assert.throws(() => binding.mutation.clear("ctx"), /could not be discarded durably/);
      item.admissionReceipts = [receipt];
      // Confirmed completion permits local cleanup only, even after worker replacement.
      const retained = source.read();
      await assembly.leader.onTransportChanged("ctx");
      await new Promise<void>(done => setImmediate(done));
      binding.dispatchNext("ctx");
      assert.deepEqual(effects, ["prompt"]);
      assert.equal(binding.mutation.clear("ctx"), 1);
      assert.deepEqual(source.read(), retained);
    }
    assert.deepEqual(ledger.read().leases, []);
  } finally { await shutdown?.(); await rm(dir, { recursive: true, force: true }); }
});
}

for (const outcome of ["accepted", "rejected", "lost-ack"] as const) {
strictFileTest(`Control reconciliation uses native serial handoff custody (${outcome})`, async () => {
  const dir = await realpath(await mkdtemp(join(tmpdir(), "pi-telegram-control-reconciliation-")));
  const botToken = "123:fixture-control-reconciliation";
  const configPath = join(dir, "telegram.json");
  const donorOwner = { instanceId: "donor", processId: process.pid,
    processBirthId: `${process.pid}:fixture-donor`, sessionGeneration: 1 };
  const recipientOwner = { instanceId: "recipient", processId: process.pid + 1,
    processBirthId: `${process.pid + 1}:fixture-recipient`, sessionGeneration: 1 };
  const target = { chatId: 7, threadId: 42 };
  const lifecycles: Array<Updates.TelegramUpdateAdmissionLifecycleRuntime<string>> = [];
  try {
    await writeFile(configPath, JSON.stringify({ profiles: { work: { botToken, allowedUserId: 7 } } }));
    const config = Config.createTelegramConfigStore({ agentDir: dir, configPath });
    const journalSerialization = Journal.createTelegramJournalSourceSerialization(() => join(dir, "journals.transaction"));
    await config.load(); config.activateProfile("work");
    const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({ botToken });
    const ledger = WorkspaceAdmission.createTelegramWorkspaceAdmissionLedger({ path: join(dir, "admission.json"),
      profileKey: "fixture:work", owner: donorOwner, getProcessLiveness: () => "alive" });
    const options: Journal.TelegramInputJournalStoreOptions = {
      path: join(dir, "inbox.work.json"), profileName: "work", botIdentity, queueRuntimeIdentity: donorOwner,
      workspaceAdmission: ledger, sourceAccess: { directory: dir, limits: {
        maxFiles: 1000, maxBytes: 10_000_000, maxEntries: 100, maxWork: 100_000 } },
      withSourceSerialization: journalSerialization,
      withPairingAdmission: publish => config.withPairingAdmission("work", botIdentity.tokenSha256, publish),
      getInputContext: () => ({ owner: donorOwner, recipientBindingKey: "workspace:donor" }),
    };
    const donor = Journal.createTelegramInputJournalStore(options);
    const recipient = Journal.createTelegramInputJournalStore({ ...options, queueRuntimeIdentity: recipientOwner,
      getInputContext: () => ({ owner: recipientOwner, recipientBindingKey: "workspace:recipient" }) });
    const key = Journal.createTelegramUpdateJournalBindingKey(options);
    donor.appendBatch([{ update_id: 1, message: { message_id: 11, chat: { id: 7, type: "private" },
      from: { id: 7, is_bot: false }, text: "/status" } }], 1);
    const input = donor.acquireInput({ updateId: 1, recipientBindingKey: "workspace:donor" });
    donor.startInput(input.receipt);
    const queued = donor.queueInputs({ queueKind: "control", receiptId: "fixture-control", receipts: [input.receipt] }).queueReceipt;
    const original = donor.read().entries;
    const createLifecycle = (journal: Journal.TelegramInputJournalStore, owner: typeof donorOwner, recipientBindingKey: string) => {
      const lifecycle = Updates.createTelegramUpdateAdmissionLifecycleRuntime<string>({
        resolveBinding: Updates.createTelegramInputCustodyLifecycleBindingResolver({ isEnabled: () => true,
          resolveInputJournal: () => ({ runtimeKey: owner.instanceId, recoveryKey: key, journal }),
          getRecipientBindingKey: () => recipientBindingKey }),
        createWorker: (port, binding) => Updates.createTelegramUpdateAdmissionWorkerRuntime({
          journal: port, inputCustody: binding.journal.inputCustody,
          getJournalBindingKey: () => key, getRecipientBindingKey: () => recipientBindingKey,
          getQueueOwnerIdentity: () => owner, hasAuthority: () => true,
          defaultHandle: async () => { assert.fail("Queued control must not replay raw input"); },
        }),
      });
      lifecycles.push(lifecycle);
      return lifecycle;
    };
    const donorLifecycle = createLifecycle(donor, donorOwner, "workspace:donor");
    const recipientLifecycle = createLifecycle(recipient, recipientOwner, "workspace:recipient");
    const donorQueue = Queue.createTelegramQueueStore<string>();
    const recipientQueue = Queue.createTelegramQueueStore<string>();
    const effects: string[] = [];
    const item: Queue.PendingTelegramControlItem<string> = { kind: "control", controlType: "status", chatId: 7,
      target, transportStamp: { profile: "work", generation: "fixture" }, replyToMessageId: 11,
      queueOrder: 1, queueLane: "control", laneOrder: 1, statusSummary: "status",
      admissionReceipts: [{ ...queued, journalBindingKey: key }], execute: async () => { effects.push("donor"); } };
    donorQueue.setQueuedItems([item]);
    const deferredDispatch = Queue.createTelegramDeferredQueueDispatchRuntime<string>();
    deferredDispatch.bind("recipient");
    const queueBinding = Bindings.createTelegramQueueBindingRuntime({ store: recipientQueue, deferredDispatch,
      admission: { getSettlement: () => recipientLifecycle,
        hasPendingQueueMutationForItem: recipientLifecycle.hasPendingQueueMutationForItem },
      queue: { allocateItemOrder: () => 2 }, activeTurn: { has: () => false },
      lifecycle: { isCompactionInProgress: () => false, hasDispatchPending: () => false },
      transportStamp: { isActive: () => true }, isIdle: () => true, hasPendingMessages: () => false,
      updateStatus: () => {}, sendTextReply: async () => undefined,
      promptDispatch: { startTypingLoop: () => {}, onPromptDispatchFailure: () => {}, onPromptDispatchStart: () => {} },
      sendUserMessage: () => { assert.fail("Control handoff must not start a model turn"); },
    });
    const accept = Updates.createTelegramQueueHandoffRecipientRuntime({
      staging: Queue.createTelegramQueueHandoffStagingRuntime({ liveStore: recipientQueue,
        createControlExecution: Updates.createTelegramQueueHandoffControlExecutionFactory({
          isContextCurrent: ctx => ctx === "recipient", showStatus: async () => { effects.push("recipient"); },
          openModelMenu: async () => { assert.fail("Wrong control reconstructed"); },
        }) }),
      getRecipientOwner: () => recipientOwner, getLifecycleForBinding: binding => binding === key ? recipientLifecycle : undefined,
      isTransportStampActive: () => true, dispatchNext: queueBinding.dispatchNext,
    });
    let transfers = 0;
    const reconcile = Updates.createTelegramQueueHandoffReconciler<string>({
      ownsDirect: () => true, isFollowerRegistered: () => false, isBusEnabled: () => true,
      listFollowers: () => [{ instanceId: "recipient", pid: recipientOwner.processId,
        processBirthId: recipientOwner.processBirthId, sessionGeneration: 1, registrationGeneration: "g1",
        connectedAtMs: 1, lastHeartbeatMs: 1, profileKey: "recipient", target }],
      createRecipientJournalBindingKey: () => key, getQueuedItems: donorQueue.getQueuedItems,
      getReceiptOwner: donorLifecycle.getQueueReceiptOwner, getLifecycleForReceipt: () => donorLifecycle,
      donorInstanceId: "donor", createHandoffToken: Journal.createTelegramUpdateQueueHandoffToken,
      createRequestId: () => "fixture-handoff", stageThroughFollower: async () => { throw Error("Wrong route"); },
      async routeThroughLeader(envelope) {
        transfers++;
        assert.equal("execute" in envelope.payload, false);
        const payload = structuredClone(envelope.payload);
        const decoded = Bus.parseTelegramBusEnvelope(Bus.encodeTelegramBusEnvelope({
          ...envelope, kind: "leader.offerQueueHandoff", payload,
        }));
        assert.ok(decoded?.kind === "leader.offerQueueHandoff", "The serial payload must survive the real wire codec");
        if (outcome === "rejected") return { kind: "bus.ack", requestId: envelope.requestId, ok: false };
        const result = await accept(decoded, "recipient");
        if (outcome === "lost-ack") throw new Error("fixture lost acceptance ACK");
        return { kind: "bus.ack", requestId: envelope.requestId, ok: true, result };
      },
      removeDonorItem: receipt => Queue.removeTelegramQueueItemByReceipt({ receipt, store: donorQueue }),
    });
    await donorLifecycle.onSessionStart("donor");
    await recipientLifecycle.onSessionStart("recipient");
    await new Promise<void>(done => setImmediate(done));
    await reconcile("donor");
    await new Promise<void>(done => setImmediate(done));
    assert.equal(transfers, 1);
    assert.deepEqual(effects, outcome === "rejected" ? [] : ["recipient"]);
    assert.deepEqual(donorQueue.getQueuedItems(), outcome === "accepted" ? [] : [item]);
    assert.deepEqual(recipientQueue.getQueuedItems(), []);
    assert.deepEqual(donor.read().entries, outcome === "rejected" ? original : []);
    assert.equal(donorLifecycle.isItemReady(item), outcome === "rejected");
    if (outcome === "lost-ack") {
      await reconcile("donor");
      assert.equal(transfers, 1, "Unknown post-effect outcome must not replay transport or the control");
      assert.deepEqual(effects, ["recipient"]);
    }
    assert.deepEqual(ledger.read().leases, []);
  } finally {
    for (const lifecycle of lifecycles) await lifecycle.onSessionShutdown();
    await rm(dir, { recursive: true, force: true });
  }
});
}

for (const stopped of [false, true]) {
strictFileTest(`Deferred media enqueue keeps custody behind its execution fence (${stopped ? "stopped" : "active"})`, async () => {
  const dir = await realpath(await mkdtemp(join(tmpdir(), "pi-telegram-deferred-custody-")));
  const botToken = "123:fixture-deferred-custody";
  const configPath = join(dir, "telegram.json");
  const owner = { instanceId: "deferred", processId: process.pid,
    processBirthId: `${process.pid}:fixture-deferred`, sessionGeneration: 1 };
  const entered = Promise.withResolvers<void>();
  const downloaded = Promise.withResolvers<void>();
  const references = Journal.createTelegramUpdateJournalReferenceRegistry({ maxActive: 1 });
  let shutdown: (() => Promise<void>) | undefined;
  let clearMedia: (() => void) | undefined;
  try {
    await writeFile(configPath, JSON.stringify({ profiles: { work: { botToken, allowedUserId: 7 } } }));
    await writeFile(join(dir, "fixture.txt"), "fixture attachment");
    const config = Config.createTelegramConfigStore({ agentDir: dir, configPath });
    const journalSerialization = Journal.createTelegramJournalSourceSerialization(() => join(dir, "journals.transaction"));
    await config.load(); config.activateProfile("work");
    const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({ botToken });
    const ledger = WorkspaceAdmission.createTelegramWorkspaceAdmissionLedger({ path: join(dir, "admission.json"),
      profileKey: "fixture:work", owner, getProcessLiveness: () => "alive" });
    const options: Journal.TelegramInputJournalStoreOptions = {
      path: join(dir, "inbox.work.json"), profileName: "work", botIdentity, queueRuntimeIdentity: owner,
      workspaceAdmission: ledger, sourceAccess: { directory: dir, limits: {
        maxFiles: 1000, maxBytes: 10_000_000, maxEntries: 100, maxWork: 100_000 } },
      withSourceSerialization: journalSerialization,
      withPairingAdmission: publish => config.withPairingAdmission("work", botIdentity.tokenSha256, publish),
      getInputContext: () => ({ owner, recipientBindingKey: "workspace:deferred" }),
    };
    const source = Journal.createTelegramInputJournalStore(options);
    const bindingKey = Journal.createTelegramUpdateJournalBindingKey(options);
    let queuedReports = 0;
    let queueCommits = 0;
    let completionCommits = 0;
    const boundSource: Journal.TelegramInputJournalStore = { ...source,
      queueInputs(input) {
        queueCommits++;
        assert.equal(references.list().length, 1, "Custody transition must have a participating reference");
        return source.queueInputs(input);
      },
      completeInput(input) {
        completionCommits++;
        assert.equal(references.list().length, 1);
        return source.completeInput(input);
      },
    };
    const message = { message_id: 11, chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false },
      media_group_id: "fixture-album", caption: "deferred attachment",
      document: { file_id: "fixture", file_unique_id: "fixture", file_name: "fixture.txt" } };
    const store = Queue.createTelegramQueueStore<string>();
    const prepareTurn = Turns.createTelegramPromptTurnRuntimePreparer<typeof message, string>({
      allocateQueueOrder: () => 1, getAdmissionScope: () => "fixture", getAdmissionJournalBinding: () => bindingKey,
      async downloadFile() { entered.resolve(); await downloaded.promise; return join(dir, "fixture.txt"); },
    });
    const enqueue = Queue.createTelegramPromptEnqueueController<typeof message, string>({
      ...store, prepareTurn, hasPendingDispatch: () => false,
      getFoldQueuedPromptsIntoHistory: () => false, setFoldQueuedPromptsIntoHistory: () => {},
      assertExecutionCurrent: messages => Updates.assertTelegramUpdateExecutionCurrent(messages[0]),
      updateStatus: () => {}, dispatchNextQueuedTelegramTurn: () => {},
    });
    const groups = Media.createTelegramMediaGroupController<typeof message, string>({
      setTimer: () => ({ unref() {} }) as ReturnType<typeof setTimeout>, clearTimer: () => {},
    });
    clearMedia = groups.clear;
    const media = Media.createTelegramMediaGroupDispatchRuntime({ mediaGroups: groups,
      onDeferredMessage: Updates.reportTelegramUpdateDeferred,
      async dispatchMessages(messages: typeof message[], ctx: string) {
        await enqueue.enqueue(messages, ctx, turn => {
          queuedReports++;
          Updates.reportTelegramQueueAdmission(messages, turn.admissionReceipts ?? []);
        });
      },
    });
    const admission = Updates.createTelegramUpdateAdmissionRuntimeBinding<string>({ isFollowerRegistered: () => false });
    const assembly = Updates.createTelegramUpdateAdmissionLifecycleAssembly({ runtimeBinding: admission,
      acquireSourceReference: (_role, binding) => references.acquire({
        referenceClass: "leader-lifecycle", recoveryKey: binding.recoveryKey }),
      worker: { getQueueOwnerIdentity: () => owner,
        defaultHandle: async (update: Updates.TelegramUpdateFlow, ctx: string) => {
          await media.handleMessage(update.message as typeof message, ctx);
        } },
      leader: { hasAuthority: () => true,
        resolveBinding: Updates.createTelegramInputCustodyLifecycleBindingResolver({ isEnabled: () => true,
          resolveInputJournal: () => ({ runtimeKey: "fixture", recoveryKey: bindingKey, journal: boundSource }),
          getRecipientBindingKey: () => "workspace:deferred" }) },
      follower: { resolveBinding: () => undefined, isRegistered: () => false,
        getGeneration: () => undefined, prepareUpdateForExecution: update => update },
    });
    shutdown = admission.onSessionShutdown;
    source.appendBatch([{ update_id: 1, message }], 1);
    await assembly.leader.onSessionStart("ctx");
    await new Promise<void>(done => setImmediate(done));
    assert.equal(assembly.leader.getState()?.deferredClaimCount, 1);
    const retained = source.read();
    assert.equal(retained.entries[0]?.inputClaim?.phase, "running");
    const flushing = groups.flushMessage(11).then(value => ({ value }), error => ({ error }));
    await entered.promise;
    if (stopped) { groups.clear(); await assembly.leader.onSessionShutdown(); }
    downloaded.resolve();
    const result = await flushing;
    await new Promise<void>(done => setImmediate(done));
    assert.equal(completionCommits, 0);
    if (stopped) {
      assert.ok("error" in result && result.error instanceof Error && result.error.name === "AbortError");
      assert.deepEqual(store.getQueuedItems(), []);
      assert.deepEqual(source.read(), retained, "Expired preparation must not settle outcome-unknown custody");
      assert.deepEqual([queuedReports, queueCommits], [0, 0]);
      assert.deepEqual(references.list(), []);
    } else {
      assert.deepEqual(result, { value: true });
      assert.deepEqual([queuedReports, queueCommits], [1, 1]);
      assert.equal(source.read().entries[0]?.state, "queued");
      assert.equal(admission.getSettlement()?.isItemReady(store.getQueuedItems()[0]!), true);
      assert.equal(references.list().length, 1);
    }
    assert.deepEqual(ledger.read().leases, []);
  } finally {
    downloaded.resolve(); clearMedia?.(); await shutdown?.(); await rm(dir, { recursive: true, force: true });
  }
});
}

for (const scenario of ["acknowledged", "lost-registration-reply"] as const) {
strictFileTest(`Telegram new gateway composes follower suspension, session registration and predecessor pending adoption (${scenario})`, async () => {
  let replyLost = false;
  const dir = await realpath(await mkdtemp(join(tmpdir(), "pi-telegram-new-session-custody-")));
  const contexts = Lifecycle.createTelegramSessionContextStore<ExtensionContext>();
  const references = Journal.createTelegramUpdateJournalReferenceRegistry({ maxActive: 1 });
  let sessionId = "session-a", sequence = 0, mayExecute = false, replacementStarted = false;
  const ctx = { cwd: "/fixture", sessionManager: { getSessionId: () => sessionId }, ui: { notify() {} } } as unknown as ExtensionContext;
  const registrationState = BusFollower.createTelegramBusFollowerRegistrationState();
  const registry = Bus.createTelegramBusFollowerRegistry();
  const journalSerialization = Journal.createTelegramJournalSourceSerialization(() => join(dir, "journals.transaction"));
  const recipientKey = "manual:same-process";
  const protocol = Bus.createTelegramBusProtocolIdentity({ runtimeBuild: "fixture-new",
    capabilities: [Bus.TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION] });
  const leaderPath = join(dir, "leader.sock"), followerPath = join(dir, "follower.sock");
  const lock = { busSocketPath: leaderPath, busSecret: "fixture" };
  const enteredPreparation = Promise.withResolvers<void>(), finishPreparation = Promise.withResolvers<void>();
  const replacementDone = Promise.withResolvers<void>();
  const failures: unknown[] = [], executions: { id: number; session: string }[] = [];
  const adoptions: (Journal.TelegramSessionPendingAdoptionResult | undefined)[] = [];
  const journals = Journal.createTelegramUpdateJournalBindingRuntime({
    base: { getProfileName: () => "work", getBotToken: () => "123:fixture-new-custody", getBotId: () => 7,
      withSourceSerialization: journalSerialization },
    getLeaderJournalPath: () => join(dir, "tmp", "pi-telegram", "inbox.work.json"),
    getFollowerJournalPath: (key, profileName, sid) => Paths.resolveTelegramSessionJournalPath(sid!, key, dir, profileName),
    getActiveFollowerBindingKey: () => recipientKey,
    getActiveFollowerSessionId: () => registrationState.getSessionId(sessionId), isFollowerRegistered: registrationState.isRegistered,
  });
  const oldSource = journals.createRecipientResolver(recipientKey, "session-a")()!;
  oldSource.journal.appendBatch([{ update_id: 1, message: { text: "old pending custody" } }]);
  let invoke!: Parameters<Commands.TelegramSessionActionRuntimeDeps["registerCommand"]>[1]["handler"];
  const action = Commands.createTelegramSessionActionRuntime({
    registerCommand(_name, options) { invoke = options.handler; },
    async sendUserMessage(text) {
      replacementStarted = true;
      try {
        assert.equal(typeof text, "string");
        if (typeof text !== "string") throw new Error("Expected the typed internal command");
        await invoke(text.split(" ")[1]!, { ...ctx, async newSession() {
          assert.equal(oldSource.journal.read().entries.some(entry => entry.updateId === 2), false, "Gateway follows exact source completion");
          await hooks.onSessionShutdown({ type: "session_shutdown", reason: "new" }, ctx);
          assert.equal(registrationState.isRegistered(), false);
          assert.ok(BusFollower.getTelegramFollowerSessionHandoff());
          assert.equal(workers.getState()?.phase, "stopped");
          assert.deepEqual(references.list(), [], "Old worker reference ends before successor preparation");
          sessionId = "session-b";
          await hooks.onSessionStart({ type: "session_start", reason: "new" }, ctx);
          return { cancelled: false };
        } } as Parameters<typeof invoke>[1]);
        replacementDone.resolve();
      } catch (error) { replacementDone.reject(error); }
    },
    notifyResult: async (_target, result) => { failures.push(result); }, recordRuntimeEvent: (_category, error) => { failures.push(error); },
  });
  action.register();
  let currentWorker: Updates.TelegramUpdateWorkerRuntime<ExtensionContext> | undefined;
  const workers = Updates.createTelegramUpdateAdmissionLifecycleRuntime<ExtensionContext>({
    resolveBinding: journals.resolveFollower,
    prepareBinding() { adoptions.push(journals.prepareActiveFollowerSuccession(() => registrationState.isRegistered())); },
    acquireSourceReference: binding => references.acquire({ referenceClass: "follower-lifecycle", recoveryKey: binding.recoveryKey }),
    createWorker(journal, binding) {
      const capturedSession = sessionId;
      currentWorker = Updates.createTelegramUpdateWorkerRuntime({ journal, getJournalBindingKey: () => binding.recoveryKey,
        hasAuthority: () => mayExecute && registrationState.isRegistered(),
        shouldHoldPendingInput: entry => capturedSession === "session-a" && entry.updateId === 1,
        shouldReviewHistoricalInput: entry => capturedSession === "session-a" && entry.updateId === 1,
        async executeUpdate(update) {
          executions.push({ id: update.update_id, session: capturedSession });
          if ((update.message as { text?: string } | undefined)?.text === "/new") assert.ok(action.scheduleAfterUpdate(update.update_id, { chatId: 7, threadId: 42, messageId: 2 }));
          return { kind: "complete" };
        },
        onUpdateCompleted: id => action.onUpdateCompleted(id),
      });
      return currentWorker;
    },
  });
  const baseLeaderHandler = BusLeader.createTelegramBusLeaderEnvelopeHandler({ authSecret: "fixture", protocolIdentity: protocol,
    followerRegistry: registry, provisionFollowerTarget: () => ({ chatId: 7, threadId: 42, slot: "A" }) });
  // The leader commits the successor registration, but the follower never observes its reply.
  async function leaderHandler(envelope: Bus.TelegramBusEnvelope) {
    const response = await baseLeaderHandler(envelope);
    if (scenario === "lost-registration-reply" && !replyLost && envelope.kind === "follower.register" &&
        envelope.registration.sessionId === "session-b") {
      replyLost = true;
      throw new Error("fixture lost registration reply");
    }
    return response;
  }
  const leader = Bus.createTelegramBusLocalServer({ socketPath: leaderPath,
    handleEnvelope: leaderHandler });
  const assembly = BusFollower.createTelegramBusFollowerRuntimeAssembly({ instanceId: "fixture-follower", registrationState,
    recordRuntimeEvent: () => {}, receiver: { socketPath: followerPath, getAuthSecret: () => "fixture", getContext: () => contexts.get(),
      getRecipientBindingKey: () => recipientKey,
      durableAdmission: BusFollower.createTelegramBusFollowerDurableAdmissionRuntime({ journal: workers, signalWorker: workers.signal }) },
    recovery: { getLeaderState: () => ({ kind: "inactive" }), setLifecyclePhase() {}, updateStatus() {}, promoteToLeader: async () => false },
    registration: { protocolIdentity: protocol, getFollowerBusSocketPath: () => followerPath, getProfileKey: () => recipientKey,
      createRequestId: () => `new:${++sequence}`, getSessionId: () => sessionId, getSessionGeneration: contexts.getGeneration,
      isContextActive: contexts.isCurrent, async onRegistered(current) {
        if (sessionId === "session-b") { enteredPreparation.resolve(); await finishPreparation.promise; }
        await workers.onTransportChanged(current);
      } },
  });
  const hooks = Lifecycle.createTelegramBridgeSessionLifecycleAssembly({ contextStore: contexts,
    queue: { getCurrentModel: () => undefined, loadConfig: async () => {}, setQueuedItems() {}, setCurrentModel() {}, setPendingModelSwitch() {},
      syncCounters() {}, syncFlags() {}, bindDeferredDispatchContext() {}, prepareTempDir: async () => {}, updateStatus() {},
      unbindDeferredDispatchContext() {}, discardQueuedItems() {}, clearModelMenuState() {}, getActiveTurnChatId: () => undefined,
      clearPreview: async () => {}, clearActiveTurn() {}, clearAbort() {} },
    follower: { instanceId: "fixture-follower", registrationState, registrationRuntime: assembly.registration,
      getLeaderState: () => ({ kind: "active-elsewhere", lock }), updateStatus() {}, recordRuntimeEvent(_category: string, error: unknown) {
        if (error instanceof Error) failures.push(error);
      }, async suspendPolling() { assembly.registration.stop(); await assembly.receiver.stop(); } },
    services: { resumeGroupedInput() {}, suspendGroupedInput() {}, delivery: { onSessionStart: async () => {}, onSessionShutdown: async () => {} },
      polling: { onSessionStart: workers.onSessionStart }, inboundWorker: { onSessionShutdown: workers.onSessionShutdown },
      capabilityMonitor: { start() {}, stop() {} }, queueWatchdog: { start() {}, stop() {} } },
  } as unknown as Parameters<typeof Lifecycle.createTelegramBridgeSessionLifecycleAssembly>[0]);
  const send = (id: number, text: string, generation: string) => Bus.sendTelegramBusLocalEnvelope({ socketPath: followerPath, timeoutMs: 1000,
    envelope: { kind: "leader.forwardMessage", requestId: `delivery:${++sequence}`, auth: "fixture", recipientInstanceId: "fixture-follower",
      recipientRegistrationGeneration: generation, delivery: Bus.createTelegramBusFollowerDeliveryIdentity({ kind: "leader.forwardMessage", recipientBindingKey: recipientKey, sourceUpdateId: id }),
      message: { message_id: id, chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, text, pi_telegram_source_update_id: id }, sentAtMs: 1 } });
  try {
    BusFollower.setTelegramFollowerSessionHandoff(undefined);
    await leader.start(); await hooks.onSessionStart({ type: "session_start", reason: "startup" }, ctx);
    assert.ok(await assembly.registration.registerWithLeader(ctx, lock));
    const oldGeneration = registrationState.getGeneration()!;
    let successorGeneration: string, oldPending: ReturnType<typeof oldSource.journal.read>;
    mayExecute = true;
    const ack = await send(2, "/new", oldGeneration);
    assert.equal(ack?.kind === "bus.ack" && ack.ok, true);
    if (scenario === "lost-registration-reply") {
      finishPreparation.resolve();
      await replacementDone.promise;
      assert.equal(replyLost, true);
      assert.equal(registry.get("fixture-follower")?.sessionId, "session-b", "Leader committed the successor");
      assert.equal(registrationState.isRegistered(), false);
      assert.equal(assembly.getReadySessionId(), undefined);
      assert.deepEqual(adoptions, [undefined], "No adoption without an observed successor registration");
      assert.deepEqual(oldSource.journal.read().entries.map(entry => entry.updateId), [1]);
      assert.deepEqual(executions, [{ id: 2, session: "session-a" }]);
      assert.ok(failures.length > 0 && failures.every(failure => /Telegram bus handler failed/.test(String(failure))),
        `The lost reply is reported as itself, not a protocol mismatch: ${failures.map(String).join("; ")}`);
      failures.length = 0;
      oldPending = oldSource.journal.read();
      const handoff = BusFollower.getTelegramFollowerSessionHandoff()!;
      assert.ok(handoff, "Session handoff stays available for retry");
      assert.ok(await assembly.registration.registerWithLeader(ctx, lock,
        { target: handoff.target, previousInstanceId: handoff.instanceId }));
      BusFollower.setTelegramFollowerSessionHandoff(undefined);
      successorGeneration = registrationState.getGeneration()!;
    } else {
    await Promise.race([enteredPreparation.promise, replacementDone.promise.then(() => { throw new Error(`Replacement ended before preparation: ${failures.map(String).join("; ")}`); })]);
    successorGeneration = registrationState.getGeneration()!;
    assert.notEqual(successorGeneration, oldGeneration);
    assert.equal(registry.get("fixture-follower")?.sessionId, "session-b");
    assert.equal(assembly.getReadySessionId(), undefined);
    oldPending = oldSource.journal.read();
    const refused = await send(1, "successor before preparation", successorGeneration);
    assert.equal(refused?.kind === "bus.ack" && refused.ok, false);
    assert.deepEqual(oldSource.journal.read(), oldPending);
    finishPreparation.resolve(); await replacementDone.promise;
    }
    assert.deepEqual(failures, []);
    assert.equal(assembly.getReadySessionId(), "session-b");
    assert.equal(BusFollower.getTelegramFollowerSessionHandoff(), undefined);
    assert.equal(journals.getActiveRecoveryKey(), workers.getJournalBindingKey());
    assert.notEqual(journals.getActiveRecoveryKey(), oldSource.recoveryKey);
    assert.deepEqual(references.list(), [{ referenceClass: "follower-lifecycle", recoveryKey: journals.getActiveRecoveryKey() }]);
    const late = await send(3, "stale predecessor delivery", oldGeneration);
    assert.equal(late?.kind === "bus.ack" && late.ok, false);
    assert.deepEqual(adoptions, [undefined, { adoptedUpdateIds: [1], retainedUpdateIds: [] }],
      "Startup records the first session; /new adopts its unclaimed pending input before the successor worker exists");
    assert.equal(oldPending.entries[0]?.updateId, 1);
    assert.deepEqual(oldSource.journal.read().entries, [], "Predecessor keeps only the committed adoption tombstone");
    assert.equal(journals.inspectSourceAbandonment(oldSource.recoveryKey, 1)?.operatorAuthorityId,
      `${Journal.TELEGRAM_SESSION_ADOPTION_AUTHORITY_PREFIX}session-b`);
    const next = await send(4, "successor after preparation", successorGeneration);
    assert.equal(next?.kind === "bus.ack" && next.ok, true);
    await currentWorker!.waitForDrain();
    assert.deepEqual(executions, [{ id: 2, session: "session-a" }, { id: 1, session: "session-b" }, { id: 4, session: "session-b" }]);
    assert.deepEqual(journals.resolveActive()!.journal.read().entries, []);
  } finally {
    finishPreparation.resolve(); if (replacementStarted) await replacementDone.promise.catch(() => {});
    assembly.registration.stop(); await assembly.receiver.stop(); await workers.onSessionShutdown(); await leader.stop();
    BusFollower.setTelegramFollowerSessionHandoff(undefined); await rm(dir, { recursive: true, force: true });
  }
});
}

for (const outcome of ["ready", "context-replaced", "preparation-failed", "context-refresh", "same-context-refresh", "refresh-failed"] as const) {
strictFileTest(`Follower registration fences native binding readiness (${outcome})`, async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-registration-readiness-"));
  const ctx = { cwd: "/fixture" };
  const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:fixture-readiness" });
  const oldJournal = Journal.createTelegramUpdateJournalStore({ path: join(dir, "old.json"), botIdentity });
  const nextJournal = Journal.createTelegramUpdateJournalStore({ path: join(dir, "next.json"), botIdentity });
  oldJournal.appendBatch([{ update_id: 1 }]);
  let selected = oldJournal;
  let releaseHandler!: () => void;
  const heldHandler = new Promise<void>(resolve => { releaseHandler = resolve; });
  let executions = 0;
  const executionContexts: typeof ctx[] = [];
  let activeCtx = ctx;
  let sessionGeneration = 1;
  // A prior generation's real unsettled handler keeps the replacement stop pending.
  let authority = true;
  const blockedLifecycle = Updates.createTelegramUpdateAdmissionLifecycleRuntime<typeof ctx>({
    resolveBinding: () => ({ runtimeKey: selected === oldJournal ? "old" : "next",
      recoveryKey: selected === oldJournal ? "old" : "next", journal: selected }),
    createWorker: journal => Updates.createTelegramUpdateWorkerRuntime({ journal,
      hasAuthority: () => authority,
      executeUpdate: async (_update, current) => {
        executions++; executionContexts.push(current); await heldHandler; return { kind: "complete" };
      },
    }),
  });
  const registrationState = BusFollower.createTelegramBusFollowerRegistrationState();
  const protocol = Bus.createTelegramBusProtocolIdentity({ runtimeBuild: "fixture",
    capabilities: [Bus.TELEGRAM_BUS_CAPABILITY_DURABLE_FOLLOWER_ADMISSION] });
  const leaderPath = join(dir, "leader.sock");
  const followerPath = join(dir, "follower.sock");
  const leader = Bus.createTelegramBusLocalServer({ socketPath: leaderPath,
    handleEnvelope: BusLeader.createTelegramBusLeaderEnvelopeHandler({
      authSecret: "fixture", protocolIdentity: protocol, followerRegistry: Bus.createTelegramBusFollowerRegistry(),
      provisionFollowerTarget: () => ({ chatId: 7, threadId: 42, slot: "A" }),
    }),
  });
  let enteredRegistration!: () => void;
  const registrationEntered = new Promise<void>(resolve => { enteredRegistration = resolve; });
  let sequence = 0;
  let contextActive = true;
  let failRefresh = false;
  const assembly = BusFollower.createTelegramBusFollowerRuntimeAssembly({ instanceId: "fixture-follower", registrationState,
    recordRuntimeEvent: () => {},
    receiver: { socketPath: followerPath, getAuthSecret: () => "fixture", getContext: () => activeCtx,
      getRecipientBindingKey: () => "fixture:recipient",
      durableAdmission: BusFollower.createTelegramBusFollowerDurableAdmissionRuntime({
        journal: blockedLifecycle, signalWorker: () => blockedLifecycle.signal(),
      }),
    },
    recovery: { getLeaderState: () => ({ kind: "inactive" }), setLifecyclePhase: () => {},
      updateStatus: () => {}, promoteToLeader: async () => false },
    registration: { protocolIdentity: protocol, getFollowerBusSocketPath: () => followerPath,
      createRequestId: () => `fixture:${++sequence}`, getProfileKey: () => "fixture:recipient",
      getSessionId: () => "fixture-session", getSessionGeneration: () => sessionGeneration,
      isContextActive: current => current === activeCtx && contextActive,
      async onRegistered(current) {
        const replacement = blockedLifecycle.onTransportChanged(current);
        enteredRegistration();
        await replacement;
        if (outcome === "preparation-failed" || failRefresh) throw new Error("fixture binding preparation failed");
      },
    },
  });
  let registration: Promise<boolean> | undefined;
  try {
    await blockedLifecycle.onSessionStart(ctx);
    await new Promise<void>(done => setImmediate(done));
    assert.equal(executions, 1);
    await blockedLifecycle.onSessionShutdown();
    await blockedLifecycle.onSessionStart(ctx);
    await new Promise<void>(done => setImmediate(done));
    assert.equal(blockedLifecycle.getState()?.blockedReason, "prior-generation-executing");
    selected = nextJournal;
    authority = false;
    await leader.start();
    registration = assembly.registration.registerWithLeader(ctx, { busSocketPath: leaderPath, busSecret: "fixture" });
    await registrationEntered;
    const generation = registrationState.getGeneration()!;
    assert.ok(generation);
    const send = (sourceUpdateId = 2) => Bus.sendTelegramBusLocalEnvelope({ socketPath: followerPath, timeoutMs: 1000,
      envelope: { kind: "leader.forwardMessage", requestId: `delivery:${++sequence}`, auth: "fixture",
        recipientInstanceId: "fixture-follower", recipientRegistrationGeneration: generation,
        delivery: Bus.createTelegramBusFollowerDeliveryIdentity({ kind: "leader.forwardMessage",
          recipientBindingKey: "fixture:recipient", sourceUpdateId }),
        message: { message_id: sourceUpdateId, chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false },
          text: "new binding only", pi_telegram_source_update_id: sourceUpdateId }, sentAtMs: 1 },
    });
    const before = oldJournal.read();
    const pending = await send();
    assert.equal(pending?.kind === "bus.ack" && pending.ok, false,
      "Published registration must not admit work into a retained old binding");
    assert.deepEqual(oldJournal.read(), before);
    assert.deepEqual(nextJournal.read().entries, []);
    if (outcome === "context-replaced") contextActive = false;
    releaseHandler();
    const prepared = outcome !== "context-replaced" && outcome !== "preparation-failed";
    if (outcome === "preparation-failed") {
      await assert.rejects(registration, /fixture binding preparation failed/);
      assert.equal(registrationState.getGeneration(), undefined);
    } else if (outcome === "context-replaced") {
      assert.equal(await registration, false);
      assert.equal(registrationState.getGeneration(), undefined);
    } else {
      assert.equal(await registration, true);
      const ready = await send();
      assert.equal(ready?.kind === "bus.ack" && ready.ok, prepared);
    }
    assert.deepEqual(oldJournal.read(), before);
    assert.deepEqual(nextJournal.read().entries.map(entry => entry.updateId), prepared ? [2] : []);
    assert.equal(executions, 1);
    if (outcome === "context-refresh" || outcome === "same-context-refresh" || outcome === "refresh-failed") {
      await blockedLifecycle.onSessionShutdown();
      if (outcome !== "same-context-refresh") activeCtx = { cwd: ctx.cwd };
      sessionGeneration++;
      const retained = nextJournal.read();
      const early = await send(3);
      assert.equal(early?.kind === "bus.ack" && early.ok, false,
        "An unchanged registration cannot admit into a stopped previous-session worker");
      assert.deepEqual(nextJournal.read(), retained);
      const refreshErrors: unknown[] = [];
      const refresh = BusFollower.createTelegramBusFollowerSessionRefreshHook({ registrationState,
        registrationRuntime: assembly.registration, getLeaderState: () => ({ kind: "inactive" }),
        isSessionActive: current => current === activeCtx, updateStatus: () => {},
        recordRuntimeEvent: (_category, value) => { if (value instanceof Error) refreshErrors.push(value); } });
      if (outcome === "refresh-failed") {
        failRefresh = true;
        await refresh({}, activeCtx);
        assert.equal(refreshErrors.length, 1);
        assert.equal(registrationState.getGeneration(), generation);
        const refused = await send(3);
        assert.equal(refused?.kind === "bus.ack" && refused.ok, false);
        assert.deepEqual(nextJournal.read(), retained);
        failRefresh = false;
      }
      await refresh({}, activeCtx);
      assert.equal(registrationState.getGeneration(), generation, "Session refresh must preserve bus membership");
      assert.notEqual(blockedLifecycle.getState()?.phase, "stopped", "Refresh must prepare the current worker");
      const after = await send(3);
      assert.equal(after?.kind === "bus.ack" && after.ok, true);
      authority = true; blockedLifecycle.signal();
      await new Promise<void>(done => setImmediate(done));
      assert.deepEqual(nextJournal.read().entries, []);
      assert.deepEqual(executionContexts, [ctx, activeCtx, activeCtx]);
    }
  } finally {
    releaseHandler();
    await registration?.catch(() => undefined);
    assembly.registration.stop();
    await assembly.receiver.stop(); await leader.stop();
    await blockedLifecycle.onSessionShutdown();
    await rm(dir, { recursive: true, force: true });
  }
});
}

for (const scenario of ["business-same-chat", "business-other-chat", "business-discard-failure",
  "reaction-held", "reaction-failed", "reaction-excluded", "reaction-excluded-repaired", "source-reference-released", "handoff-cancel-after-stop", "handoff-cancel-reference-denied",
  "handoff-accepted-lost-ack-after-stop", "denial-completion-active", "denial-completion-stopped"] as const) {
  strictFileTest(`Prepared pending mutations preserve namespace and accepted work (${scenario})`, async () => {
    const dir = await realpath(await mkdtemp(join(tmpdir(), "pi-telegram-pending-mutation-")));
    const configPath = join(dir, "telegram.json");
    const botToken = "123:fixture-pending-mutation";
    const owner = { instanceId: "pending", processId: process.pid,
      processBirthId: `${process.pid}:fixture-pending`, sessionGeneration: 1 };
    let shutdown: (() => Promise<void>) | undefined;
    let releaseReaction!: () => void;
    const heldReaction = new Promise<void>(resolve => { releaseReaction = resolve; });
    let businessObserved = 0;
    const unregister = Updates.registerTelegramUpdateHandler(update => {
      if (update && typeof update === "object" && "deleted_business_messages" in update) businessObserved++;
    });
    try {
      await writeFile(configPath, JSON.stringify({ profiles: { work: { botToken, allowedUserId: 7 } } }));
      const config = Config.createTelegramConfigStore({ agentDir: dir, configPath });
      const journalSerialization = Journal.createTelegramJournalSourceSerialization(() => join(dir, "journals.transaction"));
      await config.load(); config.activateProfile("work");
      const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({ botToken });
      const ledger = WorkspaceAdmission.createTelegramWorkspaceAdmissionLedger({
        path: join(dir, "workspace-admission.json"), profileKey: "fixture:work", owner,
        getProcessLiveness: () => "alive" });
      const options: Journal.TelegramInputJournalStoreOptions = {
        path: join(dir, "inbox.work.json"), profileName: "work", botIdentity,
        queueRuntimeIdentity: owner, workspaceAdmission: ledger,
        sourceAccess: { directory: dir, limits: {
          maxFiles: 1000, maxBytes: 10_000_000, maxEntries: 100, maxWork: 100_000 } },
        withSourceSerialization: journalSerialization,
        withPairingAdmission: publish => config.withPairingAdmission("work", botIdentity.tokenSha256, publish),
        getInputContext: () => ({ owner, recipientBindingKey: "workspace:pending" }),
      };
      const source = Journal.createTelegramInputJournalStore(options);
      const journalBindingKey = Journal.createTelegramUpdateJournalBindingKey(options);
      source.appendBatch([1, 2].map(update_id => ({ update_id, message: {
        chat: { id: 7, type: "private" }, from: { id: 7, is_bot: false }, message_id: update_id + 10,
        text: update_id === 1 ? "governed" : "independent",
      } })), 2);
      const items = [1, 2].map(id => {
        const input = source.acquireInput({ updateId: id, recipientBindingKey: "workspace:pending" });
        source.startInput(input.receipt);
        const receipt = source.queueInputs({ queueKind: "prompt", receiptId: `pending-${id}`,
          receipts: [input.receipt] }).queueReceipt;
        const item: Queue.PendingTelegramTurn = {
          kind: "prompt", chatId: 7, replyToMessageId: id + 10, sourceMessageIds: [id + 10],
          queueOrder: id, queueLane: id === 1 ? "default" : "priority", laneOrder: id,
          statusSummary: id === 1 ? "governed" : "independent", queuedAttachments: [], historyText: "",
          content: [{ type: "text", text: id === 1 ? "governed" : "independent" }],
          admissionReceipts: [{ ...receipt, journalBindingKey }],
        };
        return item;
      });
      const queuedEntries = source.read().entries;
      const completionAttempts: number[][] = [];
      const references = Journal.createTelegramUpdateJournalReferenceRegistry({ maxActive: 1 });
      let refuseObservation = false;
      let observationReads = 0;
      let cancellationCalls = 0;
      let nativeCancellationCalls = 0;
      const rawCompletionReferences: number[] = [];
      const boundSource: Journal.TelegramInputJournalStore = { ...source,
        read() {
          if (scenario === "source-reference-released") {
            assert.equal(references.list().length, 1, "Every actual source read needs a live participating reference");
            observationReads++;
            if (refuseObservation) throw new Error("fixture observation unavailable");
          }
          return source.read();
        },
        cancelQueuedHandoff(input) {
          if (scenario.startsWith("handoff-")) {
            cancellationCalls++;
            assert.equal(references.list().some(ref => ref.recoveryKey === journalBindingKey), true,
              "Late donor cancellation must hold its source reference");
          }
          nativeCancellationCalls++;
          return source.cancelQueuedHandoff(input);
        },
        completeInput(receipt) {
          rawCompletionReferences.push(references.list().length);
          return source.completeInput(receipt);
        },
        completeQueued(receipts) {
          completionAttempts.push(receipts.flatMap(receipt => [...receipt.sourceUpdateIds]));
          if (scenario === "business-discard-failure") throw new Error("fixture discard refused");
          return source.completeQueued(receipts);
        },
      };
      const resolveBinding = Updates.createTelegramInputCustodyLifecycleBindingResolver({
        isEnabled: () => true,
        resolveInputJournal: () => ({ runtimeKey: "pending-runtime", recoveryKey: journalBindingKey, journal: boundSource }),
        getRecipientBindingKey: () => "workspace:pending",
      });
      const removalAttempts: number[][] = [];
      let reactionCalls = 0;
      let denialReplies = 0;
      let binding!: Bindings.TelegramQueueBindingRuntime<string>;
      const updates = Updates.createTelegramPairedUpdateRuntime<string>({
        getAllowedUserId: config.getAllowedUserId, persistAllowedUserId: config.persistAllowedUserId,
        updateStatus: () => {}, removePendingMediaGroupMessages: ids => { removalAttempts.push(ids); },
        removeQueuedTelegramTurnsByMessageIds: (ids, ctx, scope) => {
          removalAttempts.push(ids); return binding.mutation.removeByMessageIds(ids, ctx, scope);
        },
        applyQueuedTelegramTurnReactionByMessageId: (id, disposition, ctx, scope) =>
          binding.mutation.applyReactionByMessageId(id, disposition, ctx, scope),
        flushPendingMediaGroupMessage: async () => {
          reactionCalls++; await heldReaction;
          if (scenario === "reaction-failed") throw new Error("fixture mutation outcome unknown");
          return false;
        },
        answerCallbackQuery: async () => {}, answerGuestQuery: async () => {},
        handleAuthorizedTelegramCallbackQuery: async () => {},
        sendTextReply: async () => { denialReplies++; await heldReaction; return undefined; },
        handleAuthorizedTelegramMessage: async () => { assert.fail("Queued input must not replay"); },
        handleAuthorizedTelegramEditedMessage: () => {},
      });
      let transport = true;
      const admission = Updates.createTelegramUpdateAdmissionRuntimeBinding<string>({ isFollowerRegistered: () => false });
      const assembly = Updates.createTelegramUpdateAdmissionLifecycleAssembly({ runtimeBinding: admission,
        acquireSourceReference(role, binding) {
          return references.acquire({ referenceClass: role === "leader" ? "leader-lifecycle" : "follower-lifecycle",
            recoveryKey: binding.recoveryKey });
        },
        worker: { getQueueOwnerIdentity: () => owner, isContextCurrent: (ctx: string) => ctx === "ctx",
          defaultHandle: updates.handleUpdate },
        leader: { resolveBinding, hasAuthority: () => transport },
        follower: { resolveBinding: () => undefined, isRegistered: () => false,
          getGeneration: () => undefined, prepareUpdateForExecution: update => update },
      });
      shutdown = admission.onSessionShutdown;
      const store = Queue.createTelegramQueueStore<string>();
      const deferredDispatch = Queue.createTelegramDeferredQueueDispatchRuntime<string>();
      deferredDispatch.bind("ctx");
      const effects: string[] = [];
      binding = Bindings.createTelegramQueueBindingRuntime({ store, admission, deferredDispatch,
        queue: { allocateItemOrder: () => 3 }, activeTurn: { has: () => false },
        lifecycle: { isCompactionInProgress: () => false, hasDispatchPending: () => false },
        transportStamp: { isActive: () => true }, isIdle: () => true, hasPendingMessages: () => false,
        updateStatus: () => {}, sendTextReply: async () => undefined,
        promptDispatch: { startTypingLoop: () => {}, onPromptDispatchFailure: () => {}, onPromptDispatchStart: () => {} },
        sendUserMessage: content => {
          effects.push(content.map(part => part.type === "text" ? part.text : "image").join(""));
          // Simulate agent_start consuming only the successfully handed-off prompt.
          store.setQueuedItems(store.getQueuedItems().filter(item => item.kind !== "prompt" || item.content !== content));
        },
      });
      for (const item of items) binding.mutation.append(item, "ctx");
      await assembly.leader.onSessionStart("ctx");
      await new Promise<void>(done => setImmediate(done));
      transport = false;
      assembly.leader.signal();
      await new Promise<void>(done => setImmediate(done));
      if (scenario.startsWith("denial-completion-")) {
        source.appendBatch([{ update_id: 3, message: { message_id: 13, chat: { id: 8, type: "private" },
          from: { id: 8, is_bot: false }, text: "unauthorized" } }], 3);
        transport = true; assembly.leader.signal();
        await new Promise<void>(done => setImmediate(done));
        assert.equal(denialReplies, 1, "The real denial branch must reach its awaited reply");
        const retained = source.read();
        const running = retained.entries.find(entry => entry.updateId === 3)!;
        assert.equal(running.inputClaim?.phase, "running");
        const stopped = scenario === "denial-completion-stopped";
        if (stopped) {
          await assembly.leader.onSessionShutdown();
          assert.deepEqual(references.list(), []);
        }
        releaseReaction();
        await new Promise<void>(done => setImmediate(done));
        if (stopped) {
          assert.deepEqual(source.read(), retained,
            "An awaited denial returning after stop must not complete retained running custody");
          assert.deepEqual(rawCompletionReferences, [], "No completion attempt may outlive execution authority");
          await assembly.leader.onSessionStart("ctx");
          await new Promise<void>(done => setImmediate(done));
        } else {
          assert.deepEqual(source.read().entries, queuedEntries);
          assert.deepEqual(rawCompletionReferences, [1]);
        }
        transport = false; assembly.leader.signal();
        await new Promise<void>(done => setImmediate(done));
        binding.dispatchNext("ctx"); binding.dispatchNext("ctx");
        assert.deepEqual(effects, ["independent", "governed"]);
        assert.deepEqual(source.read().entries, stopped ? [running] : []);
        assert.equal(denialReplies, 1, "Restart must not replay the interrupted denial");
        return;
      }
      if (scenario.startsWith("handoff-")) {
        const retainedItems = store.getQueuedItems();
        const recipientOwner = { ...owner, instanceId: "recipient", processId: process.pid + 1,
          processBirthId: `${process.pid + 1}:fixture-recipient` };
        let rejectRemote!: (error: Error) => void;
        const remote = new Promise<Queue.TelegramQueueHandoffStageResult>((_resolve, reject) => { rejectRemote = reject; });
        const receipt = items[0]!.admissionReceipts![0]!;
        const expectedOwner = assembly.leader.getQueueReceiptOwner(receipt)!;
        assert.ok(expectedOwner);
        const handoff = Updates.coordinateTelegramQueueHandoff({
          item: { ...items[0]!, target: { chatId: 7, threadId: 42 },
            transportStamp: { profile: "work", generation: "fixture" } },
          expectedOwner, recipientOwner, handoffToken: Journal.createTelegramUpdateQueueHandoffToken(),
          lifecycle: assembly.leader,
          stageRemote(input) {
            if (scenario === "handoff-accepted-lost-ack-after-stop") {
              const recipient = Journal.createTelegramInputJournalStore({ ...options,
                queueRuntimeIdentity: recipientOwner,
                getInputContext: () => ({ owner: recipientOwner, recipientBindingKey: "workspace:recipient" }) });
              recipient.acceptQueuedHandoff({ queueKind: receipt.queueKind, receiptId: receipt.receiptId,
                sourceUpdateIds: receipt.sourceUpdateIds, expectedOwner, recipientOwner, handoffToken: input.handoffToken });
            }
            return remote;
          },
          removeDonorItem: () => { assert.fail("A rejected/lost ACK must retain donor memory"); },
        });
        await assembly.leader.onSessionShutdown();
        assert.deepEqual(references.list(), []);
        const retained = source.read();
        const occupy = scenario === "handoff-cancel-reference-denied"
          ? references.acquire({ referenceClass: "polling-cursor", recoveryKey: "fixture:other" }) : undefined;
        rejectRemote(new Error("fixture remote ACK unavailable"));
        const result = await handoff;
        assert.equal(result.status, "retained");
        assert.equal(result.status === "retained" && result.cancelled, scenario === "handoff-cancel-after-stop");
        assert.equal(cancellationCalls, scenario === "handoff-cancel-reference-denied" ? 0 : 1);
        assert.equal(nativeCancellationCalls, scenario === "handoff-cancel-reference-denied" ? 0 : 1,
          "A retained post-accept result must come from the native CAS, not a swallowed reference assertion");
        occupy?.();
        assert.deepEqual(references.list(), []);
        if (scenario === "handoff-cancel-after-stop") assert.deepEqual(source.read().entries, queuedEntries);
        else assert.deepEqual(source.read(), retained, "Denied or post-accept cancellation cannot rewrite durable custody");
        assert.deepEqual(store.getQueuedItems(), retainedItems);
        assert.deepEqual(effects, []);
        assert.equal(admission.getSettlement()?.isItemReady(items[0]!), false);
        return;
      }
      if (scenario === "source-reference-released") {
        await assembly.leader.onSessionShutdown();
        assert.deepEqual(references.list(), []);
        const stoppedReads = observationReads;
        assert.equal(admission.getSettlement()?.isItemReady(items[0]!), false);
        assert.equal(assembly.leader.getQueueReceiptOwner(items[0]!.admissionReceipts![0]!), undefined);
        assert.equal(observationReads, stoppedReads, "Stopped receipt projections must not read a released source");
        binding.dispatchNext("ctx");
        assert.deepEqual(effects, [], "Observation cannot restore stopped execution authority");
        assert.ok(observationReads > stoppedReads, "Actual queue dispatch observes pending mutations");
        assert.deepEqual(references.list(), [], "A stopped-source observation releases its scoped reference");
        const occupy = references.acquire({ referenceClass: "polling-cursor", recoveryKey: "fixture:other" });
        const deniedReads = observationReads;
        assert.throws(() => admission.hasPendingQueueMutationForItem(items[0]!), /reference registry is unavailable or full/);
        assert.equal(observationReads, deniedReads, "Reference denial must precede source I/O");
        occupy();
        refuseObservation = true;
        assert.throws(() => admission.hasPendingQueueMutationForItem(items[0]!), /fixture observation unavailable/);
        assert.deepEqual(references.list(), [], "Failed observations also release their reference");
        refuseObservation = false;
        assert.equal(assembly.leader.getJournalEntryCount(), 2);
        assert.deepEqual(source.read().entries, queuedEntries);
        assert.deepEqual(references.list(), []);
        transport = true;
        await assembly.leader.onSessionStart("ctx");
        await new Promise<void>(done => setImmediate(done));
        transport = false;
        assembly.leader.signal();
        await new Promise<void>(done => setImmediate(done));
        binding.dispatchNext("ctx");
        binding.dispatchNext("ctx");
        assert.deepEqual(effects, ["independent", "governed"], "Restart preserves accepted work across transport loss");
        assert.deepEqual(source.read().entries, []);
        await assembly.leader.onTransportChanged(undefined);
        assert.deepEqual(references.list(), []);
        const forgottenReads = observationReads;
        assert.equal(admission.hasPendingQueueMutationForItem(items[0]!), false);
        assert.throws(() => assembly.leader.getJournalEntryCount(), /worker is not active/);
        assert.equal(observationReads, forgottenReads, "A forgotten source cannot be observed");
        return;
      }
      const business = scenario.startsWith("business-");
      const mutation = business
        ? { update_id: 3, deleted_business_messages: { business_connection_id: "separate-business-account",
          chat: { id: scenario === "business-other-chat" ? 8 : 7, type: "private" }, message_ids: [11] } }
        : { update_id: 3, message_reaction: { chat: { id: 7, type: "private" }, user: { id: 7, is_bot: false },
          message_id: 11, old_reaction: [], new_reaction: [{ type: "emoji", emoji: "👎" }] } };
      const excluded = scenario === "reaction-excluded" || scenario === "reaction-excluded-repaired";
      if (excluded) {
        config.setProfile("work", { botToken });
        await config.persist();
        assert.equal(config.getAllowedUserId(), undefined);
      }
      source.appendBatch([mutation], 3);
      if (excluded) {
        const veto = source.read().entries.find(entry => entry.updateId === 3)!;
        assert.equal(veto.preApprovalExcluded, true);
        if (scenario === "reaction-excluded-repaired") {
          assert.equal(await config.persistAllowedUserId(7), true);
          source.appendBatch([mutation], 3);
        }
        const reopened = Journal.createTelegramInputJournalStore(options);
        assert.deepEqual(reopened.read().entries.find(entry => entry.updateId === 3), veto,
          "Re-pairing, duplicate append and reopening cannot turn an excluded mutation into intent");
        const beforeMixedRemoval = source.read();
        assert.throws(() => Updates.createTelegramInputCustodyWorkerJournalPort(source).removeCompleted([3, 1]));
        assert.deepEqual(source.read(), beforeMixedRemoval, "A mixed request cannot partially remove the exclusion");
        binding.dispatchNext("ctx");
        binding.dispatchNext("ctx");
        assert.deepEqual(effects, ["independent", "governed"],
          "An immutable exclusion cannot strand previously accepted work after transport loss");
        assert.equal(admission.hasPendingQueueMutationForItem(items[0]!), false);
        assert.deepEqual(store.getQueuedItems(), []);
        assert.deepEqual(source.read().entries, [veto], "Observation does not settle or rewrite the excluded input");
        if (scenario === "reaction-excluded-repaired") await assembly.leader.onSessionShutdown();
        transport = true;
        if (scenario === "reaction-excluded-repaired") await assembly.leader.onSessionStart("ctx");
        else assembly.leader.signal();
        await new Promise<void>(done => setImmediate(done));
        assert.deepEqual(source.read().entries, [], "Restoration or restart must drain excluded input without execution");
        assert.equal(source.read().acceptedThroughUpdateId, 3);
        assert.deepEqual(effects, ["independent", "governed"]);
        assert.equal(reactionCalls, 0);
        assert.deepEqual(ledger.read().leases, []);
        return;
      }
      assert.equal(admission.hasPendingQueueMutationForItem(items[0]!), !business);
      assert.equal(admission.hasPendingQueueMutationForItem(items[1]!), false);
      if (!business) {
        binding.dispatchNext("ctx");
        binding.dispatchNext("ctx");
        assert.deepEqual(effects, ["independent"], "Transport loss preserves unrelated accepted progress");
        assert.equal(store.getQueuedItems().length, 1);
      }
      transport = true;
      assembly.leader.signal();
      if (business) {
        await new Promise<void>(done => setImmediate(done));
        assert.deepEqual({ removalAttempts, completionAttempts,
          queued: store.getQueuedItems().map(item => item.queueOrder).sort(),
          durable: source.read().entries.map(entry => entry.updateId),
        }, { removalAttempts: [], completionAttempts: [], queued: [1, 2], durable: [1, 2] },
        "Business deletion cannot govern an independent bot-chat namespace, even with equal chat/message IDs");
        assert.deepEqual(source.read().entries, queuedEntries);
        assert.equal(businessObserved, 1, "Raw companion handlers retain their observation surface");
      } else {
        await new Promise<void>(done => setImmediate(done));
        assert.equal(reactionCalls, 1, "A newer explicit wake must survive an older authority-lost drain result");
        assert.equal(source.read().entries.find(entry => entry.updateId === 3)?.inputClaim?.phase, "running");
        binding.dispatchNext("ctx");
        assert.deepEqual(effects, ["independent"]);
        releaseReaction();
        await new Promise<void>(done => setImmediate(done));
        if (scenario === "reaction-held") {
          assert.equal(admission.hasPendingQueueMutationForItem(items[0]!), false);
          binding.dispatchNext("ctx");
          assert.deepEqual(store.getQueuedItems(), []);
          assert.deepEqual(source.read().entries, []);
        } else {
          assembly.leader.signal();
          await new Promise<void>(done => setImmediate(done));
          assert.equal(reactionCalls, 1, "Outcome-unknown mutation cannot be replayed");
          assert.equal(admission.hasPendingQueueMutationForItem(items[0]!), true);
          binding.dispatchNext("ctx");
          assert.equal(store.getQueuedItems().length, 1);
          assert.deepEqual(source.read().entries.map(entry => entry.updateId), [1, 3]);
        }
        assert.deepEqual(effects, ["independent"], "The governed prompt must not run after Skip or unknown intent");
      }
      assert.deepEqual(ledger.read().leases, []);
    } finally {
      releaseReaction(); unregister(); await shutdown?.(); await rm(dir, { recursive: true, force: true });
    }
  });
}

async function assertAsynchronousEnqueueProgress(foldHistory: boolean, deferAgentStart = false): Promise<void> {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-enqueue-race-"));
  const owner = {
    instanceId: `enqueue-${process.pid}`, processId: process.pid,
    processBirthId: ProcessIdentity.getTelegramProcessBirthIdentity(process.pid, "enqueue"),
    sessionGeneration: 1,
  };
  const journal = Journal.createTelegramUpdateJournalStore({
    path: join(dir, "inbox.json"),
    botIdentity: Journal.createTelegramUpdateJournalBotIdentity({ botToken: "123:enqueue-race" }),
    queueRuntimeIdentity: owner,
  });
  const store = Queue.createTelegramQueueStore<string>();
  const activeTurn = Queue.createTelegramActiveTurnStore();
  const entered = Promise.withResolvers<void>();
  const processing = Promise.withResolvers<void>();
  const callbacks: Array<() => void> = [];
  const deferred = Queue.createTelegramDeferredQueueDispatchRuntime<string>({
    setTimer(callback) {
      callbacks.push(callback);
      return { unref() {} } as ReturnType<typeof setTimeout>;
    },
    clearTimer: () => {},
  });
  deferred.bind("ctx");
  const flushDispatch = () => { for (const callback of callbacks.splice(0)) callback(); };
  let order = 0;
  let idle = false;
  let pending = false;
  let compacting = false;
  let piPending = false;
  const sent: number[] = [];
  const errors: string[] = [];
  const message = (id: number) => ({
    message_id: id, chat: { id: 7 }, pi_telegram_source_update_id: id,
    ...(id === 4
      ? { voice: { file_id: "voice-4", file_unique_id: "voice-4", duration: 1 } }
      : { text: `prompt-${id}` }),
  });
  const prepareTurn = Turns.createTelegramPromptTurnRuntimePreparer<ReturnType<typeof message>, string>({
    allocateQueueOrder: () => ++order,
    getAdmissionScope: () => "enqueue-race",
    getAdmissionJournalBinding: () => "enqueue-race",
    downloadFile: async () => join(dir, "voice.ogg"),
    async processAttachments(files, rawText) {
      if (files.length > 0) {
        entered.resolve();
        await processing.promise;
        rawText = "prompt-4";
      }
      return { rawText, promptFiles: files };
    },
  });
  let folding = false;
  const enqueue = Queue.createTelegramPromptEnqueueController<ReturnType<typeof message>, string>({
    ...store, prepareTurn,
    hasPendingDispatch: () => pending,
    getFoldQueuedPromptsIntoHistory: () => folding,
    setFoldQueuedPromptsIntoHistory: (value) => { folding = value; },
    updateStatus: () => {},
    dispatchNextQueuedTelegramTurn: (ctx) => binding.dispatchNext(ctx),
  });
  const worker = Updates.createTelegramUpdateWorkerRuntime<string>({
    journal, hasAuthority: () => true,
    getQueueOwnerIdentity: () => owner,
    getJournalBindingKey: () => "enqueue-race",
    async executeUpdate(update, ctx) {
      const turn = await enqueue.enqueue([update.message as ReturnType<typeof message>], ctx);
      const receipt = turn.admissionReceipts!.find((entry) => entry.sourceUpdateIds.includes(update.update_id))!;
      return { kind: "queued", ...receipt };
    },
    onQueueReceiptCommitted: () => deferred.request(binding.dispatchNext),
  });
  const settlement = Updates.createTelegramQueueAdmissionSettlementRuntime(worker);
  const startAgentTurn = () => Queue.handleTelegramAgentStartRuntime({
    queuedItems: store.getQueuedItems(), hasPendingDispatch: pending, hasActiveTurn: activeTurn.has(),
    setQueuedItems: store.setQueuedItems, setActiveTurn: activeTurn.set,
    clearDispatchPending: () => { pending = false; },
    resetToolExecutions: () => {}, resetPendingModelSwitch: () => {},
    setFoldQueuedPromptsIntoHistory: () => {}, createPreviewState: () => {},
    startTypingLoop: () => {}, updateStatus: () => {},
  });
  const binding = Bindings.createTelegramQueueBindingRuntime({
    store, activeTurn, deferredDispatch: deferred,
    queue: { allocateItemOrder: () => ++order },
    lifecycle: { isCompactionInProgress: () => compacting, hasDispatchPending: () => pending },
    admission: { getSettlement: () => settlement, hasPendingQueueMutationForItem: () => false },
    transportStamp: { isActive: () => true },
    isIdle: () => idle, hasPendingMessages: () => piPending,
    updateStatus: () => {}, sendTextReply: async () => undefined,
    promptDispatch: {
      startTypingLoop: () => {},
      onPromptDispatchStart: () => { pending = true; },
      onPromptDispatchFailure: (_ctx, error) => { errors.push(error); },
    },
    sendUserMessage() {
      const head = store.getQueuedItems()[0]!;
      assert.equal(settlement.isItemReady(head), false);
      assert.equal(journal.read().entries.some((entry) => entry.updateId === head.replyToMessageId), false);
      sent.push(head.replyToMessageId);
      idle = false;
      if (!deferAgentStart || sent.length > 1) startAgentTurn();
    },
    recordRuntimeEvent: (_category, error) => { errors.push(String(error)); },
  });
  try {
    binding.watchdog.start("ctx");
    journal.appendBatch([1, 2, 3].map((id) => ({ update_id: id, message: message(id) })));
    worker.start("ctx");
    await worker.waitForDrain();
    flushDispatch();
    assert.deepEqual(sent, []);
    folding = foldHistory;
    journal.appendBatch([{ update_id: 4, message: message(4) }]);
    worker.signal();
    await entered.promise;
    idle = true;
    binding.watchdog.poke();
    assert.deepEqual(sent, [1]);
    processing.resolve();
    await worker.waitForDrain();
    flushDispatch();
    const remaining = foldHistory ? [4] : [2, 3, 4];
    if (deferAgentStart) {
      assert.equal(pending, true);
      assert.deepEqual(store.getQueuedItems().map((item) => item.replyToMessageId), [1, ...remaining]);
      startAgentTurn();
      assert.equal(activeTurn.getReplyToMessageId(), 1);
    }
    assert.deepEqual(store.getQueuedItems().map((item) => item.replyToMessageId), remaining);
    assert.deepEqual(store.getQueuedItems().flatMap((item) =>
      item.admissionReceipts!.flatMap((receipt) => receipt.sourceUpdateIds)), [2, 3, 4]);
    if (foldHistory) {
      const text = getRuntimeHarnessTextBlock((store.getQueuedItems()[0] as Queue.PendingTelegramTurn).content).text!;
      assert.match(text, /1\. prompt-2\n\n2\. prompt-3/);
      assert.doesNotMatch(text, /prompt-1/);
    }
    assert.deepEqual(journal.read().entries.map((entry) => entry.updateId), [2, 3, 4]);
    assert.equal(store.getQueuedItems().every(settlement.isItemReady), true);
    assert.deepEqual(sent, [1]);
    activeTurn.clear();
    idle = true;
    compacting = true;
    deferred.request(binding.dispatchNext);
    flushDispatch();
    compacting = false;
    piPending = true;
    binding.watchdog.poke();
    assert.deepEqual(sent, [1]);
    piPending = false;
    for (const id of remaining) {
      activeTurn.clear();
      idle = true;
      if (id === 3) binding.watchdog.poke();
      else { deferred.request(binding.dispatchNext); flushDispatch(); }
      assert.equal(sent.at(-1), id);
      await worker.waitForDrain();
    }
    binding.watchdog.poke();
    assert.deepEqual(sent, [1, ...remaining]);
    assert.deepEqual(store.getQueuedItems(), []);
    assert.deepEqual(journal.read().entries, []);
    assert.deepEqual(errors, []);
  } finally {
    processing.resolve();
    binding.watchdog.stop();
    deferred.unbind();
    await worker.stop();
    await rm(dir, { recursive: true, force: true });
  }
}

test("Asynchronous enqueue preserves receipt-owned FIFO progress across agent start and settlement", () =>
  assertAsynchronousEnqueueProgress(false));
test("Asynchronous enqueue folds only surviving history and committed receipts", () =>
  assertAsynchronousEnqueueProgress(true));
test("Asynchronous enqueue keeps the handed-off head until delayed agent_start", () =>
  assertAsynchronousEnqueueProgress(true, true));

test("Durable worker keeps a poison source while draining independent journal tail", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-poison-tail-"));
  const path = join(dir, "inbox.json");
  const journal = Journal.createTelegramUpdateJournalStore({
    path,
    botIdentity: Journal.createTelegramUpdateJournalBotIdentity({
      botToken: "123:poison-tail",
    }),
    getNowMs: () => 1_000,
  });
  journal.appendBatch([
    { update_id: 1, message: { text: "poison" } },
    { update_id: 2, message: { text: "valid-a" } },
    { update_id: 3, message: { text: "valid-b" } },
  ]);
  const executed: number[] = [];
  const worker = Updates.createTelegramUpdateWorkerRuntime({
    journal,
    hasAuthority: () => true,
    getNowMs: () => 2_000,
    classifyExecutionFailure: () => ({
      disposition: "terminal",
      failureClass: "invalid-update",
      summary: "Deterministic poison update.",
    }),
    executeUpdate(update) {
      executed.push(update.update_id);
      if (update.update_id === 1) throw new Error("poison");
      return { kind: "complete" };
    },
  });
  try {
    worker.start("ctx");
    await worker.waitForDrain();
    assert.deepEqual(executed, [1, 2, 3]);
    assert.deepEqual(
      journal.read().entries.map((entry) => ({
        updateId: entry.updateId,
        state: entry.state,
        failureClass: entry.failure?.failureClass,
      })),
      [{ updateId: 1, state: "retry-wait", failureClass: "invalid-update" }],
    );
    assert.equal(worker.getState().retryWaitCount, 1);
    assert.equal(worker.getState().failedCount, 0);
    assert.equal(worker.getState().journalEntryCount, 1);
  } finally {
    await worker.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("Live replacement process cannot replay or settle another process queue receipt", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-queue-owner-"));
  const path = join(dir, "inbox.json");
  const processBirthId = ProcessIdentity.getTelegramProcessBirthIdentity(
    process.pid,
    "fixture-owner",
  );
  const queueOwnerIdentity = {
    instanceId: `owner-${process.pid}`,
    processId: process.pid,
    processBirthId,
    sessionGeneration: 1,
  };
  const journal = Journal.createTelegramUpdateJournalStore({
    path,
    botIdentity: Journal.createTelegramUpdateJournalBotIdentity({
      botToken: "123:queue-owner-worker",
    }),
    queueRuntimeIdentity: {
      instanceId: queueOwnerIdentity.instanceId,
      processId: process.pid,
      processBirthId,
    },
  });
  journal.appendBatch([{ update_id: 1, message: { text: "once" } }]);
  const receipt = {
    queueKind: "prompt" as const,
    receiptId: "process-a-receipt",
    sourceUpdateIds: [1],
  };
  let processAHasTransport = true;
  let processAExecutions = 0;
  const processA = Updates.createTelegramUpdateWorkerRuntime({
    journal,
    hasAuthority: () => processAHasTransport,
    getQueueOwnerIdentity: () => queueOwnerIdentity,
    executeUpdate() {
      processAExecutions += 1;
      return { kind: "queued", ...receipt };
    },
  });
  try {
    processA.start("process-a-context");
    await processA.waitForDrain();
    assert.equal(processAExecutions, 1);
    assert.equal(processA.isQueueReceiptCommitted(receipt), true);

    processAHasTransport = false;
    const replacement = await runQueueOwnerReplacementProcess(path);
    assert.deepEqual(replacement, {
      executionCount: 0,
      foreignQueuedCount: 1,
      queuedClaimCount: 0,
      entryCount: 1,
      directCompletionError: "conflict",
    });
    assert.equal(journal.read().entries.length, 1);

    processA.completeQueueReceipts({
      receipts: [receipt],
      ctx: "process-a-context",
      reason: "prompt-handoff",
    });
    await processA.waitForDrain();
    assert.equal(processAExecutions, 1);
    assert.deepEqual(journal.read().entries, []);
  } finally {
    await processA.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("Transport takeover hands one live-owned prompt to one exact recipient journal", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-transport-handoff-"));
  const journalPath = join(dir, "inbox.json");
  const recipientJournalPath = journalPath;
  const ownersPath = join(dir, "owners.json");
  const socketPath = join(dir, "recipient.sock");
  const donorCwd = "/repo/queue-donor";
  const donorInstanceId = `donor-${process.pid}`;
  const donorProcessBirthId = ProcessIdentity.getTelegramProcessBirthIdentity(
    process.pid,
    donorInstanceId,
  );
  const donorOwnerIdentity = {
    instanceId: donorInstanceId,
    processId: process.pid,
    processBirthId: donorProcessBirthId,
    sessionGeneration: 1,
  };
  const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({
    botToken: "123:queue-owner-worker",
  });
  const donorJournalBindingKey = Journal.createTelegramUpdateJournalBindingKey({
    path: journalPath,
    botIdentity,
  });
  const journal = Journal.createTelegramUpdateJournalStore({
    path: journalPath,
    botIdentity,
    queueRuntimeIdentity: donorOwnerIdentity,
  });
  journal.appendBatch([{ update_id: 1, message: { text: "handoff once" } }]);
  const receipt = {
    queueKind: "prompt" as const,
    receiptId: "transport-handoff-receipt",
    sourceUpdateIds: [1],
    journalBindingKey: donorJournalBindingKey,
  };
  const target = { chatId: 7, threadId: 42 };
  const donorQueue = Queue.createTelegramQueueStore<string>();
  let donorExecutions = 0;
  let authenticatedHandoffs = 0;
  const donorWorker = Updates.createTelegramUpdateWorkerRuntime({
    journal,
    hasAuthority: () => true,
    getJournalBindingKey: () => donorJournalBindingKey,
    getQueueOwnerIdentity: () => donorOwnerIdentity,
    executeUpdate() {
      donorExecutions += 1;
      return { kind: "queued", ...receipt };
    },
    onQueueReceiptCommitted(committedReceipt) {
      donorQueue.setQueuedItems([{
        kind: "prompt",
        chatId: target.chatId,
        target,
        transportStamp: { profile: "default", generation: "1" },
        replyToMessageId: 10,
        queueOrder: 1,
        queueLane: "default",
        laneOrder: 1,
        statusSummary: "handoff once",
        admissionReceipts: [committedReceipt],
        sourceMessageIds: [10],
        queuedAttachments: [],
        content: [{ type: "text", text: "handoff once" }],
        historyText: "handoff once",
      }]);
    },
  });
  const donorLifecycle = Updates.createTelegramUpdateAdmissionLifecycleRuntime<string>({
    resolveBinding: () => ({
      runtimeKey: journalPath,
      recoveryKey: donorJournalBindingKey,
      journal,
    }),
    getQueueOwnerIdentity: () => donorOwnerIdentity,
    createWorker: () => donorWorker,
  });
  const donorLock = Locks.createTelegramLockRuntime<{ cwd: string }>({
    locksPath: ownersPath,
    instanceId: donorInstanceId,
  });
  let replacement:
    | QueueOwnerTransportHandoffProcess
    | undefined;
  try {
    const acquired = donorLock.acquire({ cwd: donorCwd });
    assert.equal(acquired.ok, true);
    await donorLifecycle.onSessionStart("donor-context");
    await donorWorker.waitForDrain();
    assert.equal(donorExecutions, 1);
    assert.equal(donorQueue.getQueuedItems().length, 1);
    assert.equal(journal.read().entries[0]?.queueOwner?.processId, process.pid);

    replacement = spawnQueueOwnerTransportHandoffProcess({
      journalPath,
      recipientJournalPath,
      ownersPath,
      socketPath,
      authSecret: "handoff-secret",
      donorInstanceId,
      donorCwd,
      recipientInstanceId: "recipient-process-b",
      recipientProfileKey: "manual:recipient-process-b",
      recipientRegistrationGeneration: "recipient-generation-b",
      target,
    });
    const ready = await replacement.ready;
    assert.deepEqual(
      {
        phase: ready.phase,
        transportOwned: ready.transportOwned,
        executionCount: ready.executionCount,
        foreignQueuedCount: ready.foreignQueuedCount,
      },
      {
        phase: "ready",
        transportOwned: true,
        executionCount: 0,
        foreignQueuedCount: 1,
      },
    );
    assert.equal(donorLock.owns({ cwd: donorCwd }), false);
    assert.equal(process.pid > 0, true);

    const registry = Bus.createTelegramBusFollowerRegistry();
    registry.register({
      instanceId: "recipient-process-b",
      profileKey: "manual:recipient-process-b",
      target,
      busSocketPath: socketPath,
      registrationGeneration: "recipient-generation-b",
      protocol: Bus.createTelegramBusProtocolIdentity({
        runtimeBuild: "fixture",
        capabilities: [Bus.TELEGRAM_BUS_CAPABILITY_QUEUE_HANDOFF],
      }),
      pid: ready.pid,
      processBirthId: ready.processBirthId,
      sessionGeneration: 1,
      connectedAtMs: Date.now(),
    });
    const reconcile = Updates.createTelegramQueueHandoffReconciler<string>({
      ownsDirect: () => false,
      isFollowerRegistered: () => true,
      isBusEnabled: () => true,
      canHandoffWithLeader: () => true,
      listFollowers: () => registry.list().filter(
        (follower) => follower.instanceId !== donorInstanceId,
      ),
      createRecipientJournalBindingKey: () => ready.recipientJournalBindingKey,
      getQueuedItems: donorQueue.getQueuedItems,
      getReceiptOwner(receipt) {
        const owner = donorLifecycle.getQueueReceiptOwner(receipt);
        assert.ok(owner, `missing donor owner for ${JSON.stringify(receipt)}`);
        return owner;
      },
      getLifecycleForReceipt(receipt) {
        assert.equal(receipt.journalBindingKey, donorJournalBindingKey);
        return donorLifecycle;
      },
      createHandoffToken: Journal.createTelegramUpdateQueueHandoffToken,
      createRequestId: () => "transport-handoff:1",
      donorInstanceId,
      async stageThroughFollower(input) {
        const response = await Bus.sendTelegramBusLocalEnvelope({
          socketPath,
          timeoutMs: 1_000,
          envelope: {
            kind: "leader.offerQueueHandoff",
            requestId: "transport-handoff:1",
            auth: "handoff-secret",
            recipientInstanceId: input.recipient.instanceId,
            recipientRegistrationGeneration:
              input.recipient.registrationGeneration!,
            donorInstanceId,
            donorProcessId: input.expectedOwner.processId,
            donorProcessBirthId: input.expectedOwner.processBirthId,
            donorSessionGeneration: input.expectedOwner.sessionGeneration,
            donorAcquisitionId: input.expectedOwner.acquisitionId,
            donorAcquiredAtMs: input.expectedOwner.acquiredAtMs,
            handoffToken: input.handoffToken,
            payload: input.payload,
            sentAtMs: Date.now(),
          },
        });
        if (
          response?.kind !== "bus.ack" ||
          !response.ok ||
          !response.result ||
          typeof response.result !== "object"
        ) {
          throw new Error(
            response?.kind === "bus.ack"
              ? response.message ?? "handoff rejected"
              : "missing handoff response",
          );
        }
        authenticatedHandoffs += 1;
        return response.result as Queue.TelegramQueueHandoffStageResult;
      },
      async routeThroughLeader(input) {
        const response = await Bus.sendTelegramBusLocalEnvelope({
          socketPath,
          timeoutMs: 1_000,
          envelope: {
            kind: "leader.offerQueueHandoff",
            requestId: input.requestId,
            auth: input.auth,
            recipientInstanceId: input.recipientInstanceId,
            recipientRegistrationGeneration:
              input.recipientRegistrationGeneration,
            donorInstanceId: input.donorInstanceId,
            donorProcessId: input.donorProcessId,
            donorProcessBirthId: input.donorProcessBirthId,
            donorSessionGeneration: input.donorSessionGeneration,
            donorAcquisitionId: input.donorAcquisitionId,
            donorAcquiredAtMs: input.donorAcquiredAtMs,
            handoffToken: input.handoffToken,
            payload: input.payload,
            sentAtMs: input.sentAtMs,
          },
        });
        if (response?.kind === "bus.ack" && response.ok) {
          authenticatedHandoffs += 1;
        }
        return response ?? {
          kind: "bus.ack",
          requestId: input.requestId,
          ok: false,
          message: "missing handoff response",
        };
      },
      removeDonorItem(exactReceipt) {
        return Queue.removeTelegramQueueItemByReceipt({
          receipt: exactReceipt,
          store: donorQueue,
        });
      },
      recordFailure(error) {
        throw error;
      },
    });
    await reconcile("donor-context");

    assert.equal(registry.list().length, 1);
    assert.deepEqual(registry.list()[0]?.target, target);
    assert.equal(donorQueue.getQueuedItems().length, 0);
    assert.equal(authenticatedHandoffs, 1);
    assert.equal(donorExecutions, 1);
    assert.deepEqual(donorQueue.getQueuedItems(), []);
    assert.equal(journal.read().entries[0]?.queueOwner?.processId, ready.pid);
    assert.equal(journal.read().entries[0]?.queueHandoff, undefined);
    const recipientFile = JSON.parse(
      await readFile(recipientJournalPath, "utf8"),
    ) as { entries: unknown[] };
    assert.equal(recipientFile.entries.length, 1);

    const stopped = await replacement.stop();
    replacement = undefined;
    assert.deepEqual(stopped, {
      phase: "stopped",
      executionCount: 0,
      foreignQueuedCount: 1,
      donorEntryCount: 1,
      recipientEntryCount: 1,
      recipientQueueCount: 1,
      handoffCount: 1,
      controlExecutions: [],
      droppedHandoffAck: false,
    });
  } finally {
    if (replacement) {
      replacement.child.kill("SIGKILL");
      await new Promise((resolve) => replacement?.child.once("close", resolve));
    }
    await donorLifecycle.onSessionShutdown();
    donorLock.release();
    await rm(dir, { recursive: true, force: true });
  }
}, 10_000);

test("Queued control handoff reconstructs one local execution in the recipient process", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-control-handoff-"));
  const journalPath = join(dir, "inbox.json");
  const ownersPath = join(dir, "owners.json");
  const socketPath = join(dir, "recipient.sock");
  const donorCwd = "/repo/control-donor";
  const donorInstanceId = `control-donor-${process.pid}`;
  const donorProcessBirthId = ProcessIdentity.getTelegramProcessBirthIdentity(
    process.pid,
    donorInstanceId,
  );
  const donorOwnerIdentity = {
    instanceId: donorInstanceId,
    processId: process.pid,
    processBirthId: donorProcessBirthId,
    sessionGeneration: 1,
  };
  const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({
    botToken: "123:queue-owner-worker",
  });
  const donorJournalBindingKey = Journal.createTelegramUpdateJournalBindingKey({
    path: journalPath,
    botIdentity,
  });
  const journal = Journal.createTelegramUpdateJournalStore({
    path: journalPath,
    botIdentity,
    queueRuntimeIdentity: donorOwnerIdentity,
  });
  journal.appendBatch([{ update_id: 1, callback_query: { id: "control" } }]);
  const receipt = {
    queueKind: "control" as const,
    receiptId: "control-handoff-receipt",
    sourceUpdateIds: [1],
    journalBindingKey: donorJournalBindingKey,
  };
  const target = { chatId: 7, threadId: 44 };
  const donorQueue = Queue.createTelegramQueueStore<string>();
  let donorControlExecutions = 0;
  const donorWorker = Updates.createTelegramUpdateWorkerRuntime({
    journal,
    hasAuthority: () => true,
    getJournalBindingKey: () => donorJournalBindingKey,
    getQueueOwnerIdentity: () => donorOwnerIdentity,
    executeUpdate: () => ({ kind: "queued", ...receipt }),
    onQueueReceiptCommitted(committedReceipt) {
      donorQueue.setQueuedItems([{
        kind: "control",
        controlType: "status",
        chatId: target.chatId,
        target,
        transportStamp: { profile: "default", generation: "1" },
        replyToMessageId: 12,
        queueOrder: 1,
        queueLane: "control",
        laneOrder: 1,
        statusSummary: "status",
        admissionReceipts: [committedReceipt],
        execute: async () => {
          donorControlExecutions += 1;
        },
      }]);
    },
  });
  const donorLifecycle = Updates.createTelegramUpdateAdmissionLifecycleRuntime<string>({
    resolveBinding: () => ({
      runtimeKey: journalPath,
      recoveryKey: donorJournalBindingKey,
      journal,
    }),
    getQueueOwnerIdentity: () => donorOwnerIdentity,
    createWorker: () => donorWorker,
  });
  const donorLock = Locks.createTelegramLockRuntime<{ cwd: string }>({
    locksPath: ownersPath,
    instanceId: donorInstanceId,
  });
  let replacement: QueueOwnerTransportHandoffProcess | undefined;
  try {
    assert.equal(donorLock.acquire({ cwd: donorCwd }).ok, true);
    await donorLifecycle.onSessionStart("donor-context");
    await donorWorker.waitForDrain();
    replacement = spawnQueueOwnerTransportHandoffProcess({
      journalPath,
      recipientJournalPath: journalPath,
      ownersPath,
      socketPath,
      authSecret: "handoff-secret",
      donorInstanceId,
      donorCwd,
      recipientInstanceId: "recipient-control",
      recipientProfileKey: "manual:recipient-control",
      recipientRegistrationGeneration: "recipient-generation-control",
      target,
    });
    const ready = await replacement.ready;
    const expectedOwner = donorLifecycle.getQueueReceiptOwner(receipt);
    assert.ok(expectedOwner);
    const item = donorQueue.getQueuedItems()[0];
    assert.ok(item);
    const result = await Updates.coordinateTelegramQueueHandoff({
      item,
      expectedOwner,
      recipientOwner: {
        instanceId: "recipient-control",
        processId: ready.pid,
        processBirthId: ready.processBirthId,
        sessionGeneration: 1,
      },
      handoffToken: Journal.createTelegramUpdateQueueHandoffToken(),
      lifecycle: donorLifecycle,
      async stageRemote(input) {
        const response = await Bus.sendTelegramBusLocalEnvelope({
          socketPath,
          timeoutMs: 1_000,
          envelope: {
            kind: "leader.offerQueueHandoff",
            requestId: "control-handoff:1",
            auth: "handoff-secret",
            recipientInstanceId: "recipient-control",
            recipientRegistrationGeneration: "recipient-generation-control",
            donorInstanceId,
            donorProcessId: input.expectedOwner.processId,
            donorProcessBirthId: input.expectedOwner.processBirthId,
            donorSessionGeneration: input.expectedOwner.sessionGeneration,
            donorAcquisitionId: input.expectedOwner.acquisitionId,
            donorAcquiredAtMs: input.expectedOwner.acquiredAtMs,
            handoffToken: input.handoffToken,
            payload: {
              ...input.payload,
              admissionReceipts: [{
                ...input.payload.admissionReceipts[0]!,
                journalBindingKey: ready.recipientJournalBindingKey,
              }],
            },
            sentAtMs: Date.now(),
          },
        });
        if (
          response?.kind !== "bus.ack" ||
          !response.ok ||
          !response.result ||
          typeof response.result !== "object"
        ) {
          throw new Error("control handoff was rejected");
        }
        return response.result as Queue.TelegramQueueHandoffStageResult;
      },
      removeDonorItem: () =>
        Queue.removeTelegramQueueItemByReceipt({ receipt, store: donorQueue }),
    });
    assert.equal(result.status, "transferred");
    assert.equal(donorControlExecutions, 0);
    assert.deepEqual(donorQueue.getQueuedItems(), []);
    assert.equal(journal.read().entries[0]?.queueOwner?.processId, ready.pid);

    const stopped = await replacement.stop("execute-control");
    replacement = undefined;
    assert.deepEqual(stopped, {
      phase: "stopped",
      executionCount: 0,
      foreignQueuedCount: 1,
      donorEntryCount: 1,
      recipientEntryCount: 1,
      recipientQueueCount: 1,
      handoffCount: 1,
      controlExecutions: ["status"],
      droppedHandoffAck: false,
    });
  } finally {
    if (replacement) {
      replacement.child.kill("SIGKILL");
      await new Promise((resolve) => replacement?.child.once("close", resolve));
    }
    await donorLifecycle.onSessionShutdown();
    donorLock.release();
    await rm(dir, { recursive: true, force: true });
  }
}, 10_000);

test("Lost handoff ACK cannot cancel accepted cross-process authority", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-handoff-ack-loss-"));
  const journalPath = join(dir, "inbox.json");
  const ownersPath = join(dir, "owners.json");
  const socketPath = join(dir, "recipient.sock");
  const donorCwd = "/repo/ack-loss-donor";
  const donorInstanceId = `ack-loss-donor-${process.pid}`;
  const donorProcessBirthId = ProcessIdentity.getTelegramProcessBirthIdentity(
    process.pid,
    donorInstanceId,
  );
  const donorOwnerIdentity = {
    instanceId: donorInstanceId,
    processId: process.pid,
    processBirthId: donorProcessBirthId,
    sessionGeneration: 1,
  };
  const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({
    botToken: "123:queue-owner-worker",
  });
  const donorJournalBindingKey = Journal.createTelegramUpdateJournalBindingKey({
    path: journalPath,
    botIdentity,
  });
  const journal = Journal.createTelegramUpdateJournalStore({
    path: journalPath,
    botIdentity,
    queueRuntimeIdentity: donorOwnerIdentity,
  });
  journal.appendBatch([{ update_id: 1, message: { text: "ack lost" } }]);
  const receipt = {
    queueKind: "prompt" as const,
    receiptId: "ack-loss-receipt",
    sourceUpdateIds: [1],
    journalBindingKey: donorJournalBindingKey,
  };
  const target = { chatId: 7, threadId: 43 };
  const donorQueue = Queue.createTelegramQueueStore<string>();
  const donorWorker = Updates.createTelegramUpdateWorkerRuntime({
    journal,
    hasAuthority: () => true,
    getJournalBindingKey: () => donorJournalBindingKey,
    getQueueOwnerIdentity: () => donorOwnerIdentity,
    executeUpdate: () => ({ kind: "queued", ...receipt }),
    onQueueReceiptCommitted(committedReceipt) {
      donorQueue.setQueuedItems([{
        kind: "prompt",
        chatId: target.chatId,
        target,
        transportStamp: { profile: "default", generation: "1" },
        replyToMessageId: 11,
        queueOrder: 1,
        queueLane: "default",
        laneOrder: 1,
        statusSummary: "ack lost",
        admissionReceipts: [committedReceipt],
        sourceMessageIds: [11],
        queuedAttachments: [],
        content: [{ type: "text", text: "ack lost" }],
        historyText: "ack lost",
      }]);
    },
  });
  const donorLifecycle = Updates.createTelegramUpdateAdmissionLifecycleRuntime<string>({
    resolveBinding: () => ({
      runtimeKey: journalPath,
      recoveryKey: donorJournalBindingKey,
      journal,
    }),
    getQueueOwnerIdentity: () => donorOwnerIdentity,
    createWorker: () => donorWorker,
  });
  const donorLock = Locks.createTelegramLockRuntime<{ cwd: string }>({
    locksPath: ownersPath,
    instanceId: donorInstanceId,
  });
  let replacement: QueueOwnerTransportHandoffProcess | undefined;
  try {
    assert.equal(donorLock.acquire({ cwd: donorCwd }).ok, true);
    await donorLifecycle.onSessionStart("donor-context");
    await donorWorker.waitForDrain();
    replacement = spawnQueueOwnerTransportHandoffProcess({
      journalPath,
      recipientJournalPath: journalPath,
      ownersPath,
      socketPath,
      authSecret: "handoff-secret",
      donorInstanceId,
      donorCwd,
      recipientInstanceId: "recipient-ack-loss",
      recipientProfileKey: "manual:recipient-ack-loss",
      recipientRegistrationGeneration: "recipient-generation-ack-loss",
      target,
      dropHandoffAck: true,
    });
    const ready = await replacement.ready;
    const lifecycle = donorLifecycle;
    const expectedOwner = lifecycle.getQueueReceiptOwner(receipt);
    assert.ok(expectedOwner);
    const item = donorQueue.getQueuedItems()[0];
    assert.ok(item);
    let cancellationAttempts = 0;
    const result = await Updates.coordinateTelegramQueueHandoff({
      item,
      expectedOwner,
      recipientOwner: {
        instanceId: "recipient-ack-loss",
        processId: ready.pid,
        processBirthId: ready.processBirthId,
        sessionGeneration: 1,
      },
      handoffToken: Journal.createTelegramUpdateQueueHandoffToken(),
      lifecycle: {
        offerQueueReceiptHandoff: lifecycle.offerQueueReceiptHandoff,
        acceptQueueReceiptHandoff: lifecycle.acceptQueueReceiptHandoff,
        cancelQueueReceiptHandoff(input) {
          cancellationAttempts += 1;
          return lifecycle.cancelQueueReceiptHandoff(input);
        },
      },
      async stageRemote(input) {
        const response = await Bus.sendTelegramBusLocalEnvelope({
          socketPath,
          timeoutMs: 5_000,
          envelope: {
            kind: "leader.offerQueueHandoff",
            requestId: "ack-loss:1",
            auth: "handoff-secret",
            recipientInstanceId: "recipient-ack-loss",
            recipientRegistrationGeneration: "recipient-generation-ack-loss",
            donorInstanceId,
            donorProcessId: input.expectedOwner.processId,
            donorProcessBirthId: input.expectedOwner.processBirthId,
            donorSessionGeneration: input.expectedOwner.sessionGeneration,
            donorAcquisitionId: input.expectedOwner.acquisitionId,
            donorAcquiredAtMs: input.expectedOwner.acquiredAtMs,
            handoffToken: input.handoffToken,
            payload: {
              ...input.payload,
              admissionReceipts: [{
                ...input.payload.admissionReceipts[0]!,
                journalBindingKey: ready.recipientJournalBindingKey,
              }],
            },
            sentAtMs: Date.now(),
          },
        });
        throw new Error(`unexpected handoff response: ${JSON.stringify(response)}`);
      },
      removeDonorItem: () =>
        Queue.removeTelegramQueueItemByReceipt({ receipt, store: donorQueue }),
    });
    assert.equal(result.status, "retained");
    if (result.status !== "retained") assert.fail("expected retained result");
    assert.equal(result.cancelled, false);
    assert.equal(cancellationAttempts, 1);
    assert.equal(donorQueue.getQueuedItems().length, 1);
    assert.equal(journal.read().entries[0]?.queueOwner?.processId, ready.pid);
    assert.equal(journal.read().entries[0]?.queueHandoff, undefined);

    const stopped = await replacement.stop();
    replacement = undefined;
    assert.deepEqual(stopped, {
      phase: "stopped",
      executionCount: 0,
      foreignQueuedCount: 1,
      donorEntryCount: 1,
      recipientEntryCount: 1,
      recipientQueueCount: 1,
      handoffCount: 1,
      controlExecutions: [],
      droppedHandoffAck: true,
    });
  } finally {
    if (replacement) {
      replacement.child.kill("SIGKILL");
      await new Promise((resolve) => replacement?.child.once("close", resolve));
    }
    await donorLifecycle.onSessionShutdown();
    donorLock.release();
    await rm(dir, { recursive: true, force: true });
  }
}, 10_000);

test("Live owner remains fenced when replacement races dead-owner recovery", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-live-owner-race-"));
  const path = join(dir, "inbox.json");
  const processBirthId = ProcessIdentity.getTelegramProcessBirthIdentity(
    process.pid,
    "fixture-live-owner",
  );
  const ownerIdentity = {
    instanceId: `live-owner-${process.pid}`,
    processId: process.pid,
    processBirthId,
    sessionGeneration: 1,
  };
  const ownerJournal = Journal.createTelegramUpdateJournalStore({
    path,
    botIdentity: Journal.createTelegramUpdateJournalBotIdentity({
      botToken: "123:queue-owner-worker",
    }),
    queueRuntimeIdentity: {
      instanceId: ownerIdentity.instanceId,
      processId: process.pid,
      processBirthId,
    },
  });
  ownerJournal.appendBatch([
    { update_id: 1, message: { text: "live owner" } },
  ]);
  const receipt = {
    queueKind: "prompt" as const,
    receiptId: "process-a-receipt",
    sourceUpdateIds: [1],
  };
  const ownerWorker = Updates.createTelegramUpdateWorkerRuntime({
    journal: ownerJournal,
    hasAuthority: () => true,
    getQueueOwnerIdentity: () => ownerIdentity,
    executeUpdate: () => ({ kind: "queued", ...receipt }),
  });
  try {
    ownerWorker.start("live-owner-context");
    await ownerWorker.waitForDrain();
    const replacement = await runQueueOwnerReplacementProcess(
      path,
      "recover",
    );
    assert.deepEqual(replacement, {
      executionCount: 0,
      foreignQueuedCount: 1,
      queuedClaimCount: 0,
      entryCount: 1,
      directCompletionError: "conflict",
      recoveryStatus:
        "owner-alive",
    });
    assert.equal(ownerWorker.isQueueReceiptCommitted(receipt), true);
    ownerWorker.completeQueueReceipts({
      receipts: [receipt],
      ctx: "live-owner-context",
      reason: "prompt-handoff",
    });
    await ownerWorker.waitForDrain();
    assert.deepEqual(ownerJournal.read().entries, []);
  } finally {
    await ownerWorker.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("Live Windows queue owner remains unrecoverable and unreplayable without a birth proof", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-windows-owner-"));
  const path = join(dir, "inbox.json");
  const foreignOwner = {
    instanceId: "windows-owner",
    processId: 4242,
    processBirthId: "4242:generation:windows-owner",
    sessionGeneration: 1,
    acquisitionId: "windows-acquisition",
    acquiredAtMs: 1,
  };
  await writeFile(
    path,
    JSON.stringify({
      version: 1,
      profile: "default",
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({
        botToken: "123:queue-owner-worker",
      }),
      entries: [
        {
          updateId: 1,
          update: { update_id: 1, message: { text: "windows owner" } },
          admittedAtMs: 1,
          state: "queued",
          queueKind: "prompt",
          queueReceiptId: "windows-owner-receipt",
          queueOwner: foreignOwner,
        },
      ],
    }),
    "utf8",
  );
  const replacementIdentity = {
    instanceId: "windows-replacement",
    processId: 5252,
    processBirthId: "5252:generation:windows-replacement",
    sessionGeneration: 1,
  };
  const journal = Journal.createTelegramUpdateJournalStore({
    path,
    botIdentity: Journal.createTelegramUpdateJournalBotIdentity({
      botToken: "123:queue-owner-worker",
    }),
    queueRuntimeIdentity: replacementIdentity,
    getQueueProcessLiveness: (owner) =>
      ProcessIdentity.getTelegramProcessLiveness(owner, {
        platform: "win32",
        isProcessAlive: () => true,
      }),
  });
  let executionCount = 0;
  const worker = Updates.createTelegramUpdateWorkerRuntime({
    journal,
    hasAuthority: () => true,
    getQueueOwnerIdentity: () => replacementIdentity,
    executeUpdate() {
      executionCount += 1;
      return { kind: "complete" };
    },
  });
  try {
    worker.start("windows-replacement-context");
    await worker.waitForDrain();
    assert.equal(executionCount, 0);
    assert.equal(worker.getState().foreignQueuedCount, 1);
    const result = journal.recoverDeadQueueOwner({
      queueKind: "prompt",
      receiptId: "windows-owner-receipt",
      sourceUpdateIds: [1],
      deadOwner: foreignOwner,
      recoveryOwner: replacementIdentity,
    });
    assert.equal(result.status, "owner-unverifiable");
    worker.signal();
    await worker.waitForDrain();
    assert.equal(executionCount, 0);
    assert.deepEqual(journal.read().entries[0]?.queueOwner, foreignOwner);
    assert.throws(
      () =>
        journal.completeQueued([
          {
            queueKind: "prompt",
            receiptId: "windows-owner-receipt",
            sourceUpdateIds: [1],
            queueOwner: foreignOwner,
          },
        ]),
      (error) =>
        error instanceof Journal.TelegramUpdateJournalError &&
        error.code === "conflict",
    );
  } finally {
    await worker.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("Replacement registration stays live while its process races dead-owner recovery", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-registration-recovery-race-"));
  const journalPath = join(dir, "inbox.json");
  const socketPath = join(dir, "leader.sock");
  const startPath = join(dir, "start");
  const instanceId = "replacement-race";
  const botIdentity = Journal.createTelegramUpdateJournalBotIdentity({
    botToken: "123:queue-owner-worker",
  });
  await writeFile(
    journalPath,
    JSON.stringify({
      version: 1,
      profile: "default",
      botIdentity,
      entries: [{
        updateId: 1,
        update: { update_id: 1, message: { text: "race" } },
        admittedAtMs: 1,
        state: "queued",
        queueKind: "prompt",
        queueReceiptId: "registration-race-receipt",
        queueOwner: {
          instanceId,
          processId: 2_000_000_000,
          processBirthId: "2000000000:start:dead",
          sessionGeneration: 1,
          acquisitionId: "stale-acquisition",
          acquiredAtMs: 1,
        },
      }],
    }),
    "utf8",
  );
  try {
    const result = await runRegistrationRecoveryRaceProcess({
      journalPath,
      socketPath,
      startPath,
      instanceId,
      profileKey: "manual:replacement-race",
      registrationGeneration: "replacement-race:generation-1",
      target: { chatId: 7, threadId: 45, slot: "A" },
    });
    assert.deepEqual(result, {
      phase: "result",
      registrationOk: true,
      recoveryStatus: "owner-alive",
      registeredPid: result.registeredPid,
      registeredProcessBirthId: result.registeredProcessBirthId,
      ownerAlive: true,
      journalState: "queued",
      journalOwnerPid: result.registeredPid,
    });
    assert.equal(result.registeredPid > 0, true);
    assert.notEqual(result.registeredProcessBirthId, "2000000000:start:dead");
    const journal = Journal.createTelegramUpdateJournalStore({
      path: journalPath,
      botIdentity,
    });
    assert.equal(journal.read().entries[0]?.state, "queued");
    assert.equal(
      journal.read().entries[0]?.queueOwner?.acquisitionId,
      "stale-acquisition",
    );
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}, 10_000);

test("Replacement process discards dead session-owned queue authority", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-dead-owner-"));
  const path = join(dir, "inbox.json");
  const deadOwner = {
    instanceId: "dead-owner-instance",
    processId: 2_000_000_000,
    processBirthId: "2000000000:start:dead",
    sessionGeneration: 1,
    acquisitionId: "dead-acquisition",
    acquiredAtMs: 1,
  };
  await writeFile(
    path,
    JSON.stringify({
      version: 1,
      profile: "default",
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({
        botToken: "123:queue-owner-worker",
      }),
      entries: [
        {
          updateId: 1,
          update: { update_id: 1, message: { text: "recover once" } },
          admittedAtMs: 1,
          state: "queued",
          queueKind: "prompt",
          queueReceiptId: "process-a-receipt",
          queueOwner: deadOwner,
        },
      ],
    }),
    "utf8",
  );
  try {
    const replacement = await runQueueOwnerReplacementProcess(
      path,
      "recover",
    );
    assert.deepEqual(replacement, {
      executionCount: 0,
      foreignQueuedCount: 0,
      queuedClaimCount: 0,
      entryCount: 0,
      directCompletionError: "conflict",
      recoveryStatus: "recovered",
    });
    const journal = Journal.createTelegramUpdateJournalStore({
      path,
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({
        botToken: "123:queue-owner-worker",
      }),
    });
    assert.deepEqual(journal.read().entries, []);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Extension startup preserves queued authority owned by another process", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const agentDir = await ensureRuntimeAgentDir();
  const journalPath = join(agentDir, "tmp", "pi-telegram", "inbox.json");
  const journal = Journal.createTelegramUpdateJournalStore({
    path: journalPath,
    botIdentity: Journal.createTelegramUpdateJournalBotIdentity({
      botToken: "123:recovery",
    }),
  });
  const dispatched: RuntimeHarnessMessage[] = [];
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => dispatched.push(content),
  });
  const restoreFetch = setRuntimeTestFetch(async (input) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook" || method === "setMyCommands") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getMe") {
      return createRuntimeTelegramApiResponse({
        id: 123,
        username: "recovery_bot",
        has_topics_enabled: false,
      });
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:recovery",
      allowedUserId: 77,
      lastUpdateId: 502,
    });
    journal.appendBatch([
      {
        update_id: 501,
        message: {
          message_id: 501,
          chat: { id: 77, type: "private" },
          from: { id: 77, is_bot: false, first_name: "Owner" },
          text: "owned comment",
        },
      },
      {
        update_id: 502,
        message: {
          message_id: 502,
          chat: { id: 77, type: "private" },
          from: { id: 77, is_bot: false, first_name: "Owner" },
          text: "owned follow-up",
        },
      },
    ]);
    const queued = journal.markQueued({
      queueKind: "prompt",
      receiptId: "foreign-process-receipt",
      sourceUpdateIds: [501, 502],
      owner: {
        instanceId: "foreign-instance",
        processId: process.pid,
        processBirthId: ProcessIdentity.getTelegramProcessBirthIdentity(
          process.pid,
          "foreign-live-owner",
        ),
        sessionGeneration: 4,
      },
    });
    await writeRuntimeTelegramLocks({});
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext({ cwd: "/repo/journal-recovery" });
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await flushMicrotasks();
    await waitForTimeout(20);

    assert.deepEqual(dispatched, []);
    const snapshot = journal.read();
    assert.deepEqual(
      snapshot.entries.map((entry) => ({
        updateId: entry.updateId,
        state: entry.state,
        queueReceiptId: entry.queueReceiptId,
        queueOwner: entry.queueOwner,
      })),
      [
        {
          updateId: 501,
          state: "queued",
          queueReceiptId: "foreign-process-receipt",
          queueOwner: queued.queueOwner,
        },
        {
          updateId: 502,
          state: "queued",
          queueReceiptId: "foreign-process-receipt",
          queueOwner: queued.queueOwner,
        },
      ],
    );
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await rm(journalPath, { force: true });
    await writeRuntimeTelegramLocks({});
    await telegramConfig.restore();
  }
});

test("Extension runtime coalesces a cross-batch forward comment into one Pi turn", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const sentMessages: RuntimeHarnessMessage[] = [];
  let resolveDispatch!: (value: RuntimeHarnessMessage) => void;
  const dispatched = new Promise<RuntimeHarnessMessage>((resolve) => {
    resolveDispatch = resolve;
  });
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      sentMessages.push(content);
      resolveDispatch(content);
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            update_id: 10,
            message: {
              message_id: 50,
              chat: { id: 77, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Owner" },
              text: "Мой комментарий",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) {
        return createRuntimeTelegramApiResponse([
          {
            update_id: 11,
            message: {
              message_id: 51,
              chat: { id: 77, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Owner" },
              forward_origin: {
                type: "user",
                sender_user: {
                  id: 88,
                  is_bot: false,
                  first_name: "Source",
                  username: "source",
                },
              },
              text: "Пересланный текст",
            },
          },
        ]);
      }
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext();
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    const dispatchedContent = await dispatched;
    assert.equal(sentMessages.length, 1);
    const promptBlock = getRuntimeHarnessTextBlock(dispatchedContent);
    assert.equal(
      promptBlock.text,
      "[telegram] Мой комментарий\n\n[forward|from:source] Пересланный текст",
    );
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime fences queued final and preview after polling ownership moves away", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const extension = await getRuntimeTelegramExtension();
  let resolveDispatch: (() => void) | undefined;
  const dispatched = new Promise<void>((resolve) => {
    resolveDispatch = resolve;
  });
  const draftTexts: string[] = [];
  const sentTexts: string[] = [];
  const sentBodies: Array<Record<string, unknown>> = [];
  const editedTexts: string[] = [];
  let releasePolling: (() => void) | undefined;
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: () => {
      resolveDispatch?.();
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 7,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "please answer",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) {
        return new Promise<Response>((resolve) => {
          releasePolling = () => resolve(createRuntimeTelegramApiResponse([]));
        });
      }
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessageDraft") {
      draftTexts.push(String(body?.text ?? ""));
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "sendRichMessageDraft") {
      const richMessage = body?.rich_message as
        | { markdown?: string }
        | undefined;
      draftTexts.push(String(richMessage?.markdown ?? ""));
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "sendRichMessage") {
      const richMessage = body?.rich_message as
        | { markdown?: string }
        | undefined;
      sentTexts.push(String(richMessage?.markdown ?? ""));
      sentBodies.push(body ?? {});
      return createRuntimeTelegramApiResponse({
        message_id: 100 + sentTexts.length,
      });
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      sentTexts.push(String(body?.text ?? ""));
      sentBodies.push(body ?? {});
      return createRuntimeTelegramApiResponse({
        message_id: 100 + sentTexts.length,
      });
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "editMessageText") {
      editedTexts.push(String(body?.text ?? ""));
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    mock.timers.enable({ apis: ["setTimeout"] });
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    extension(pi);
    const ctx = createRuntimeExtensionContext();
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await dispatched;
    await handlers.get("agent_start")?.({}, ctx);
    await writeRuntimeTelegramLocks({
      default: {
        pid: process.pid + 1_000_000,
        cwd: "/tmp/other-pi-instance",
      },
    });
    mock.timers.tick(1100);
    await flushMicrotasks(20);
    await handlers.get("message_update")?.(
      {
        message: {
          role: "assistant",
          content: [{ type: "text", text: "Draft **preview**" }],
        },
      },
      ctx,
    );
    mock.timers.tick(1000);
    await flushMicrotasks(50);
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            content: [{ type: "text", text: "Final **answer**" }],
          },
        ],
      },
      ctx,
    );
    mock.timers.tick(0);
    await flushMicrotasks(20);
    await waitForTimeout(20);
    assert.deepEqual(draftTexts, []);
    assert.deepEqual(sentTexts, []);
    assert.deepEqual(sentBodies, []);
    assert.deepEqual(editedTexts, []);
    assert.deepEqual(
      (await readRuntimeTelegramLocks()).default,
      { pid: process.pid + 1_000_000, cwd: "/tmp/other-pi-instance" },
    );
    releasePolling?.();
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    mock.timers.reset();
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Cancelled threaded startup cannot restart health or overwrite a replacement after conflict teardown", async (t) => {
  t.mock.timers.enable({ apis: ["setInterval"] });
  for (const outcome of ["resolve", "reject"] as const) {
    for (const reconnect of [false, true]) {
      const dir = await mkdtemp(join(tmpdir(), "pi-telegram-conflict-startup-"));
      const ctx = { cwd: "/startup-fixture" };
      const lock = Locks.createTelegramLockRuntime<typeof ctx>({
        locksPath: join(dir, "owners.json"), instanceId: "fixture",
      });
      const state = Polling.createTelegramThreadCapabilityStateRuntime();
      let botState: Polling.TelegramThreadCapabilityState = {};
      let releaseOld!: () => void;
      let releaseNew!: () => void;
      let enteredOld!: () => void;
      let enteredNew!: () => void;
      const oldGate = new Promise<void>((resolve) => { releaseOld = resolve; });
      const newGate = new Promise<void>((resolve) => { releaseNew = resolve; });
      const oldWaiting = new Promise<void>((resolve) => { enteredOld = resolve; });
      const newWaiting = new Promise<void>((resolve) => { enteredNew = resolve; });
      let workers = 0;
      let healthStarts = 0;
      let probes = 0;
      const health = Sync.createTelegramLeaderHealthRuntime({
        callGetMe: async () => { probes++; },
        getSyncState: () => ({}), setSyncState: () => {}, recordEvent: () => {},
      });
      const controller = Polling.createTelegramPollingController<typeof ctx>({
        hasBotToken: () => true, stopTypingLoop: () => {}, updateStatus: () => {},
        runPollLoop: async (_ctx, signal) => {
          await new Promise<void>((resolve) => {
            if (signal.aborted) resolve();
            else signal.addEventListener("abort", () => resolve(), { once: true });
          });
        },
      });
      const admission = Polling.createTelegramPollingAdmissionRuntime({
        polling: controller, canStart: lock.owns,
        worker: { onSessionStart: async () => {
          workers++;
          if (workers === 2) {
            enteredOld();
            await oldGate;
            if (outcome === "reject") throw new Error("Old worker failed");
          } else if (workers === 3) {
            enteredNew();
            await newGate;
          }
        } },
      });
      const ports = Polling.createTelegramThreadAwarePollingPorts({
        getAllowedUserId: () => 1,
        callApi: async <TResponse,>() => ({ has_topics_enabled: true }) as TResponse,
        topicTargetStore: {
          load: async () => {}, persist: async () => {},
          getBotState: () => botState, setBotState: (value) => { botState = value; },
        },
        isBusRuntimeEnabled: state.isBusRuntimeEnabled,
        isTopicModeUnavailableError: () => true,
        getPollingStartedWithTelegramBus: state.isBusPollingStarted,
        setPollingStartedWithTelegramBus: state.setBusPollingStarted,
        setForceFreshLeaderThreadOnNextStart: state.setForceFreshLeaderThread,
        setTopicModeUnavailable: state.setTopicModeUnavailable,
        startClassicPolling: () => assert.fail("Obsolete startup must not fall back to classic polling"),
        stopClassicPolling: admission.stop,
        startBusLeaderPolling: admission.start, stopBusLeaderPolling: admission.stop,
        startLeaderHealth: () => { healthStarts++; health.start(); }, stopLeaderHealth: health.stop,
        registerFollowerWithLeader: async () => false, stopFollowerRegistration: () => {}, recordEvent: () => {},
      });
      const runtime = Locks.createTelegramLockedPollingRuntime({
        lock, hasBotToken: () => true, isContextCurrent: (context) => context === ctx,
        ownershipCheckMs: 1_000_000, ownershipRefreshMs: 1_000_000,
        startPolling: ports.startPolling, stopPolling: ports.stopPolling, updateStatus: () => {},
      });
      try {
        assert.equal((await runtime.start(ctx)).ok, true);
        const stale = runtime.start(ctx, { forceFreshLeaderThread: true });
        await oldWaiting;
        await runtime.onPersistentConflict(ctx, 10);
        assert.equal(controller.isActive(), false);
        assert.equal(lock.owns(ctx), false);
        const replacement = reconnect ? runtime.start(ctx, { forceFreshLeaderThread: true }) : undefined;
        if (replacement) await newWaiting;
        releaseOld();
        assert.equal((await stale).ok, false);
        assert.equal(healthStarts, 1, "Late completion must not restart leader health");
        assert.equal(state.isBusPollingStarted(), reconnect);
        assert.equal(state.shouldForceFreshLeaderThread(), reconnect, "Old finally must not clear replacement startup options");
        assert.equal(state.isTopicModeUnavailable(), false);
        t.mock.timers.tick(60_000);
        await flushMicrotasks();
        assert.equal(probes, 0, "Stopped health must have no surviving timer");
        if (replacement) {
          releaseNew();
          assert.equal((await replacement).ok, true);
          assert.equal(controller.isActive(), true);
          assert.equal(healthStarts, 2);
          t.mock.timers.tick(60_000);
          await flushMicrotasks();
          assert.equal(probes, 1, "Only the valid replacement starts health");
        }
      } finally {
        releaseOld();
        releaseNew();
        await runtime.stop();
        health.stop();
        await rm(dir, { recursive: true, force: true });
      }
    }
  }
});

for (const loss of ["ownership", "persistent-conflict"] as const) {
test(`Extension runtime preserves accepted work and fences delivery after ${loss}`, async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const sentMessages: RuntimeHarnessMessage[] = [];
  const sentBodies: Array<Record<string, unknown>> = [];
  const { handlers, commands, pi, getActiveTools } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      sentMessages.push(content);
    },
  });
  let getUpdatesCalls = 0;
  let releaseConflicts!: () => void;
  const conflictGate = new Promise<void>((resolve) => { releaseConflicts = resolve; });
  const ctx = createRuntimeExtensionContext({ cwd: "/repo/queue-owner-a" });
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 7,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "first accepted",
            },
          },
          {
            _: "other",
            update_id: 2,
            message: {
              message_id: 8,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "second queued",
            },
          },
        ]);
      }
      if (loss === "persistent-conflict") {
        await conflictGate;
        return createRuntimeTelegramApiErrorResponse(409,
          "Conflict: terminated by other getUpdates request");
      }
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendRichMessage") {
      sentBodies.push(body ?? {});
      return createRuntimeTelegramApiResponse({ message_id: 100 });
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      sentBodies.push(body ?? {});
      return createRuntimeTelegramApiResponse({ message_id: 100 });
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    await writeRuntimeTelegramLocks({});
    (await getRuntimeTelegramExtension())(pi);
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForCondition(() => sentMessages.length === 1);
    assert.match(
      getRuntimeHarnessMessageText(sentMessages[0] as RuntimeHarnessMessage),
      /^\[telegram\] first accepted$/,
    );
    // The transport section names the polling journal hosted by the leading session.
    const journalPath = Locks.readTelegramLockJournalPath((await readRuntimeTelegramLocks()).default)!;
    assert.match(journalPath, /sessions[\\/][^\\/]+[\\/]inbox\.json$/u);
    const runtimeJournal = Journal.createTelegramUpdateJournalStore({
      path: journalPath,
      botIdentity: Journal.createTelegramUpdateJournalBotIdentity({
        botToken: "123:abc",
      }),
    });
    await waitForAsyncCondition(async () =>
      runtimeJournal
        .read()
        .entries.some(
          (entry) => entry.updateId === 2 && entry.state === "queued",
        ),
    );
    await handlers.get("agent_start")?.({}, ctx);
    if (loss === "ownership") {
      await writeRuntimeTelegramLocks({
        default: { pid: process.pid + 1_000_000, cwd: "/repo/queue-owner-b" },
      });
    } else {
      assert.ok(getActiveTools().includes("telegram_message"));
      releaseConflicts();
      await waitForCondition(() => !getActiveTools().includes("telegram_message"), 30_000);
      assert.equal(getUpdatesCalls, 11);
      assert.equal(Locks.parseTelegramLockEntry((await readRuntimeTelegramLocks()).default), undefined, "Released: no owner remains");
      assert.equal(Locks.readTelegramLockJournalPath((await readRuntimeTelegramLocks()).default), journalPath, "The pointer keeps polling custody");
      assert.ok(runtimeJournal.read().entries.some((entry) => entry.updateId === 2 && entry.state === "queued"));
    }
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            content: [{ type: "text", text: "First **final**" }],
          },
        ],
      },
      ctx,
    );
    // Follow-up dispatch is intentionally routed through the session-bound
    // deferred queue timer; wait on real time instead of setImmediate turns.
    await waitForCondition(() => sentMessages.length === 2);
    assert.deepEqual(sentBodies, []);
    assert.match(
      getRuntimeHarnessMessageText(sentMessages[1] as RuntimeHarnessMessage),
      /^\[telegram\] second queued$/,
    );
  } finally {
    releaseConflicts();
    await handlers.get("session_shutdown")?.({}, ctx);
    restoreFetch();
    await telegramConfig.restore();
  }
}, 40_000);
}

test("Extension runtime ignores the retired proactive opt-out while Telegram is connected", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const sentBodies: Array<Record<string, unknown>> = [];
  const typingBodies: Array<Record<string, unknown>> = [];
  const { handlers, commands, pi, getActiveTools } = createRuntimePiHarness();
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      sentBodies.push(parseJsonRequestBody(init) ?? {});
      return createRuntimeTelegramApiResponse({ message_id: 100 });
    }
    if (method === "sendChatAction") {
      typingBodies.push(parseJsonRequestBody(init) ?? {});
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
      assistant: { proactivePush: false },
    });
    await writeRuntimeTelegramLocks({});
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext({
      cwd: "/repo/proactive-disabled-owner",
    });
    await handlers.get("session_start")?.({}, ctx);
    assert.deepEqual(getActiveTools(), ["read", "foreign_tool"]);
    await commands.get("telegram-connect")?.handler("", ctx);
    assert.deepEqual(getActiveTools(), [
      "read",
      "foreign_tool",
      "telegram_attach",
      "telegram_bind",
      "telegram_channel_post",
      "telegram_channel_posts",
      "telegram_message",
    ]);
    await flushMicrotasks(20);
    await handlers.get("input")?.(
      { source: "interactive", text: "local request" },
      ctx,
    );
    await handlers.get("agent_start")?.({}, ctx);
    await waitForCondition(() => typingBodies.length === 1);
    assert.deepEqual(typingBodies[0], { chat_id: 77, action: "typing" });
    const assistantMessage = {
      role: "assistant",
      content: [{ type: "text", text: "Local **done**" }],
    };
    await handlers.get("message_update")?.(
      {
        message: assistantMessage,
        assistantMessageEvent: {
          type: "text_end",
          contentIndex: 0,
          content: "Local **done**",
          partial: assistantMessage,
        },
      },
      ctx,
    );
    await handlers.get("message_update")?.(
      {
        message: assistantMessage,
        assistantMessageEvent: {
          type: "done",
          reason: "stop",
          message: assistantMessage,
        },
      },
      ctx,
    );
    await waitForCondition(() => sentBodies.length === 1);
    assert.equal(sentBodies[0]?.chat_id, 77);
    assert.match(
      String(
        (sentBodies[0]?.rich_message as { markdown?: string } | undefined)
          ?.markdown ?? "",
      ),
      /Local \*\*done\*\*/,
    );
    await handlers.get("agent_end")?.(
      { type: "agent_end", messages: [assistantMessage] },
      ctx,
    );
    await commands.get("telegram-disconnect")?.handler("", ctx);
    assert.deepEqual(getActiveTools(), ["read", "foreign_tool"]);
    assert.deepEqual(
      await handlers.get("before_agent_start")?.(
        { prompt: "local after disconnect", systemPrompt: "base" },
        ctx,
      ),
      { systemPrompt: "base" },
    );
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

strictFileTest("Channel post tool does not resend lost success or outcome across reconnect replacement", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const { handlers, commands, tools, pi } = createRuntimePiHarness();
  let sends = 0;
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") return createRuntimeTelegramApiResponse(true);
    if (method === "getUpdates") throw new DOMException("stop", "AbortError");
    if (method === "getChat") return createRuntimeTelegramApiResponse({
      id: -100123, type: "channel", username: "public_channel", title: "Public Channel",
    });
    if (method === "sendRichMessage") {
      sends += 1;
      const body = parseJsonRequestBody(init) ?? {};
      if ((body.rich_message as { markdown?: unknown } | undefined)?.markdown === "Ambiguous") {
        throw new Error("lost Bot API response");
      }
      return createRuntimeTelegramApiResponse({
        message_id: 91, chat: { id: -100123, type: "channel" },
      });
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({ botToken: "123:abc", allowedUserId: 77, lastUpdateId: 0 });
    await writeRuntimeTelegramLocks({});
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext({ cwd: "/repo/channel-replacement" });
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    const tool = tools.get("telegram_message");
    assert.ok(tool);
    await tool.execute("stable-channel-operation", {
      text: "Channel post", chat_id: -100123, channel: true,
    });
    await commands.get("telegram-disconnect")?.handler("", ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await tool.execute("stable-channel-operation", {
      text: "Channel post", chat_id: -100123, channel: true,
    });
    assert.equal(sends, 1);
    await assert.rejects(tool.execute("ambiguous-channel-operation", {
      text: "Ambiguous", chat_id: -100123, channel: true,
    }), /channel publication failed/u);
    await commands.get("telegram-disconnect")?.handler("", ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await assert.rejects(tool.execute("ambiguous-channel-operation", {
      text: "Ambiguous", chat_id: -100123, channel: true,
    }), /channel publication failed/u);
    assert.equal(sends, 2);
    await commands.get("telegram-disconnect")?.handler("", ctx);
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

strictFileTest("Channel media tool publishes one local upload and edits its caption without replay", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const { handlers, commands, tools, pi } = createRuntimePiHarness();
  const agentDir = await ensureRuntimeAgentDir();
  const mediaPath = join(agentDir, "channel-cover.jpg");
  await writeFile(mediaPath, Buffer.from("fake-jpeg-bytes"));
  const calls: Array<{ method: string; caption?: unknown; mediaPresent?: boolean }> = [];
  let sends = 0;
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") return createRuntimeTelegramApiResponse(true);
    if (method === "getUpdates") throw new DOMException("stop", "AbortError");
    if (method === "getChat") return createRuntimeTelegramApiResponse({
      id: -100123, type: "channel", username: "public_channel", title: "Public Channel",
    });
    if (method === "sendPhoto" || method === "sendVideo") {
      sends += 1;
      const body = init?.body;
      const fileField = method === "sendPhoto" ? "photo" : "video";
      calls.push({ method,
        caption: body instanceof FormData ? body.get("caption") : undefined,
        mediaPresent: body instanceof FormData && body.get(fileField) !== null });
      return createRuntimeTelegramApiResponse({
        message_id: 92, chat: { id: -100123, type: "channel" },
      });
    }
    if (method === "editMessageCaption") {
      calls.push({ method, caption: (parseJsonRequestBody(init) ?? {}).caption });
      return createRuntimeTelegramApiResponse({
        message_id: 92, chat: { id: -100123, type: "channel" },
      });
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({ botToken: "123:abc", allowedUserId: 77, lastUpdateId: 0 });
    await writeRuntimeTelegramLocks({});
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext({ cwd: "/repo/channel-media" });
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    const messageTool = tools.get("telegram_message");
    const mutationTool = tools.get("telegram_channel_post");
    const listTool = tools.get("telegram_channel_posts");
    assert.ok(messageTool);
    assert.ok(mutationTool);
    assert.ok(listTool);
    await messageTool.execute("stable-media-operation", {
      text: "Cover **title**", media: mediaPath, chat_id: -100123, channel: true,
    });
    assert.equal(sends, 1);
    await messageTool.execute("stable-media-operation", {
      text: "Cover **title**", media: mediaPath, chat_id: -100123, channel: true,
    });
    assert.equal(sends, 1);
    const listed = await listTool.execute("media-list", { chat_id: -100123, limit: 1 }) as {
      details: { records: Array<{ operationId: string; media?: { kind: string; fileName: string } }> } };
    assert.equal(listed.details.records[0]?.operationId, "stable-media-operation");
    assert.equal(listed.details.records[0]?.media?.kind, "photo");
    assert.equal(listed.details.records[0]?.media?.fileName, "channel-cover.jpg");
    await mutationTool.execute("media-edit-call", {
      action: "edit", operation_id: "stable-media-operation", markdown: "||Hidden|| update",
    });
    const videoPath = join(agentDir, "channel-clip.mp4");
    await writeFile(videoPath, Buffer.from("fake-mp4-bytes"));
    try {
      await messageTool.execute("stable-media-video-operation", {
        text: "Clip", media: videoPath, chat_id: -100123, channel: true,
      });
    } finally {
      await rm(videoPath, { force: true });
    }
    assert.deepEqual(calls.map(call => call.method),
      ["sendPhoto", "editMessageCaption", "sendVideo"]);
    assert.equal(calls[0]?.caption, "Cover <b>title</b>");
    assert.equal(calls[0]?.mediaPresent, true);
    assert.equal(calls[1]?.caption, "<tg-spoiler>Hidden</tg-spoiler> update");
    assert.equal(calls[2]?.caption, "Clip");
    assert.equal(calls[2]?.mediaPresent, true);
    await commands.get("telegram-disconnect")?.handler("", ctx);
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
    await rm(mediaPath, { force: true });
  }
});

test("Extension runtime resolves stale same-cwd lock before proactive local result", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const sentBodies: Array<Record<string, unknown>> = [];
  let getUpdatesCalls = 0;
  const { handlers, pi } = createRuntimePiHarness();
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendRichMessage") {
      sentBodies.push(parseJsonRequestBody(init) ?? {});
      return createRuntimeTelegramApiResponse({ message_id: 100 });
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      sentBodies.push(parseJsonRequestBody(init) ?? {});
      return createRuntimeTelegramApiResponse({ message_id: 100 });
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    const cwd = "/repo/proactive-stale-owner";
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
      assistant: { proactivePush: true },
    });
    await writeRuntimeTelegramLocks({
      default: {
        pid: process.pid + 1_000_000,
        cwd,
      },
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext({ cwd });
    await handlers.get("session_start")?.({}, ctx);
    await waitForEventLoopCondition(() => getUpdatesCalls >= 1, 5000);
    await handlers.get("input")?.(
      { source: "extension", text: "autonomous continuation" },
      ctx,
    );
    await handlers.get("agent_start")?.({}, ctx);
    const assistantMessage = {
      role: "assistant",
      content: [{ type: "text", text: "Local **done**" }],
    };
    await handlers.get("message_update")?.(
      {
        message: assistantMessage,
        assistantMessageEvent: {
          type: "text_end",
          contentIndex: 0,
          content: "Local **done**",
          partial: assistantMessage,
        },
      },
      ctx,
    );
    await handlers.get("message_update")?.(
      {
        message: assistantMessage,
        assistantMessageEvent: {
          type: "done",
          reason: "stop",
          message: assistantMessage,
        },
      },
      ctx,
    );
    await flushMicrotasks(20);
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            content: [{ type: "text", text: "Local **done**" }],
          },
        ],
      },
      ctx,
    );
    assert.equal(sentBodies.length, 1);
    assert.equal(sentBodies[0]?.chat_id, 77);
    assert.match(
      String(
        (sentBodies[0]?.rich_message as { markdown?: string } | undefined)
          ?.markdown ?? "",
      ),
      /Local \*\*done\*\*/,
    );
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime sends proactive checkpoints and final once in source order", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const sentBodies: Array<Record<string, unknown>> = [];
  const { handlers, commands, pi } = createRuntimePiHarness();
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendRichMessage") {
      sentBodies.push(parseJsonRequestBody(init) ?? {});
      return createRuntimeTelegramApiResponse({ message_id: 100 });
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      sentBodies.push(parseJsonRequestBody(init) ?? {});
      return createRuntimeTelegramApiResponse({ message_id: 100 });
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
      assistant: { proactivePush: true },
    });
    await writeRuntimeTelegramLocks({});
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext({
      cwd: "/repo/proactive-owner",
    });
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await flushMicrotasks(20);
    await handlers.get("input")?.(
      { source: "interactive", text: "local request" },
      ctx,
    );
    await handlers.get("agent_start")?.({}, ctx);
    const checkpointMessage = {
      role: "assistant",
      content: [{ type: "text", text: "Checkpoint **one**" }],
    };
    await handlers.get("message_update")?.(
      {
        message: checkpointMessage,
        assistantMessageEvent: {
          type: "text_end",
          contentIndex: 0,
          content: "Checkpoint **one**",
          partial: checkpointMessage,
        },
      },
      ctx,
    );
    await handlers.get("message_update")?.(
      {
        message: checkpointMessage,
        assistantMessageEvent: {
          type: "toolcall_start",
          contentIndex: 1,
          partial: checkpointMessage,
        },
      },
      ctx,
    );
    const reasoningMessage = {
      role: "assistant",
      content: [{ type: "thinking", thinking: "private reasoning" }],
    };
    await handlers.get("message_update")?.(
      {
        message: reasoningMessage,
        assistantMessageEvent: {
          type: "thinking_end",
          contentIndex: 0,
          content: "private reasoning",
          partial: reasoningMessage,
        },
      },
      ctx,
    );
    const assistantMessage = {
      role: "assistant",
      content: [{ type: "text", text: "Local **done**" }],
    };
    await handlers.get("message_update")?.(
      {
        message: assistantMessage,
        assistantMessageEvent: {
          type: "text_end",
          contentIndex: 0,
          content: "Local **done**",
          partial: assistantMessage,
        },
      },
      ctx,
    );
    await handlers.get("message_update")?.(
      {
        message: assistantMessage,
        assistantMessageEvent: {
          type: "done",
          reason: "stop",
          message: assistantMessage,
        },
      },
      ctx,
    );
    await flushMicrotasks(20);
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            content: [{ type: "text", text: "Local **done**" }],
          },
        ],
      },
      ctx,
    );
    await flushMicrotasks(20);
    assert.equal(sentBodies.length, 2);
    assert.deepEqual(
      sentBodies.map((body) => body.chat_id),
      [77, 77],
    );
    const sentMarkdown = sentBodies.map(
      (body) =>
        (body.rich_message as { markdown?: string } | undefined)?.markdown ?? "",
    );
    assert.match(sentMarkdown[0] ?? "", /Checkpoint \*\*one\*\*/);
    assert.match(sentMarkdown[1] ?? "", /Local \*\*done\*\*/);
    assert.equal(
      sentMarkdown.some((text) => text.includes("private reasoning")),
      false,
    );
    await commands.get("telegram-disconnect")?.handler("", ctx);
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime drops queued proactive blocks after session replacement", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  let sendCalls = 0;
  let getUpdatesCalls = 0;
  let markFirstSendStarted!: () => void;
  const firstSendStarted = new Promise<void>((resolve) => {
    markFirstSendStarted = resolve;
  });
  let releaseFirstSend!: () => void;
  const firstSendGate = new Promise<void>((resolve) => {
    releaseFirstSend = resolve;
  });
  const { handlers, pi } = createRuntimePiHarness();
  const restoreFetch = setRuntimeTestFetch(async (input) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendRichMessage") {
      sendCalls += 1;
      if (sendCalls === 1) {
        markFirstSendStarted();
        await firstSendGate;
      }
      return createRuntimeTelegramApiResponse({ message_id: 100 + sendCalls });
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    const cwd = "/repo/proactive-replacement";
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
      assistant: { proactivePush: true },
    });
    await writeRuntimeTelegramLocks({
      default: { pid: process.pid + 1_000_000, cwd },
    });
    (await getRuntimeTelegramExtension())(pi);
    const oldCtx = createRuntimeExtensionContext({ cwd });
    await handlers.get("session_start")?.({}, oldCtx);
    await waitForEventLoopCondition(() => getUpdatesCalls >= 1, 5000);
    await handlers.get("input")?.(
      { source: "extension", text: "replacement probe" },
      oldCtx,
    );
    await handlers.get("agent_start")?.({}, oldCtx);
    const checkpointMessage = {
      role: "assistant",
      content: [{ type: "text", text: "Old checkpoint" }],
    };
    await handlers.get("message_update")?.(
      {
        message: checkpointMessage,
        assistantMessageEvent: {
          type: "text_end",
          contentIndex: 0,
          content: "Old checkpoint",
          partial: checkpointMessage,
        },
      },
      oldCtx,
    );
    await handlers.get("message_update")?.(
      {
        message: checkpointMessage,
        assistantMessageEvent: {
          type: "toolcall_start",
          contentIndex: 1,
          partial: checkpointMessage,
        },
      },
      oldCtx,
    );
    const finalMessage = {
      role: "assistant",
      content: [{ type: "text", text: "Old queued final" }],
    };
    await handlers.get("message_update")?.(
      {
        message: finalMessage,
        assistantMessageEvent: {
          type: "text_end",
          contentIndex: 0,
          content: "Old queued final",
          partial: finalMessage,
        },
      },
      oldCtx,
    );
    await handlers.get("message_update")?.(
      {
        message: finalMessage,
        assistantMessageEvent: {
          type: "done",
          reason: "stop",
          message: finalMessage,
        },
      },
      oldCtx,
    );
    await firstSendStarted;
    await handlers.get("session_shutdown")?.({}, oldCtx);
    const newCtx = createRuntimeExtensionContext({ cwd });
    await handlers.get("session_start")?.({}, newCtx);
    releaseFirstSend();
    await flushMicrotasks(50);
    assert.equal(sendCalls, 1);
    await handlers.get("session_shutdown")?.({}, newCtx);
  } finally {
    releaseFirstSend();
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime skips proactive local result without Telegram lock ownership", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const sentBodies: Array<Record<string, unknown>> = [];
  const { handlers, pi } = createRuntimePiHarness();
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "sendMessage" || method === "sendRichMessage") {
      sentBodies.push(parseJsonRequestBody(init) ?? {});
      return createRuntimeTelegramApiResponse({ message_id: 100 });
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
      assistant: { proactivePush: true },
    });
    await writeRuntimeTelegramLocks({
      default: {
        pid: process.pid + 1_000_000,
        cwd: "/repo/another-instance",
      },
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext({
      cwd: "/repo/proactive-non-owner",
    });
    await handlers.get("session_start")?.({}, ctx);
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            content: [{ type: "text", text: "Local **done**" }],
          },
        ],
      },
      ctx,
    );
    assert.deepEqual(sentBodies, []);
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

for (const draftPreviews of [false, true]) {
test(`Extension runtime delivers anchored Telegram commentary once before final with preview ${draftPreviews ? "on" : "off"}`, async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const sentMessages: RuntimeHarnessMessage[] = [];
  const deliveredMarkdown: string[] = [];
  const replyAnchors: unknown[] = [];
  let dispatched = false;
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      sentMessages.push(content);
      dispatched = true;
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init) ?? {};
    if (method === "deleteWebhook" || method === "setMyCommands") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 10,
              chat: { id: 77, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "show checkpoints",
            },
          },
        ]);
      }
      return await new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          reject(new DOMException("stop", "AbortError"));
        });
      });
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "sendRichMessageDraft") return createRuntimeTelegramApiResponse(true);
    if (method === "sendRichMessage") {
      replyAnchors.push(body.reply_parameters);
      deliveredMarkdown.push(
        String(
          (body.rich_message as { markdown?: string } | undefined)?.markdown ??
            "",
        ),
      );
      return createRuntimeTelegramApiResponse({
        message_id: 100 + deliveredMarkdown.length,
      });
    }
    if (method === "sendMessage") {
      return createRuntimeTelegramApiResponse({ message_id: 200 });
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
      assistant: { activity: "quiet", proactivePush: true, draftPreviews },
    });
    await writeRuntimeTelegramLocks({});
    (await getRuntimeTelegramExtension())(pi);
    const idleCtx = createRuntimeExtensionContext();
    const activeCtx = createRuntimeExtensionContext({
      sessionManager: idleCtx.sessionManager,
      isIdle: () => false,
    });
    await handlers.get("session_start")?.({}, idleCtx);
    await commands.get("telegram-connect")?.handler("", idleCtx);
    await waitForCondition(() => dispatched);
    assert.match(
      getRuntimeHarnessTextBlock(sentMessages[0]).text ?? "",
      /^\[telegram\] show checkpoints/,
    );
    await handlers.get("agent_start")?.({}, activeCtx);
    const checkpointMessage = {
      role: "assistant",
      content: [{ type: "text", text: "Checkpoint **visible**" }],
    };
    await handlers.get("message_start")?.({ message: checkpointMessage }, activeCtx);
    await handlers.get("message_update")?.(
      {
        message: checkpointMessage,
        assistantMessageEvent: {
          type: "text_end",
          contentIndex: 0,
          content: "Checkpoint **visible**",
          partial: checkpointMessage,
        },
      },
      activeCtx,
    );
    await handlers.get("message_update")?.(
      {
        message: checkpointMessage,
        assistantMessageEvent: {
          type: "toolcall_start",
          contentIndex: 1,
          partial: checkpointMessage,
        },
      },
      activeCtx,
    );
    await handlers.get("message_end")?.({ message: { ...checkpointMessage, stopReason: "toolUse" } }, activeCtx);
    await waitForCondition(() => deliveredMarkdown.length === 1);
    const finalMessage = {
      role: "assistant",
      content: [{ type: "text", text: "Final **answer**" }],
    };
    await handlers.get("message_start")?.({ message: finalMessage }, activeCtx);
    await handlers.get("message_update")?.(
      {
        message: finalMessage,
        assistantMessageEvent: {
          type: "text_end",
          contentIndex: 0,
          content: "Final **answer**",
          partial: finalMessage,
        },
      },
      activeCtx,
    );
    await handlers.get("message_update")?.(
      {
        message: finalMessage,
        assistantMessageEvent: {
          type: "toolcall_start",
          contentIndex: 1,
          partial: finalMessage,
        },
      },
      activeCtx,
    );
    await waitForCondition(() => deliveredMarkdown.length === 2);
    await Promise.all([
      handlers.get("agent_end")?.({ messages: [finalMessage] }, activeCtx),
      handlers.get("agent_end")?.({ messages: [finalMessage] }, activeCtx),
    ]);
    await waitForCondition(() => deliveredMarkdown.length >= 2);
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(deliveredMarkdown.length, 2, "One active Telegram turn may publish its final answer only once");
    assert.deepEqual(deliveredMarkdown, [
      "Checkpoint **visible**",
      "Final **answer**",
    ]);
    // Preserve the transport's existing once-per-prompt anchor policy.
    assert.deepEqual(replyAnchors, [
      { message_id: 10, allow_sending_without_reply: true },
      undefined,
    ]);
    await handlers.get("session_shutdown")?.({}, idleCtx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

}

test("Extension runtime preserves both busy Next notices through follower queue admission", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const sentMessages: RuntimeHarnessMessage[] = [];
  const secondUpdates = createRuntimeDeferredResponse();
  const thirdUpdates = createRuntimeDeferredResponse();
  const fourthUpdates = createRuntimeDeferredResponse();
  let idle = true;
  let abortCount = 0;
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      sentMessages.push(content);
    },
  });
  let getUpdatesCalls = 0;
  const sendTexts: string[] = [];
  const sendBodies: Array<Record<string, unknown>> = [];
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 10,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "first request",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) return secondUpdates.promise;
      if (getUpdatesCalls === 3) return thirdUpdates.promise;
      if (getUpdatesCalls === 4) return fourthUpdates.promise;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      if (!body) throw new Error("Telegram send body is unavailable.");
      sendTexts.push(getRuntimeTelegramApiText(body));
      sendBodies.push(body);
      return createRuntimeTelegramApiResponse({
        message_id: 100 + sendTexts.length,
      });
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext({
      isIdle: () => idle,
      abort: () => {
        abortCount += 1;
      },
    });
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForCondition(() => sentMessages.length === 1);
    idle = false;
    await handlers.get("agent_start")?.({}, ctx);
    secondUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 2,
          message: {
            message_id: 11,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "follow up",
          },
        },
      ]),
    );
    await waitForCondition(() => getUpdatesCalls >= 3);
    await waitForTimeout(1_200);
    thirdUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 3,
          message: {
            message_id: 12,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "/next",
          },
        },
      ]),
    );
    await waitForCondition(() => abortCount === 1);
    idle = true;
    const abortedMessage = {
      role: "assistant",
      stopReason: "aborted",
      content: [{ type: "text", text: "" }],
    };
    await handlers.get("agent_end")?.({ messages: [abortedMessage] }, ctx);
    await handlers.get("agent_settled")?.({}, ctx);
    await waitForCondition(() => sentMessages.length === 2);
    await waitForCondition(() =>
      sendTexts.includes("<b>⏹️ This operation was aborted.</b>")
    );
    const lifecycleNotices = sendTexts.filter((text) =>
      text === "<b>⏹️ This operation was aborted.</b>" ||
      text === "<b>⏩ Dispatching next queued turn.</b>"
    );
    assert.deepEqual(lifecycleNotices, [
      "<b>⏹️ This operation was aborted.</b>",
      "<b>⏩ Dispatching next queued turn.</b>",
    ]);
    assert.equal(
      getRuntimeHarnessMessageText(sentMessages[1]!),
      "[telegram] follow up",
    );
    await handlers.get("agent_start")?.({}, ctx);
    const completedMessage = {
      role: "assistant",
      stopReason: "stop",
      content: [{ type: "text", text: "follow-up answer" }],
    };
    await handlers.get("message_end")?.({ message: completedMessage }, ctx);
    await handlers.get("agent_end")?.({ messages: [completedMessage] }, ctx);
    await handlers.get("agent_settled")?.({}, ctx);
    await waitForCondition(() => sendTexts.includes("follow-up answer"));
    const dispatchBody = sendBodies.find((body) =>
      getRuntimeTelegramApiText(body) ===
        "<b>⏩ Dispatching next queued turn.</b>"
    );
    const finalBody = sendBodies.find((body) =>
      getRuntimeTelegramApiText(body) === "follow-up answer"
    );
    assert.equal(dispatchBody?.parse_mode, "HTML");
    assert.deepEqual(dispatchBody?.reply_parameters, {
      message_id: 11,
      allow_sending_without_reply: true,
    });
    assert.equal(finalBody?.reply_parameters, undefined);
    fourthUpdates.resolve(createRuntimeTelegramApiResponse([]));
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime keeps rapid ordinary messages as distinct queued turns", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const sentMessages: RuntimeHarnessMessage[] = [];
  const rapidUpdates = createRuntimeDeferredResponse();
  const finalUpdates = createRuntimeDeferredResponse();
  let idle = true;
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      sentMessages.push(content);
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 20,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "first request",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) return rapidUpdates.promise;
      if (getUpdatesCalls === 3) return finalUpdates.promise;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext({ isIdle: () => idle });
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForCondition(() => sentMessages.length === 1);
    idle = false;
    await handlers.get("agent_start")?.({}, ctx);
    rapidUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 2,
          message: {
            message_id: 21,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "rapid second",
          },
        },
        {
          _: "other",
          update_id: 3,
          message: {
            message_id: 22,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "rapid third",
          },
        },
      ]),
    );
    await waitForCondition(() => getUpdatesCalls >= 3);
    await flushMicrotasks(20);
    const emptyCompletion = {
      role: "assistant",
      stopReason: "stop",
      content: [{ type: "text", text: "" }],
    };
    idle = true;
    await handlers.get("agent_end")?.({ messages: [emptyCompletion] }, ctx);
    await handlers.get("agent_settled")?.({}, ctx);
    await waitForCondition(() => sentMessages.length === 2);
    assert.equal(
      getRuntimeHarnessMessageText(sentMessages[1]!),
      "[telegram] rapid second",
    );
    idle = false;
    await handlers.get("agent_start")?.({}, ctx);
    idle = true;
    await handlers.get("agent_end")?.({ messages: [emptyCompletion] }, ctx);
    await handlers.get("agent_settled")?.({}, ctx);
    await waitForCondition(() => sentMessages.length === 3);
    assert.equal(
      getRuntimeHarnessMessageText(sentMessages[2]!),
      "[telegram] rapid third",
    );
    assert.equal(
      sentMessages.some((message) =>
        getRuntimeHarnessMessageText(message).includes(
          "rapid second\n\nrapid third",
        )
      ),
      false,
    );
    finalUpdates.resolve(createRuntimeTelegramApiResponse([]));
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime clears queued follow-ups after a Telegram stop", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const sentMessages: RuntimeHarnessMessage[] = [];
  let firstDispatchResolved = false;
  const secondUpdates = createRuntimeDeferredResponse();
  const thirdUpdates = createRuntimeDeferredResponse();
  const fourthUpdates = createRuntimeDeferredResponse();
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      sentMessages.push(content);
      firstDispatchResolved = true;
    },
  });
  let getUpdatesCalls = 0;
  const sendTexts: string[] = [];
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 10,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "first request",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) return secondUpdates.promise;
      if (getUpdatesCalls === 3) return thirdUpdates.promise;
      if (getUpdatesCalls === 4) return fourthUpdates.promise;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      sendTexts.push(getRuntimeTelegramApiText(body));
      return createRuntimeTelegramApiResponse({
        message_id: 100 + sendTexts.length,
      });
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const idleCtx = createRuntimeExtensionContext();
    let aborted = false;
    const activeCtx = createRuntimeExtensionContext({
      sessionManager: idleCtx.sessionManager,
      isIdle: () => false,
      abort: () => {
        aborted = true;
      },
    });
    await handlers.get("session_start")?.({}, idleCtx);
    await commands.get("telegram-connect")?.handler("", idleCtx);
    await waitForCondition(() => firstDispatchResolved);
    await handlers.get("agent_start")?.({}, activeCtx);
    secondUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 2,
          message: {
            message_id: 11,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "follow up",
          },
        },
      ]),
    );
    await waitForCondition(() => getUpdatesCalls >= 3);
    thirdUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 3,
          message: {
            message_id: 12,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "/stop",
          },
        },
      ]),
    );
    await waitForCondition(() => aborted);
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            stopReason: "aborted",
            content: [{ type: "text", text: "" }],
          },
        ],
      },
      idleCtx,
    );
    const dispatchCountBeforeNextTurn = sentMessages.length;
    fourthUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 4,
          message: {
            message_id: 13,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "new request",
          },
        },
      ]),
    );
    await waitForCondition(
      () => sentMessages.length === dispatchCountBeforeNextTurn + 1,
    );
    const promptText =
      getRuntimeHarnessTextBlock(sentMessages.at(-1)).text ?? "";
    assert.equal(promptText, "[telegram] new request");
    assert.equal(promptText.includes("follow up"), false);
    assert.equal(
      sendTexts.some((text) => text.startsWith("<b>⏹️ Aborted current turn.")),
      true,
    );
    await handlers.get("session_shutdown")?.({}, idleCtx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime handles immediate status before queued prompt after agent end", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const runtimeEvents: string[] = [];
  let firstDispatchResolved = false;
  let shutdownCtx: unknown;
  const secondUpdates = createRuntimeDeferredResponse();
  const thirdUpdates = createRuntimeDeferredResponse();
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      recordRuntimeDispatchEvent(runtimeEvents, content);
      firstDispatchResolved = true;
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook")
      return createRuntimeTelegramApiResponse(true);
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 20,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "first request",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) return secondUpdates.promise;
      if (getUpdatesCalls === 3) return thirdUpdates.promise;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      runtimeEvents.push(`send:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse({
        message_id: 100 + runtimeEvents.length,
      });
    }
    if (method === "sendChatAction")
      return createRuntimeTelegramApiResponse(true);
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const baseCtx = createRuntimeExtensionContext({
      cwd: process.cwd(),
      sessionManager: {
        getEntries: () => [],
      },
      modelRegistry: {
        refresh: () => {},
        getAvailable: () => [],
        isUsingOAuth: () => false,
      },
      getContextUsage: () => undefined,
    });
    const idleCtx = {
      ...baseCtx,
      isIdle: () => true,
    };
    const activeCtx = {
      ...baseCtx,
      isIdle: () => false,
    };
    shutdownCtx = idleCtx;
    await handlers.get("session_start")?.({}, idleCtx);
    await commands.get("telegram-connect")?.handler("", idleCtx);
    await waitForCondition(() => firstDispatchResolved);
    await handlers.get("agent_start")?.({}, activeCtx);
    secondUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 2,
          message: {
            message_id: 21,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "/status",
          },
        },
      ]),
    );
    await waitForCondition(() => getUpdatesCalls >= 3);
    thirdUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 3,
          message: {
            message_id: 22,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "follow up after status",
          },
        },
      ]),
    );
    await waitForCondition(() => runtimeEvents.length >= 1);
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            content: [{ type: "text", text: "" }],
          },
        ],
      },
      idleCtx,
    );
    await waitForCondition(() => runtimeEvents.length >= 3);
    assert.equal(runtimeEvents[0], "dispatch:[telegram] first request");
    assert.match(runtimeEvents[1] ?? "", /^send:<b>Pi Telegram<\/b>/);
    assert.equal(
      runtimeEvents[2],
      "dispatch:[telegram] follow up after status",
    );
  } finally {
    if (shutdownCtx) await handlers.get("session_shutdown")?.({}, shutdownCtx);
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime opens immediate model menu before queued prompt after agent end", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const runtimeEvents: string[] = [];
  const modelA = createRuntimeModel("openai", "gpt-a", true);
  const modelB = createRuntimeModel("anthropic", "claude-b", false);
  let firstDispatchResolved = false;
  const secondUpdates = createRuntimeDeferredResponse();
  const thirdUpdates = createRuntimeDeferredResponse();
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      recordRuntimeDispatchEvent(runtimeEvents, content);
      firstDispatchResolved = true;
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 23,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "first request",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) return secondUpdates.promise;
      if (getUpdatesCalls === 3) return thirdUpdates.promise;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      runtimeEvents.push(`send:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse({
        message_id: 100 + runtimeEvents.length,
      });
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const baseCtx = createRuntimeExtensionContext({
      cwd: process.cwd(),
      model: modelA,
      sessionManager: {
        getEntries: () => [],
      },
      modelRegistry: {
        refresh: () => {},
        getAvailable: () => [modelA, modelB],
        isUsingOAuth: () => false,
      },
      getContextUsage: () => undefined,
    });
    const idleCtx = {
      ...baseCtx,
      isIdle: () => true,
    };
    const activeCtx = {
      ...baseCtx,
      isIdle: () => false,
    };
    await handlers.get("session_start")?.({}, idleCtx);
    await commands.get("telegram-connect")?.handler("", idleCtx);
    await waitForCondition(() => firstDispatchResolved);
    await handlers.get("agent_start")?.({}, activeCtx);
    secondUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 2,
          message: {
            message_id: 24,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "/model",
          },
        },
      ]),
    );
    await waitForCondition(() => getUpdatesCalls >= 3);
    thirdUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 3,
          message: {
            message_id: 25,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "follow up after model",
          },
        },
      ]),
    );
    await waitForCondition(() => runtimeEvents.length >= 1);
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            content: [{ type: "text", text: "" }],
          },
        ],
      },
      idleCtx,
    );
    await waitForCondition(() => runtimeEvents.length >= 3, 5_000);
    assert.equal(runtimeEvents[0], "dispatch:[telegram] first request");
    assert.equal(runtimeEvents[1], "send:<b>🤖 Choose a model:</b>");
    assert.equal(runtimeEvents[2], "dispatch:[telegram] follow up after model");
    await handlers.get("session_shutdown")?.({}, idleCtx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime keeps queued turns blocked until compaction settles", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const runtimeEvents: string[] = [];
  const failureParseModes: string[] = [];
  let compactHooks:
    | {
        onComplete: () => void;
        onError: (error: unknown) => void;
      }
    | undefined;
  const secondUpdates = createRuntimeDeferredResponse();
  const thirdUpdates = createRuntimeDeferredResponse();
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      recordRuntimeDispatchEvent(runtimeEvents, content);
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 30,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "/compact",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) {
        return secondUpdates.promise;
      }
      if (getUpdatesCalls === 3) {
        return thirdUpdates.promise;
      }
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      const text = getRuntimeTelegramApiText(body);
      runtimeEvents.push(`send:${text}`);
      if (text.includes("Compaction failed!")) {
        failureParseModes.push(String(body?.parse_mode ?? ""));
      }
      return createRuntimeTelegramApiResponse({
        message_id: 100 + runtimeEvents.length,
      });
    }
    if (method === "sendChatAction") {
      runtimeEvents.push(
        `typing:${String(body?.chat_id ?? "")}:${String(body?.action ?? "")}`,
      );
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "editMessageText") {
      runtimeEvents.push(`edit:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "answerCallbackQuery") {
      runtimeEvents.push(`answer:${String(body?.callback_query_id ?? "")}`);
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext({
      compact: (hooks: {
        onComplete: () => void;
        onError: (error: unknown) => void;
      }) => {
        compactHooks = hooks;
        runtimeEvents.push("compact:start");
      },
    });
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForCondition(() =>
      runtimeEvents.includes("send:<b>Compact session?</b>"),
    );
    secondUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 2,
          callback_query: {
            id: "confirm-compact",
            from: { id: 77, is_bot: false, first_name: "Test" },
            message: {
              message_id: 101,
              chat: { id: 99, type: "private" },
            },
            data: "compact:confirm",
          },
        },
      ]),
    );
    await waitForCondition(() => runtimeEvents.includes("compact:start"));
    await waitForCondition(
      () =>
        runtimeEvents.includes("edit:<b>🗜 Compaction started.</b>") &&
        runtimeEvents.includes("typing:99:typing"),
    );
    assert.equal(
      runtimeEvents.indexOf("edit:<b>🗜 Compaction started.</b>") <
        runtimeEvents.indexOf("typing:99:typing"),
      true,
    );

    thirdUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 3,
          message: {
            message_id: 31,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "follow up after compaction",
          },
        },
      ]),
    );
    await waitForCondition(() => getUpdatesCalls >= 3);
    assert.equal(
      runtimeEvents.some(
        (event) => event === "dispatch:[telegram] follow up after compaction",
      ),
      false,
    );
    compactHooks?.onError(
      new Error(
        "Turn prefix summarization failed: This operation was aborted",
      ),
    );
    await waitForCondition(() =>
      runtimeEvents.includes("dispatch:[telegram] follow up after compaction"),
    );
    await waitForCondition(() =>
      runtimeEvents.includes(
        "send:<b>⚠️ Compaction failed! This operation was aborted.</b>",
      ),
    );
    assert.deepEqual(failureParseModes, ["HTML"]);
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

for (const outcome of ["completed", "failed", "cancelled", "shutdown"] as const) {
  test(`Extension runtime retains ${outcome} compaction notice authority after observer timeout`, async (t) => {
    const telegramConfig = await createRuntimeTelegramConfigFixture();
    const notices: string[] = [];
    let expireObserver: (() => void) | undefined;
    const originalSetTimeout = globalThis.setTimeout;
    t.mock.method(globalThis, "setTimeout", (...args: Parameters<typeof setTimeout>) => {
      const timer = originalSetTimeout(...args);
      if (args[1] === 300_000) expireObserver = () => { clearTimeout(timer); args[0](); };
      return timer;
    });
    const { handlers, commands, pi } = createRuntimePiHarness();
    const restoreFetch = setRuntimeTestFetch(async (input, init) => {
      const method = getRuntimeTelegramApiMethod(input);
      if (method === "deleteWebhook" || method === "sendChatAction") return createRuntimeTelegramApiResponse(true);
      if (method === "getUpdates") throw new DOMException("stop", "AbortError");
      if (method === "sendMessage" || method === "sendRichMessage") {
        notices.push(getRuntimeTelegramApiText(parseJsonRequestBody(init)));
        return createRuntimeTelegramApiResponse({ message_id: 100 + notices.length });
      }
      throw new Error(`Unexpected Telegram API method: ${method}`);
    });
    const ctx = createRuntimeExtensionContext({ cwd: "/repo/compaction-timeout" });
    try {
      await telegramConfig.write({ botToken: "123:abc", allowedUserId: 77, lastUpdateId: 0 });
      await writeRuntimeTelegramLocks({});
      (await getRuntimeTelegramExtension())(pi);
      await handlers.get("session_start")?.({}, ctx);
      await commands.get("telegram-connect")?.handler("", ctx);
      await handlers.get("session_before_compact")?.({ signal: new AbortController().signal }, ctx);
      await waitForCondition(() => notices.length === 1);
      assert.ok(expireObserver);
      expireObserver();
      assert.deepEqual(notices, ["**🗜 Compaction started.**"], "Timeout is not a Pi terminal result");
      if (outcome === "shutdown") await handlers.get("session_shutdown")?.({ reason: "reload" }, ctx);
      if (outcome === "completed" || outcome === "shutdown") {
        await handlers.get("session_compact")?.({}, ctx);
        await handlers.get("session_compact")?.({}, ctx);
      } else {
        const event = { aborted: outcome === "cancelled", reason: "threshold", willRetry: false, fromExtension: false };
        await handlers.get("session_compact_failed")?.(event, ctx);
        await handlers.get("session_compact_failed")?.(event, ctx);
      }
      if (outcome !== "shutdown") await waitForCondition(() => notices.length === 2);
      await flushMicrotasks(30);
      assert.deepEqual(notices, ["**🗜 Compaction started.**", ...(outcome === "shutdown" ? [] : [
        outcome === "completed" ? "**✅ Compaction completed.**" :
          outcome === "cancelled" ? "**⚠️ Compaction cancelled.**" : "**⚠️ Compaction failed.**",
      ])]);
    } finally {
      expireObserver?.();
      await commands.get("telegram-disconnect")?.handler("", ctx);
      await handlers.get("session_shutdown")?.({}, ctx);
      restoreFetch();
      await telegramConfig.restore();
    }
  });
}

test("Extension runtime compaction notices cannot overtake a pending local final delivery", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const committed: string[] = [];
  let finalStarted = false;
  let releaseFinal!: () => void;
  const finalGate = new Promise<void>((resolve) => { releaseFinal = resolve; });
  const { handlers, commands, pi } = createRuntimePiHarness();
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") return createRuntimeTelegramApiResponse(true);
    if (method === "getUpdates") throw new DOMException("stop", "AbortError");
    if (method === "sendChatAction") return createRuntimeTelegramApiResponse(true);
    if (method === "sendMessage" || method === "sendRichMessage") {
      const text = getRuntimeTelegramApiText(parseJsonRequestBody(init));
      if (text === "Ordered local final") {
        finalStarted = true;
        await finalGate;
      }
      committed.push(text);
      return createRuntimeTelegramApiResponse({ message_id: 100 + committed.length });
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  let settled: Promise<unknown> | undefined;
  const ctx = createRuntimeExtensionContext({ cwd: "/repo/compaction-order" });
  try {
    await telegramConfig.write({
      botToken: "123:abc", allowedUserId: 77, lastUpdateId: 0,
      assistant: { proactivePush: true },
    });
    await writeRuntimeTelegramLocks({});
    (await getRuntimeTelegramExtension())(pi);
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await flushMicrotasks(20);
    await handlers.get("input")?.({ source: "interactive", text: "local request" }, ctx);
    await handlers.get("agent_start")?.({}, ctx);
    const message = {
      role: "assistant", stopReason: "stop",
      content: [{ type: "text", text: "Ordered local final" }],
    };
    await handlers.get("message_update")?.({
      message, assistantMessageEvent: {
        type: "text_end", contentIndex: 0, content: "Ordered local final", partial: message,
      },
    }, ctx);
    await handlers.get("message_update")?.({
      message, assistantMessageEvent: { type: "done", reason: "stop", message },
    }, ctx);
    await handlers.get("message_end")?.({ message }, ctx);
    await waitForCondition(() => finalStarted);
    await handlers.get("agent_end")?.({ messages: [message] }, ctx);
    await handlers.get("session_before_compact")?.({ signal: new AbortController().signal }, ctx);
    await handlers.get("session_compact")?.({}, ctx);
    settled = Promise.resolve(handlers.get("agent_settled")?.({}, ctx));
    await flushMicrotasks(30);
    assert.deepEqual(committed, []);
    releaseFinal();
    await settled;
    await waitForCondition(() => committed.length === 3);
    assert.deepEqual(committed, [
      "Ordered local final", "**🗜 Compaction started.**", "**✅ Compaction completed.**",
    ]);
  } finally {
    releaseFinal();
    await settled;
    await commands.get("telegram-disconnect")?.handler("", ctx);
    await handlers.get("session_shutdown")?.({}, ctx);
    restoreFetch();
    await telegramConfig.restore();
  }
});

for (const [compactionBeforeAgentEnd, emptyFinal] of [[false, false], [true, false], [true, true]] as const) {
test(`Extension runtime delivers the final answer before observed auto-compaction notices${compactionBeforeAgentEnd ? " arriving before agent_end" : ""}${emptyFinal ? " without final publication" : ""}`, async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  await writeRuntimeTelegramLocks({});
  const runtimeEvents: string[] = [];
  let firstDispatchResolve: (() => void) | undefined;
  const firstDispatched = new Promise<void>((resolve) => {
    firstDispatchResolve = resolve;
  });
  const secondUpdates = createRuntimeDeferredResponse();
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      recordRuntimeDispatchEvent(runtimeEvents, content);
      firstDispatchResolve?.();
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook")
      return createRuntimeTelegramApiResponse(true);
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 41,
              chat: { id: 77, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "first telegram turn",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) return secondUpdates.promise;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      runtimeEvents.push(`send:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse({
        message_id: 100 + runtimeEvents.length,
      });
    }
    if (method === "editMessageText") {
      runtimeEvents.push(`edit:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "sendMessageDraft" || method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext();
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await firstDispatched;
    await handlers.get("agent_start")?.({}, ctx);
    secondUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 2,
          message: {
            message_id: 42,
            chat: { id: 77, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "queued during active turn",
          },
        },
      ]),
    );
    await waitForCondition(() => getUpdatesCalls >= 3);
    await handlers.get("session_before_compact")?.(
      { signal: new AbortController().signal },
      ctx,
    );
    await handlers.get("session_compact")?.({}, ctx);
    await waitForCondition(() =>
      runtimeEvents.includes("send:**✅ Compaction completed.**"),
    );
    const midTurnStartedIndex = runtimeEvents.indexOf(
      "send:**🗜 Compaction started.**",
    );
    const midTurnCompletedIndex = runtimeEvents.indexOf(
      "send:**✅ Compaction completed.**",
    );
    assert.equal(midTurnStartedIndex < midTurnCompletedIndex, true);
    await handlers.get("session_before_compact")?.(
      { signal: new AbortController().signal },
      ctx,
    );
    await handlers.get("session_compact_failed")?.(
      {
        reason: "threshold",
        aborted: false,
        willRetry: false,
        fromExtension: false,
        errorMessage: "Auto-compaction failed: boom",
      },
      ctx,
    );
    await waitForCondition(() =>
      runtimeEvents.includes("send:**⚠️ Compaction failed.**"),
    );
    assert.equal(
      runtimeEvents.lastIndexOf("send:**🗜 Compaction started.**") <
        runtimeEvents.lastIndexOf("send:**⚠️ Compaction failed.**"),
      true,
    );
    const noticeBaseline = runtimeEvents.length;
    const finalText = emptyFinal ? "" : "done";
    await handlers.get("message_end")?.(
      {
        message: {
          role: "assistant",
          content: [{ type: "text", text: finalText }],
          stopReason: "stop",
        },
      },
      ctx,
    );
    const endAgent = async () => {
      await handlers.get("agent_end")?.({
        messages: [{ role: "assistant", content: [{ type: "text", text: finalText }] }],
      }, ctx);
    };
    if (!compactionBeforeAgentEnd) await endAgent();
    await handlers.get("session_before_compact")?.(
      { signal: new AbortController().signal },
      ctx,
    );
    await handlers.get("session_compact")?.({}, ctx);
    if (compactionBeforeAgentEnd) await endAgent();
    if (emptyFinal) {
      await waitForCondition(() => runtimeEvents.slice(noticeBaseline).includes("send:**✅ Compaction completed.**"));
    } else if (!runtimeEvents.slice(noticeBaseline).some((event) => event === "send:done" || event === "edit:done")) {
      assert.equal(runtimeEvents.slice(noticeBaseline).some((event) => event.includes("Compaction")), false);
    }
    assert.equal(
      runtimeEvents.includes("dispatch:[telegram] queued during active turn"),
      false,
    );
    await handlers.get("agent_settled")?.({}, ctx);
    await waitForCondition(() =>
      runtimeEvents.slice(noticeBaseline).includes("send:**✅ Compaction completed.**"),
    ).catch((error) => { throw new Error(runtimeEvents.join("\n"), { cause: error }); });
    const finalReplyIndex = runtimeEvents.findIndex(
      (event) => event === "send:done" || event === "edit:done",
    );
    const compactionStartedIndex = runtimeEvents.lastIndexOf(
      "send:**🗜 Compaction started.**",
    );
    const compactionCompletedIndex = runtimeEvents.lastIndexOf(
      "send:**✅ Compaction completed.**",
    );
    assert.equal(finalReplyIndex === -1, emptyFinal);
    if (!emptyFinal) assert.equal(finalReplyIndex < compactionStartedIndex, true);
    assert.equal(compactionStartedIndex < compactionCompletedIndex, true);
    await waitForCondition(() =>
      runtimeEvents.includes("dispatch:[telegram] queued during active turn"),
    );
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});
}

test("Extension runtime coalesces media-group updates into one delayed dispatch", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const runtimeEvents: string[] = [];
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      recordRuntimeDispatchEvent(runtimeEvents, content);
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 40,
              media_group_id: "album-1",
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              caption: "first caption",
            },
          },
          {
            _: "other",
            update_id: 2,
            message: {
              message_id: 41,
              media_group_id: "album-1",
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              caption: "second caption",
            },
          },
        ]);
      }
      throw new DOMException("stop", "AbortError");
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext();
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForEventLoopCondition(() => getUpdatesCalls >= 2, 5000);
    assert.equal(runtimeEvents.length, 0);
    await waitForCondition(() => runtimeEvents.length === 1, 3000);
    assert.equal(
      runtimeEvents[0],
      "dispatch:[telegram] first caption\n\nsecond caption",
    );
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime coalesces likely split long text updates into one dispatch", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const runtimeEvents: string[] = [];
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      recordRuntimeDispatchEvent(runtimeEvents, content);
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 50,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "x".repeat(3600),
            },
          },
          {
            _: "other",
            update_id: 2,
            message: {
              message_id: 51,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "tail",
            },
          },
        ]);
      }
      throw new DOMException("stop", "AbortError");
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext();
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForEventLoopCondition(() => getUpdatesCalls >= 1, 5000);
    await flushMicrotasks();
    assert.equal(runtimeEvents.length, 0);
    await waitForCondition(() => runtimeEvents.length === 1, 3000);
    assert.equal(
      runtimeEvents[0],
      `dispatch:[telegram] ${"x".repeat(3600)}\n\ntail`,
    );
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime clears pending split-text dispatch on shutdown", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const runtimeEvents: string[] = [];
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      recordRuntimeDispatchEvent(runtimeEvents, content);
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 60,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "x".repeat(3600),
            },
          },
        ]);
      }
      throw new DOMException("stop", "AbortError");
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext();
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForEventLoopCondition(() => getUpdatesCalls >= 2, 5000);
    await handlers.get("session_shutdown")?.({}, ctx);
    await new Promise((resolve) => setTimeout(resolve, 900));
    assert.deepEqual(runtimeEvents, []);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime clears pending media-group dispatch on shutdown", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const runtimeEvents: string[] = [];
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      recordRuntimeDispatchEvent(runtimeEvents, content);
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 61,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              media_group_id: "album-1",
              text: "album item",
            },
          },
        ]);
      }
      throw new DOMException("stop", "AbortError");
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeExtensionContext();
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForEventLoopCondition(() => getUpdatesCalls >= 2, 5000);
    await handlers.get("session_shutdown")?.({}, ctx);
    await new Promise((resolve) => setTimeout(resolve, 1200));
    assert.deepEqual(runtimeEvents, []);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime applies reaction priority and removal before the next dispatch", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const runtimeEvents: string[] = [];
  let firstDispatchResolved = false;
  const secondUpdates = createRuntimeDeferredResponse();
  const thirdUpdates = createRuntimeDeferredResponse();
  const fourthUpdates = createRuntimeDeferredResponse();
  const fifthUpdates = createRuntimeDeferredResponse();
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      recordRuntimeDispatchEvent(runtimeEvents, content);
      firstDispatchResolved = true;
    },
  });
  let getUpdatesCalls = 0;
  const restoreFetch = setRuntimeTestFetch(async (input) => {
    const method = getRuntimeTelegramApiMethod(input);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 30,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "first request",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) return secondUpdates.promise;
      if (getUpdatesCalls === 3) return thirdUpdates.promise;
      if (getUpdatesCalls === 4) return fourthUpdates.promise;
      if (getUpdatesCalls === 5) return fifthUpdates.promise;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const idleCtx = createRuntimeExtensionContext();
    const activeCtx = createRuntimeExtensionContext({
      isIdle: () => false,
      sessionManager: idleCtx.sessionManager,
    });
    await handlers.get("session_start")?.({}, idleCtx);
    await commands.get("telegram-connect")?.handler("", idleCtx);
    await waitForCondition(() => firstDispatchResolved);
    await handlers.get("agent_start")?.({}, activeCtx);
    secondUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 2,
          message: {
            message_id: 31,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "older waiting",
          },
        },
      ]),
    );
    await waitForCondition(() => getUpdatesCalls >= 3);
    thirdUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 3,
          message: {
            message_id: 32,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "newer waiting",
          },
        },
      ]),
    );
    await waitForCondition(() => getUpdatesCalls >= 4);
    fourthUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 4,
          message_reaction: {
            chat: { id: 99, type: "private" },
            message_id: 32,
            user: { id: 77, is_bot: false, first_name: "Test" },
            old_reaction: [],
            new_reaction: [{ type: "emoji", emoji: "👍" }],
            date: 1,
          },
        },
      ]),
    );
    await waitForCondition(() => getUpdatesCalls >= 5);
    fifthUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 5,
          message_reaction: {
            chat: { id: 99, type: "private" },
            message_id: 31,
            user: { id: 77, is_bot: false, first_name: "Test" },
            old_reaction: [],
            new_reaction: [{ type: "emoji", emoji: "👎" }],
            date: 2,
          },
        },
      ]),
    );
    await waitForCondition(() => getUpdatesCalls >= 6);
    await flushMicrotasks(50);
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            content: [{ type: "text", text: "" }],
          },
        ],
      },
      idleCtx,
    );
    await waitForCondition(() => runtimeEvents.length === 2);
    assert.equal(runtimeEvents[0], "dispatch:[telegram] first request");
    assert.equal(runtimeEvents[1], "dispatch:[telegram] newer waiting");
    await handlers.get("agent_start")?.({}, activeCtx);
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            content: [{ type: "text", text: "" }],
          },
        ],
      },
      idleCtx,
    );
    await flushMicrotasks();
    assert.deepEqual(runtimeEvents, [
      "dispatch:[telegram] first request",
      "dispatch:[telegram] newer waiting",
    ]);
    await handlers.get("session_shutdown")?.({}, idleCtx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime applies idle model picks immediately and refreshes status", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const previousArgv = [...process.argv];
  const runtimeEvents: string[] = [];
  const statusEvents: string[] = [];
  const modelA = createRuntimeModel("openai", "gpt-a", true);
  const modelB = createRuntimeModel("anthropic", "claude-b", true);
  const setModels: Array<string> = [];
  const thinkingLevels: Array<string> = [];
  let shutdownCtx: unknown;
  const secondUpdates = createRuntimeDeferredResponse();
  const { handlers, commands, pi } = createRuntimePiHarness({
    getThinkingLevel: () => thinkingLevels.at(-1) ?? "medium",
    setModel: async (model) => {
      setModels.push(`${model.provider}/${model.id}`);
      return true;
    },
    setThinkingLevel: (level) => {
      thinkingLevels.push(level);
    },
  });
  let getUpdatesCalls = 0;
  let nextMessageId = 100;
  const callbackAnswers: string[] = [];
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook")
      return createRuntimeTelegramApiResponse(true);
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 60,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "/model",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) return secondUpdates.promise;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      runtimeEvents.push(`send:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse({ message_id: nextMessageId++ });
    }
    if (method === "editMessageText") {
      runtimeEvents.push(`edit:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "answerCallbackQuery") {
      callbackAnswers.push(String(body?.text ?? ""));
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "sendChatAction")
      return createRuntimeTelegramApiResponse(true);
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    process.argv = [
      previousArgv[0] ?? "node",
      previousArgv[1] ?? "index.ts",
      "--models=anthropic/claude-b:high",
    ];
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeModelContext({
      model: modelA,
      availableModels: [modelA, modelB],
      setStatus: (_slot, text) => {
        statusEvents.push(text);
      },
    });
    shutdownCtx = ctx;
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForCondition(() =>
      runtimeEvents.some((event) => event === "send:<b>🤖 Choose a model:</b>"),
    );
    const statusCountBeforePick = statusEvents.length;
    secondUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 2,
          callback_query: {
            id: "cb-idle-1",
            from: { id: 77, is_bot: false, first_name: "Test" },
            data: "model:pick:0",
            message: {
              message_id: 100,
              chat: { id: 99, type: "private" },
            },
          },
        },
      ]),
    );
    await waitForCondition(() => setModels.length === 1);
    assert.deepEqual(setModels, ["anthropic/claude-b"]);
    assert.deepEqual(thinkingLevels, ["high"]);
    assert.equal(callbackAnswers.includes("Switched to claude-b"), true);
    assert.equal(statusEvents.length > statusCountBeforePick, true);
    assert.equal(
      runtimeEvents.some(
        (event) =>
          event.startsWith("edit:<b>Pi Telegram</b>") ||
          event.startsWith("edit:<b>🤖 Choose a model:</b>"),
      ),
      true,
    );
  } finally {
    if (shutdownCtx) await handlers.get("session_shutdown")?.({}, shutdownCtx);
    process.argv = previousArgv;
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime switches a local run in flight and continues in the model-menu target", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const runtimeEvents: string[] = [];
  const modelA = createRuntimeModel("openai", "gpt-a", true);
  const modelB = createRuntimeModel("anthropic", "claude-b", false);
  let idle = true;
  let aborted = false;
  const setModels: Array<string> = [];
  const secondUpdates = createRuntimeDeferredResponse();
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      recordRuntimeDispatchEvent(runtimeEvents, content);
    },
    setModel: async (model) => {
      setModels.push(`${model.provider}/${model.id}`);
      return true;
    },
    setThinkingLevel: () => {},
  });
  let getUpdatesCalls = 0;
  let nextMessageId = 100;
  const callbackAnswers: string[] = [];
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 40,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "/model",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) return secondUpdates.promise;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      runtimeEvents.push(`send:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse({ message_id: nextMessageId++ });
    }
    if (method === "editMessageText") {
      runtimeEvents.push(`edit:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "answerCallbackQuery") {
      callbackAnswers.push(String(body?.text ?? ""));
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeModelContext({
      model: modelA,
      availableModels: [modelA, modelB],
      isIdle: () => idle,
      abort: () => {
        aborted = true;
      },
    });
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForCondition(() =>
      runtimeEvents.some((event) => event === "send:<b>🤖 Choose a model:</b>"),
    );
    idle = false;
    await handlers.get("agent_start")?.({}, ctx);
    secondUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 2,
          callback_query: {
            id: "cb-1",
            from: { id: 77, is_bot: false, first_name: "Test" },
            data: "model:pick:1",
            message: {
              message_id: 100,
              chat: { id: 99, type: "private" },
            },
          },
        },
      ]),
    );
    await waitForCondition(() => aborted);
    assert.deepEqual(setModels, ["anthropic/claude-b"]);
    assert.equal(
      callbackAnswers.includes("Switching to claude-b and continuing…"),
      true,
    );
    idle = true;
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            stopReason: "aborted",
            content: [{ type: "text", text: "" }],
          },
        ],
      },
      ctx,
    );
    await waitForCondition(() =>
      runtimeEvents.some((event) =>
        event.includes(
          "Continue from the last unfinished step. Model: anthropic/claude-b",
        ),
      ),
    );
    assert.equal(
      runtimeEvents.some((event) =>
        event.includes(
          "dispatch:[telegram] Continue from the last unfinished step. Model: anthropic/claude-b",
        ),
      ),
      true,
    );
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime preserves long-session queue through abort, next, and model switch", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const runtimeEvents: string[] = [];
  const modelA = createRuntimeModel("openai", "gpt-a", true);
  const modelB = createRuntimeModel("anthropic", "claude-b", false);
  let idle = true;
  let abortCount = 0;
  const setModels: Array<string> = [];
  const updates = Array.from({ length: 5 }, () =>
    createRuntimeDeferredResponse(),
  );
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      recordRuntimeDispatchEvent(runtimeEvents, content);
    },
    setModel: async (model) => {
      setModels.push(`${model.provider}/${model.id}`);
      return true;
    },
    setThinkingLevel: () => {},
  });
  let getUpdatesCalls = 0;
  let nextMessageId = 100;
  const callbackAnswers: string[] = [];
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook")
      return createRuntimeTelegramApiResponse(true);
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 70,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "/model",
            },
          },
        ]);
      }
      const update = updates[getUpdatesCalls - 2];
      if (update) return update.promise;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      runtimeEvents.push(`send:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse({ message_id: nextMessageId++ });
    }
    if (method === "editMessageText") {
      runtimeEvents.push(`edit:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "answerCallbackQuery") {
      callbackAnswers.push(String(body?.text ?? ""));
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "sendChatAction")
      return createRuntimeTelegramApiResponse(true);
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeModelContext({
      model: modelA,
      availableModels: [modelA, modelB],
      isIdle: () => idle,
      abort: () => {
        abortCount += 1;
      },
    });
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForCondition(() =>
      runtimeEvents.includes("send:<b>🤖 Choose a model:</b>"),
    );
    updates[0].resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 2,
          message: {
            message_id: 71,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "first long-session request",
          },
        },
      ]),
    );
    await waitForCondition(() =>
      runtimeEvents.includes("dispatch:[telegram] first long-session request"),
    );
    idle = false;
    await handlers.get("agent_start")?.({}, ctx);
    updates[1].resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 3,
          message: {
            message_id: 72,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "queued after abort",
          },
        },
      ]),
    );
    await waitForCondition(() => getUpdatesCalls >= 4);
    assert.equal(
      runtimeEvents.includes("dispatch:[telegram] queued after abort"),
      false,
    );
    updates[2].resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 4,
          message: {
            message_id: 73,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "/abort",
          },
        },
      ]),
    );
    await waitForCondition(() => abortCount === 1);
    idle = true;
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            stopReason: "aborted",
            content: [{ type: "text", text: "" }],
          },
        ],
      },
      ctx,
    );
    assert.equal(
      runtimeEvents.includes("dispatch:[telegram] queued after abort"),
      false,
    );
    updates[3].resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 5,
          message: {
            message_id: 74,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "/next",
          },
        },
      ]),
    );
    await waitForCondition(() =>
      runtimeEvents.includes("dispatch:[telegram] queued after abort"),
    );
    idle = false;
    await handlers.get("agent_start")?.({}, ctx);
    updates[4].resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 6,
          callback_query: {
            id: "cb-long-session",
            from: { id: 77, is_bot: false, first_name: "Test" },
            data: "model:pick:1",
            message: {
              message_id: 100,
              chat: { id: 99, type: "private" },
            },
          },
        },
      ]),
    );
    await waitForCondition(() => abortCount === 2);
    assert.deepEqual(setModels, ["anthropic/claude-b"]);
    assert.equal(
      callbackAnswers.includes("Switching to claude-b and continuing…"),
      true,
    );
    idle = true;
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            stopReason: "aborted",
            content: [{ type: "text", text: "" }],
          },
        ],
      },
      ctx,
    );
    assert.equal(
      runtimeEvents.includes("dispatch:[telegram] queued after abort"),
      true,
    );
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});

test("Extension runtime delays model-switch abort until the active tool finishes", async () => {
  const telegramConfig = await createRuntimeTelegramConfigFixture();
  const runtimeEvents: string[] = [];
  const modelA = createRuntimeModel("openai", "gpt-a", true);
  const modelB = createRuntimeModel("anthropic", "claude-b", false);
  let idle = true;
  let aborted = false;
  const setModels: Array<string> = [];
  const secondUpdates = createRuntimeDeferredResponse();
  const thirdUpdates = createRuntimeDeferredResponse();
  const { handlers, commands, pi } = createRuntimePiHarness({
    sendUserMessage: (content) => {
      recordRuntimeDispatchEvent(runtimeEvents, content);
    },
    setModel: async (model) => {
      setModels.push(`${model.provider}/${model.id}`);
      return true;
    },
    setThinkingLevel: () => {},
  });
  let getUpdatesCalls = 0;
  let nextMessageId = 100;
  const callbackAnswers: string[] = [];
  const restoreFetch = setRuntimeTestFetch(async (input, init) => {
    const method = getRuntimeTelegramApiMethod(input);
    const body = parseJsonRequestBody(init);
    if (method === "deleteWebhook") {
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "getUpdates") {
      getUpdatesCalls += 1;
      if (getUpdatesCalls === 1) {
        return createRuntimeTelegramApiResponse([
          {
            _: "other",
            update_id: 1,
            message: {
              message_id: 50,
              chat: { id: 99, type: "private" },
              from: { id: 77, is_bot: false, first_name: "Test" },
              text: "/model",
            },
          },
        ]);
      }
      if (getUpdatesCalls === 2) return secondUpdates.promise;
      if (getUpdatesCalls === 3) return thirdUpdates.promise;
      throw new DOMException("stop", "AbortError");
    }
    if (method === "sendMessage" || method === "sendRichMessage") {
      runtimeEvents.push(`send:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse({ message_id: nextMessageId++ });
    }
    if (method === "editMessageText") {
      runtimeEvents.push(`edit:${getRuntimeTelegramApiText(body)}`);
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "answerCallbackQuery") {
      callbackAnswers.push(String(body?.text ?? ""));
      return createRuntimeTelegramApiResponse(true);
    }
    if (method === "sendChatAction") {
      return createRuntimeTelegramApiResponse(true);
    }
    throw new Error(`Unexpected Telegram API method: ${method}`);
  });
  try {
    await telegramConfig.write({
      botToken: "123:abc",
      allowedUserId: 77,
      lastUpdateId: 0,
    });
    (await getRuntimeTelegramExtension())(pi);
    const ctx = createRuntimeModelContext({
      model: modelA,
      availableModels: [modelA, modelB],
      isIdle: () => idle,
      abort: () => {
        aborted = true;
      },
    });
    await handlers.get("session_start")?.({}, ctx);
    await commands.get("telegram-connect")?.handler("", ctx);
    await waitForCondition(() =>
      runtimeEvents.some((event) => event === "send:<b>🤖 Choose a model:</b>"),
    );
    secondUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 2,
          message: {
            message_id: 51,
            chat: { id: 99, type: "private" },
            from: { id: 77, is_bot: false, first_name: "Test" },
            text: "first request",
          },
        },
      ]),
    );
    await waitForCondition(() =>
      runtimeEvents.some(
        (event) => event === "dispatch:[telegram] first request",
      ),
    );
    idle = false;
    await handlers.get("agent_start")?.({}, ctx);
    await handlers.get("tool_execution_start")?.({}, ctx);
    thirdUpdates.resolve(
      createRuntimeTelegramApiResponse([
        {
          _: "other",
          update_id: 3,
          callback_query: {
            id: "cb-2",
            from: { id: 77, is_bot: false, first_name: "Test" },
            data: "model:pick:1",
            message: {
              message_id: 100,
              chat: { id: 99, type: "private" },
            },
          },
        },
      ]),
    );
    await waitForCondition(() =>
      callbackAnswers.includes(
        "Switched to claude-b. Restarting after the current tool finishes…",
      ),
    );
    assert.deepEqual(setModels, ["anthropic/claude-b"]);
    assert.equal(aborted, false);
    await handlers.get("tool_execution_end")?.({}, ctx);
    await waitForCondition(() => aborted);
    idle = true;
    await handlers.get("agent_end")?.(
      {
        messages: [
          {
            role: "assistant",
            stopReason: "aborted",
            content: [{ type: "text", text: "" }],
          },
        ],
      },
      ctx,
    );
    await waitForCondition(() =>
      runtimeEvents.some((event) =>
        event.includes(
          "dispatch:[telegram] Continue from the last unfinished step. Model: anthropic/claude-b",
        ),
      ),
    );
    await handlers.get("session_shutdown")?.({}, ctx);
  } finally {
    restoreFetch();
    await telegramConfig.restore();
  }
});
