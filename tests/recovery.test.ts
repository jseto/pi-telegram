/**
 * Regression tests for Telegram runtime storage housekeeping
 * Covers legacy recovery-folder removal and orphaned session journal sweeps
 */

import assert from "node:assert/strict";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  removeTelegramLegacyRecoveryStorage,
  createTelegramSessionFolderSweeper,
} from "../lib/recovery.ts";

function createRuntimePaths(): { dir: string } {
  return { dir: mkdtempSync(join(tmpdir(), "pi-telegram-recovery-")) };
}

test("Legacy recovery folders are removed from the runtime root and session folders only", () => {
  const paths = createRuntimePaths();
  try {
    const sessionRecovery = join(paths.dir, "sessions", "session-a", "recovery");
    mkdirSync(join(paths.dir, "recovery", "1-2-x"), { recursive: true });
    writeFileSync(join(paths.dir, "recovery", "1-2-x", "owners.json"), "{truncated");
    mkdirSync(sessionRecovery, { recursive: true });
    writeFileSync(join(paths.dir, "sessions", "session-a", "journal.0123456789abcdef.json"), "{}");
    writeFileSync(join(paths.dir, "recovery-note"), "kept");
    assert.deepEqual(removeTelegramLegacyRecoveryStorage(paths.dir).sort(), [join(paths.dir, "recovery"), sessionRecovery].sort());
    assert.equal(existsSync(join(paths.dir, "recovery")), false);
    assert.equal(existsSync(sessionRecovery), false);
    assert.equal(readFileSync(join(paths.dir, "sessions", "session-a", "journal.0123456789abcdef.json"), "utf8"), "{}");
    assert.equal(readFileSync(join(paths.dir, "recovery-note"), "utf8"), "kept");
    assert.deepEqual(removeTelegramLegacyRecoveryStorage(paths.dir), [], "Idempotent");
  } finally {
    rmSync(paths.dir, { recursive: true, force: true });
  }
});

test("Session sweeper removes unbound current-profile journals and empty folders only", () => {
  const paths = createRuntimePaths();
  try {
    const sessions = join(paths.dir, "sessions");
    const family = (session: string, profileSuffix = "") => {
      mkdirSync(join(sessions, session, `journal.0123456789abcdef${profileSuffix}.json.segments`), { recursive: true });
      writeFileSync(join(sessions, session, `journal.0123456789abcdef${profileSuffix}.json`), "{}");
    };
    family("bound"); family("unbound"); family("shared"); family("shared", ".other"); family("live");
    mkdirSync(join(sessions, "%61"), { recursive: true });
    let now = 0, kept: (string | undefined)[] = ["bound", "live", undefined];
    const sweeper = createTelegramSessionFolderSweeper({ getSessionsDir: () => sessions, getProfileName: () => "default",
      getKeptSessionIds: () => kept, getNowMs: () => now, intervalMs: 1000 });
    const removed = sweeper.sweep();
    assert.deepEqual(readdirSync(sessions).sort(), ["%61", "bound", "live", "shared"].sort(),
      "Unbound folder removed; noncanonical names untouched");
    assert.deepEqual(readdirSync(join(sessions, "shared")).sort(), ["journal.0123456789abcdef.other.json", "journal.0123456789abcdef.other.json.segments"],
      "Another profile's family keeps the shared folder");
    assert.ok(removed.includes(join(sessions, "unbound")));
    kept = ["live"];
    now = 500;
    assert.deepEqual(sweeper.sweep(), [], "Throttled between sweeps");
    now = 1000;
    sweeper.sweep();
    assert.deepEqual(readdirSync(sessions).sort(), ["%61", "live", "shared"].sort(), "A session that lost its slot is swept later");
  } finally {
    rmSync(paths.dir, { recursive: true, force: true });
  }
});
