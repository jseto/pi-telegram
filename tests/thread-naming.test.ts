/** Thread naming value policy, compatibility and dialog state; no transport or Workspace mutation. */
import assert from "node:assert/strict";
import test from "node:test";
import * as Naming from "../lib/thread-naming.ts";
import {
  chooseTelegramThreadName,
  createTelegramThreadNameDialogRuntime,
  getTelegramManualThreadDisplayNameValidationError,
  getTelegramTopicIdentityName,
  getTelegramTopicName,
  isTelegramTopicThreadNameValidForSlot,
  TELEGRAM_THREAD_NAME_DIALOG_TTL_MS,
} from "../lib/thread-naming.ts";

const target = { chatId: 7, threadId: 42 };

for (const boundary of ["current", "source", "successor", "expiry", "clock-source"] as const) {
  test(`Thread-name exact publication checks transient source authority without retaining it (${boundary})`, () => {
    let now = 100, source = true, publishing = false, granted = false;
    const runtime = createTelegramThreadNameDialogRuntime({ nowMs() {
      if (boundary === "clock-source" && granted) source = false;
      return now;
    } });
    const lifetime = runtime.prepare({ scope: "session:1", target, isCurrent() {
      if (boundary === "source" && publishing) source = false;
      return true;
    } })!;
    const assertPublicationCurrent = () => {
      if (!source) throw new Error("Source revoked");
      granted = true;
      if (boundary === "successor") runtime.open({ scope: "session:2", target, dialogMessageId: 20 });
      if (boundary === "expiry") now += TELEGRAM_THREAD_NAME_DIALOG_TTL_MS;
    };
    publishing = true;
    if (boundary === "source" || boundary === "clock-source") {
      assert.throws(() => lifetime.publish(10, assertPublicationCurrent), /Source revoked/);
      assert.equal(runtime.inspect(target), undefined);
    } else if (boundary === "current") {
      const candidate = lifetime.publish(10, assertPublicationCurrent)!;
      assert.equal(candidate.expiresAtMs, 100 + TELEGRAM_THREAD_NAME_DIALOG_TTL_MS);
      source = false;
      assert.equal(runtime.capture(candidate)!.consumeName("Navigator").kind, "name", "Future input cannot inherit the completed source guard");
    } else {
      assert.equal(lifetime.publish(10, assertPublicationCurrent), undefined);
      assert.equal(runtime.inspect(target)?.dialogMessageId, boundary === "successor" ? 20 : undefined);
      lifetime.finish();
      assert.equal(runtime.inspect(target)?.dialogMessageId, boundary === "successor" ? 20 : undefined);
    }
  });
}

for (const ending of ["finish", "cancel", "expiry", "reopen"] as const) {
  test(`Thread-name recipient callback stays independent of input ${ending}`, () => {
    let now = 100, current = true;
    const runtime = createTelegramThreadNameDialogRuntime({ nowMs: () => now });
    const assertAuthority = () => { if (!current) throw new Error("recipient ended"); };
    const input = { scope: "session:1", target, assertAuthority };
    const prepared = runtime.prepare(input)!;
    const candidate = prepared.publish(10)!;
    input.assertAuthority = () => { throw new Error("replacement callback"); };
    const lifetime = runtime.capture(candidate)!;
    assert.equal(lifetime.assertAuthority, assertAuthority);
    assert.equal("assertAuthority" in candidate, false, "Candidate views never expose callbacks");
    assert.equal("assertAuthority" in runtime.inspect(target)!, false);
    if (ending === "cancel") lifetime.select("cancel");
    else if (ending === "expiry") { now += TELEGRAM_THREAD_NAME_DIALOG_TTL_MS; assert.equal(lifetime.isCurrent(), false); }
    else if (ending === "reopen") {
      lifetime.consumeName("Navigator");
      const reopened = lifetime.reopen()!;
      assert.equal(runtime.capture(reopened)!.assertAuthority, assertAuthority);
    } else lifetime.finish();
    assert.doesNotThrow(lifetime.assertAuthority!);
    current = false;
    assert.throws(lifetime.assertAuthority!, /recipient ended/);
    assert.equal(runtime.open({ scope: "ordinary", target, dialogMessageId: 20 }).phase, "input");
    assert.equal(runtime.capture({ scope: "ordinary", target, dialogMessageId: 20 })!.assertAuthority, undefined);
  });
}

test("Thread-name dialog replaces duplicates and consumes one exact-target name", () => {
  let now = 100;
  const runtime = createTelegramThreadNameDialogRuntime({ nowMs: () => now });
  runtime.open({ scope: "session:1", target, dialogMessageId: 10 });
  runtime.open({ scope: "session:1", target, dialogMessageId: 11 });
  assert.deepEqual(runtime.select({
    scope: "session:1", target, dialogMessageId: 10, action: "cancel",
  }), { kind: "expired" });
  assert.deepEqual(runtime.consumeName({
    scope: "session:1", target: { chatId: 7, threadId: 43 }, text: "Wrong",
  }), { kind: "none" });
  assert.deepEqual(runtime.consumeName({
    scope: "session:1", target, text: "  Navigator  ",
  }), { kind: "name", name: "Navigator" });
  assert.deepEqual(runtime.consumeName({
    scope: "session:1", target, text: "Again",
  }), { kind: "none" });
  assert.equal(runtime.inspect(target), undefined);
  now++;
});

test("Thread-name dialog reset and cancel are consume-once", () => {
  const runtime = createTelegramThreadNameDialogRuntime();
  for (const action of ["reset", "cancel"] as const) {
    runtime.open({ scope: "session:1", target, dialogMessageId: 20 });
    assert.deepEqual(runtime.select({
      scope: "session:1", target, dialogMessageId: 20, action,
    }), { kind: action });
    assert.deepEqual(runtime.select({
      scope: "session:1", target, dialogMessageId: 20, action,
    }), { kind: "expired" });
  }
});

test("Thread-name dialog rejects stale scope, expiry, and empty input", () => {
  let now = 1_000;
  const runtime = createTelegramThreadNameDialogRuntime({ nowMs: () => now });
  runtime.open({ scope: "session:1", target, dialogMessageId: 30 });
  assert.deepEqual(runtime.select({
    scope: "session:2", target, dialogMessageId: 30, action: "cancel",
  }), { kind: "expired" });
  assert.deepEqual(runtime.consumeName({
    scope: "session:1", target, text: "   ",
  }), { kind: "empty" });
  assert.equal(runtime.inspect(target)?.phase, "input");
  now += TELEGRAM_THREAD_NAME_DIALOG_TTL_MS;
  assert.deepEqual(runtime.consumeName({
    scope: "session:1", target, text: "Late",
  }), { kind: "none" });
  assert.equal(runtime.inspect(target), undefined);
});

test("Thread-name dialog scope cleanup invalidates every target in that session", () => {
  const runtime = createTelegramThreadNameDialogRuntime();
  runtime.open({ scope: "session:1", target, dialogMessageId: 40 });
  runtime.open({
    scope: "session:1", target: { chatId: 7, threadId: 43 }, dialogMessageId: 41,
  });
  runtime.open({
    scope: "session:2", target: { chatId: 7, threadId: 44 }, dialogMessageId: 42,
  });
  runtime.clearScope("session:1");
  assert.equal(runtime.inspect(target), undefined);
  assert.equal(runtime.inspect({ chatId: 7, threadId: 43 }), undefined);
  assert.ok(runtime.inspect({ chatId: 7, threadId: 44 }));
});

for (const phase of ["delivery", "input", "name", "reset"] as const) {
  for (const boundary of ["replace", "clear", "expiry", "recipient", "throw"] as const) {
    test(`Thread-name prepared lifetime refuses ${boundary} during ${phase}`, () => {
      let now = 100;
      let authority = true;
      let throws = false;
      const runtime = createTelegramThreadNameDialogRuntime({ nowMs: () => now });
      const lifetime = runtime.prepare({
        scope: "session:1", target,
        isCurrent() {
          if (throws) throw new Error("ended recipient");
          return authority;
        },
      });
      assert.ok(lifetime);
      if (phase !== "delivery") assert.ok(lifetime.publish(10));
      if (phase === "name") {
        assert.deepEqual(lifetime.consumeName(" Navigator "), { kind: "name", name: "Navigator" });
      }
      if (phase === "reset") assert.deepEqual(lifetime.select("reset"), { kind: "reset" });
      assert.equal(lifetime.isCurrent(), true);
      const neighbor = runtime.open({
        scope: "other-session", target: { chatId: 7, threadId: 43 }, dialogMessageId: 10,
      });
      if (boundary === "replace") {
        // Identical scope/target/message IDs cannot renew an earlier preparation.
        runtime.open({ scope: "session:1", target, dialogMessageId: 10 });
      }
      if (boundary === "clear") runtime.clearScope("session:1");
      if (boundary === "expiry") now += TELEGRAM_THREAD_NAME_DIALOG_TTL_MS;
      if (boundary === "recipient") authority = false;
      if (boundary === "throw") throws = true;
      assert.equal(lifetime.isCurrent(), false);
      authority = true;
      throws = false;
      now = 100;
      assert.equal(lifetime.isCurrent(), false, "observed loss never renews the captured lifetime");
      assert.equal(lifetime.publish(99), undefined);
      assert.deepEqual(lifetime.consumeName("Late"), { kind: "none" });
      assert.deepEqual(lifetime.select("cancel"), { kind: "expired" });
      assert.equal(lifetime.reopen(), undefined);
      lifetime.finish();
      assert.deepEqual(runtime.inspect({ chatId: 7, threadId: 43 }), neighbor);
      if (boundary === "replace") {
        assert.equal(runtime.inspect(target)?.dialogMessageId, 10, "stale finish cannot clear replacement");
      } else {
        assert.equal(runtime.inspect(target), undefined);
      }
    });
  }
}

test("Thread-name prepared lifetime captures target and recipient callback before delivery", () => {
  const runtime = createTelegramThreadNameDialogRuntime();
  let authority = true;
  const input = {
    scope: "session:1", target: { ...target }, isCurrent: () => authority,
  };
  const lifetime = runtime.prepare(input);
  assert.ok(lifetime);
  input.scope = "replacement";
  input.target.threadId = 88;
  input.isCurrent = () => true;
  assert.equal(runtime.inspect(target), undefined, "delivery is not published input");
  assert.equal(runtime.capture({ scope: "session:1", target, dialogMessageId: 10 }), undefined);
  const published = lifetime.publish(10);
  assert.ok(published);
  published.target.threadId = 99;
  assert.equal(runtime.inspect(target)?.target.threadId, 42);
  assert.equal(runtime.inspect({ chatId: 7, threadId: 88 }), undefined);
  assert.equal(lifetime.publish(11), undefined, "publication never repeats");
  authority = false;
  assert.equal(lifetime.isCurrent(), false);
  assert.equal(runtime.inspect(target), undefined);
});

test("Thread-name prepared publication refuses invalid IDs without publishing or replay", () => {
  for (const id of [0, -1, 1.5, NaN, Infinity]) {
    const runtime = createTelegramThreadNameDialogRuntime();
    const lifetime = runtime.prepare({ scope: "session:1", target });
    assert.ok(lifetime);
    assert.equal(lifetime.publish(id), undefined);
    assert.equal(runtime.inspect(target), undefined);
    assert.equal(lifetime.publish(10), undefined);
    assert.equal(lifetime.isCurrent(), false);
  }
});

test("Thread-name captured input reopens only its own consumption at the original deadline", () => {
  let now = 100;
  const runtime = createTelegramThreadNameDialogRuntime({ nowMs: () => now });
  const candidate = runtime.open({ scope: "session:1", target, dialogMessageId: 10 });
  assert.equal(runtime.capture({ scope: "wrong", target, dialogMessageId: 10 }), undefined);
  assert.equal(runtime.capture({ scope: "session:1", target, dialogMessageId: 11 }), undefined);
  const lifetime = runtime.capture({ scope: "session:1", target, dialogMessageId: 10 });
  const competitor = runtime.capture({ scope: "session:1", target, dialogMessageId: 10 });
  assert.ok(lifetime);
  assert.ok(competitor);
  assert.deepEqual(lifetime.consumeName("  "), { kind: "empty" });
  assert.equal(lifetime.reopen(), undefined);
  assert.deepEqual(lifetime.consumeName("First"), { kind: "name", name: "First" });
  assert.equal(runtime.inspect(target), undefined);
  assert.deepEqual(competitor.consumeName("Other"), { kind: "none" });
  assert.equal(competitor.reopen(), undefined, "another capture cannot reopen issued work");
  now += 1_000;
  assert.deepEqual(lifetime.reopen(), candidate);
  assert.equal(lifetime.isCurrent(), false, "reopening ends the issued lifetime");
  const reset = runtime.capture({ scope: "session:1", target, dialogMessageId: 10 });
  assert.ok(reset);
  assert.deepEqual(reset.select("reset"), { kind: "reset" });
  assert.equal(reset.select("reset").kind, "expired");
  now = candidate.expiresAtMs;
  assert.equal(reset.reopen(), undefined);
  assert.equal(runtime.inspect(target), undefined);
});

test("Thread-name cancellation and finish terminalize only the captured lifetime", () => {
  const runtime = createTelegramThreadNameDialogRuntime();
  for (const action of ["cancel", "name", "reset"] as const) {
    const lifetime = runtime.prepare({ scope: "session:1", target });
    assert.ok(lifetime);
    assert.ok(lifetime.publish(10));
    if (action === "name") lifetime.consumeName("Navigator");
    else lifetime.select(action);
    lifetime.finish();
    assert.equal(lifetime.isCurrent(), false);
    assert.equal(lifetime.reopen(), undefined);
    assert.equal(runtime.inspect(target), undefined);
  }
  runtime.open({ scope: "session:1", target, dialogMessageId: 10 });
  const captured = runtime.capture({ scope: "session:1", target, dialogMessageId: 10 });
  assert.ok(captured);
  runtime.consumeName({ scope: "session:1", target, text: "Ordinary" });
  assert.equal(captured.isCurrent(), false, "legacy consumption ends a prepared capture too");
  assert.equal(captured.reopen(), undefined);
});

test("Thread-name recipient checks cannot lend authority through synchronous replacement", () => {
  for (const boundary of ["replace", "clear"] as const) {
    const runtime = createTelegramThreadNameDialogRuntime();
    let mutate = false;
    const lifetime = runtime.prepare({
      scope: "session:1", target,
      isCurrent() {
        if (mutate) {
          mutate = false;
          if (boundary === "replace") runtime.open({ scope: "session:1", target, dialogMessageId: 77 });
          else runtime.clearScope("session:1");
        }
        return true;
      },
    });
    assert.ok(lifetime);
    mutate = true;
    assert.equal(lifetime.publish(10), undefined);
    assert.equal(lifetime.isCurrent(), false);
    assert.equal(runtime.inspect(target)?.dialogMessageId, boundary === "replace" ? 77 : undefined);
  }
});

test("Thread-name post-observation claim check preserves another exact consumer", () => {
  const runtime = createTelegramThreadNameDialogRuntime();
  let consume: (() => void) | undefined;
  const prepared = runtime.prepare({
    scope: "session:1", target,
    isCurrent() {
      const effect = consume;
      consume = undefined;
      effect?.();
      return true;
    },
  });
  assert.ok(prepared?.publish(10));
  const winner = runtime.capture({ scope: "session:1", target, dialogMessageId: 10 });
  const loser = runtime.capture({ scope: "session:1", target, dialogMessageId: 10 });
  assert.ok(winner);
  assert.ok(loser);
  consume = () => assert.deepEqual(winner.select("reset"), { kind: "reset" });
  assert.equal(loser.isCurrent(), false);
  loser.finish();
  assert.equal(winner.isCurrent(), true);
  assert.ok(winner.reopen(), "the unrelated losing observer cannot revoke issued reset");
});

test("Thread-name held delivery cannot publish after newer delivery or session cleanup", async () => {
  for (const boundary of ["replace", "clear"] as const) {
    const runtime = createTelegramThreadNameDialogRuntime();
    const send = Promise.withResolvers<number>();
    const lifetime = runtime.prepare({ scope: "session:1", target });
    assert.ok(lifetime);
    const delivery = (async () => lifetime.publish(await send.promise))();
    if (boundary === "replace") {
      const next = runtime.prepare({ scope: "session:1", target });
      assert.ok(next?.publish(11));
    } else runtime.clearScope("session:1");
    send.resolve(10);
    assert.equal(await delivery, undefined);
    assert.equal(runtime.inspect(target)?.dialogMessageId, boundary === "replace" ? 11 : undefined);
  }
});

test("Thread-name held name/reset failure cannot reopen a replacement after await", async () => {
  for (const kind of ["name", "reset"] as const) {
    const runtime = createTelegramThreadNameDialogRuntime();
    const result = Promise.withResolvers<void>();
    const lifetime = runtime.prepare({ scope: "session:1", target });
    assert.ok(lifetime);
    assert.ok(lifetime.publish(10));
    if (kind === "name") assert.deepEqual(lifetime.consumeName("First"), { kind: "name", name: "First" });
    else assert.deepEqual(lifetime.select("reset"), { kind: "reset" });
    const operation = (async () => {
      await result.promise;
      return { current: lifetime.isCurrent(), reopened: lifetime.reopen() };
    })();
    const replacement = runtime.open({ scope: "session:1", target, dialogMessageId: 10 });
    result.resolve();
    assert.deepEqual(await operation, { current: false, reopened: undefined });
    assert.deepEqual(runtime.inspect(target), replacement);
  }
});

test("Thread-name hidden delivery/consumption expiry cannot be renewed by clock rewind", () => {
  for (const phase of ["delivery", "reset"] as const) {
    let now = 100;
    const runtime = createTelegramThreadNameDialogRuntime({ nowMs: () => now });
    const lifetime = runtime.prepare({ scope: "session:1", target });
    assert.ok(lifetime);
    if (phase === "reset") {
      assert.ok(lifetime.publish(10));
      lifetime.select("reset");
    }
    now += TELEGRAM_THREAD_NAME_DIALOG_TTL_MS;
    assert.equal(runtime.inspect(target), undefined);
    now = 100;
    assert.equal(lifetime.isCurrent(), false, "inspect must also expire unpublished or consumed entries");
  }
});

test("Thread-name preparation captures authority before clock and preserves a reentrant successor", () => {
  let observe: (() => void) | undefined;
  const runtime = createTelegramThreadNameDialogRuntime({
    nowMs() {
      const effect = observe;
      observe = undefined;
      effect?.();
      return 100;
    },
  });
  const input = { scope: "session:1", target, isCurrent: () => false };
  observe = () => { input.isCurrent = () => true; };
  assert.equal(runtime.prepare(input), undefined, "clock cannot replace the captured callback");
  for (const boundary of ["clock", "recipient"] as const) {
    const publish = () => runtime.open({ scope: "session:1", target, dialogMessageId: 77 });
    if (boundary === "clock") observe = publish;
    assert.equal(runtime.prepare({
      scope: "session:1", target,
      isCurrent() {
        if (boundary === "recipient") publish();
        return true;
      },
    }), undefined, "an older preparation cannot overwrite a reentrant successor");
    assert.equal(runtime.inspect(target)?.dialogMessageId, 77);
  }
});

test("Thread-name supplied recipient checks require positive boolean authority", () => {
  for (const value of [undefined, null, 0, 1, "true"]) {
    const runtime = createTelegramThreadNameDialogRuntime();
    const guard = (() => value) as unknown as () => boolean;
    assert.equal(runtime.prepare({ scope: "session:1", target, isCurrent: guard }), undefined);
  }
});

test("Thread-name capture and ordinary actions keep their inputs across recipient observations", () => {
  const runtime = createTelegramThreadNameDialogRuntime();
  let observe: (() => void) | undefined;
  const open = () => {
    const lifetime = runtime.prepare({
      scope: "session:1", target,
      isCurrent() {
        const effect = observe;
        observe = undefined;
        effect?.();
        return true;
      },
    });
    assert.ok(lifetime?.publish(10));
  };
  open();
  const captureInput = { scope: "session:1", target: { ...target }, dialogMessageId: 99 };
  observe = () => { captureInput.dialogMessageId = 10; captureInput.target.threadId = 88; };
  assert.equal(runtime.capture(captureInput), undefined, "a later observed ID cannot authorize capture");
  const selected = { scope: "session:1", target, dialogMessageId: 10, action: "cancel" as "cancel" | "reset" };
  observe = () => { selected.action = "reset"; };
  assert.deepEqual(runtime.select(selected), { kind: "cancel" });
  assert.equal(runtime.inspect(target), undefined);
  open();
  const consumed = { scope: "session:1", target, text: "Original" };
  observe = () => { consumed.text = "Replacement"; };
  assert.deepEqual(runtime.consumeName(consumed), { kind: "name", name: "Original" });
});

test("Thread-name refused preparation preserves ordinary current input and other scopes", () => {
  const runtime = createTelegramThreadNameDialogRuntime();
  const candidate = runtime.open({ scope: "session:1", target, dialogMessageId: 10 });
  assert.equal(runtime.prepare({ scope: "replacement", target, isCurrent: () => false }), undefined);
  assert.deepEqual(runtime.inspect(target), candidate);
  assert.deepEqual(runtime.select({ scope: "session:1", target, dialogMessageId: 10, action: "cancel" }), { kind: "cancel" });
});

test("Baked thread names stay compact for narrow Telegram tabs", () => {
  for (const slot of "ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
    const seen = new Set<string>();
    for (let index = 0; index < 5; index += 1) {
      const name = chooseTelegramThreadName({
        slot,
        getRandom: () => index / 5,
      });
      assert.ok(name, `Expected baked name for slot ${slot}`);
      assert.equal(name.startsWith(slot), true);
      assert.ok(
        name.length >= 4 && name.length <= 6,
        `${name} should be 4-6 letters`,
      );
      seen.add(name);
    }
    assert.equal(seen.size, 5, `Expected five names for slot ${slot}`);
  }
});

test("Baked thread names skip identities reserved by Workspace bindings", () => {
  assert.equal(
    chooseTelegramThreadName({
      slot: "C",
      getRandom: () => 0,
      occupied: ["Cedar", "Comet", "Cipher", "Coral"],
    }),
    "Cinder",
  );
});

test("Baked thread names can be selected from timestamp entropy", () => {
  const first = chooseTelegramThreadName({
    slot: "C",
    entropy: 1_720_000_000_001,
  });
  const second = chooseTelegramThreadName({
    slot: "C",
    entropy: 1_720_000_000_001,
  });
  const nearby = chooseTelegramThreadName({
    slot: "C",
    entropy: 1_720_000_000_002,
  });

  assert.equal(first, second);
  assert.ok(first?.startsWith("C"));
  assert.ok(nearby?.startsWith("C"));
});

test("Thread recovery identities remain compact capitalized Latin names", () => {
  assert.equal(getTelegramTopicIdentityName("Jname"), "Jname");
  assert.equal(getTelegramTopicIdentityName("  Jname  "), "Jname");
  assert.equal(isTelegramTopicThreadNameValidForSlot("Jname", "J"), true);
  for (const name of [
    "J", "name", "Follower", "J identity", "J-identity", "Word Word",
    "wasd_123!?+$@", "🌙 J-identity",
  ]) {
    assert.equal(isTelegramTopicThreadNameValidForSlot(name, "J"), false, name);
  }
});

test("Manual Thread display names accept bounded printable ASCII", () => {
  for (const name of [
    "Jname", "name", "Follower", "J identity", "J-identity", "Word Word",
    "wasd_123!?+$@",
  ]) {
    assert.equal(getTelegramManualThreadDisplayNameValidationError(name), undefined, name);
  }
  assert.match(getTelegramManualThreadDisplayNameValidationError("A") ?? "", /reset/);
  assert.match(getTelegramManualThreadDisplayNameValidationError("   ") ?? "", /empty/);
  assert.match(getTelegramManualThreadDisplayNameValidationError("🌙") ?? "", /printable ASCII/);
  assert.match(getTelegramManualThreadDisplayNameValidationError("line\nbreak") ?? "", /printable ASCII/);
  assert.match(getTelegramManualThreadDisplayNameValidationError("x".repeat(97)) ?? "", /96/);
});

test("Thread titles are trimmed and capped to Telegram's 128 character limit", () => {
  const name = getTelegramTopicName(
    {
      instanceId: "inst-a",
      profileKey: "cwd:/repo",
      threadName: `repo ${"x".repeat(200)}`,
    },
    "  Pi   {threadName}  ",
  );
  assert.equal(name.length, 128);
  assert.match(name, /^Pi repo x+/);
});

test("Thread palette order and exhaustion preserve fallback and random edge behavior", () => {
  const occupied = Array.from("ABCDEFGHIJKLMNOPQRSTUVWXYZ").flatMap(slot =>
    Array.from({ length: 5 }, (_, index) => Naming.chooseTelegramThreadName({ slot, getRandom: () => index / 5 })!));
  assert.equal(new Set(occupied).size, 130);
  assert.equal(Naming.chooseTelegramThreadName({ slot: "A", occupied }), undefined);
  assert.equal(Naming.chooseTelegramThreadName({ slot: "A", occupied: occupied.slice(0, 5), getRandom: () => 0 }), "Beacon");
  assert.equal(Naming.chooseTelegramThreadName({ slot: undefined }), undefined);
  assert.equal(Naming.chooseTelegramThreadName({ slot: "a" }), undefined);
  assert.equal(Naming.chooseTelegramThreadName({ slot: "A", getRandom: () => -1 }), "Atlas");
  assert.equal(Naming.chooseTelegramThreadName({ slot: "A", getRandom: () => Infinity }), "Ashen");
  assert.throws(() => Naming.chooseTelegramThreadName({ slot: "A", getRandom: () => NaN }), TypeError);
  assert.equal(Naming.chooseTelegramThreadName({ slot: "A", entropy: 0 }), "Atlas");
});

test("Thread identity normalization and title fallback retain their different bounds", () => {
  assert.equal(Naming.normalizeTelegramTopicTargetThreadName("  Alpha\t Beta \n"), "Alpha Beta");
  assert.equal(Naming.getTelegramTopicIdentityName("  🌙 Alpha  "), "🌙 Alpha");
  assert.equal(Naming.getTelegramThreadNameLeadingSlot(" Jname "), "J");
  assert.equal(Naming.getTelegramThreadNameLeadingSlot("name"), undefined);
  assert.equal(Naming.normalizeTelegramTopicTargetThreadName("x".repeat(100)).length, 96);
  assert.equal(Naming.getTelegramTopicName({ instanceId: "id", profileKey: "profile", threadName: " " }, "{threadName}"), "profile");
  assert.equal(Naming.getTelegramTopicName({ instanceId: "id", profileKey: "profile" }, "", "A"), "A");
  assert.equal(Naming.getTelegramTopicName({ instanceId: "id", profileKey: "profile" }, ""), "Pi");
});
