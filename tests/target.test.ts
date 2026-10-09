/**
 * Telegram target regression tests
 * Zones: telegram transport, routing, multi-instance bus
 * Covers exact address-value identity shared by live routing, delivery and Workspace
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  areTelegramTargetsEqual,
  createTelegramPrivateTarget,
  createTelegramThreadTarget,
  getTelegramTargetKey,
  getTelegramTargetThreadParams,
  isTelegramThreadTarget,
  parseTelegramIntegerId,
  parseTelegramTarget,
} from "../lib/target.ts";

test("Telegram target helpers model private chat targets", () => {
  const target = createTelegramPrivateTarget(7);
  assert.deepEqual(target, { chatId: 7 });
  assert.equal(getTelegramTargetKey(target), "7:private");
  assert.equal(isTelegramThreadTarget(target), false);
  assert.deepEqual(getTelegramTargetThreadParams(target), {});
});

test("Telegram target helpers model topic thread targets", () => {
  const target = createTelegramThreadTarget(-100123, 42);
  assert.deepEqual(target, { chatId: -100123, threadId: 42 });
  assert.equal(getTelegramTargetKey(target), "-100123:42");
  assert.equal(isTelegramThreadTarget(target), true);
  assert.deepEqual(getTelegramTargetThreadParams(target), {
    message_thread_id: 42,
  });
});

test("Telegram target equality is exact address-value equality without normalization", () => {
  const groups = [
    [{ chatId: 1 }, { chatId: 1, threadId: undefined }],
    [{ chatId: -1 }],
    [{ chatId: 1, threadId: 0 }],
    [{ chatId: 1, threadId: 2 }, { chatId: 1, threadId: 2 }],
    [{ chatId: 1, threadId: 3 }],
    [{ chatId: 2, threadId: 2 }],
    [{ chatId: 0, threadId: 0 }],
  ];
  for (const [leftGroup, leftTargets] of groups.entries()) {
    for (const [rightGroup, rightTargets] of groups.entries()) {
      for (const left of leftTargets) {
        for (const right of rightTargets) {
          assert.equal(areTelegramTargetsEqual(left, right), leftGroup === rightGroup,
            `${JSON.stringify(left)} vs ${JSON.stringify(right)}`);
        }
      }
    }
  }
  const invalid = { chatId: NaN, threadId: 2 };
  assert.equal(areTelegramTargetsEqual(invalid, invalid), false, "reference identity cannot bypass field equality");
  assert.equal(getTelegramTargetKey({ chatId: 1, threadId: undefined }), "1:private");
  assert.equal(getTelegramTargetKey({ chatId: -1, threadId: 0 }), "-1:0");
});

test("Target wire parsers keep only exact address fields and integer ids", () => {
  assert.deepEqual(parseTelegramTarget({ chatId: 7, threadId: 42, extra: true }), { chatId: 7, threadId: 42 });
  assert.deepEqual(parseTelegramTarget({ chatId: 7, threadId: "42" }), { chatId: 7 });
  for (const value of [undefined, null, [], "7", { chatId: "7" }]) assert.equal(parseTelegramTarget(value), undefined);
  assert.equal(parseTelegramIntegerId(42), 42);
  assert.equal(parseTelegramIntegerId("-100"), -100);
  for (const value of [1.5, "", " ", "1.5", "x", null, undefined]) assert.equal(parseTelegramIntegerId(value), undefined);
});
