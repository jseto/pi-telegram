/**
 * Regression tests for Telegram bus-aware API runtime
 * Verifies follower outbound API calls route through the local bus while leaders use direct transport
 */

import assert from "node:assert/strict";
import test from "node:test";

import { createTelegramBusAwareApiRuntime, createTelegramSelectedMenuTextApi } from "../lib/bus-api.ts";
import { createTelegramExtensionSectionRegistry, handleTelegramSectionCallback, type TelegramSectionRuntimeDeps } from "../lib/sections.ts";
import { createTelegramBusLeaderApiProxy } from "../lib/bus-leader.ts";
import { callTelegram, createTelegramApiClient, createTelegramBridgeApiRuntime, type TelegramBridgeApiRuntime, type TelegramApiCallOptions } from "../lib/telegram-api.ts";
import { captureTelegramStaleTargetRequestRecovery, type TelegramSyncState } from "../lib/sync.ts";
import type { TelegramTopicTargetRecord } from "../lib/threads.ts";

test("Selected text API preparation proves availability without activating the future recipient", async () => {
  let assertions = 0;
  const config = { operationId: "operation", registrationGeneration: "generation", target: { chatId: 7, threadId: 42 },
    assertAuthority() { assertions++; throw new Error("Future recipient is not released"); },
    deliver: async () => assert.fail("No issuance") };
  const api = createTelegramSelectedMenuTextApi(config);
  assert.equal(assertions, 0);
  for (const change of [{ deliver: undefined }, { assertAuthority: undefined }, { target: { chatId: 7, threadId: 0 } }, { operationId: "" }, { registrationGeneration: "" }]) {
    assert.throws(() => createTelegramSelectedMenuTextApi({ ...config, ...change } as unknown as Parameters<typeof createTelegramSelectedMenuTextApi>[0]));
  }
  assert.equal(assertions, 0);
  await assert.rejects(api.sendMessage({ chat_id: 7, message_thread_id: 42, text: "Status" }, { assertAuthority: config.assertAuthority }));
  assert.equal(assertions, 1);
});

for (const edit of [false, true]) for (const fault of ["current", "callback", "callback-missing", "target", "thread", "extra", "preview", "markup", "mode", "reply", "reply-missing", "lost", "late", "observation", "capture"] as const) {
  test(`Selected text API ${edit ? "edit" : "send"} preserves closed captured menu body (${fault})`, async () => {
    let current = true, calls = 0;
    const assertAuthority = () => { if (!current) throw new Error("Recipient changed"); };
    const gate = Promise.withResolvers<void>(), effects: unknown[] = [];
    const target = { chatId: 7, threadId: 42 }, options = { assertAuthority }, config = { operationId: "operation", registrationGeneration: "generation", target, assertAuthority,
      async deliver(effect: { kind: "send-text" | "edit-text"; text: string }, assertion: () => void) {
        assert.equal(assertion, assertAuthority); calls++; effects.push(structuredClone(effect)); await gate.promise;
        if (fault === "lost") throw new Error("Issued reply lost");
        return { operationId: fault === "observation" ? "other" : "operation", registrationGeneration: "generation", recipient: { target: { chatId: 7, threadId: 42 }, sessionId: "session", sessionGeneration: 1,
          processId: 1, processBirthId: "birth", profileKey: "profile", journalBindingKey: "journal" }, effect: effect.kind, messageId: 11 };
      } };
    const api = createTelegramSelectedMenuTextApi(config);
    const body: Record<string, unknown> = { chat_id: 7, message_thread_id: 42, text: "<b>Status</b>", parse_mode: "HTML", reply_markup: { inline_keyboard: [[{ text: "Queue", callback_data: "queue" }]] },
      ...(edit ? { message_id: 11 } : { reply_parameters: { message_id: 11, allow_sending_without_reply: true } }) };
    if (fault === "callback") options.assertAuthority = () => {};
    if (fault === "callback-missing") Reflect.deleteProperty(options, "assertAuthority");
    if (fault === "target") body.chat_id = 8;
    if (fault === "thread") body.message_thread_id = 99;
    if (fault === "extra") body.foreign = true;
    if (fault === "preview") body.link_preview_options = { is_disabled: true };
    if (fault === "markup") body.reply_markup = { inline_keyboard: [[{ text: "Site", url: "https://example.com" }]] };
    if (fault === "mode") body.parse_mode = "Markdown";
    if (fault === "reply") body.reply_parameters = { message_id: 11, allow_sending_without_reply: false };
    if (fault === "reply-missing") body.reply_parameters = { allow_sending_without_reply: true };
    const task = edit ? api.editMessageText(body as Parameters<typeof api.editMessageText>[0], options) : api.sendMessage(body as Parameters<typeof api.sendMessage>[0], options);
    const early = ["callback", "callback-missing", "target", "thread", "extra", "preview", "markup", "mode", "reply", "reply-missing"].includes(fault);
    if (fault === "late") current = false;
    if (fault === "capture") { body.text = "changed"; target.threadId = 99; config.assertAuthority = () => {}; options.assertAuthority = () => {}; config.operationId = "changed"; }
    gate.resolve();
    if (early || ["lost", "late", "observation"].includes(fault)) await assert.rejects(task);
    else assert.deepEqual(await task, edit ? "edited" : { message_id: 11 });
    assert.equal(calls, early ? 0 : 1);
    if (calls) assert.deepEqual(effects[0], { kind: edit ? "edit-text" : "send-text", text: "<b>Status</b>", parseMode: "HTML",
      replyMarkup: { inline_keyboard: [[{ text: "Queue", callback_data: "queue" }]] }, ...(edit ? { messageId: 11 } : { replyToMessageId: 11 }) });
  });
}

for (const direct of [true, false]) {
  test(`Typed callback answers send authored toast text verbatim (direct=${direct})`, async () => {
    const calls: unknown[] = [];
    const runtime = createTelegramBusAwareApiRuntime({ directRuntime: createDirectRuntime(calls), ownsDirect: () => direct,
      async callFollowerApi(method, args) { calls.push({ method, args }); return { message_id: 1 }; } });
    // Each toast is written in its final form at its call site; the boundary never rewrites punctuation.
    const cases: Array<string | undefined> = [undefined, "", "✅ Saved", "⚠️ Wrote a period.", "Continue?", "Working…", "v1.2.3"];
    for (const text of cases) {
      const expected = text;
      calls.length = 0;
      await runtime.answerCallbackQuery("callback", text);
      assert.deepEqual(calls, direct ? [{ kind: "answer-callback", callbackQueryId: "callback", text: expected }] :
        [{ method: "call", args: ["answerCallbackQuery", { callback_query_id: "callback", ...(expected !== undefined ? { text: expected } : {}) }] }]);
    }
    calls.length = 0;
    const body = { chat_id: 100, text: "An in-chat notice." };
    await runtime.sendMessage(body);
    assert.deepEqual(calls, direct ? [{ kind: "message", body }] : [{ method: "call", args: ["sendMessage", body] }]);
    calls.length = 0;
    const raw = { callback_query_id: "raw", text: "Explicit raw API payload." };
    await runtime.call("answerCallbackQuery", raw);
    assert.deepEqual(calls, direct ? [{ kind: "call", method: "answerCallbackQuery", body: raw }] :
      [{ method: "call", args: ["answerCallbackQuery", raw, undefined] }]);
  });
}

for (const direct of [true, false]) {
  test(`Companion section toasts reach the client exactly as the companion wrote them (direct=${direct})`, async () => {
    const calls: unknown[] = [], edited: string[] = [];
    const runtime = createTelegramBusAwareApiRuntime({ directRuntime: createDirectRuntime(calls), ownsDirect: () => direct,
      async callFollowerApi(method, args) { calls.push({ method, args }); return { message_id: 1 }; } });
    const registry = createTelegramExtensionSectionRegistry();
    registry.register({ id: "@fixture/companion", label: "Companion", render: () => ({ text: "Companion." }),
      async handleCallback(ctx) {
        await ctx.answerCallback(ctx.payload || undefined);
        await ctx.edit({ text: "An in-chat notice." });
        return "handled" as const;
      } });
    const deps: TelegramSectionRuntimeDeps = {
      answerCallbackQuery: runtime.answerCallbackQuery,
      editInteractiveMessage: async (_chat, _message, text) => { edited.push(text); },
      sendInteractiveMessage: async () => undefined, sendRichMessage: async () => undefined,
      enqueuePrompt: async () => {}, deleteMessage: async () => {},
    };
    for (const [text, expected] of [["✅ Saved", "✅ Saved"], ["⚠️ Operation failed.", "⚠️ Operation failed."],
      ["Continue?", "Continue?"], ["", undefined]] as const) {
      calls.length = 0;
      assert.equal(await handleTelegramSectionCallback(registry, "0", "control", text, 100, 1, "companion-callback", deps), true);
      assert.deepEqual(calls, direct ? [{ kind: "answer-callback", callbackQueryId: "companion-callback", text: expected }] :
        [{ method: "call", args: ["answerCallbackQuery", { callback_query_id: "companion-callback", ...(expected !== undefined ? { text: expected } : {}) }] }]);
      assert.equal(edited.at(-1), "An in-chat notice.");
    }
  });
}

for (const direct of [true, false]) {
  for (const multipart of [true, false]) {
    test(`Per-call API authority ${direct ? "stays local" : "cannot cross IPC"} for ${multipart ? "multipart" : "JSON"}`, async () => {
      const calls: unknown[] = [];
      let guardedDirectCalls = 0;
      const options: TelegramApiCallOptions = { assertAuthority() {} };
      const runtime = createTelegramBusAwareApiRuntime({ ownsDirect: () => direct,
        directRuntime: { ...createDirectRuntime(calls),
          async call<TResponse>(_method: string, _body: Record<string, unknown>, supplied?: TelegramApiCallOptions) {
            assert.equal(supplied?.assertAuthority, options.assertAuthority);
            guardedDirectCalls++;
            return true as TResponse;
          },
          async callMultipart<TResponse>(_method: string, _fields: Record<string, string>, _field: string, _path: string,
            _name: string, supplied?: TelegramApiCallOptions) {
            assert.equal(supplied?.assertAuthority, options.assertAuthority);
            guardedDirectCalls++;
            return true as TResponse;
          },
        },
        async callFollowerApi(method, args) { calls.push({ method, args }); return true; },
      });
      const request = multipart ? runtime.callMultipart("sendDocument", { chat_id: "7" }, "document", "/unused", "fixture.txt", options)
        : runtime.call("sendMessage", { chat_id: 7, text: "menu" }, options);
      if (direct) assert.equal(await request, true);
      else await assert.rejects(request, { name: "TelegramApiAuthorityError", requestIssued: false });
      assert.equal(guardedDirectCalls, direct ? 1 : 0);
      assert.deepEqual(calls, [], "A guarded effect is never serialized without its enforceable lifetime.");
    });
  }
}

for (const direct of [true, false]) {
  test(`Guarded BotFather typed API ${direct ? "preserves direct authority" : "refuses follower serialization"}`, async () => {
    const calls: unknown[] = [], options: Pick<TelegramApiCallOptions, "assertAuthority"> = { assertAuthority() {} };
    const commands = [{ command: "start", description: "Open menu" }];
    let guardedCalls = 0;
    const runtime = createTelegramBusAwareApiRuntime({ ownsDirect: () => direct,
      directRuntime: { ...createDirectRuntime(calls), async setMyCommands(value, supplied) {
        assert.equal(value, commands);
        assert.equal(supplied?.assertAuthority, options.assertAuthority);
        guardedCalls++;
        return true;
      } },
      callFollowerApi: async () => assert.fail("A process-local lifetime cannot become a wire grant"),
    });
    if (direct) assert.equal(await runtime.setMyCommands(commands, options), true);
    else await assert.rejects(runtime.setMyCommands(commands, options), { name: "TelegramApiAuthorityError", requestIssued: false });
    assert.equal(guardedCalls, direct ? 1 : 0);
    assert.deepEqual(calls, []);
  });
}

for (const direct of [true, false]) {
  test(`Typed callback authority ${direct ? "forwards the exact local guard" : "refuses before follower IPC"}`, async () => {
    const calls: unknown[] = [], options: Pick<TelegramApiCallOptions, "assertAuthority"> = { assertAuthority() {} };
    let guardedCalls = 0;
    const runtime = createTelegramBusAwareApiRuntime({ ownsDirect: () => direct,
      directRuntime: { ...createDirectRuntime(calls), async answerCallbackQuery(id, text, supplied) {
        assert.equal(id, "captured-query");
        assert.equal(text, "✅ Saved");
        assert.equal(supplied?.assertAuthority, options.assertAuthority);
        guardedCalls++;
      } },
      callFollowerApi: async () => assert.fail("A callback closure cannot become a wire grant"),
    });
    const request = runtime.answerCallbackQuery("captured-query", "✅ Saved", options);
    if (direct) await request;
    else await assert.rejects(request, { name: "TelegramApiAuthorityError", requestIssued: false });
    assert.equal(guardedCalls, direct ? 1 : 0);
    assert.deepEqual(calls, []);
  });
}

for (const direct of [true, false]) {
  test(`Typed callback authority captures its guard before direct-owner observation (direct=${direct})`, async () => {
    const guard = () => {};
    const options: Pick<TelegramApiCallOptions, "assertAuthority"> = { assertAuthority: guard };
    let guardedCalls = 0;
    const runtime = createTelegramBusAwareApiRuntime({
      ownsDirect() { options.assertAuthority = undefined; return direct; },
      directRuntime: { ...createDirectRuntime([]), async answerCallbackQuery(_id, _text, supplied) {
        assert.equal(supplied?.assertAuthority, guard);
        guardedCalls++;
      } },
      callFollowerApi: async () => assert.fail("Observation cannot drop the captured guard before IPC"),
    });
    const request = runtime.answerCallbackQuery("captured-query", "✅ Saved", options);
    if (direct) await request;
    else await assert.rejects(request, { name: "TelegramApiAuthorityError", requestIssued: false });
    assert.equal(guardedCalls, direct ? 1 : 0);
  });
}

test("Direct bus delivery invalidates only exact still-current stale request targets without replay", async () => {
  const originalFetch = globalThis.fetch;
  try {
    for (const surface of ["reply", "menu", "activity", "multipart"] as const) {
      for (const race of ["none", "epoch", "generation", "profile", "binding", "load", "unconfirmed", "permission"] as const) {
        let epoch = 1;
        let generation = 1;
        let profile = "default";
        let requests = 0;
        let invalidations = 0;
        let recovered = 0;
        let state: TelegramSyncState = {};
        let record: TelegramTopicTargetRecord | undefined = { profileKey: "leader", instanceId: "leader", target: { chatId: 100, threadId: 42 }, status: "active", createdAtMs: 1, updatedAtMs: 1 };
        globalThis.fetch = async () => {
          requests++;
          if (race === "epoch") epoch++;
          if (race === "generation") generation++;
          if (race === "profile") profile = "other";
          if (race === "binding") record = { ...record!, instanceId: "replacement", updatedAtMs: 2 };
          return new Response(JSON.stringify({ ok: false, description: race === "unconfirmed" ? "Bad Request: possibly message thread not found later" : "Bad Request: message thread not found" }), { status: race === "permission" ? 403 : 400 });
        };
        const direct = createTelegramBridgeApiRuntime({
          client: {
            ...createTelegramApiClient(() => "test-token"),
            callMultipart: (method, fields) => callTelegram("test-token", method, fields),
          },
          tempDir: "/unused", maxFileSizeBytes: 1, tempFileMaxAgeMs: 1,
          recordRuntimeEvent: () => {},
          captureRequestErrorHandler: (body) => captureTelegramStaleTargetRequestRecovery(body, {
            topicTargetStore: {
              load: async () => { if (race === "load") generation++; },
              list: () => record ? [record] : [],
              markStaleByTarget: () => { record = undefined; invalidations++; return true; },
              persist: async () => {},
              invalidateTarget: async (_target, isCurrent) => {
                if (race === "load") generation++;
                if (!isCurrent()) return false;
                record = undefined;
                invalidations++;
                return true;
              },
            },
            getCurrentLeaderEpoch: () => epoch, getSessionGeneration: () => generation, getProfileName: () => profile,
            getSyncState: () => state, setSyncState: (next) => { state = next; }, recordEvent: () => {},
            onRecovered: () => { recovered++; },
          }),
        });
        const runtime = createTelegramBusAwareApiRuntime({ directRuntime: direct, ownsDirect: () => true, callFollowerApi: async () => assert.fail("unexpected follower call") });
        await assert.rejects(async () => {
          const body = { chat_id: 100, message_thread_id: 42 };
          if (surface === "reply") return runtime.sendRichMessage({ ...body, rich_message: { markdown: "answer" } });
          if (surface === "menu") return runtime.sendMessage({ ...body, text: "menu" });
          if (surface === "activity") return runtime.sendTypingAction(100, { message_thread_id: 42 });
          return runtime.callMultipart("sendDocument", { chat_id: "100", message_thread_id: "42" }, "document", "/unused", "unused");
        });
        assert.equal(requests, 1, `${surface}/${race}: replay`);
        assert.equal(invalidations, race === "none" ? 1 : 0, `${surface}/${race}: invalidation`);
        assert.equal(recovered, invalidations);
        if (race === "none") assert.equal(state["target-bindings"]?.status, "suspect");
      }
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
});

function createDirectRuntime(calls: unknown[]): TelegramBridgeApiRuntime {
  return {
    call: async <TResponse>(method: string, body: Record<string, unknown>) => {
      calls.push({ kind: "call", method, body });
      return { ok: true } as TResponse;
    },
    callMultipart: async <TResponse>(
      method: string,
      fields: Record<string, string>,
      fileField: string,
      filePath: string,
      fileName: string,
    ) => {
      calls.push({
        kind: "multipart",
        method,
        fields,
        fileField,
        filePath,
        fileName,
      });
      return { ok: true } as TResponse;
    },
    downloadFile: async (fileId, suggestedName) => {
      calls.push({ kind: "download", fileId, suggestedName });
      return "/tmp/file";
    },
    deleteWebhook: async () => {
      calls.push({ kind: "delete-webhook" });
      return true;
    },
    getUpdates: async (body) => {
      calls.push({ kind: "get-updates", body });
      return [];
    },
    setMyCommands: async (commands) => {
      calls.push({ kind: "commands", commands });
      return true;
    },
    sendChatAction: async (chatId, action, options) => {
      calls.push({ kind: "chat-action", chatId, action, options });
      return true;
    },
    sendTypingAction: async (chatId, options) => {
      calls.push({ kind: "typing", chatId, options });
      return true;
    },
    sendRecordVoiceAction: async (chatId, options) => {
      calls.push({ kind: "record-voice", chatId, options });
      return true;
    },
    sendMessageDraft: async (chatId, draftId, text, options) => {
      calls.push({ kind: "draft", chatId, draftId, text, options });
      return true;
    },
    sendMessage: async (body) => {
      calls.push({ kind: "message", body });
      return { message_id: 1 };
    },
    sendRichMessage: async (body) => {
      calls.push({ kind: "rich", body });
      return { message_id: 2 };
    },
    sendRichMessageDraft: async (body) => {
      calls.push({ kind: "rich-draft", body });
      return true;
    },
    editMessageText: async (body) => {
      calls.push({ kind: "edit", body });
      return "edited";
    },
    editMessageReplyMarkup: async (chatId, messageId, replyMarkup) => {
      calls.push({ kind: "edit-reply-markup", chatId, messageId, replyMarkup });
    },
    answerCallbackQuery: async (callbackQueryId, text) => {
      calls.push({ kind: "answer-callback", callbackQueryId, text });
    },
    answerGuestQuery: async (guestQueryId, text) => {
      calls.push({ kind: "answer-guest", guestQueryId, text });
    },
    answerGuestQueryForInlineMessage: async (guestQueryId, text, options) => {
      calls.push({ kind: "answer-guest-inline", guestQueryId, text, options });
      return "inline-1";
    },
    editGuestInlineMessage: async (inlineMessageId, content) => {
      calls.push({ kind: "edit-guest-inline", inlineMessageId, content });
    },
    deleteMessage: async (chatId, messageId) => {
      calls.push({ kind: "delete", chatId, messageId });
    },
    prepareTempDir: async () => 0,
  };
}

test("Bus-aware API runtime uses direct transport while this instance owns Telegram", async () => {
  const directCalls: unknown[] = [];
  const busCalls: unknown[] = [];
  const runtime = createTelegramBusAwareApiRuntime({
    directRuntime: createDirectRuntime(directCalls),
    ownsDirect: () => true,
    callFollowerApi: async (method, args) => {
      busCalls.push({ method, args });
      return { message_id: 99 };
    },
  });

  assert.deepEqual(
    await runtime.sendRichMessage({
      chat_id: 1,
      rich_message: { markdown: "hi" },
    }),
    {
      message_id: 2,
    },
  );
  assert.deepEqual(directCalls, [
    { kind: "rich", body: { chat_id: 1, rich_message: { markdown: "hi" } } },
  ]);
  assert.deepEqual(busCalls, []);
});

test("Bus-aware API runtime keeps the guest ACK experiment on direct transport", async () => {
  const directCalls: unknown[] = [];
  const direct = createTelegramBusAwareApiRuntime({
    directRuntime: createDirectRuntime(directCalls),
    ownsDirect: () => true,
    callFollowerApi: async () => assert.fail("unexpected follower call"),
  });
  assert.equal(
    await direct.answerGuestQueryForInlineMessage("guest-1", "ack"),
    "inline-1",
  );
  await direct.editGuestInlineMessage("inline-1", {
    richMessage: { markdown: "done" },
  });
  assert.deepEqual(directCalls, [
    {
      kind: "answer-guest-inline",
      guestQueryId: "guest-1",
      text: "ack",
      options: undefined,
    },
    {
      kind: "edit-guest-inline",
      inlineMessageId: "inline-1",
      content: { richMessage: { markdown: "done" } },
    },
  ]);

  const busCalls: unknown[] = [];
  const follower = createTelegramBusAwareApiRuntime({
    directRuntime: createDirectRuntime([]),
    ownsDirect: () => false,
    callFollowerApi: async (method, args) => {
      busCalls.push({ method, args });
      return true;
    },
  });
  await assert.rejects(
    follower.answerGuestQueryForInlineMessage("guest-1", "ack"),
    /answerGuestQueryForInlineMessage requires direct transport ownership/,
  );
  await assert.rejects(
    follower.editGuestInlineMessage("inline-1", { text: "done" }),
    /editGuestInlineMessage requires direct transport ownership/,
  );
  assert.deepEqual(busCalls, []);
});

test("Bus-aware polling methods fail closed without direct ownership", async () => {
  const directCalls: unknown[] = [];
  const busCalls: unknown[] = [];
  const runtime = createTelegramBusAwareApiRuntime({
    directRuntime: createDirectRuntime(directCalls),
    ownsDirect: () => false,
    callFollowerApi: async (method, args) => {
      busCalls.push({ method, args });
      return true;
    },
  });

  await assert.rejects(
    runtime.deleteWebhook(),
    /deleteWebhook requires direct transport ownership/,
  );
  await assert.rejects(
    runtime.getUpdates({ offset: 1 }),
    /getUpdates requires direct transport ownership/,
  );
  assert.deepEqual(directCalls, []);
  assert.deepEqual(busCalls, []);
});

test("Bus-aware API runtime routes follower outbound calls through the leader", async () => {
  const directCalls: unknown[] = [];
  const busCalls: unknown[] = [];
  const runtime = createTelegramBusAwareApiRuntime({
    directRuntime: createDirectRuntime(directCalls),
    ownsDirect: () => false,
    callFollowerApi: async (method, args) => {
      busCalls.push({ method, args });
      if (args[0] === "sendRichMessage") return { message_id: 77 };
      if (method === "downloadFile") return "/tmp/leader-photo.png";
      return true;
    },
  });

  assert.deepEqual(
    await runtime.sendRichMessage({
      chat_id: 1,
      rich_message: {
        markdown: "hi\n\n![](tg://photo?id=result)",
        media: [
          {
            id: "result",
            media: { type: "photo", media: "cached-photo" },
          },
        ],
      },
    }),
    {
      message_id: 77,
    },
  );
  assert.equal(
    await runtime.sendChatAction(1, "typing", { message_thread_id: 5 }),
    true,
  );
  assert.equal(
    await runtime.call("sendChatAction", { chat_id: 1, action: "typing" }),
    true,
  );
  assert.equal(
    await runtime.sendMessageDraft(1, 2, "draft", { message_thread_id: 5 }),
    true,
  );
  await runtime.deleteMessage(1, 9);
  await runtime.answerCallbackQuery("cb1", "Done");
  await runtime.answerGuestQuery("guest1", "Hello", { parseMode: "Markdown" });
  await runtime.answerGuestQuery("guest2", undefined, {
    result: {
      type: "photo",
      id: "photo-1",
      photo_file_id: "cached-photo",
      caption: "Result",
    },
  });
  assert.equal(
    await runtime.downloadFile("file1", "photo.png"),
    "/tmp/leader-photo.png",
  );

  assert.deepEqual(directCalls, []);
  assert.deepEqual(busCalls, [
    {
      method: "call",
      args: [
        "sendRichMessage",
        {
          chat_id: 1,
          rich_message: {
            markdown: "hi\n\n![](tg://photo?id=result)",
            media: [
              {
                id: "result",
                media: { type: "photo", media: "cached-photo" },
              },
            ],
          },
        },
      ],
    },
    {
      method: "call",
      args: [
        "sendChatAction",
        { chat_id: 1, action: "typing", message_thread_id: 5 },
      ],
    },
    {
      method: "call",
      args: ["sendChatAction", { chat_id: 1, action: "typing" }, undefined],
    },
    {
      method: "call",
      args: [
        "sendMessageDraft",
        { chat_id: 1, draft_id: 2, text: "draft", message_thread_id: 5 },
      ],
    },
    {
      method: "call",
      args: ["deleteMessage", { chat_id: 1, message_id: 9 }],
    },
    {
      method: "call",
      args: ["answerCallbackQuery", { callback_query_id: "cb1", text: "Done" }],
    },
    {
      method: "call",
      args: [
        "answerGuestQuery",
        {
          guest_query_id: "guest1",
          result: {
            type: "article",
            id: "1",
            title: "Response",
            input_message_content: {
              message_text: "Hello",
              parse_mode: "Markdown",
            },
          },
        },
      ],
    },
    {
      method: "call",
      args: [
        "answerGuestQuery",
        {
          guest_query_id: "guest2",
          result: {
            type: "photo",
            id: "photo-1",
            photo_file_id: "cached-photo",
            caption: "Result",
          },
        },
      ],
    },
    {
      method: "downloadFile",
      args: ["file1", "photo.png"],
    },
  ]);
});

test("Attachment source survives the bus-aware wrapper and the leader proxy into the scoped file name", async () => {
  const names: string[] = [];
  const direct = createTelegramBridgeApiRuntime({
    client: { ...createTelegramApiClient(() => "test-token"),
      async downloadFile(_fileId, fileName) { names.push(fileName); return `/tmp/${fileName}`; } },
    tempDir: "/unused", maxFileSizeBytes: 1, tempFileMaxAgeMs: 1, getBotScope: () => "@pi_bot", recordRuntimeEvent: () => {},
  });
  const source = { kind: "voice", messageId: 175683, chat: { id: 7, type: "private" } };
  const leader = createTelegramBusAwareApiRuntime({ directRuntime: direct, ownsDirect: () => true,
    callFollowerApi: async () => assert.fail("leader downloads directly") });
  assert.equal(await leader.downloadFile("file", "voice-175683.ogg", source), "/tmp/voice-pi_bot-175683.ogg");
  const proxy = createTelegramBusLeaderApiProxy({ call: async () => true, callMultipart: async () => true,
    downloadFile: direct.downloadFile });
  const follower = createTelegramBusAwareApiRuntime({ directRuntime: direct, ownsDirect: () => false,
    callFollowerApi: (method, args) => proxy(method, structuredClone(args)) });
  assert.equal(await follower.downloadFile("file", "voice-9.ogg", { ...source, messageId: 9 }), "/tmp/voice-pi_bot-9.ogg");
  // A malformed follower source is ignored rather than trusted; the plain generated name remains.
  assert.equal(await proxy("downloadFile", ["file", "voice-10.ogg", { kind: "voice", messageId: "10/../x" }]), "/tmp/voice-10.ogg");
  assert.equal(await follower.downloadFile("file", "photo-11.jpg", { kind: "photo", messageId: 11, chat: source.chat, scope: "@peer" }), "/tmp/photo-peer-11.jpg",
    "an explicit guest scope replaces the bot scope across the proxy");
  assert.deepEqual(names, ["voice-pi_bot-175683.ogg", "voice-pi_bot-9.ogg", "voice-10.ogg", "photo-peer-11.jpg"]);
});

test("Bus-aware API runtime applies follower default thread to scoped actions", async () => {
  const busCalls: unknown[] = [];
  const runtime = createTelegramBusAwareApiRuntime({
    directRuntime: createDirectRuntime([]),
    ownsDirect: () => false,
    getDefaultTarget: () => ({ chatId: 1, threadId: 5 }),
    callFollowerApi: async (method, args) => {
      busCalls.push({ method, args });
      return true;
    },
  });

  await runtime.sendTypingAction(1);
  await runtime.sendRecordVoiceAction(1);
  await runtime.sendMessageDraft(1, 2, "draft");
  await runtime.sendTypingAction(2);

  assert.deepEqual(busCalls, [
    {
      method: "call",
      args: [
        "sendChatAction",
        { chat_id: 1, action: "typing", message_thread_id: 5 },
      ],
    },
    {
      method: "call",
      args: [
        "sendChatAction",
        { chat_id: 1, action: "record_voice", message_thread_id: 5 },
      ],
    },
    {
      method: "call",
      args: [
        "sendMessageDraft",
        { chat_id: 1, draft_id: 2, text: "draft", message_thread_id: 5 },
      ],
    },
    {
      method: "call",
      args: ["sendChatAction", { chat_id: 2, action: "typing" }],
    },
  ]);
});

test("Bus-aware API runtime marks follower sends to a different thread", async () => {
  const busCalls: unknown[] = [];
  const runtime = createTelegramBusAwareApiRuntime({
    directRuntime: createDirectRuntime([]),
    ownsDirect: () => false,
    getDefaultTarget: () => ({ chatId: 1, threadId: 5 }),
    callFollowerApi: async (method, args) => {
      busCalls.push({ method, args });
      return { message_id: 9 };
    },
  });
  await runtime.sendRichMessage({
    chat_id: 1,
    message_thread_id: 7,
    rich_message: { markdown: "Cross-target" },
  });
  await runtime.sendRichMessage({
    chat_id: 1,
    message_thread_id: 5,
    rich_message: { markdown: "Own target" },
  });
  assert.deepEqual(busCalls, [
    {
      method: "call",
      args: [
        "sendRichMessage",
        {
          chat_id: 1,
          message_thread_id: 7,
          rich_message: { markdown: "Cross-target" },
          __piTelegramCrossTargetDelivery: true,
        },
      ],
    },
    {
      method: "call",
      args: [
        "sendRichMessage",
        {
          chat_id: 1,
          message_thread_id: 5,
          rich_message: { markdown: "Own target" },
        },
      ],
    },
  ]);
});

test("Bus-aware API runtime routes follower multipart uploads through the leader", async () => {
  const busCalls: unknown[] = [];
  const runtime = createTelegramBusAwareApiRuntime({
    directRuntime: createDirectRuntime([]),
    ownsDirect: () => false,
    callFollowerApi: async (method, args) => {
      busCalls.push({ method, args });
      return { ok: true };
    },
  });

  const richMessage = {
    markdown: "Voice\n\n![](tg://audio?id=voice)",
    media: [
      {
        id: "voice",
        media: { type: "voice_note", media: "attach://voice_upload" },
      },
    ],
  };
  assert.deepEqual(
    await runtime.callMultipart(
      "sendRichMessage",
      { chat_id: "1", rich_message: JSON.stringify(richMessage) },
      "voice_upload",
      "/tmp/voice.ogg",
      "voice.ogg",
    ),
    { ok: true },
  );
  assert.deepEqual(busCalls, [
    {
      method: "callMultipart",
      args: [
        "sendRichMessage",
        { chat_id: "1", rich_message: JSON.stringify(richMessage) },
        "voice_upload",
        "/tmp/voice.ogg",
        "voice.ogg",
        undefined,
      ],
    },
  ]);
});

test("follower editMessageText normalizes an unmodified-message rejection to unchanged", async () => {
  const runtime = createTelegramBusAwareApiRuntime({
    directRuntime: createDirectRuntime([]),
    ownsDirect: () => false,
    callFollowerApi: async () => {
      throw new Error(
        "Telegram API editMessageText failed: HTTP 400: Bad Request: message is not modified: specified new message content and reply markup are exactly the same as a current content and reply markup of the message",
      );
    },
  });
  const result = await runtime.editMessageText({
    chat_id: 7,
    message_id: 42,
    text: "same body",
    parse_mode: "HTML",
  });
  assert.equal(result, "unchanged");
});

test("follower editMessageText rethrows non-unmodified failures", async () => {
  const runtime = createTelegramBusAwareApiRuntime({
    directRuntime: createDirectRuntime([]),
    ownsDirect: () => false,
    callFollowerApi: async () => {
      throw new Error("Telegram API editMessageText failed: HTTP 400: Bad Request: message text is empty");
    },
  });
  await assert.rejects(
    runtime.editMessageText({
      chat_id: 7,
      message_id: 42,
      text: "",
      parse_mode: "HTML",
    }),
    /message text is empty/,
  );
});
