/**
 * Telegram menu and inline-keyboard rendering helpers
 * Zones: telegram ui, controls, status menu
 * Owns app-menu/status state, inline UI text, and callback composition while model/thinking/queue menu details live in dedicated domains
 */
import { createTelegramMenuDelivery, handleTelegramModelMenuCallbackAction, openTelegramModelMenu, sendTelegramModelMenuMessage, updateTelegramModelMenuMessage, } from "./menu-model.js";
import { handleTelegramStatusMenuCallbackAction, openTelegramStatusMenu, sendTelegramStatusMessage, updateTelegramStatusMessage, } from "./menu-status.js";
import { handleTelegramThinkingMenuCallbackAction, openTelegramThinkingMenu, updateTelegramThinkingMenuMessage, } from "./menu-thinking.js";
import { handleTelegramSectionCallback, handleTelegramSectionOpen, handleTelegramSectionSettingsOpen, parseTelegramSectionCallback, } from "./sections.js";
export { applyTelegramModelPageSelection, applyTelegramModelScopeSelection, buildModelMenuReplyMarkup, buildModelPageMenuReplyMarkup, buildTelegramModelCallbackPlan, buildTelegramModelMenuRenderPayload, buildTelegramModelMenuState, buildTelegramModelMenuStateRuntime, buildTelegramModelPageMenuRenderPayload, createTelegramModelMenuRuntime, createTelegramModelMenuStateBuilder, formatScopedModelButtonText, getModelMenuItems, getStoredTelegramModelMenuState, getTelegramModelMenuPage, getTelegramModelSelection, handleTelegramModelMenuCallbackAction, MODEL_MENU_TITLE, MODEL_PAGE_MENU_TITLE, openTelegramModelMenu, pruneStoredTelegramModelMenus, resolveCachedTelegramModelMenuInputs, sendTelegramModelMenuMessage, storeTelegramModelMenuState, TELEGRAM_MODEL_PAGE_SIZE, updateTelegramModelMenuMessage, } from "./menu-model.js";
export { buildStatusReplyMarkup, buildTelegramStatusMenuRenderPayload, handleTelegramStatusMenuCallbackAction, openTelegramStatusMenu, sendTelegramStatusMessage, updateTelegramStatusMessage, } from "./menu-status.js";
export { buildTelegramThinkingMenuRenderPayload, buildThinkingMenuReplyMarkup, buildThinkingMenuText, handleTelegramThinkingMenuCallbackAction, openTelegramThinkingMenu, updateTelegramThinkingMenuMessage, } from "./menu-thinking.js";
export async function handleTelegramMenuCallbackEntry(callbackQueryId, data, state, deps) {
    if (!data) {
        await deps.answerCallbackQuery(callbackQueryId);
        return;
    }
    if (!state) {
        await deps.answerCallbackQuery(callbackQueryId, "Interactive message expired");
        return;
    }
    const handled = (await deps.handleStatusAction()) ||
        (await deps.handleThinkingAction()) ||
        (await deps.handleModelAction());
    if (!handled) {
        await deps.answerCallbackQuery(callbackQueryId);
    }
}
export async function handleStoredTelegramMenuCallback(query, deps) {
    const state = deps.getStoredModelMenuState(query.message?.message_id, query.message?.chat?.id);
    await handleTelegramMenuCallbackEntry(query.id, query.data, state, {
        handleStatusAction: async () => {
            if (!state)
                return false;
            return deps.handleStatusAction(state);
        },
        handleThinkingAction: async () => {
            if (!state)
                return false;
            return deps.handleThinkingAction(state);
        },
        handleModelAction: async () => {
            if (!state)
                return false;
            return deps.handleModelAction(state);
        },
        answerCallbackQuery: deps.answerCallbackQuery,
    });
}
export function createTelegramMenuCallbackHandler(deps) {
    return (query, ctx) => handleTelegramMenuCallbackRuntime(query, ctx, deps);
}
export function createTelegramMenuCallbackHandlerForContext(deps) {
    const { getActiveToolExecutions, ...ports } = deps;
    return createTelegramMenuCallbackHandler({
        ...ports,
        hasActiveToolExecutions: () => getActiveToolExecutions() > 0,
    });
}
export async function handleTelegramMenuCallbackRuntime(query, ctx, deps) {
    if (query.data === "menu:back") {
        const state = deps.getStoredModelMenuState(query.message?.message_id, query.message?.chat?.id);
        if (!state) {
            await deps.answerCallbackQuery(query.id, "Interactive message expired");
            return;
        }
        await deps.updateStatusMessage(state, ctx);
        await deps.answerCallbackQuery(query.id);
        return;
    }
    // Section callbacks: dispatch before built-in menu handling
    if (deps.sectionRegistry && query.data?.startsWith("section:")) {
        const parsed = parseTelegramSectionCallback(query.data);
        if (parsed) {
            const message = query.message;
            const chatId = message?.chat?.id;
            const messageId = message?.message_id;
            const target = typeof chatId === "number"
                ? typeof message?.message_thread_id === "number"
                    ? { chatId, threadId: message.message_thread_id }
                    : { chatId }
                : undefined;
            if (typeof chatId === "number" && typeof messageId === "number") {
                const { token, action, payload } = parsed;
                const sectionDeps = {
                    answerCallbackQuery: deps.answerCallbackQuery,
                    target,
                    editInteractiveMessage: deps.editInteractiveMessage ?? (async () => { }),
                    sendInteractiveMessage: deps.sendInteractiveMessage ?? (async () => undefined),
                    sendRichMessage: deps.sendSectionRichMessage ??
                        (async () => {
                            throw new Error("Rich Message delivery is unavailable");
                        }),
                    enqueuePrompt: deps.enqueueSectionPrompt
                        ? (prompt) => deps.enqueueSectionPrompt(prompt, ctx, target, query)
                        : async () => { },
                    deleteMessage: deps.deleteMessage ?? (async () => { }),
                };
                if (action === "open") {
                    const state = deps.getStoredModelMenuState(messageId, chatId);
                    if (!state) {
                        await deps.answerCallbackQuery(query.id, "Interactive message expired");
                        return;
                    }
                    const handled = await handleTelegramSectionOpen(deps.sectionRegistry, token, chatId, messageId, query.id, sectionDeps);
                    if (handled)
                        return;
                }
                else if (action === "settings") {
                    const handled = await handleTelegramSectionSettingsOpen(deps.sectionRegistry, token, chatId, messageId, query.id, sectionDeps);
                    if (handled)
                        return;
                }
                else {
                    const handled = await handleTelegramSectionCallback(deps.sectionRegistry, token, action, payload, chatId, messageId, query.id, sectionDeps);
                    if (handled)
                        return;
                }
            }
        }
    }
    await handleStoredTelegramMenuCallback(query, {
        getStoredModelMenuState: deps.getStoredModelMenuState,
        handleStatusAction: async (state) => handleTelegramStatusMenuCallbackAction(query.id, query.data, deps.getActiveModel(ctx), {
            updateModelMenuMessage: () => deps.updateModelMenuMessage(state, ctx),
            updateThinkingMenuMessage: () => deps.updateThinkingMenuMessage(state, ctx),
            updateSettingsMenuMessage: () => deps.updateSettingsMenuMessage?.(state, ctx) ?? Promise.resolve(),
            answerCallbackQuery: deps.answerCallbackQuery,
            isVoiceReplyActive: deps.isVoiceReplyActive,
        }),
        handleThinkingAction: async (state) => handleTelegramThinkingMenuCallbackAction(query.id, query.data, deps.getActiveModel(ctx), {
            setThinkingLevel: (level) => {
                deps.setThinkingLevel(level);
                deps.updateStatus(ctx);
            },
            getCurrentThinkingLevel: deps.getThinkingLevel,
            updateThinkingMenuMessage: () => deps.updateThinkingMenuMessage(state, ctx),
            answerCallbackQuery: deps.answerCallbackQuery,
            isVoiceReplyActive: deps.isVoiceReplyActive,
        }),
        handleModelAction: async (state) => {
            try {
                return await handleTelegramModelMenuCallbackAction(query.id, {
                    data: query.data,
                    state,
                    activeModel: deps.getActiveModel(ctx),
                    currentThinkingLevel: deps.getThinkingLevel(),
                    isIdle: deps.isIdle(ctx),
                    canRestartBusyRun: deps.hasAbortHandler(),
                    hasActiveToolExecutions: deps.hasActiveToolExecutions(),
                }, {
                    updateModelMenuMessage: () => deps.updateModelMenuMessage(state, ctx),
                    updateStatusMessage: () => deps.updateStatusMessage(state, ctx),
                    answerCallbackQuery: deps.answerCallbackQuery,
                    persistScopedModelPatterns: deps.persistScopedModelPatterns
                        ? (patterns) => deps.persistScopedModelPatterns(patterns, ctx)
                        : undefined,
                    setModel: deps.setModel,
                    setCurrentModel: (model) => deps.setCurrentModel(model, ctx),
                    setThinkingLevel: (level) => {
                        deps.setThinkingLevel(level);
                        deps.updateStatus(ctx);
                    },
                    stagePendingModelSwitch: (selection, continuationTurn) => {
                        deps.stagePendingModelSwitch(selection, ctx, continuationTurn);
                    },
                    restartInterruptedTelegramTurn: (selection, continuationTurn) => deps.restartInterruptedTelegramTurn(selection, ctx, continuationTurn),
                });
            }
            catch (error) {
                const message = error instanceof Error ? error.message : String(error);
                await deps.answerCallbackQuery(query.id, message);
                return true;
            }
        },
        answerCallbackQuery: deps.answerCallbackQuery,
    });
}
export function createTelegramMenuActionRuntime(deps) {
    return {
        updateModelMenuMessage: (state, ctx) => updateTelegramModelMenuMessage(state, deps.getActiveModel(ctx), deps),
        updateThinkingMenuMessage: (state, ctx) => updateTelegramThinkingMenuMessage(state, deps.getActiveModel(ctx), deps.getThinkingLevel(), deps),
        updateStatusMessage: (state, ctx) => updateTelegramStatusMessage(state, deps.buildStatusHtml(ctx), deps.getActiveModel(ctx), deps.getThinkingLevel(), deps, deps.getQueueItemCount?.() ?? 0, deps.sectionRegistry, deps.isVoiceReplyActive?.(), deps.getPendingCancellationCount?.() ?? 0),
        sendStatusMessage: (chatId, replyToMessageId, ctx, threadId, options) => {
            const delivery = createTelegramMenuDelivery({ chatId, threadId }, options, deps);
            return openTelegramStatusMenu({
                isIdle: () => deps.isIdle(ctx),
                sendBusyMessage: async () => {
                    await deps.sendTextReply(chatId, replyToMessageId, "<b>⏳ Cannot open status while Pi is busy. Send /abort, /next, or /stop.</b>", {
                        target: { chatId, threadId },
                        parseMode: "HTML",
                        ...(delivery.assertAuthority
                            ? { assertAuthority: delivery.assertAuthority }
                            : {}),
                    });
                },
                getModelMenuState: () => deps.getModelMenuState(chatId, ctx, threadId),
                buildStatusHtml: () => deps.buildStatusHtml(ctx),
                getActiveModel: () => deps.getActiveModel(ctx),
                getThinkingLevel: deps.getThinkingLevel,
                getQueueItemCount: deps.getQueueItemCount,
                sendStatusMenu: (state, statusHtml, activeModel, thinkingLevel, queueItemCount) => sendTelegramStatusMessage(state, statusHtml, activeModel, thinkingLevel, {
                    ...deps,
                    sendInteractiveMessage: delivery.sendInteractiveMessage,
                }, queueItemCount, deps.sectionRegistry, deps.isVoiceReplyActive?.(), deps.getPendingCancellationCount?.() ?? 0),
                storeModelMenuState: delivery.storeModelMenuState,
            });
        },
        openModelMenu: (chatId, replyToMessageId, ctx, threadId, options) => {
            const delivery = createTelegramMenuDelivery({ chatId, threadId }, options, deps);
            const assertAuthority = delivery.assertAuthority;
            return openTelegramModelMenu({
                isIdle: () => deps.isIdle(ctx),
                canOfferInFlightModelSwitch: () => deps.canOfferInFlightModelSwitch(ctx),
                sendBusyMessage: async () => {
                    await deps.sendTextReply(chatId, replyToMessageId, "<b>⏳ Cannot switch model while Pi is busy. Send /abort, /next, or /stop.</b>", {
                        target: { chatId, threadId },
                        parseMode: "HTML",
                        ...(assertAuthority ? { assertAuthority } : {}),
                    });
                },
                sendNoModelsMessage: async () => {
                    await deps.sendTextReply(chatId, replyToMessageId, "<b>🚫 No available models with configured auth.</b>", {
                        target: { chatId, threadId },
                        parseMode: "HTML",
                        ...(assertAuthority ? { assertAuthority } : {}),
                    });
                },
                getModelMenuState: () => deps.getModelMenuState(chatId, ctx, threadId),
                getActiveModel: () => deps.getActiveModel(ctx),
                sendModelMenu: (state, activeModel) => sendTelegramModelMenuMessage(state, activeModel, {
                    ...deps,
                    sendInteractiveMessage: delivery.sendInteractiveMessage,
                }),
                storeModelMenuState: delivery.storeModelMenuState,
            });
        },
        openThinkingMenu: (chatId, _replyToMessageId, ctx, threadId, options) => {
            const delivery = createTelegramMenuDelivery({ chatId, threadId }, options, deps);
            return openTelegramThinkingMenu({
                getModelMenuState: () => deps.getModelMenuState(chatId, ctx, threadId),
                getActiveModel: () => deps.getActiveModel(ctx),
                getThinkingLevel: deps.getThinkingLevel,
                storeModelMenuState: delivery.storeModelMenuState,
                editInteractiveMessage: deps.editInteractiveMessage,
                sendInteractiveMessage: delivery.sendInteractiveMessage,
                isVoiceReplyActive: deps.isVoiceReplyActive,
            });
        },
    };
}
