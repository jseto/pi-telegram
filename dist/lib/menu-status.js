/**
 * Telegram status menu UI helpers
 * Zones: telegram ui, status controls, menu composition
 * Owns status-menu payloads, status callback handling, and status-menu message rendering
 */
import { formatTelegramCommandEmojiPrefix } from "./commands.js";
import { refuseUnavailableTelegramThinkingControls } from "./menu-thinking.js";
import { editTelegramMenuMessage, formatStatusButtonLabel, sendTelegramMenuMessage, } from "./menu-model.js";
import { getCanonicalModelId, } from "./model.js";
import { getTelegramSectionMainMenuRows, } from "./sections.js";
function isTelegramStatusMenuCallbackAction(data, action) {
    return data === `menu:${action}` || data === `status:${action}`;
}
export async function openTelegramStatusMenu(deps) {
    const state = await deps.getModelMenuState();
    const messageId = await deps.sendStatusMenu(state, deps.buildStatusHtml(), deps.getActiveModel(), deps.getThinkingLevel(), deps.getQueueItemCount?.() ?? 0);
    if (messageId === undefined)
        return;
    state.messageId = messageId;
    state.mode = "status";
    deps.storeModelMenuState(state);
}
export async function handleTelegramStatusMenuCallbackAction(callbackQueryId, data, activeModel, deps) {
    if (isTelegramStatusMenuCallbackAction(data, "model")) {
        await deps.updateModelMenuMessage();
        await deps.answerCallbackQuery(callbackQueryId);
        return true;
    }
    if (isTelegramStatusMenuCallbackAction(data, "settings")) {
        if (!deps.updateSettingsMenuMessage)
            return false;
        await deps.updateSettingsMenuMessage();
        await deps.answerCallbackQuery(callbackQueryId);
        return true;
    }
    if (!isTelegramStatusMenuCallbackAction(data, "thinking"))
        return false;
    if (await refuseUnavailableTelegramThinkingControls(callbackQueryId, activeModel, deps))
        return true;
    await deps.updateThinkingMenuMessage();
    await deps.answerCallbackQuery(callbackQueryId);
    return true;
}
export function buildStatusReplyMarkup(activeModel, currentThinkingLevel, queueItemCount = 0, sectionRegistry, isVoiceReplyActive, pendingCancellationCount = 0) {
    const rows = [];
    rows.push([
        {
            text: formatStatusButtonLabel(`${formatTelegramCommandEmojiPrefix("model")}Model`, activeModel ? getCanonicalModelId(activeModel) : "unknown"),
            callback_data: "menu:model",
        },
    ]);
    if (activeModel?.reasoning && !isVoiceReplyActive) {
        rows.push([
            {
                text: formatStatusButtonLabel(`${formatTelegramCommandEmojiPrefix("thinking")}Thinking`, currentThinkingLevel),
                callback_data: "menu:thinking",
            },
        ]);
    }
    rows.push([
        {
            text: `${queueItemCount === 0 ? "⌛" : "⏳"} Queue: ${queueItemCount}`,
            callback_data: "menu:queue",
        },
    ]);
    if (pendingCancellationCount > 0)
        rows.push([
            {
                text: `❌ Pending cancellations: ${pendingCancellationCount}`,
                callback_data: "reroutecancel:review:open",
            },
        ]);
    if (sectionRegistry) {
        const sectionRows = getTelegramSectionMainMenuRows(sectionRegistry);
        for (const row of sectionRows) {
            rows.push([row]);
        }
    }
    rows.push([
        {
            text: "⚙️ Settings",
            callback_data: "menu:settings",
        },
    ]);
    return { inline_keyboard: rows };
}
export function buildTelegramStatusMenuRenderPayload(statusText, activeModel, currentThinkingLevel, queueItemCount = 0, sectionRegistry, isVoiceReplyActive, pendingCancellationCount = 0) {
    return {
        nextMode: "status",
        text: statusText,
        mode: "html",
        replyMarkup: buildStatusReplyMarkup(activeModel, currentThinkingLevel, queueItemCount, sectionRegistry, isVoiceReplyActive, pendingCancellationCount),
    };
}
export async function updateTelegramStatusMessage(state, statusText, activeModel, currentThinkingLevel, deps, queueItemCount = 0, sectionRegistry, isVoiceReplyActive, pendingCancellationCount = 0) {
    await editTelegramMenuMessage(state, buildTelegramStatusMenuRenderPayload(statusText, activeModel, currentThinkingLevel, queueItemCount, sectionRegistry, isVoiceReplyActive, pendingCancellationCount), deps);
}
export function sendTelegramStatusMessage(state, statusText, activeModel, currentThinkingLevel, deps, queueItemCount = 0, sectionRegistry, isVoiceReplyActive, pendingCancellationCount = 0) {
    return sendTelegramMenuMessage(state, buildTelegramStatusMenuRenderPayload(statusText, activeModel, currentThinkingLevel, queueItemCount, sectionRegistry, isVoiceReplyActive, pendingCancellationCount), deps);
}
