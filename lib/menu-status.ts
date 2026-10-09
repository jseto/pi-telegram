/**
 * Telegram status menu UI helpers
 * Zones: telegram ui, status controls, menu composition
 * Owns status-menu payloads, status callback handling, and status-menu message rendering
 */

import { formatTelegramCommandEmojiPrefix } from "./commands.ts";
import { refuseUnavailableTelegramThinkingControls } from "./menu-thinking.ts";
import {
  editTelegramMenuMessage,
  formatStatusButtonLabel,
  sendTelegramMenuMessage,
  type TelegramMenuMessageRuntimeDeps,
  type TelegramMenuRenderPayload,
  type TelegramModelMenuState,
  type TelegramReplyMarkup,
} from "./menu-model.ts";
import {
  getCanonicalModelId,
  type MenuModel,
  type ThinkingLevel,
} from "./model.ts";
import {
  getTelegramSectionMainMenuRows,
  type TelegramSectionRegistry,
} from "./sections.ts";

export interface TelegramStatusMenuCallbackDeps {
  updateModelMenuMessage: () => Promise<void>;
  updateThinkingMenuMessage: () => Promise<void>;
  updateSettingsMenuMessage?: () => Promise<void>;
  answerCallbackQuery: (
    callbackQueryId: string,
    text?: string,
  ) => Promise<void>;
  isVoiceReplyActive?: () => boolean;
}

export interface TelegramStatusMenuOpenDeps<
  TModel extends MenuModel = MenuModel,
> {
  isIdle: () => boolean;
  sendBusyMessage: () => Promise<void>;
  getModelMenuState: () => Promise<TelegramModelMenuState<TModel>>;
  buildStatusHtml: () => string;
  getActiveModel: () => TModel | undefined;
  getThinkingLevel: () => ThinkingLevel;
  getQueueItemCount?: () => number;
  sendStatusMenu: (
    state: TelegramModelMenuState<TModel>,
    statusHtml: string,
    activeModel: TModel | undefined,
    thinkingLevel: ThinkingLevel,
    queueItemCount: number,
  ) => Promise<number | undefined>;
  storeModelMenuState: (state: TelegramModelMenuState<TModel>) => void;
}

function isTelegramStatusMenuCallbackAction(
  data: string | undefined,
  action: "model" | "thinking" | "settings",
): boolean {
  return data === `menu:${action}` || data === `status:${action}`;
}

export async function openTelegramStatusMenu<
  TModel extends MenuModel = MenuModel,
>(deps: TelegramStatusMenuOpenDeps<TModel>): Promise<void> {
  const state = await deps.getModelMenuState();
  const messageId = await deps.sendStatusMenu(
    state,
    deps.buildStatusHtml(),
    deps.getActiveModel(),
    deps.getThinkingLevel(),
    deps.getQueueItemCount?.() ?? 0,
  );
  if (messageId === undefined) return;
  state.messageId = messageId;
  state.mode = "status";
  deps.storeModelMenuState(state);
}

export async function handleTelegramStatusMenuCallbackAction(
  callbackQueryId: string,
  data: string | undefined,
  activeModel: MenuModel | undefined,
  deps: TelegramStatusMenuCallbackDeps,
): Promise<boolean> {
  if (isTelegramStatusMenuCallbackAction(data, "model")) {
    await deps.updateModelMenuMessage();
    await deps.answerCallbackQuery(callbackQueryId);
    return true;
  }
  if (isTelegramStatusMenuCallbackAction(data, "settings")) {
    if (!deps.updateSettingsMenuMessage) return false;
    await deps.updateSettingsMenuMessage();
    await deps.answerCallbackQuery(callbackQueryId);
    return true;
  }
  if (!isTelegramStatusMenuCallbackAction(data, "thinking")) return false;
  if (
    await refuseUnavailableTelegramThinkingControls(
      callbackQueryId,
      activeModel,
      deps,
    )
  )
    return true;
  await deps.updateThinkingMenuMessage();
  await deps.answerCallbackQuery(callbackQueryId);
  return true;
}

export function buildStatusReplyMarkup(
  activeModel: MenuModel | undefined,
  currentThinkingLevel: ThinkingLevel,
  queueItemCount = 0,
  sectionRegistry?: TelegramSectionRegistry,
  isVoiceReplyActive?: boolean,
  pendingCancellationCount = 0,
): TelegramReplyMarkup {
  const rows: Array<Array<{ text: string; callback_data: string }>> = [];
  rows.push([
    {
      text: formatStatusButtonLabel(
        `${formatTelegramCommandEmojiPrefix("model")}Model`,
        activeModel ? getCanonicalModelId(activeModel) : "unknown",
      ),
      callback_data: "menu:model",
    },
  ]);
  if (activeModel?.reasoning && !isVoiceReplyActive) {
    rows.push([
      {
        text: formatStatusButtonLabel(
          `${formatTelegramCommandEmojiPrefix("thinking")}Thinking`,
          currentThinkingLevel,
        ),
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

export function buildTelegramStatusMenuRenderPayload(
  statusText: string,
  activeModel: MenuModel | undefined,
  currentThinkingLevel: ThinkingLevel,
  queueItemCount = 0,
  sectionRegistry?: TelegramSectionRegistry,
  isVoiceReplyActive?: boolean,
  pendingCancellationCount = 0,
): TelegramMenuRenderPayload {
  return {
    nextMode: "status",
    text: statusText,
    mode: "html",
    replyMarkup: buildStatusReplyMarkup(
      activeModel,
      currentThinkingLevel,
      queueItemCount,
      sectionRegistry,
      isVoiceReplyActive,
      pendingCancellationCount,
    ),
  };
}

export async function updateTelegramStatusMessage(
  state: TelegramModelMenuState,
  statusText: string,
  activeModel: MenuModel | undefined,
  currentThinkingLevel: ThinkingLevel,
  deps: TelegramMenuMessageRuntimeDeps,
  queueItemCount = 0,
  sectionRegistry?: TelegramSectionRegistry,
  isVoiceReplyActive?: boolean,
  pendingCancellationCount = 0,
): Promise<void> {
  await editTelegramMenuMessage(
    state,
    buildTelegramStatusMenuRenderPayload(
      statusText,
      activeModel,
      currentThinkingLevel,
      queueItemCount,
      sectionRegistry,
      isVoiceReplyActive,
      pendingCancellationCount,
    ),
    deps,
  );
}

export function sendTelegramStatusMessage(
  state: TelegramModelMenuState,
  statusText: string,
  activeModel: MenuModel | undefined,
  currentThinkingLevel: ThinkingLevel,
  deps: TelegramMenuMessageRuntimeDeps,
  queueItemCount = 0,
  sectionRegistry?: TelegramSectionRegistry,
  isVoiceReplyActive?: boolean,
  pendingCancellationCount = 0,
): Promise<number | undefined> {
  return sendTelegramMenuMessage(
    state,
    buildTelegramStatusMenuRenderPayload(
      statusText,
      activeModel,
      currentThinkingLevel,
      queueItemCount,
      sectionRegistry,
      isVoiceReplyActive,
      pendingCancellationCount,
    ),
    deps,
  );
}
