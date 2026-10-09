/**
 * Telegram menu and inline-keyboard rendering helpers
 * Zones: telegram ui, controls, status menu
 * Owns app-menu/status state, inline UI text, and callback composition while model/thinking/queue menu details live in dedicated domains
 */

import {
  createTelegramMenuDelivery,
  handleTelegramModelMenuCallbackAction,
  openTelegramModelMenu,
  sendTelegramModelMenuMessage,
  updateTelegramModelMenuMessage,
  type TelegramMenuMessageRuntimeDeps,
  type TelegramModelMenuState,
  type TelegramReplyMarkup,
} from "./menu-model.ts";
import {
  handleTelegramStatusMenuCallbackAction,
  openTelegramStatusMenu,
  sendTelegramStatusMessage,
  updateTelegramStatusMessage,
} from "./menu-status.ts";
import {
  handleTelegramThinkingMenuCallbackAction,
  openTelegramThinkingMenu,
  updateTelegramThinkingMenuMessage,
} from "./menu-thinking.ts";
import {
  type MenuModel,
  type ScopedTelegramModel,
  type TelegramModelSwitchContinuationSource,
  type ThinkingLevel,
} from "./model.ts";
import {
  handleTelegramSectionCallback,
  handleTelegramSectionOpen,
  handleTelegramSectionSettingsOpen,
  parseTelegramSectionCallback,
  type TelegramSectionRuntimeDeps,
  type TelegramSectionRegistry,
} from "./sections.ts";
import type {
  TelegramApiCallOptions,
  TelegramInputRichMessage,
} from "./telegram-api.ts";

export {
  applyTelegramModelPageSelection,
  applyTelegramModelScopeSelection,
  buildModelMenuReplyMarkup,
  buildModelPageMenuReplyMarkup,
  buildTelegramModelCallbackPlan,
  buildTelegramModelMenuRenderPayload,
  buildTelegramModelMenuState,
  buildTelegramModelMenuStateRuntime,
  buildTelegramModelPageMenuRenderPayload,
  createTelegramModelMenuRuntime,
  createTelegramModelMenuStateBuilder,
  formatScopedModelButtonText,
  getModelMenuItems,
  getStoredTelegramModelMenuState,
  getTelegramModelMenuPage,
  getTelegramModelSelection,
  handleTelegramModelMenuCallbackAction,
  MODEL_MENU_TITLE,
  MODEL_PAGE_MENU_TITLE,
  openTelegramModelMenu,
  pruneStoredTelegramModelMenus,
  resolveCachedTelegramModelMenuInputs,
  sendTelegramModelMenuMessage,
  storeTelegramModelMenuState,
  TELEGRAM_MODEL_PAGE_SIZE,
  updateTelegramModelMenuMessage,
} from "./menu-model.ts";
export type {
  BuildTelegramModelCallbackPlanParams,
  BuildTelegramModelMenuStateParams,
  CachedTelegramModelMenuInputs,
  MenuSettingsManager,
  StoredTelegramModelMenuState,
  TelegramMenuMessageRuntimeDeps,
  TelegramMenuMutationResult,
  TelegramMenuRenderPayload,
  TelegramMenuSelectionResult,
  TelegramModelCallbackPlan,
  TelegramModelMenuCallbackDeps,
  TelegramModelMenuInputCacheDeps,
  TelegramModelMenuOpenDeps,
  TelegramModelMenuPage,
  TelegramModelMenuRuntime,
  TelegramModelMenuRuntimeContext,
  TelegramModelMenuRuntimeOptions,
  TelegramModelMenuState,
  TelegramModelMenuStateBuilderContext,
  TelegramModelMenuStateBuilderDeps,
  TelegramModelMenuStoreOptions,
  TelegramModelScope,
  TelegramReplyMarkup,
} from "./menu-model.ts";
export {
  buildStatusReplyMarkup,
  buildTelegramStatusMenuRenderPayload,
  handleTelegramStatusMenuCallbackAction,
  openTelegramStatusMenu,
  sendTelegramStatusMessage,
  updateTelegramStatusMessage,
} from "./menu-status.ts";
export type {
  TelegramStatusMenuCallbackDeps,
  TelegramStatusMenuOpenDeps,
} from "./menu-status.ts";
export {
  buildTelegramThinkingMenuRenderPayload,
  buildThinkingMenuReplyMarkup,
  buildThinkingMenuText,
  handleTelegramThinkingMenuCallbackAction,
  openTelegramThinkingMenu,
  updateTelegramThinkingMenuMessage,
} from "./menu-thinking.ts";
export type {
  TelegramThinkingMenuCallbackDeps,
  TelegramThinkingMenuOpenDeps,
} from "./menu-thinking.ts";

export interface TelegramMenuCallbackEntryDeps {
  handleStatusAction: () => Promise<boolean>;
  handleThinkingAction: () => Promise<boolean>;
  handleModelAction: () => Promise<boolean>;
  answerCallbackQuery: (
    callbackQueryId: string,
    text?: string,
  ) => Promise<void>;
}

export interface MenuCallbackQuery {
  id: string;
  data?: string;
  message?: {
    message_id?: number;
    message_thread_id?: number;
    chat?: { id?: number };
  };
}

export interface StoredTelegramMenuCallbackDeps<
  TModel extends MenuModel = MenuModel,
> {
  getStoredModelMenuState: (
    messageId: number | undefined,
    chatId?: number,
  ) => TelegramModelMenuState<TModel> | undefined;
  handleStatusAction: (
    state: TelegramModelMenuState<TModel>,
  ) => Promise<boolean>;
  handleThinkingAction: (
    state: TelegramModelMenuState<TModel>,
  ) => Promise<boolean>;
  handleModelAction: (
    state: TelegramModelMenuState<TModel>,
  ) => Promise<boolean>;
  answerCallbackQuery: (
    callbackQueryId: string,
    text?: string,
  ) => Promise<void>;
}

export interface TelegramMenuCallbackRuntimeDeps<
  TContext,
  TModel extends MenuModel = MenuModel,
> {
  getStoredModelMenuState: (
    messageId: number | undefined,
    chatId?: number,
  ) => TelegramModelMenuState<TModel> | undefined;
  getActiveModel: (ctx: TContext) => TModel | undefined;
  getThinkingLevel: () => ThinkingLevel;
  setThinkingLevel: (level: ThinkingLevel) => void;
  updateStatus: (ctx: TContext) => void;
  updateModelMenuMessage: (
    state: TelegramModelMenuState<TModel>,
    ctx: TContext,
  ) => Promise<void>;
  updateThinkingMenuMessage: (
    state: TelegramModelMenuState<TModel>,
    ctx: TContext,
  ) => Promise<void>;
  updateStatusMessage: (
    state: TelegramModelMenuState<TModel>,
    ctx: TContext,
  ) => Promise<void>;
  updateSettingsMenuMessage?: (
    state: TelegramModelMenuState<TModel>,
    ctx: TContext,
  ) => Promise<void>;
  answerCallbackQuery: (
    callbackQueryId: string,
    text?: string,
  ) => Promise<void>;
  isIdle: (ctx: TContext) => boolean;
  hasAbortHandler: () => boolean;
  hasActiveToolExecutions: () => boolean;
  persistScopedModelPatterns?: (
    patterns: string[],
    ctx: TContext,
  ) => Promise<void>;
  setModel: (model: TModel) => Promise<boolean>;
  setCurrentModel: (model: TModel, ctx: TContext) => void;
  stagePendingModelSwitch: (
    selection: ScopedTelegramModel<TModel>,
    ctx: TContext,
    continuationTurn: TelegramModelSwitchContinuationSource,
  ) => void;
  restartInterruptedTelegramTurn: (
    selection: ScopedTelegramModel<TModel>,
    ctx: TContext,
    continuationTurn: TelegramModelSwitchContinuationSource,
  ) => Promise<boolean> | boolean;
  sectionRegistry?: TelegramSectionRegistry;
  editInteractiveMessage?: (
    chatId: number,
    messageId: number,
    text: string,
    mode: "markdown" | "html" | "plain",
    replyMarkup: TelegramReplyMarkup,
  ) => Promise<void>;
  sendInteractiveMessage?: (
    chatId: number,
    text: string,
    mode: "markdown" | "html" | "plain",
    replyMarkup: TelegramReplyMarkup,
    options?: { target?: { chatId: number; threadId?: number } },
  ) => Promise<number | undefined>;
  sendSectionRichMessage?: (
    chatId: number,
    message: TelegramInputRichMessage,
    options?: { target?: { chatId: number; threadId?: number } },
  ) => Promise<number | undefined>;
  enqueueSectionPrompt?: (
    prompt: string,
    ctx: TContext,
    target?: { chatId: number; threadId?: number },
    source?: unknown,
  ) => Promise<void>;
  deleteMessage?: (chatId: number, messageId: number) => Promise<void>;
  isVoiceReplyActive?: () => boolean;
}

export interface TelegramMenuActionRuntimeDeps<
  TContext,
  TModel extends MenuModel = MenuModel,
> extends TelegramMenuMessageRuntimeDeps {
  getModelMenuState: (
    chatId: number,
    ctx: TContext,
    threadId?: number,
  ) => Promise<TelegramModelMenuState<TModel>>;
  getActiveModel: (ctx: TContext) => TModel | undefined;
  getThinkingLevel: () => ThinkingLevel;
  getQueueItemCount?: () => number;
  getPendingCancellationCount?: () => number;
  buildStatusHtml: (ctx: TContext) => string;
  storeModelMenuState: (state: TelegramModelMenuState<TModel>) => void;
  isIdle: (ctx: TContext) => boolean;
  canOfferInFlightModelSwitch: (ctx: TContext) => boolean;
  sendTextReply: (
    chatId: number,
    replyToMessageId: number,
    text: string,
    options?: {
      target?: { chatId: number; threadId?: number };
      parseMode?: "HTML";
      assertAuthority?: TelegramApiCallOptions["assertAuthority"];
    },
  ) => Promise<unknown>;
  sectionRegistry?: TelegramSectionRegistry;
  isVoiceReplyActive?: () => boolean;
}

export interface TelegramMenuActionRuntime<
  TContext,
  TModel extends MenuModel = MenuModel,
> {
  updateModelMenuMessage: (
    state: TelegramModelMenuState<TModel>,
    ctx: TContext,
  ) => Promise<void>;
  updateThinkingMenuMessage: (
    state: TelegramModelMenuState<TModel>,
    ctx: TContext,
  ) => Promise<void>;
  updateStatusMessage: (
    state: TelegramModelMenuState<TModel>,
    ctx: TContext,
  ) => Promise<void>;
  sendStatusMessage: (
    chatId: number,
    replyToMessageId: number,
    ctx: TContext,
    threadId?: number,
    options?: Pick<TelegramApiCallOptions, "assertAuthority">,
  ) => Promise<void>;
  openModelMenu: (
    chatId: number,
    replyToMessageId: number,
    ctx: TContext,
    threadId?: number,
    options?: Pick<TelegramApiCallOptions, "assertAuthority">,
  ) => Promise<void>;
  openThinkingMenu: (
    chatId: number,
    replyToMessageId: number,
    ctx: TContext,
    threadId?: number,
    options?: Pick<TelegramApiCallOptions, "assertAuthority">,
  ) => Promise<void>;
}

export async function handleTelegramMenuCallbackEntry(
  callbackQueryId: string,
  data: string | undefined,
  state: TelegramModelMenuState | undefined,
  deps: TelegramMenuCallbackEntryDeps,
): Promise<void> {
  if (!data) {
    await deps.answerCallbackQuery(callbackQueryId);
    return;
  }
  if (!state) {
    await deps.answerCallbackQuery(
      callbackQueryId,
      "Interactive message expired",
    );
    return;
  }
  const handled =
    (await deps.handleStatusAction()) ||
    (await deps.handleThinkingAction()) ||
    (await deps.handleModelAction());
  if (!handled) {
    await deps.answerCallbackQuery(callbackQueryId);
  }
}

export async function handleStoredTelegramMenuCallback<
  TModel extends MenuModel = MenuModel,
>(
  query: MenuCallbackQuery,
  deps: StoredTelegramMenuCallbackDeps<TModel>,
): Promise<void> {
  const state = deps.getStoredModelMenuState(
    query.message?.message_id,
    query.message?.chat?.id,
  );
  await handleTelegramMenuCallbackEntry(query.id, query.data, state, {
    handleStatusAction: async () => {
      if (!state) return false;
      return deps.handleStatusAction(state);
    },
    handleThinkingAction: async () => {
      if (!state) return false;
      return deps.handleThinkingAction(state);
    },
    handleModelAction: async () => {
      if (!state) return false;
      return deps.handleModelAction(state);
    },
    answerCallbackQuery: deps.answerCallbackQuery,
  });
}

export type TelegramMenuCallbackRuntimeAdapterDeps<
  TContext,
  TModel extends MenuModel = MenuModel,
> = Omit<
  TelegramMenuCallbackRuntimeDeps<TContext, TModel>,
  "hasActiveToolExecutions"
> & {
  getActiveToolExecutions: () => number;
};

export function createTelegramMenuCallbackHandler<
  TQuery extends MenuCallbackQuery,
  TContext,
  TModel extends MenuModel = MenuModel,
>(
  deps: TelegramMenuCallbackRuntimeDeps<TContext, TModel>,
): (query: TQuery, ctx: TContext) => Promise<void> {
  return (query, ctx) => handleTelegramMenuCallbackRuntime(query, ctx, deps);
}

export function createTelegramMenuCallbackHandlerForContext<
  TQuery extends MenuCallbackQuery,
  TContext,
  TModel extends MenuModel = MenuModel,
>(
  deps: TelegramMenuCallbackRuntimeAdapterDeps<TContext, TModel>,
): (query: TQuery, ctx: TContext) => Promise<void> {
  const { getActiveToolExecutions, ...ports } = deps;
  return createTelegramMenuCallbackHandler<TQuery, TContext, TModel>({
    ...ports,
    hasActiveToolExecutions: () => getActiveToolExecutions() > 0,
  });
}

export async function handleTelegramMenuCallbackRuntime<
  TQuery extends MenuCallbackQuery,
  TContext,
  TModel extends MenuModel = MenuModel,
>(
  query: TQuery,
  ctx: TContext,
  deps: TelegramMenuCallbackRuntimeDeps<TContext, TModel>,
): Promise<void> {
  if (query.data === "menu:back") {
    const state = deps.getStoredModelMenuState(
      query.message?.message_id,
      query.message?.chat?.id,
    );
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
      const target =
        typeof chatId === "number"
          ? typeof message?.message_thread_id === "number"
            ? { chatId, threadId: message.message_thread_id }
            : { chatId }
          : undefined;
      if (typeof chatId === "number" && typeof messageId === "number") {
        const { token, action, payload } = parsed;
        const sectionDeps: TelegramSectionRuntimeDeps = {
          answerCallbackQuery: deps.answerCallbackQuery,
          target,
          editInteractiveMessage:
            deps.editInteractiveMessage ?? (async () => {}),
          sendInteractiveMessage:
            deps.sendInteractiveMessage ?? (async () => undefined),
          sendRichMessage:
            deps.sendSectionRichMessage ??
            (async () => {
              throw new Error("Rich Message delivery is unavailable");
            }),
          enqueuePrompt: deps.enqueueSectionPrompt
            ? (prompt: string) =>
                deps.enqueueSectionPrompt!(prompt, ctx, target, query)
            : async () => {},
          deleteMessage: deps.deleteMessage ?? (async () => {}),
        };
        if (action === "open") {
          const state = deps.getStoredModelMenuState(messageId, chatId);
          if (!state) {
            await deps.answerCallbackQuery(
              query.id,
              "Interactive message expired",
            );
            return;
          }
          const handled = await handleTelegramSectionOpen(
            deps.sectionRegistry,
            token,
            chatId,
            messageId,
            query.id,
            sectionDeps,
          );
          if (handled) return;
        } else if (action === "settings") {
          const handled = await handleTelegramSectionSettingsOpen(
            deps.sectionRegistry,
            token,
            chatId,
            messageId,
            query.id,
            sectionDeps,
          );
          if (handled) return;
        } else {
          const handled = await handleTelegramSectionCallback(
            deps.sectionRegistry,
            token,
            action,
            payload,
            chatId,
            messageId,
            query.id,
            sectionDeps,
          );
          if (handled) return;
        }
      }
    }
  }
  await handleStoredTelegramMenuCallback(query, {
    getStoredModelMenuState: deps.getStoredModelMenuState,
    handleStatusAction: async (state) =>
      handleTelegramStatusMenuCallbackAction(
        query.id,
        query.data,
        deps.getActiveModel(ctx),
        {
          updateModelMenuMessage: () => deps.updateModelMenuMessage(state, ctx),
          updateThinkingMenuMessage: () =>
            deps.updateThinkingMenuMessage(state, ctx),
          updateSettingsMenuMessage: () =>
            deps.updateSettingsMenuMessage?.(state, ctx) ?? Promise.resolve(),
          answerCallbackQuery: deps.answerCallbackQuery,
          isVoiceReplyActive: deps.isVoiceReplyActive,
        },
      ),
    handleThinkingAction: async (state) =>
      handleTelegramThinkingMenuCallbackAction(
        query.id,
        query.data,
        deps.getActiveModel(ctx),
        {
          setThinkingLevel: (level) => {
            deps.setThinkingLevel(level);
            deps.updateStatus(ctx);
          },
          getCurrentThinkingLevel: deps.getThinkingLevel,
          updateThinkingMenuMessage: () =>
            deps.updateThinkingMenuMessage(state, ctx),
          answerCallbackQuery: deps.answerCallbackQuery,
          isVoiceReplyActive: deps.isVoiceReplyActive,
        },
      ),
    handleModelAction: async (state) => {
      try {
        return await handleTelegramModelMenuCallbackAction(
          query.id,
          {
            data: query.data,
            state,
            activeModel: deps.getActiveModel(ctx),
            currentThinkingLevel: deps.getThinkingLevel(),
            isIdle: deps.isIdle(ctx),
            canRestartBusyRun: deps.hasAbortHandler(),
            hasActiveToolExecutions: deps.hasActiveToolExecutions(),
          },
          {
            updateModelMenuMessage: () =>
              deps.updateModelMenuMessage(state, ctx),
            updateStatusMessage: () => deps.updateStatusMessage(state, ctx),
            answerCallbackQuery: deps.answerCallbackQuery,
            persistScopedModelPatterns: deps.persistScopedModelPatterns
              ? (patterns) => deps.persistScopedModelPatterns!(patterns, ctx)
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
            restartInterruptedTelegramTurn: (selection, continuationTurn) =>
              deps.restartInterruptedTelegramTurn(
                selection,
                ctx,
                continuationTurn,
              ),
          },
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        await deps.answerCallbackQuery(query.id, message);
        return true;
      }
    },
    answerCallbackQuery: deps.answerCallbackQuery,
  });
}

export function createTelegramMenuActionRuntime<
  TContext,
  TModel extends MenuModel = MenuModel,
>(
  deps: TelegramMenuActionRuntimeDeps<TContext, TModel>,
): TelegramMenuActionRuntime<TContext, TModel> {
  return {
    updateModelMenuMessage: (state, ctx) =>
      updateTelegramModelMenuMessage(state, deps.getActiveModel(ctx), deps),
    updateThinkingMenuMessage: (state, ctx) =>
      updateTelegramThinkingMenuMessage(
        state,
        deps.getActiveModel(ctx),
        deps.getThinkingLevel(),
        deps,
      ),
    updateStatusMessage: (state, ctx) =>
      updateTelegramStatusMessage(
        state,
        deps.buildStatusHtml(ctx),
        deps.getActiveModel(ctx),
        deps.getThinkingLevel(),
        deps,
        deps.getQueueItemCount?.() ?? 0,
        deps.sectionRegistry,
        deps.isVoiceReplyActive?.(),
        deps.getPendingCancellationCount?.() ?? 0,
      ),
    sendStatusMessage: (chatId, replyToMessageId, ctx, threadId, options) => {
      const delivery = createTelegramMenuDelivery(
        { chatId, threadId },
        options,
        deps,
      );
      return openTelegramStatusMenu({
        isIdle: () => deps.isIdle(ctx),
        sendBusyMessage: async () => {
          await deps.sendTextReply(
            chatId,
            replyToMessageId,
            "<b>⏳ Cannot open status while Pi is busy. Send /abort, /next, or /stop.</b>",
            {
              target: { chatId, threadId },
              parseMode: "HTML",
              ...(delivery.assertAuthority
                ? { assertAuthority: delivery.assertAuthority }
                : {}),
            },
          );
        },
        getModelMenuState: () => deps.getModelMenuState(chatId, ctx, threadId),
        buildStatusHtml: () => deps.buildStatusHtml(ctx),
        getActiveModel: () => deps.getActiveModel(ctx),
        getThinkingLevel: deps.getThinkingLevel,
        getQueueItemCount: deps.getQueueItemCount,
        sendStatusMenu: (
          state,
          statusHtml,
          activeModel,
          thinkingLevel,
          queueItemCount,
        ) =>
          sendTelegramStatusMessage(
            state,
            statusHtml,
            activeModel,
            thinkingLevel,
            {
              ...deps,
              sendInteractiveMessage: delivery.sendInteractiveMessage,
            },
            queueItemCount,
            deps.sectionRegistry,
            deps.isVoiceReplyActive?.(),
            deps.getPendingCancellationCount?.() ?? 0,
          ),
        storeModelMenuState: delivery.storeModelMenuState,
      });
    },
    openModelMenu: (chatId, replyToMessageId, ctx, threadId, options) => {
      const delivery = createTelegramMenuDelivery(
        { chatId, threadId },
        options,
        deps,
      );
      const assertAuthority = delivery.assertAuthority;
      return openTelegramModelMenu({
        isIdle: () => deps.isIdle(ctx),
        canOfferInFlightModelSwitch: () =>
          deps.canOfferInFlightModelSwitch(ctx),
        sendBusyMessage: async () => {
          await deps.sendTextReply(
            chatId,
            replyToMessageId,
            "<b>⏳ Cannot switch model while Pi is busy. Send /abort, /next, or /stop.</b>",
            {
              target: { chatId, threadId },
              parseMode: "HTML",
              ...(assertAuthority ? { assertAuthority } : {}),
            },
          );
        },
        sendNoModelsMessage: async () => {
          await deps.sendTextReply(
            chatId,
            replyToMessageId,
            "<b>🚫 No available models with configured auth.</b>",
            {
              target: { chatId, threadId },
              parseMode: "HTML",
              ...(assertAuthority ? { assertAuthority } : {}),
            },
          );
        },
        getModelMenuState: () => deps.getModelMenuState(chatId, ctx, threadId),
        getActiveModel: () => deps.getActiveModel(ctx),
        sendModelMenu: (state, activeModel) =>
          sendTelegramModelMenuMessage(state, activeModel, {
            ...deps,
            sendInteractiveMessage: delivery.sendInteractiveMessage,
          }),
        storeModelMenuState: delivery.storeModelMenuState,
      });
    },
    openThinkingMenu: (chatId, _replyToMessageId, ctx, threadId, options) => {
      const delivery = createTelegramMenuDelivery(
        { chatId, threadId },
        options,
        deps,
      );
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
