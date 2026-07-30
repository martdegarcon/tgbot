"""
Telegram-бот WithSolution: «Получить бесплатный сайт».

Логика:
  1. Пользователь нажимает единственную кнопку «🎁 Получить бесплатный сайт».
  2. Бот присылает ссылку на вступление в канал. Канал настроен так, что
     вступление идёт по заявке (Channel Type → Private → "Approve New Members" /
     «Одобрение заявок на вступление») — то есть саму заявку в канал модератор
     (админ) потом рассмотрит и одобрит вручную в Telegram, в своём темпе.
  3. Но пользователю НЕ нужно ждать этого одобрения, чтобы пользоваться ботом:
     в момент, когда он нажимает "Запросить вступление" в Telegram, приходит
     событие chat_join_request — бот ловит его сам, автоматически, и сразу же,
     без каких-либо действий модератора, продолжает сценарий.
     На случай, если бот был выключен и пропустил это событие, остаётся
     кнопка «✅ Я подал заявку — продолжить», которая делает то же самое вручную.
  4. Дальше бот спрашивает:
       а) ссылку на текущий сайт (или "нет", если сайта ещё нет),
       б) что хочет получить/улучшить (например: "хочу новый дизайн",
          "хочу больше заявок").
  5. Готовая заявка уходит админу (ADMIN_IDS) и сохраняется в базу.

Установка:
    python3 -m pip install aiogram python-dotenv

Файл .env рядом со скриптом:
    BOT_TOKEN=123456:ABC...
    CHANNEL_ID=@your_channel        # или -100XXXXXXXXXX
    CHANNEL_URL=https://t.me/your_channel_or_invite_link
    ADMIN_IDS=1516639621            # твой Telegram ID (можно несколько через запятую)

ВАЖНО про канал:
  • Канал должен требовать подтверждения заявок на вступление (Private Channel →
    "Approve new members" / «Одобрять новых участников»). Без этого не будет
    события chat_join_request, на котором всё держится.
  • Бота нужно добавить в канал АДМИНИСТРАТОРОМ с правом «Добавление участников»
    (can_invite_users) — именно оно обязательно нужно Telegram, чтобы бот вообще
    получал события о новых заявках на вступление.
  • Сам факт вступления в канал модератор одобряет/отклоняет вручную в Telegram —
    это отдельный процесс и на работу бота не влияет: бот открывает доступ
    к форме сразу по факту заявки, не дожидаясь решения модератора.

Запуск:
    python3 free_site_bot.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import time
from contextlib import closing
from typing import Optional

from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    ChatJoinRequest,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeChat,
    TelegramObject,
)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# ============== НАСТРОЙКИ ==============

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_ID = os.getenv("CHANNEL_ID", "@your_channel")
CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/your_channel")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x]

WELCOME_TEXT = (
    "👋 Привет! Я бот <b>WithSolution</b>.\n\n"
    "Помогу тебе получить <b>бесплатный сайт</b>. Нажми кнопку ниже, чтобы начать 👇"
)

DB_PATH = "bot.db"
THROTTLE_SECONDS = 2

BTN_GET_SITE = "🎁 Получить бесплатный сайт"

MEMBER_STATUSES = (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)

# ======================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("free_site_bot")

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN не задан. Создай .env или экспортируй переменную окружения.")
if not ADMIN_IDS:
    logger.warning("ADMIN_IDS пуст — заявки отправлять будет некому.")

# id канала, которое реально прилетает в апдейтах (резолвится при старте)
CHANNEL_CHAT_ID: Optional[int] = None


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# ============== ХРАНИЛИЩЕ (SQLite) ==============

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(db()) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY, username TEXT,
                started_at INTEGER, unlocked INTEGER DEFAULT 0)"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS site_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, username TEXT,
                site_url TEXT, goal TEXT, created_at INTEGER)"""
        )
        conn.commit()


def upsert_user(user_id: int, username: Optional[str]) -> None:
    with closing(db()) as conn:
        conn.execute(
            """INSERT INTO users (user_id, username, started_at) VALUES (?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET username = excluded.username""",
            (user_id, username, int(time.time())),
        )
        conn.commit()


def mark_unlocked(user_id: int) -> None:
    with closing(db()) as conn:
        conn.execute("UPDATE users SET unlocked = 1 WHERE user_id = ?", (user_id,))
        conn.commit()


def is_unlocked(user_id: int) -> bool:
    with closing(db()) as conn:
        r = conn.execute("SELECT unlocked FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return bool(r and r["unlocked"])


def save_site_request(user_id: int, username: Optional[str], site_url: str, goal: str) -> None:
    with closing(db()) as conn:
        conn.execute(
            "INSERT INTO site_requests (user_id, username, site_url, goal, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, site_url, goal, int(time.time())),
        )
        conn.commit()


def list_site_requests(limit: int = 20) -> list:
    with closing(db()) as conn:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM site_requests ORDER BY id DESC LIMIT ?", (limit,)
            )
        ]


# ============== FSM ==============

class SiteForm(StatesGroup):
    site_url = State()
    goal = State()


# ============== АНТИ-СПАМ (inline-кнопки) ==============

class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, rate: float = THROTTLE_SECONDS):
        self.rate = rate
        self._last_call: dict[int, float] = {}

    async def __call__(self, handler, event: TelegramObject, data: dict):
        user = data.get("event_from_user")
        if user:
            now = time.monotonic()
            if now - self._last_call.get(user.id, 0) < self.rate:
                if isinstance(event, CallbackQuery):
                    await event.answer("Не так быстро 🙂")
                return
            self._last_call[user.id] = now
        return await handler(event, data)


bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
dp.callback_query.middleware(ThrottlingMiddleware())


# --- клавиатуры ---
def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=BTN_GET_SITE, callback_data="get_site")]])


def subscribe_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подать заявку на вступление", url=CHANNEL_URL)],
            [InlineKeyboardButton(text="✅ Я подал заявку — продолжить", callback_data="applied")],
        ]
    )


def no_site_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="У меня нет сайта", callback_data="no_site")]]
    )


async def is_subscribed(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status in MEMBER_STATUSES
    except Exception as e:
        logger.error(f"Не удалось проверить подписку user_id={user_id}: {e}")
        return False


async def start_site_form(user_id: int, chat_id: int, state: FSMContext) -> None:
    # Защита от дублей: если пользователь уже в этой форме (например, кнопка
    # "Я подал заявку" и событие chat_join_request сработали почти одновременно),
    # не шлём приветственный шаг второй раз.
    if await state.get_state() in (SiteForm.site_url.state, SiteForm.goal.state):
        return
    mark_unlocked(user_id)
    await state.set_state(SiteForm.site_url)
    await bot.send_message(
        chat_id,
        "✅ Заявка принята!\n\n"
        "<b>Шаг 1 из 2.</b> Пришли ссылку на свой текущий сайт, если он есть.\n"
        "Если сайта пока нет — напиши «нет» или нажми кнопку ниже.",
        reply_markup=no_site_keyboard(),
    )


def fsm_context_for(user_id: int) -> FSMContext:
    """FSMContext для конкретного пользователя, созданный не в ответ на его сообщение
    (нужен, чтобы самим стартовать сценарий после автоматического обнаружения подписки)."""
    key = StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id)
    return FSMContext(storage=dp.storage, key=key)


# ============== БАЗОВЫЕ ХЕНДЛЕРЫ ==============

@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    upsert_user(message.from_user.id, message.from_user.username)
    await message.answer(WELCOME_TEXT, reply_markup=start_keyboard())


@dp.message(StateFilter("*"), Command("cancel"))
async def cancel_handler(message: Message, state: FSMContext):
    if await state.get_state() is not None:
        await state.clear()
        await message.answer("Окей, отменил.", reply_markup=start_keyboard())
    else:
        await message.answer("Нажми кнопку, чтобы начать 👇", reply_markup=start_keyboard())


@dp.callback_query(F.data == "get_site")
async def cb_get_site(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await callback.answer()
    if is_unlocked(user_id) or await is_subscribed(user_id):
        # Уже подавал заявку раньше (или уже состоит в канале) — сразу форма,
        # без повторного «подпишись».
        await callback.message.edit_reply_markup(reply_markup=None)
        await state.clear()
        await start_site_form(user_id, callback.message.chat.id, state)
    else:
        await callback.message.edit_text(
            "Чтобы получить бесплатный сайт, подай заявку на вступление в наш канал 👇\n\n"
            "Как только нажмёшь «Запросить вступление» в Telegram — я сам это увижу "
            "и сразу же напишу следующий шаг. Ждать одобрения модератора не нужно, "
            "это отдельный процесс — он никак не задержит твою заявку здесь, в боте.",
            reply_markup=subscribe_keyboard(),
        )


@dp.callback_query(F.data == "applied")
async def cb_applied(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await callback.answer("Принято! ✅")
    await callback.message.edit_reply_markup(reply_markup=None)
    await start_site_form(user_id, callback.message.chat.id, state)


# ============== АВТОМАТИЧЕСКОЕ ОБНАРУЖЕНИЕ ЗАЯВКИ НА ВСТУПЛЕНИЕ ==============

@dp.chat_join_request()
async def on_join_request(event: ChatJoinRequest) -> None:
    if CHANNEL_CHAT_ID is None or event.chat.id != CHANNEL_CHAT_ID:
        return

    user = event.from_user
    # Ловим сам факт заявки на вступление — не дожидаясь, пока модератор её
    # одобрит. Одобрение/отклонение в канале идёт отдельно и на бота не влияет.
    logger.info(f"Заявка на вступление в канал (авто): user_id={user.id}")
    try:
        await start_site_form(user.id, user.id, fsm_context_for(user.id))
    except Exception as e:
        logger.error(f"Не смог автоматически написать пользователю {user.id}: {e}")


# ============== ФОРМА: ссылка на сайт + цель ==============

@dp.callback_query(SiteForm.site_url, F.data == "no_site")
async def cb_no_site(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.update_data(site_url="нет сайта")
    await state.set_state(SiteForm.goal)
    await callback.message.answer(
        "<b>Шаг 2 из 2.</b> Что хочешь получить? Например:\n"
        "«хочу обновить дизайн», «хочу получать больше заявок», «нужен сайт с нуля»."
    )


@dp.message(SiteForm.site_url, F.text)
async def site_url_received(message: Message, state: FSMContext):
    await state.update_data(site_url=message.text.strip())
    await state.set_state(SiteForm.goal)
    await message.answer(
        "<b>Шаг 2 из 2.</b> Что хочешь получить? Например:\n"
        "«хочу обновить дизайн», «хочу получать больше заявок», «нужен сайт с нуля»."
    )


@dp.message(SiteForm.site_url)
async def site_url_wrong_type(message: Message):
    await message.answer("Пришли, пожалуйста, ссылку текстом (или нажми «У меня нет сайта»).", reply_markup=no_site_keyboard())


@dp.message(SiteForm.goal, F.text)
async def goal_received(message: Message, state: FSMContext):
    data = await state.get_data()
    site_url = data.get("site_url", "нет сайта")
    goal = message.text.strip()
    await state.clear()

    save_site_request(message.from_user.id, message.from_user.username, site_url, goal)

    uname = f"@{message.from_user.username}" if message.from_user.username else "без username"
    admin_text = (
        "🆕 <b>Новая заявка на бесплатный сайт!</b>\n\n"
        f"🔗 Текущий сайт: {site_url}\n"
        f"🎯 Что хочет получить: {goal}\n\n"
        f"От: {uname} (id <code>{message.from_user.id}</code>)"
    )
    for admin in ADMIN_IDS:
        try:
            await bot.send_message(admin, admin_text)
        except Exception as e:
            logger.error(f"Не смог отправить заявку админу {admin}: {e}")

    await message.answer(
        "✅ Заявка принята! Мы посмотрим и скоро свяжемся с тобой.",
        reply_markup=start_keyboard(),
    )


@dp.message(SiteForm.goal)
async def goal_wrong_type(message: Message):
    await message.answer("Опиши, пожалуйста, текстом, что хочешь получить или улучшить.")


# ============== АДМИН: последние заявки ==============

@dp.message(Command("requests"))
async def requests_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return
    reqs = list_site_requests()
    if not reqs:
        await message.answer("Заявок пока нет.")
        return
    blocks = []
    for r in reqs:
        uname = f"@{r['username']}" if r["username"] else f"id {r['user_id']}"
        blocks.append(f"👤 {uname}\n🔗 {r['site_url']}\n🎯 {r['goal']}")
    await message.answer("📋 <b>Последние заявки:</b>\n\n" + "\n\n".join(blocks))


# ============== КОМАНДЫ В МЕНЮ ==============

async def set_commands(bot: Bot) -> None:
    common = [
        BotCommand(command="start", description="Начать / получить бесплатный сайт"),
        BotCommand(command="cancel", description="Отменить текущее действие"),
    ]
    await bot.set_my_commands(common, scope=BotCommandScopeDefault())
    admin_cmds = common + [BotCommand(command="requests", description="Последние заявки")]
    for admin in ADMIN_IDS:
        try:
            await bot.set_my_commands(admin_cmds, scope=BotCommandScopeChat(chat_id=admin))
        except Exception as e:
            logger.warning(f"Не удалось задать команды админу {admin}: {e}")


async def resolve_channel_chat_id() -> None:
    global CHANNEL_CHAT_ID
    try:
        chat = await bot.get_chat(CHANNEL_ID)
        CHANNEL_CHAT_ID = chat.id
        logger.info(f"Канал определён: {CHANNEL_ID} -> {CHANNEL_CHAT_ID}")
    except Exception as e:
        logger.error(
            f"Не удалось получить канал {CHANNEL_ID}: {e}. "
            "Проверь CHANNEL_ID и что бот добавлен в канал администратором "
            "с правом can_invite_users. Автоматическое обнаружение заявок "
            "работать не будет, останется только ручная кнопка «Я подал заявку»."
        )


async def main():
    init_db()
    await resolve_channel_chat_id()
    await set_commands(bot)
    logger.info("Бот запущен.")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
