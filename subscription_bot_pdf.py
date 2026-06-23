"""
Telegram-бот WithSolution: подписка + материалы с отложенной выдачей + меню + заявки + рассылка.

Установка:
    python3 -m pip install aiogram python-dotenv

Файл .env рядом со скриптом:
    BOT_TOKEN=123456:ABC...
    CHANNEL_ID=@your_channel
    CHANNEL_URL=https://t.me/your_channel
    ADMIN_IDS=1516639621            # твой Telegram ID (можно несколько через запятую)

Запуск:
    python3 subscription_bot_pdf.py

УПРАВЛЕНИЕ МАТЕРИАЛАМИ (команда /materials, только админ):
  • ➕ Добавить — пришли боту файл / фото / видео / текст / ссылку (сколько угодно).
  • 🟢/⚪️ — включить/выключить (что сейчас в раздаче).
  • ⏱ — задержка выдачи в часах от момента запроса (0 = сразу, 24 = через сутки…).
  • 👁 Предпросмотр — увидеть материалы так, как их получит подписчик.
  • 🗑 — удалить.
Подписчику включённые материалы приходят по графику (drip), а не все сразу.
"""

from __future__ import annotations  # совместимость с Python 3.9

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
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
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

ABOUT_TEXT = (
    "<b>WithSolution</b> — создаём сайты, которые продают.\n\n"
    "Здесь ты можешь получить полезные материалы или оставить заявку — "
    "мы свяжемся и обсудим твой проект."
)

DB_PATH = "bot.db"
THROTTLE_SECONDS = 2
DELIVERY_TICK_SECONDS = 60  # как часто проверять очередь отложенных материалов

BTN_MATERIALS = "📚 Получить материалы"
BTN_LEAD = "📝 Оставить заявку"
BTN_ABOUT = "ℹ️ О нас"
BTN_CANCEL = "❌ Отмена"
BTN_FINISH_LEAD = "✅ Завершить заявку"

KIND_LABELS = {"document": "📄 файл", "photo": "🖼 фото", "video": "🎬 видео", "text": "📝 текст/ссылка"}

# ======================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("subscription_bot")

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN не задан. Создай .env или экспортируй переменную окружения.")
if not ADMIN_IDS:
    logger.warning("ADMIN_IDS пуст — заявки, /materials и /broadcast работать не будут.")


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def fmt_delay(hours) -> str:
    h = float(hours or 0)
    if h <= 0:
        return "сразу"
    if h == int(h):
        return f"через {int(h)} ч"
    return f"через {h:g} ч"


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
                started_at INTEGER, got_materials INTEGER DEFAULT 0)"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, username TEXT,
                name TEXT, phone TEXT, comment TEXT, created_at INTEGER)"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS materials (
                id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, file_id TEXT, text TEXT,
                enabled INTEGER DEFAULT 1, delay_hours REAL DEFAULT 0, created_at INTEGER)"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, material_id INTEGER,
                send_at INTEGER, sent INTEGER DEFAULT 0)"""
        )
        conn.commit()
    # миграция старых баз: добавить delay_hours, если его ещё нет
    with closing(db()) as conn:
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(materials)")]
        if "delay_hours" not in cols:
            conn.execute("ALTER TABLE materials ADD COLUMN delay_hours REAL DEFAULT 0")
            conn.commit()


def upsert_user(user_id: int, username: Optional[str]) -> None:
    with closing(db()) as conn:
        conn.execute(
            """INSERT INTO users (user_id, username, started_at) VALUES (?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET username = excluded.username""",
            (user_id, username, int(time.time())),
        )
        conn.commit()


def mark_got_materials(user_id: int) -> None:
    with closing(db()) as conn:
        conn.execute("UPDATE users SET got_materials = 1 WHERE user_id = ?", (user_id,))
        conn.commit()


def save_lead(user_id, username, name, phone, comment) -> None:
    with closing(db()) as conn:
        conn.execute(
            "INSERT INTO leads (user_id, username, name, phone, comment, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, username, name, phone, comment, int(time.time())),
        )
        conn.commit()


def get_all_user_ids() -> list:
    with closing(db()) as conn:
        return [r["user_id"] for r in conn.execute("SELECT user_id FROM users")]


# --- материалы ---
def add_material(kind, file_id, text) -> int:
    with closing(db()) as conn:
        cur = conn.execute(
            "INSERT INTO materials (kind, file_id, text, enabled, delay_hours, created_at) VALUES (?, ?, ?, 1, 0, ?)",
            (kind, file_id, text, int(time.time())),
        )
        conn.commit()
        return cur.lastrowid


def list_materials(only_enabled: bool = False) -> list:
    q = "SELECT * FROM materials"
    if only_enabled:
        q += " WHERE enabled = 1"
    q += " ORDER BY delay_hours, id"
    with closing(db()) as conn:
        return [dict(r) for r in conn.execute(q)]


def get_material(mid: int) -> Optional[dict]:
    with closing(db()) as conn:
        r = conn.execute("SELECT * FROM materials WHERE id = ?", (mid,)).fetchone()
        return dict(r) if r else None


def toggle_material(mid: int) -> None:
    with closing(db()) as conn:
        conn.execute("UPDATE materials SET enabled = 1 - enabled WHERE id = ?", (mid,))
        conn.commit()


def set_material_delay(mid: int, hours: float) -> None:
    with closing(db()) as conn:
        conn.execute("UPDATE materials SET delay_hours = ? WHERE id = ?", (hours, mid))
        conn.commit()


def delete_material(mid: int) -> None:
    with closing(db()) as conn:
        conn.execute("DELETE FROM materials WHERE id = ?", (mid,))
        conn.commit()


# --- очередь доставки ---
def queue_delivery(user_id: int, material_id: int, send_at: int) -> None:
    with closing(db()) as conn:
        conn.execute(
            "INSERT INTO deliveries (user_id, material_id, send_at, sent) VALUES (?, ?, ?, 0)",
            (user_id, material_id, send_at),
        )
        conn.commit()


def has_pending(user_id: int) -> bool:
    with closing(db()) as conn:
        r = conn.execute("SELECT 1 FROM deliveries WHERE user_id = ? AND sent = 0 LIMIT 1", (user_id,)).fetchone()
        return r is not None


def due_deliveries() -> list:
    now = int(time.time())
    with closing(db()) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM deliveries WHERE sent = 0 AND send_at <= ? ORDER BY send_at", (now,))]


def mark_delivered(delivery_id: int) -> None:
    with closing(db()) as conn:
        conn.execute("UPDATE deliveries SET sent = 1 WHERE id = ?", (delivery_id,))
        conn.commit()


# ============== FSM ==============

class LeadForm(StatesGroup):
    name = State()
    phone = State()
    comment = State()


class Broadcast(StatesGroup):
    waiting = State()


class AddMaterial(StatesGroup):
    waiting = State()


class SetDelay(StatesGroup):
    waiting = State()


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
def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_MATERIALS)], [KeyboardButton(text=BTN_LEAD)], [KeyboardButton(text=BTN_ABOUT)]],
        resize_keyboard=True,
    )


def cancel_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=BTN_CANCEL)]], resize_keyboard=True)


def phone_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Поделиться телефоном", request_contact=True)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
    )


def lead_task_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_FINISH_LEAD)], [KeyboardButton(text=BTN_CANCEL)]],
        resize_keyboard=True,
    )


def subscribe_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_URL)],
            [InlineKeyboardButton(text="✅ Я подписался — проверить", callback_data="check_sub")],
        ]
    )


async def is_subscribed(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)
    except Exception as e:
        logger.error(f"Не удалось проверить подписку user_id={user_id}: {e}")
        return False


# ============== ВЫДАЧА МАТЕРИАЛОВ ==============

async def send_one_material(chat_id: int, m: dict) -> None:
    kind, file_id, text = m["kind"], m["file_id"], m["text"]
    try:
        if kind == "document":
            await bot.send_document(chat_id, file_id, caption=text or None)
        elif kind == "photo":
            await bot.send_photo(chat_id, file_id, caption=text or None)
        elif kind == "video":
            await bot.send_video(chat_id, file_id, caption=text or None)
        else:
            if text:
                await bot.send_message(chat_id, text)
    except Exception as e:
        logger.error(f"Не смог отправить материал #{m.get('id')} в чат {chat_id}: {e}")


async def process_due_deliveries() -> None:
    """Отправляет все материалы из очереди, у которых наступило время."""
    for d in due_deliveries():
        m = get_material(d["material_id"])
        if m:
            await send_one_material(d["user_id"], m)
        mark_delivered(d["id"])
        await asyncio.sleep(0.05)


async def deliver_materials(user_id: int) -> str:
    """Ставит включённые материалы в очередь по их задержкам и сразу отправляет те, что 'сразу'."""
    mats = list_materials(only_enabled=True)
    if not mats:
        await bot.send_message(user_id, "Материалы пока готовятся — загляни чуть позже 🙌")
        return "empty"
    if has_pending(user_id):
        return "pending"
    now = int(time.time())
    for m in mats:
        queue_delivery(user_id, m["id"], now + int(float(m["delay_hours"] or 0) * 3600))
    await process_due_deliveries()  # отправит материалы с задержкой 0
    return "queued"


async def delivery_loop() -> None:
    """Фоновая задача: периодически досылает отложенные материалы."""
    while True:
        try:
            await process_due_deliveries()
        except Exception as e:
            logger.error(f"Ошибка в delivery_loop: {e}")
        await asyncio.sleep(DELIVERY_TICK_SECONDS)


async def offer_materials(user_id: int, chat_id: int) -> None:
    """Общий путь: проверить подписку и выдать/запланировать материалы."""
    if not await is_subscribed(user_id):
        await bot.send_message(
            chat_id, "Чтобы получить материалы, подпишись на канал и нажми «Проверить» 👇",
            reply_markup=subscribe_keyboard(),
        )
        return
    mark_got_materials(user_id)
    res = await deliver_materials(user_id)
    if res == "pending":
        await bot.send_message(chat_id, "Твои материалы уже в очереди — следующий придёт по расписанию ⏳")
    elif res == "queued":
        delayed = [m for m in list_materials(only_enabled=True) if float(m["delay_hours"] or 0) > 0]
        if delayed:
            sched = ", ".join(fmt_delay(m["delay_hours"]) for m in delayed)
            await bot.send_message(chat_id, f"Это пока не всё 🙌 Остальное придёт по графику: {sched}.")


# ============== БАЗОВЫЕ ХЕНДЛЕРЫ ==============

@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    upsert_user(message.from_user.id, message.from_user.username)
    await message.answer(
        "👋 Привет! Я бот <b>WithSolution</b>.\n\nВыбери, что тебя интересует 👇",
        reply_markup=main_menu(),
    )


@dp.message(StateFilter("*"), Command("cancel"))
@dp.message(StateFilter("*"), F.text == BTN_CANCEL)
async def cancel_handler(message: Message, state: FSMContext):
    if await state.get_state() is not None:
        await state.clear()
        await message.answer("Окей, отменил. Возвращаю в меню 👇", reply_markup=main_menu())
    else:
        await message.answer("Выбирай действие в меню 👇", reply_markup=main_menu())


# ============== УПРАВЛЕНИЕ МАТЕРИАЛАМИ (админ) ==============

def materials_view():
    mats = list_materials()
    if not mats:
        text = "📦 <b>Материалы</b>\n\nПока пусто. Нажми «➕ Добавить» и пришли боту файл, фото, видео, текст или ссылку."
    else:
        blocks = ["📦 <b>Материалы для выдачи</b>\nВключённые приходят подписчику по графику.\n"]
        for m in mats:
            status = "🟢 вкл" if m["enabled"] else "⚪️ выкл"
            delay = fmt_delay(m["delay_hours"])
            preview = (m["text"] or "").replace("\n", " ").strip()
            preview = (preview[:45] + "…") if len(preview) > 45 else preview
            blocks.append(
                f"<b>#{m['id']}</b> · {KIND_LABELS.get(m['kind'], m['kind'])} · {status} · ⏱ {delay}"
                + (f"\n<i>{preview}</i>" if preview else "")
            )
        text = "\n\n".join(blocks)

    rows = [[
        InlineKeyboardButton(text="➕ Добавить", callback_data="mat_add"),
        InlineKeyboardButton(text="👁 Предпросмотр", callback_data="mat_preview"),
    ]]
    for m in mats:
        toggle = "🚫 Выкл" if m["enabled"] else "✅ Вкл"
        rows.append([
            InlineKeyboardButton(text=f"#{m['id']} {toggle}", callback_data=f"mat_tg:{m['id']}"),
            InlineKeyboardButton(text=f"⏱ {fmt_delay(m['delay_hours'])}", callback_data=f"mat_delay:{m['id']}"),
            InlineKeyboardButton(text="🗑", callback_data=f"mat_del:{m['id']}"),
        ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(Command("materials"))
async def materials_cmd(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    text, kb = materials_view()
    await message.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "mat_add")
async def cb_mat_add(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    await state.set_state(AddMaterial.waiting)
    await callback.message.answer(
        "Пришли материал в выдачу:\n"
        "• <b>файл</b> (pdf и т.п.) — можно с подписью\n"
        "• <b>фото</b> или <b>видео</b> — можно с подписью\n"
        "• <b>текст</b> или <b>ссылку</b>\n\nОтмена — кнопкой ниже.",
        reply_markup=cancel_menu(),
    )
    await callback.answer()


@dp.message(AddMaterial.waiting)
async def add_material_receive(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    kind = file_id = text = None
    if message.document:
        kind, file_id, text = "document", message.document.file_id, message.caption
    elif message.photo:
        kind, file_id, text = "photo", message.photo[-1].file_id, message.caption
    elif message.video:
        kind, file_id, text = "video", message.video.file_id, message.caption
    elif message.text:
        kind, text = "text", message.text
    else:
        await message.answer("Не понял формат 🤔 Пришли файл, фото, видео или текст/ссылку.")
        return

    mid = add_material(kind, file_id, text)
    await state.clear()
    await message.answer(
        f"✅ Добавлено как #{mid}, включено, выдача — сразу.\n"
        f"Задержку можно задать кнопкой «⏱» в списке.",
        reply_markup=main_menu(),
    )
    view_text, kb = materials_view()
    await message.answer(view_text, reply_markup=kb)


@dp.callback_query(F.data.startswith("mat_tg:"))
async def cb_mat_toggle(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    toggle_material(int(callback.data.split(":")[1]))
    text, kb = materials_view()
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer("Обновил")


@dp.callback_query(F.data.startswith("mat_del:"))
async def cb_mat_delete(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    delete_material(int(callback.data.split(":")[1]))
    text, kb = materials_view()
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer("Удалено")


@dp.callback_query(F.data.startswith("mat_delay:"))
async def cb_mat_delay(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    mid = int(callback.data.split(":")[1])
    await state.set_state(SetDelay.waiting)
    await state.update_data(mat_id=mid)
    await callback.message.answer(
        f"Через сколько часов после запроса выдавать материал #{mid}?\n"
        "Напиши число: <b>0</b> = сразу, <b>24</b> = через сутки, можно дробное (напр. 0.5).",
        reply_markup=cancel_menu(),
    )
    await callback.answer()


@dp.message(SetDelay.waiting, F.text)
async def set_delay_receive(message: Message, state: FSMContext):
    raw = message.text.strip().replace(",", ".")
    try:
        hours = float(raw)
        if hours < 0:
            raise ValueError
    except ValueError:
        await message.answer("Нужно неотрицательное число часов. Например: 0, 6, 24, 0.5")
        return
    data = await state.get_data()
    mid = data.get("mat_id")
    await state.clear()
    if mid:
        set_material_delay(mid, hours)
        await message.answer(f"✅ Материал #{mid} теперь выдаётся: {fmt_delay(hours)}.", reply_markup=main_menu())
        view_text, kb = materials_view()
        await message.answer(view_text, reply_markup=kb)


@dp.callback_query(F.data == "mat_preview")
async def cb_mat_preview(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    mats = list_materials(only_enabled=True)
    await callback.answer()
    if not mats:
        await callback.message.answer("Нет включённых материалов — предпросматривать нечего.")
        return
    await callback.message.answer("👁 <b>Так это увидит подписчик</b> (задержки в предпросмотре игнорируются):")
    for m in mats:
        prefix = f"⏱ <i>в реальной выдаче: {fmt_delay(m['delay_hours'])}</i>"
        await callback.message.answer(prefix)
        await send_one_material(callback.message.chat.id, m)
        await asyncio.sleep(0.1)


# ============== РАССЫЛКА (админ) ==============

@dp.message(Command("broadcast"))
async def broadcast_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(Broadcast.waiting)
    await message.answer(
        "📣 Пришли сообщение для рассылки (текст, фото, файл — что угодно).\n"
        "Оно уйдёт всем, кто запускал бота.\n\nОтмена — кнопкой ниже.",
        reply_markup=cancel_menu(),
    )


@dp.message(Broadcast.waiting)
async def broadcast_run(message: Message, state: FSMContext):
    await state.clear()
    user_ids = get_all_user_ids()
    sent, failed = 0, 0
    progress = await message.answer(f"Рассылка началась… 0 / {len(user_ids)}")
    for i, uid in enumerate(user_ids, 1):
        try:
            await bot.copy_message(chat_id=uid, from_chat_id=message.chat.id, message_id=message.message_id)
            sent += 1
        except Exception:
            failed += 1
        if i % 25 == 0:
            try:
                await progress.edit_text(f"Рассылка идёт… {i} / {len(user_ids)}")
            except Exception:
                pass
        await asyncio.sleep(0.05)
    await message.answer(
        f"✅ Рассылка завершена.\nДоставлено: <b>{sent}</b>, не дошло: <b>{failed}</b>",
        reply_markup=main_menu(),
    )


# ============== ЗАЯВКА В 3 ШАГА ==============

@dp.message(StateFilter(None), F.text == BTN_LEAD)
async def lead_start(message: Message, state: FSMContext):
    await state.set_state(LeadForm.name)
    await message.answer(
        "📝 <b>Оставить заявку</b>\n\n"
        "<b>Шаг 1 из 3.</b> Как к вам обращаться?",
        reply_markup=cancel_menu(),
    )


@dp.message(LeadForm.name, F.text)
async def lead_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2:
        await message.answer("Напишите, пожалуйста, как к вам обращаться.")
        return

    await state.update_data(name=name)
    await state.set_state(LeadForm.phone)
    await message.answer(
        f"Приятно, {name}!\n\n"
        "<b>Шаг 2 из 3.</b> Оставьте удобный способ связи.\n\n"
        "Можно нажать кнопку ниже и поделиться телефоном "
        "или написать вручную: телефон, Telegram, WhatsApp — как удобнее.",
        reply_markup=phone_menu(),
    )


@dp.message(LeadForm.phone)
async def lead_phone(message: Message, state: FSMContext):
    if message.contact:
        contact = message.contact.phone_number
    elif message.text:
        contact = message.text.strip()
    else:
        await message.answer("Пришлите контакт текстом или нажмите «📱 Поделиться телефоном».")
        return

    if len(contact) < 3:
        await message.answer("Слишком коротко. Напишите телефон, Telegram или другой удобный способ связи.")
        return

    await state.update_data(phone=contact, lead_items=[], lead_message_ids=[])
    await state.set_state(LeadForm.comment)
    await message.answer(
        "<b>Шаг 3 из 3.</b> Опишите задачу и прикрепите всё, что может пригодиться.\n\n"
        "Можно отправить текст, голосовое, файл, фото, ссылку на текущий сайт, "
        "ТЗ, примеры сайтов или сайты конкурентов.\n\n"
        "Можно отправить несколько сообщений. Когда закончите — нажмите «✅ Завершить заявку».",
        reply_markup=lead_task_menu(),
    )


def lead_item_summary(message: Message) -> str:
    if message.text:
        return message.text.strip()
    if message.voice:
        return "Голосовое сообщение"
    if message.document:
        filename = message.document.file_name or "документ"
        caption = f" — {message.caption.strip()}" if message.caption else ""
        return f"Файл: {filename}{caption}"
    if message.photo:
        caption = f": {message.caption.strip()}" if message.caption else ""
        return f"Фото / изображение{caption}"
    if message.video:
        caption = f": {message.caption.strip()}" if message.caption else ""
        return f"Видео{caption}"
    if message.audio:
        caption = f": {message.caption.strip()}" if message.caption else ""
        return f"Аудио{caption}"
    return "Материал без текста"


@dp.message(LeadForm.comment, F.text == BTN_FINISH_LEAD)
async def lead_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    name = data.get("name", "—")
    contact = data.get("phone", "—")
    items = data.get("lead_items", [])
    message_ids = data.get("lead_message_ids", [])

    if not items:
        await message.answer(
            "Сначала отправьте описание задачи или материалы, потом нажмите «✅ Завершить заявку».",
            reply_markup=lead_task_menu(),
        )
        return

    comment = "\n".join(f"• {item}" for item in items)
    await state.clear()

    save_lead(message.from_user.id, message.from_user.username, name, contact, comment)

    uname = f"@{message.from_user.username}" if message.from_user.username else "без username"
    admin_text = (
        "🆕 <b>Новая заявка!</b>\n\n"
        f"👤 Имя: {name}\n"
        f"📞 Связь: {contact}\n"
        f"💬 Задача / материалы:\n{comment}\n\n"
        f"От: {uname} (id <code>{message.from_user.id}</code>)"
    )

    for admin in ADMIN_IDS:
        try:
            await bot.send_message(admin, admin_text)
            for mid in message_ids:
                await bot.copy_message(
                    chat_id=admin,
                    from_chat_id=message.chat.id,
                    message_id=mid,
                )
                await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Не смог отправить заявку админу {admin}: {e}")

    await message.answer("✅ Заявка принята! Мы посмотрим задачу и скоро свяжемся.", reply_markup=main_menu())


@dp.message(LeadForm.comment)
async def lead_collect_item(message: Message, state: FSMContext):
    data = await state.get_data()
    items = data.get("lead_items", [])
    message_ids = data.get("lead_message_ids", [])

    summary = lead_item_summary(message)
    items.append(summary)
    message_ids.append(message.message_id)

    await state.update_data(lead_items=items, lead_message_ids=message_ids)
    await message.answer(
        "Принял. Можно отправить ещё описание/материалы или нажать «✅ Завершить заявку».",
        reply_markup=lead_task_menu(),
    )


# ============== МАТЕРИАЛЫ / О НАС (для всех) ==============

@dp.message(StateFilter(None), F.text == BTN_MATERIALS)
async def materials_handler(message: Message):
    await offer_materials(message.from_user.id, message.chat.id)


@dp.message(StateFilter(None), F.text == BTN_ABOUT)
async def about_handler(message: Message):
    await message.answer(ABOUT_TEXT, reply_markup=main_menu())


@dp.callback_query(F.data == "check_sub")
async def check_subscription_handler(callback: CallbackQuery):
    if await is_subscribed(callback.from_user.id):
        await callback.answer("Доступ открыт! ✅")
        await callback.message.edit_text("✅ Подписка подтверждена! Отправляю материалы…")
        await offer_materials(callback.from_user.id, callback.message.chat.id)
    else:
        await callback.answer("❌ Похоже, ты ещё не подписан. Подпишись и попробуй снова.", show_alert=True)


# ============== КОМАНДЫ В МЕНЮ ==============

async def set_commands(bot: Bot) -> None:
    common = [
        BotCommand(command="start", description="Запустить бота / меню"),
        BotCommand(command="cancel", description="Отменить текущее действие"),
    ]
    await bot.set_my_commands(common, scope=BotCommandScopeDefault())
    admin_cmds = common + [
        BotCommand(command="materials", description="Управление материалами"),
        BotCommand(command="broadcast", description="Рассылка всем"),
    ]
    for admin in ADMIN_IDS:
        try:
            await bot.set_my_commands(admin_cmds, scope=BotCommandScopeChat(chat_id=admin))
        except Exception as e:
            logger.warning(f"Не удалось задать команды админу {admin}: {e}")


async def main():
    init_db()
    await set_commands(bot)
    asyncio.create_task(delivery_loop())  # фоновая досылка отложенных материалов
    logger.info("Бот запущен.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")