import asyncio
import aiosqlite
import os
import time
import shutil
import re
import tempfile
import uuid
import mutagen
from mutagen.id3 import ID3, TIT2, TPE1, COMM, error
from mutagen.easyid3 import EasyID3
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineQueryResultCachedAudio, ReplyKeyboardMarkup, KeyboardButton, FSInputFile
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode
from aiogram.client.session.aiohttp import AiohttpSession

# --- НАСТРОЙКИ ---
TOKEN = "8607861388:AAECw_LdGWvo8QBCDgIasgPjq8Oij0ZaXnk"
DB_NAME = "music_bot.db"
SUPER_ADMIN_CODE = "timka24082011and"
ADMIN_CHAT_ID = 1023097570  # Замени на свой Telegram ID для уведомлений

bot = Bot(token=TOKEN)
dp = Dispatcher()

# Глобальные переменные
db_conn = None
active_admins = {}
admin_settings_step = {}
archive_flow = {}
admin_recording = {}
pinned_messages = {}
unpin_tasks = {}

# --- КНОПКИ ---
def get_main_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="📊 Статистика"),
                KeyboardButton(text="🔍 Помощь")
            ],
            [
                KeyboardButton(text="📝 Код доступа")
            ]
        ],
        resize_keyboard=True
    )
    return keyboard

def get_admin_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="📊 Статистика"),
                KeyboardButton(text="📝 Мои треки")
            ],
            [
                KeyboardButton(text="🔍 Помощь"),
                KeyboardButton(text="🚪 Выйти")
            ]
        ],
        resize_keyboard=True
    )
    return keyboard

# --- ФУНКЦИИ БАЗЫ ДАННЫХ ---
async def get_db():
    global db_conn
    if db_conn is None:
        db_conn = await aiosqlite.connect(DB_NAME)
    return db_conn

async def init_db():
    db = await get_db()
    
    # Таблица песен
    await db.execute("""
        CREATE TABLE IF NOT EXISTS songs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT, 
            performer TEXT, 
            description TEXT,
            search_content TEXT, 
            file_id TEXT UNIQUE,
            file_unique_id TEXT,
            added_date TIMESTAMP,
            added_by INTEGER
        )
    """)
    
    # Таблица хештегов
    await db.execute("""
        CREATE TABLE IF NOT EXISTS hashtags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
    """)
    
    # Таблица связей песен и хештегов
    await db.execute("""
        CREATE TABLE IF NOT EXISTS song_hashtags (
            song_id INTEGER,
            hashtag_id INTEGER,
            FOREIGN KEY (song_id) REFERENCES songs(id) ON DELETE CASCADE,
            FOREIGN KEY (hashtag_id) REFERENCES hashtags(id) ON DELETE CASCADE,
            PRIMARY KEY (song_id, hashtag_id)
        )
    """)
    
    # Таблица соответствий старых и новых file_id
    await db.execute("""
        CREATE TABLE IF NOT EXISTS file_id_mapping (
            old_file_id TEXT PRIMARY KEY,
            new_file_id TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    await db.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('reset_code', 'mode01')")
    await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('reset_code_2', 'mode02')")
    await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('add_code', 'mode11')")
    await db.commit()
    print(f"✅ Система готова: {datetime.now()}")

async def get_setting(key):
    db = await get_db()
    async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else None

async def set_setting(key, value):
    db = await get_db()
    await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
    await db.commit()

async def archive_and_reset_db():
    global db_conn
    if db_conn:
        await db_conn.close()
        db_conn = None
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    archive_name = f"backup_{timestamp}_{DB_NAME}"
    if os.path.exists(DB_NAME):
        shutil.move(DB_NAME, archive_name)
    await init_db()

async def get_song_count():
    try:
        db = await get_db()
        async with db.execute("SELECT COUNT(*) FROM songs") as cursor:
            count = await cursor.fetchone()
            return count[0] if count else 0
    except Exception as e:
        print(f"Ошибка получения количества песен: {e}")
        return 0

async def get_user_song_count(user_id):
    try:
        db = await get_db()
        async with db.execute("SELECT COUNT(*) FROM songs WHERE added_by = ?", (user_id,)) as cursor:
            count = await cursor.fetchone()
            return count[0] if count else 0
    except Exception as e:
        print(f"Ошибка получения количества песен пользователя: {e}")
        return 0

async def get_last_song():
    try:
        db = await get_db()
        async with db.execute(
            "SELECT id, title, performer, datetime('now') FROM songs ORDER BY id DESC LIMIT 1"
        ) as cursor:
            row = await cursor.fetchone()
            return row if row else None
    except Exception as e:
        print(f"Ошибка получения последней песни: {e}")
        return None

# --- ФУНКЦИИ ДЛЯ РАБОТЫ С ХЕШТЕГАМИ ---
async def extract_and_save_hashtags(song_id: int, description: str):
    if not description:
        return
    
    hashtags = re.findall(r'#(\w+)', description)
    
    if not hashtags:
        return
    
    db = await get_db()
    
    for tag in hashtags:
        await db.execute(
            "INSERT OR IGNORE INTO hashtags (name) VALUES (?)",
            (tag.lower(),)
        )
        
        async with db.execute("SELECT id FROM hashtags WHERE name = ?", (tag.lower(),)) as cursor:
            result = await cursor.fetchone()
            if result:
                hashtag_id = result[0]
                await db.execute(
                    "INSERT OR IGNORE INTO song_hashtags (song_id, hashtag_id) VALUES (?, ?)",
                    (song_id, hashtag_id)
                )
    
    await db.commit()

async def get_songs_by_hashtag(hashtag: str):
    db = await get_db()
    hashtag = hashtag.lower().lstrip('#')
    
    async with db.execute("""
        SELECT s.file_id, s.description FROM songs s
        JOIN song_hashtags sh ON s.id = sh.song_id
        JOIN hashtags h ON sh.hashtag_id = h.id
        WHERE h.name = ?
        ORDER BY s.id
    """, (hashtag,)) as cursor:
        return await cursor.fetchall()

# --- ФУНКЦИЯ ДЛЯ ПОЛУЧЕНИЯ АКТУАЛЬНОГО FILE_ID ---
async def get_current_file_id(old_file_id: str) -> str:
    """
    Возвращает актуальный file_id для старого
    """
    db = await get_db()
    
    async with db.execute(
        "SELECT new_file_id FROM file_id_mapping WHERE old_file_id = ?", 
        (old_file_id,)
    ) as cursor:
        row = await cursor.fetchone()
        if row:
            return row[0]
    
    return old_file_id

# --- ФУНКЦИЯ ДЛЯ ИСПРАВЛЕНИЯ МЕТАДАННЫХ ---
async def fix_audio_tags_guaranteed(old_file_id: str, new_title: str, new_performer: str, chat_id: int, caption: str = None) -> str:
    """
    РЕАЛЬНО меняет ID3 теги в файле и создает НОВЫЙ file_id
    """
    try:
        print(f"🔄 Начинаю фикс тегов для: {new_title} - {new_performer}")
        
        # Скачиваем файл
        file_info = await bot.get_file(old_file_id)
        file_path = file_info.file_path
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.mp3') as tmp_file:
            temp_filename = tmp_file.name
        
        await bot.download_file(file_path, temp_filename)
        print(f"✅ Файл скачан: {temp_filename}")
        
        # === 1. ОЧИЩАЕМ СТАРЫЕ ТЕГИ ===
        try:
            audio_id3 = ID3(temp_filename)
            audio_id3.delete()
            audio_id3.save()
            print(f"✅ Старые ID3 теги удалены")
        except:
            pass
        
        # === 2. ДОБАВЛЯЕМ НОВЫЕ ТЕГИ ===
        try:
            audio = ID3()
            audio.add(TIT2(encoding=3, text=new_title))
            audio.add(TPE1(encoding=3, text=new_performer))
            audio.add(COMM(
                encoding=3,
                lang='eng',
                desc='Fixed',
                text=f'Fixed at {time.time()} | {uuid.uuid4()}'
            ))
            audio.save(temp_filename)
            print(f"✅ Новые ID3 теги добавлены")
        except Exception as e:
            print(f"❌ Ошибка при добавлении тегов: {e}")
            return None
        
        # === 3. ИЗМЕНЯЕМ НЕСКОЛЬКО БАЙТ В ФАЙЛЕ ===
        with open(temp_filename, 'r+b') as f:
            f.seek(2000)
            data = f.read(1)
            if data:
                f.seek(2000)
                f.write(bytes([data[0] ^ 1]))
        
        # === 4. ЗАГРУЖАЕМ КАК НОВЫЙ ФАЙЛ ===
        unique_filename = f"{int(time.time())}_{uuid.uuid4()}.mp3"
        
        temp_bot = Bot(token=TOKEN, session=AiohttpSession())
        
        try:
            with open(temp_filename, 'rb') as f:
                new_message = await temp_bot.send_audio(
                    chat_id=chat_id,
                    audio=FSInputFile(temp_filename, filename=unique_filename),
                    caption=caption,
                    parse_mode=ParseMode.HTML
                )
            
            new_file_id = new_message.audio.file_id
            print(f"✅ НОВЫЙ file_id: {new_file_id}")
            
        finally:
            await temp_bot.session.close()
        
        os.unlink(temp_filename)
        return new_file_id
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None

# --- ФУНКЦИИ ДЛЯ ЗАКРЕПЛЕННЫХ СООБЩЕНИЙ ---
async def auto_unpin_after_delay(user_id: int, delay: int = 86400):
    """Автоматически открепляет сообщение через delay секунд"""
    await asyncio.sleep(delay)
    
    if user_id in pinned_messages and user_id in active_admins:
        await unpin_recording_status(user_id)
        print(f"⏰ Автоматическое открепление для пользователя {user_id}")

async def pin_recording_status(user_id: int, username: str = "Пользователь"):
    try:
        if user_id in unpin_tasks:
            unpin_tasks[user_id].cancel()
        
        if user_id in pinned_messages:
            try:
                await bot.unpin_chat_message(chat_id=user_id, message_id=pinned_messages[user_id])
            except:
                pass
        
        msg = await bot.send_message(
            user_id,
            f"🔴 <b>ЗАПИСЬ ВКЛЮЧЕНА</b>\n\n"
            f"Пользователь {username} сейчас принимает музыку.\n"
            f"Отправляйте аудиофайлы для добавления в базу.\n\n"
            f"⏱ <i>Сообщение автоматически открепится через 24 часа</i>",
            parse_mode=ParseMode.HTML
        )
        
        await bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
        pinned_messages[user_id] = msg.message_id
        
        task = asyncio.create_task(auto_unpin_after_delay(user_id))
        unpin_tasks[user_id] = task
        
        return True
    except Exception as e:
        print(f"Ошибка при закреплении сообщения: {e}")
        return False

async def unpin_recording_status(user_id: int):
    try:
        if user_id in unpin_tasks:
            unpin_tasks[user_id].cancel()
            del unpin_tasks[user_id]
        
        if user_id in pinned_messages:
            await bot.unpin_chat_message(chat_id=user_id, message_id=pinned_messages[user_id])
            del pinned_messages[user_id]
            
            await bot.send_message(
                user_id,
                "🟢 <b>ЗАПИСЬ ВЫКЛЮЧЕНА</b>\n\n"
                "Вы больше не принимаете музыку.",
                parse_mode=ParseMode.HTML
            )
        return True
    except Exception as e:
        print(f"Ошибка при откреплении сообщения: {e}")
        return False

# --- ЛОГИКА СОХРАНЕНИЯ ---
async def save_song(audio: types.Audio, message: types.Message):
    try:
        # Парсим название и исполнителя из описания
        title = audio.title
        performer = audio.performer
        
        if not title or not performer:
            if message.caption:
                lines = message.caption.split('\n')
                if len(lines) > 1:
                    second_line = lines[1].strip()
                    separators = [' — ', '—', ' - ', '-', ' – ', '–']
                    
                    for sep in separators:
                        if sep in second_line:
                            parts = second_line.split(sep, 1)
                            if len(parts) == 2:
                                if not performer:
                                    performer = parts[0].strip()
                                if not title:
                                    title = parts[1].strip()
                                break
        
        title = title or "Без названия"
        performer = performer or "Неизвестный исполнитель"
        
        print(f"📝 Парсинг: '{performer}' - '{title}'")
        
        # Исправляем теги
        new_file_id = await fix_audio_tags_guaranteed(
            old_file_id=audio.file_id,
            new_title=title,
            new_performer=performer,
            chat_id=message.chat.id,
            caption=message.caption
        )
        
        caption = message.caption or ""
        user_id = message.from_user.id if message.from_user else None
        file_unique_id = audio.file_unique_id
        
        # Формируем поисковый контент
        raw_text = f"{title} {performer} {caption}".lower().replace('ё', 'е')
        clean_search = re.sub(r'[^а-яa-z0-9\s]', '', raw_text)
        
        # Сохраняем описание как есть
        if message.caption:
            desc = message.html_text
        else:
            desc = f"<b>{performer}</b> — {title}"

        db = await get_db()
        
        # ПРОВЕРКА НА ДУБЛИКАТЫ
        existing = None
        
        # 1. Проверяем по file_unique_id (самый надежный способ)
        async with db.execute("SELECT id FROM songs WHERE file_unique_id = ?", (file_unique_id,)) as cursor:
            existing = await cursor.fetchone()
        
        # 2. Проверяем по новому file_id
        if not existing and new_file_id:
            async with db.execute("SELECT id FROM songs WHERE file_id = ?", (new_file_id,)) as cursor:
                existing = await cursor.fetchone()
        
        # 3. Проверяем по старому file_id
        if not existing:
            async with db.execute("SELECT id FROM songs WHERE file_id = ?", (audio.file_id,)) as cursor:
                existing = await cursor.fetchone()
        
        if new_file_id:
            await db.execute("""
                INSERT OR REPLACE INTO file_id_mapping (old_file_id, new_file_id)
                VALUES (?, ?)
            """, (audio.file_id, new_file_id))
        
        if existing:
            # Обновляем существующую запись
            file_id_to_use = new_file_id if new_file_id else audio.file_id
            
            await db.execute("""
                UPDATE songs 
                SET title = ?, performer = ?, description = ?, search_content = ?, 
                    added_by = ?, file_id = ?, file_unique_id = ?
                WHERE id = ?
            """, (title, performer, desc, clean_search, user_id, file_id_to_use, file_unique_id, existing[0]))
            song_id = existing[0]
            print(f"🔄 Песня обновлена")
        else:
            # Создаем новую запись
            file_id_to_use = new_file_id if new_file_id else audio.file_id
            
            cursor = await db.execute("""
                INSERT INTO songs 
                (title, performer, description, search_content, file_id, file_unique_id, added_by, added_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (title, performer, desc, clean_search, file_id_to_use, file_unique_id, user_id))
            song_id = cursor.lastrowid
            print(f"✅ Новая песня создана")
        
        await db.commit()
        
        # Сохраняем хештеги
        await extract_and_save_hashtags(song_id, desc)
        
        total_count = await get_song_count()
        last_song = await get_last_song()
        
        return True, total_count, last_song
        
    except Exception as e:
        print(f"❌ Ошибка сохранения трека: {e}")
        return False, 0, None

# --- ОБРАБОТЧИКИ ---
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer(
        "🚀 <b>Music Bot запущен!</b>\n\n"
        "Я помогу тебе хранить и искать музыку.\n\n"
        "<b>📌 Как пользоваться:</b>\n"
        "• Введи код доступа для записи музыки\n"
        "• Отправляй аудиофайлы, чтобы сохранить их\n"
        "• Используй инлайн-режим для поиска: @botusername запрос\n"
        "• Нажимай кнопки ниже для быстрых действий",
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(
        "📋 <b>Доступные команды:</b>\n\n"
        "/start - информация о боте\n"
        "/help - это сообщение\n"
        "/stats - подробная статистика по месяцам\n"
        "/record_on - включить запись\n"
        "/record_off - выключить запись\n"
        "/status - проверить статус\n\n"
        "<b>🔍 Поиск:</b>\n"
        "Используйте инлайн-режим: @botusername запрос\n\n"
        "<b>🎵 Добавление музыки:</b>\n"
        "1. Получите код доступа\n"
        "2. Отправьте аудиофайл боту\n"
        "3. Можно добавить описание в подписи",
        parse_mode=ParseMode.HTML
    )

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    try:
        db = await get_db()
        
        async with db.execute("SELECT description FROM songs") as cursor:
            rows = await cursor.fetchall()
        
        total = len(rows)
        
        if total == 0:
            await message.answer("📊 В базе пока нет песен.")
            return
        
        months_regular = {}
        months_xx = {}
        years_regular = {}
        years_xx = {}
        
        for (desc,) in rows:
            if desc:
                is_xx = '<b>ХХ.' in desc or 'ХХ.' in desc or '<b>XX.' in desc or 'XX.' in desc
                
                match = re.search(r'#Б[вВ]([А-Я][а-я]+)(\d{2})', desc)
                if match:
                    month = match.group(1)
                    year = "20" + match.group(2)
                    
                    key = f"{month} {year}"
                    
                    if is_xx:
                        months_xx[key] = months_xx.get(key, 0) + 1
                        years_xx[year] = years_xx.get(year, 0) + 1
                    else:
                        months_regular[key] = months_regular.get(key, 0) + 1
                        years_regular[year] = years_regular.get(year, 0) + 1
        
        text = f"📊 ВСЕГО ПЕСЕН: {total}\n\n"
        
        if years_regular or years_xx:
            text += "📅 ПО ГОДАМ:\n"
            all_years = set(list(years_regular.keys()) + list(years_xx.keys()))
            for year in sorted(all_years):
                regular = years_regular.get(year, 0)
                xx = years_xx.get(year, 0)
                if xx > 0:
                    text += f"  {year}: {regular}(+{xx})\n"
                else:
                    text += f"  {year}: {regular}\n"
            
            text += "\n📆 ПО МЕСЯЦАМ:\n"
            
            month_order = {
                "Январь": 1, "Февраль": 2, "Март": 3, "Апрель": 4,
                "Май": 5, "Июнь": 6, "Июль": 7, "Август": 8,
                "Сентябрь": 9, "Октябрь": 10, "Ноябрь": 11, "Декабрь": 12
            }
            
            all_months = set(list(months_regular.keys()) + list(months_xx.keys()))
            sorted_months = sorted(all_months, 
                                 key=lambda x: (x.split()[1], month_order[x.split()[0]]))
            
            for month in sorted_months:
                regular = months_regular.get(month, 0)
                xx = months_xx.get(month, 0)
                if xx > 0:
                    text += f"  {month}: {regular}(+{xx})\n"
                else:
                    text += f"  {month}: {regular}\n"
        else:
            text += "Не найдены даты в формате #БвМесяцГод"
        
        await message.answer(text)
        
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")

@dp.message(Command("record_on"))
async def record_on(message: types.Message):
    uid = message.from_user.id
    if uid in active_admins and time.time() < active_admins[uid]:
        admin_recording[uid] = True
        
        user_info = message.from_user
        name = user_info.full_name or user_info.username or "Пользователь"
        await pin_recording_status(uid, name)
        
        await message.answer("🟢 Запись включена. Теперь вы можете отправлять аудио.")
    else:
        await message.answer("⛔ У вас нет активного доступа. Введите код доступа.")

@dp.message(Command("record_off"))
async def record_off(message: types.Message):
    uid = message.from_user.id
    if uid in active_admins and time.time() < active_admins[uid]:
        admin_recording[uid] = False
        await unpin_recording_status(uid)
        await message.answer("🔴 Запись выключена. Чтобы снова включить, используйте /record_on")
    else:
        await message.answer("⛔ У вас нет активного доступа. Введите код доступа.")

@dp.message(Command("status"))
async def status(message: types.Message):
    uid = message.from_user.id
    if uid in active_admins and time.time() < active_admins[uid]:
        time_left = int((active_admins[uid] - time.time()) / 60)
        status = "включена" if admin_recording.get(uid, True) else "выключена"
        await message.answer(
            f"📊 Ваш статус:\n"
            f"⏱ Доступ истечет через: {time_left} минут\n"
            f"📝 Запись: {status}"
        )
    else:
        await message.answer("⛔ У вас нет активного доступа.")

@dp.message(F.text == "📊 Статистика")
async def button_stats(message: types.Message):
    await cmd_stats(message)

@dp.message(F.text == "🔍 Помощь")
async def button_help(message: types.Message):
    await cmd_help(message)

@dp.message(F.text == "📝 Код доступа")
async def button_access(message: types.Message):
    await message.answer(
        "🔑 <b>Код доступа</b>\n\n"
        "Введите код, который выдали администраторы.\n"
        "После ввода кода вы получите доступ на 24 часа.",
        parse_mode=ParseMode.HTML
    )

@dp.message(F.text == "📝 Мои треки")
async def button_my_tracks(message: types.Message):
    uid = message.from_user.id
    
    if uid in active_admins and time.time() < active_admins[uid]:
        user_count = await get_user_song_count(uid)
        total_count = await get_song_count()
        
        await message.answer(
            f"📊 <b>Ваша статистика:</b>\n\n"
            f"🎵 Вы добавили: <b>{user_count}</b> треков\n"
            f"📚 Всего в базе: <b>{total_count}</b> треков\n\n"
            f"💡 Продолжайте в том же духе!",
            parse_mode=ParseMode.HTML
        )
    else:
        await message.answer("⛔ Доступ только для администраторов.")

@dp.message(F.text == "🚪 Выйти")
async def button_exit(message: types.Message):
    uid = message.from_user.id
    
    if uid in active_admins:
        del active_admins[uid]
        await message.answer(
            "👋 Вы вышли из режима администратора.\n"
            "Для входа введите код доступа.",
            reply_markup=get_main_keyboard()
        )
    else:
        await message.answer("Вы и так не в режиме администратора.")

@dp.message(F.text)
async def handle_text(message: types.Message):
    uid = message.from_user.id
    text = message.text.strip()
    
    if text == SUPER_ADMIN_CODE and uid not in archive_flow:
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="⚙️ Настройка кодов")],
                [KeyboardButton(text="📦 Архивация")]
            ],
            resize_keyboard=True
        )
        await message.answer("🔑 Суперадмин-меню:", reply_markup=keyboard)
        return

    if uid not in archive_flow and uid not in admin_settings_step:
        if text == "⚙️ Настройка кодов":
            admin_settings_step[uid] = "wait_new_reset_1"
            await message.answer("⚙️ Настройка. Введите НОВЫЙ код удаления №1:", reply_markup=types.ReplyKeyboardRemove())
            return
            
        elif text == "📦 Архивация":
            archive_flow[uid] = "step2"
            r_code1 = await get_setting("reset_code")
            await message.answer(f"⚠️ Для архивации введите код №1 ({r_code1}):", reply_markup=types.ReplyKeyboardRemove())
            return

    if uid in admin_settings_step:
        step = admin_settings_step[uid]
        if step == "wait_new_reset_1":
            await set_setting("reset_code", text)
            admin_settings_step[uid] = "wait_new_reset_2"
            await message.answer(f"✅ Код №1: `{text}`\nВведите код удаления №2:")
        elif step == "wait_new_reset_2":
            await set_setting("reset_code_2", text)
            admin_settings_step[uid] = "wait_new_add"
            await message.answer(f"✅ Код №2: `{text}`\nВведите код ЗАПИСИ:")
        elif step == "wait_new_add":
            await set_setting("add_code", text)
            del admin_settings_step[uid]
            await message.answer("✅ Все коды обновлены!", reply_markup=types.ReplyKeyboardRemove())
        return

    if uid in archive_flow:
        r_code1 = await get_setting("reset_code")
        r_code2 = await get_setting("reset_code_2")
        
        if archive_flow[uid] == "step2":
            if text == r_code1:
                archive_flow[uid] = "step3"
                await message.answer("⚠️ Шаг 2/3: Введите код №2:")
            else:
                del archive_flow[uid]
                await message.answer("❌ Неверный код №1")
        elif archive_flow[uid] == "step3":
            if text == r_code2:
                archive_flow[uid] = "step4"
                await message.answer("⚠️ Шаг 3/3: Введите СУПЕРАДМИН-КОД:")
            else:
                del archive_flow[uid]
                await message.answer("❌ Неверный код №2")
        elif archive_flow[uid] == "step4":
            if text == SUPER_ADMIN_CODE:
                await archive_and_reset_db()
                await message.answer("📦 База заархивирована! Создана новая БД.", reply_markup=types.ReplyKeyboardRemove())
            else:
                await message.answer("❌ Неверный суперадмин-код")
            del archive_flow[uid]
        return

    a_code = await get_setting("add_code")
    if text == a_code:
        if uid in active_admins:
            active_admins[uid] = time.time() + 86400
            status = "включена" if admin_recording.get(uid, True) else "выключена"
            
            if admin_recording.get(uid, True):
                user_info = message.from_user
                name = user_info.full_name or user_info.username or "Пользователь"
                await pin_recording_status(uid, name)
            
            await message.answer(
                f"🔓 Доступ продлен на 24 часа!\n"
                f"📝 Статус вашей записи: {status}\n"
                f"Чтобы включить/выключить запись, используйте:\n"
                f"/record_on - включить запись\n"
                f"/record_off - выключить запись"
            )
        else:
            active_admins[uid] = time.time() + 86400
            admin_recording[uid] = True
            
            user_info = message.from_user
            name = user_info.full_name or user_info.username or "Пользователь"
            await pin_recording_status(uid, name)
            
            await message.answer(
                "🔓 Доступ на 24 часа открыт!\n\n"
                "📝 Управление записью:\n"
                "/record_on - включить запись (сейчас включена)\n"
                "/record_off - выключить запись\n"
                "/status - проверить статус"
            )

# --- ИНЛАЙН ПОИСК ---
@dp.inline_query()
async def inline_search(query: types.InlineQuery):
    raw_q = query.query.strip()
    
    if len(raw_q) < 2:
        return await query.answer([], cache_time=300)

    if raw_q.startswith('#'):
        rows = await get_songs_by_hashtag(raw_q)
        songs_data = [(file_id, desc) for file_id, desc in rows]
    else:
        db = await get_db()
        words = raw_q.lower().split()
        conditions = []
        params = []
        for word in words:
            conditions.append("search_content LIKE ?")
            params.append(f"%{word}%")
        
        sql = f"SELECT file_id, description FROM songs WHERE {' OR '.join(conditions)} LIMIT 50"
        
        async with db.execute(sql, params) as cursor:
            songs_data = await cursor.fetchall()
    
    seen = set()
    unique_songs = []
    for file_id, desc in songs_data:
        if file_id not in seen:
            seen.add(file_id)
            unique_songs.append((file_id, desc))
    
    results = []
    for i, (file_id, desc) in enumerate(unique_songs[:40]):
        try:
            current_file_id = await get_current_file_id(file_id)
            
            results.append(
                InlineQueryResultCachedAudio(
                    id=str(i),
                    audio_file_id=current_file_id,
                    caption=desc,
                    parse_mode=ParseMode.HTML
                )
            )
        except Exception as e:
            print(f"Ошибка при создании результата: {e}")
            continue
    
    await query.answer(results, cache_time=300, is_personal=False)

# --- ОБРАБОТКА АУДИО ---
@dp.message(F.audio)
async def handle_audio(message: types.Message):
    uid = message.from_user.id
    
    if uid not in active_admins or time.time() > active_admins[uid]:
        await message.reply("⛔ Введите код доступа для записи.")
        return
    
    if not admin_recording.get(uid, True):
        await message.reply("⛔ У вас выключена запись. Включите командой /record_on")
        return
    
    success, total_count, last_song = await save_song(message.audio, message)
    
    if success:
        user_count = await get_user_song_count(uid)
        
        # Получаем данные из базы
        db = await get_db()
        async with db.execute(
            "SELECT title, performer FROM songs WHERE file_unique_id = ?", 
            (message.audio.file_unique_id,)
        ) as cursor:
            song = await cursor.fetchone()
            if song:
                song_title = song[0] or "Без названия"
                song_performer = song[1] or "Неизвестный исполнитель"
            else:
                song_title = "Без названия"
                song_performer = "Неизвестный исполнитель"
        
        reply_text = f"✅ <b>Сохранено!</b>\n\n"
        reply_text += f"🎵 <b>Трек:</b> {song_title}\n"
        reply_text += f"👤 <b>Исполнитель:</b> {song_performer}\n\n"
        reply_text += f"📊 <b>Статистика:</b>\n"
        reply_text += f"   • Вы добавили: <b>{user_count}</b> треков\n"
        reply_text += f"   • Всего в базе: <b>{total_count}</b> треков\n"
        
        await message.reply(reply_text, parse_mode=ParseMode.HTML)
    else:
        await message.reply("❌ Ошибка при сохранении")

# --- ОБРАБОТКА ПОСТОВ В КАНАЛЕ ---
@dp.channel_post(F.audio)
async def handle_channel_post(message: types.Message):
    """Обрабатывает новые посты в канале"""
    print(f"📢 Новый пост в канале")
    await save_song(message.audio, message)

@dp.edited_channel_post(F.audio)
async def handle_channel_edit(message: types.Message):
    """Обрабатывает редактирование постов в канале"""
    try:
        file_unique_id = message.audio.file_unique_id
        new_description = message.html_text
        
        print(f"🔄 Пост отредактирован в канале, unique_id: {file_unique_id}")
        
        db = await get_db()
        
        # Ищем все версии этой песни по уникальному ID
        async with db.execute(
            "SELECT id, file_id FROM songs WHERE file_unique_id = ?",
            (file_unique_id,)
        ) as cursor:
            songs = await cursor.fetchall()
        
        if songs:
            for song_id, file_id in songs:
                # Обновляем описание
                await db.execute("""
                    UPDATE songs 
                    SET description = ? 
                    WHERE id = ?
                """, (new_description, song_id))
                
                # Пересоздаем search_content
                async with db.execute(
                    "SELECT title, performer FROM songs WHERE id = ?",
                    (song_id,)
                ) as cursor2:
                    row = await cursor2.fetchone()
                    if row:
                        title, performer = row
                        
                        raw_text = f"{title} {performer} {new_description}".lower().replace('ё', 'е')
                        clean_search = re.sub(r'[^а-яa-z0-9\s]', '', raw_text)
                        
                        await db.execute("""
                            UPDATE songs 
                            SET search_content = ? 
                            WHERE id = ?
                        """, (clean_search, song_id))
                
                print(f"  ✅ Обновлена песня ID {song_id}")
            
            await db.commit()
            
            # Уведомление админу (опционально)
            try:
                await bot.send_message(
                    ADMIN_CHAT_ID,
                    f"🔄 Обновлено описание для песни\n"
                    f"{new_description[:200]}..."
                )
            except:
                pass
        else:
            print(f"  ⚠️ Песня с unique_id {file_unique_id} не найдена в базе")
        
    except Exception as e:
        print(f"❌ Ошибка при обработке редактирования: {e}")

# --- ОБРАБОТКА ОШИБОК ---
@dp.errors()
async def errors_handler(event: types.ErrorEvent):
    print(f"❌ Ошибка: {event.exception}")
    return True

# --- ПРОВЕРКА МИГРАЦИИ ХЕШТЕГОВ ---
async def check_and_migrate_hashtags():
    try:
        db = await get_db()
        
        async with db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='hashtags'") as cursor:
            if not await cursor.fetchone():
                print("📋 Таблица hashtags не существует, миграция не требуется")
                return
        
        async with db.execute("""
            SELECT COUNT(*) FROM songs s
            LEFT JOIN song_hashtags sh ON s.id = sh.song_id
            WHERE sh.song_id IS NULL
        """) as cursor:
            result = await cursor.fetchone()
            missing = result[0] if result else 0
        
        if missing > 0:
            print(f"⚠️ Найдено {missing} песен без хештегов. Запусти migrate_hashtags.py для миграции")
        else:
            print(f"✅ Все {await get_song_count()} песен имеют хештеги")
            
    except Exception as e:
        print(f"❌ Ошибка при проверке хештегов: {e}")

# --- ЗАПУСК ---
async def main():
    await init_db()
    await check_and_migrate_hashtags()
    print("🤖 Бот запущен...")
    total = await get_song_count()
    print(f"📊 Всего песен в базе: {total}")
    try:
        await dp.start_polling(bot)
    finally:
        if db_conn:
            await db_conn.close()

if __name__ == "__main__":
    asyncio.run(main())