import logging
import random
import asyncio
import aiohttp
import gspread
import base64
import json
from oauth2client.service_account import ServiceAccountCredentials
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
import os
from datetime import datetime

TOKEN = os.getenv("TOKEN")  # Токен из переменных окружения
CREDENTIALS_BASE64 = os.getenv("CREDENTIALS_BASE64")  # Закодированные учетные данные
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")  # URL хоста из переменных окружения
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")  # ID Google таблицы
SHEET_ID_1 = os.getenv("SHEET_ID_1")  # ID листа в таблице с вопросами
USER_SHEET = os.getenv("USER_SHEET")  # ID листа для учёта пользователей
ADMINS_SHEET = os.getenv("ADMINS_SHEET")  # ID листа со списком администраторов
WEBHOOK_PATH = f"/webhook/{TOKEN}"
WEBHOOK_URL = WEBHOOK_HOST + WEBHOOK_PATH
PING_URL = WEBHOOK_HOST

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher()

user_data = {}
admin_ids = []  # Список ID администраторов

# 🔹 Подключение к Google Sheets
def get_questions_from_google_sheets() -> list[str]:
    """
    Получает вопросы из Google Sheets используя учетные данные из переменных окружения.
    
    Returns:
        list[str]: Список вопросов из таблицы
    """
    if not CREDENTIALS_BASE64:
        raise ValueError("Переменная окружения CREDENTIALS_BASE64 не установлена")
    
    if not SPREADSHEET_ID:
        raise ValueError("Переменная окружения SPREADSHEET_ID не установлена")
    
    # Декодируем учетные данные из base64
    credentials_json = base64.b64decode(CREDENTIALS_BASE64).decode()
    credentials_dict = json.loads(credentials_json)
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    sheet = spreadsheet.get_worksheet_by_id(int(SHEET_ID_1))  # Получаем лист по его ID
    questions = sheet.col_values(1)  # Загружаем все значения из первого столбца
    return questions

def get_google_sheets_client():
    """
    Создает и возвращает клиент для работы с Google Sheets.
    
    Returns:
        gspread.Client: Авторизованный клиент для работы с Google Sheets
    """
    if not CREDENTIALS_BASE64:
        raise ValueError("Переменная окружения CREDENTIALS_BASE64 не установлена")
    
    # Декодируем учетные данные из base64
    credentials_json = base64.b64decode(CREDENTIALS_BASE64).decode()
    credentials_dict = json.loads(credentials_json)
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    return gspread.authorize(creds)

def load_admin_ids():
    """
    Загружает список ID администраторов из Google Sheets.
    
    Returns:
        list[str]: Список ID администраторов
    """
    if not SPREADSHEET_ID or not ADMINS_SHEET:
        logging.warning("Невозможно загрузить список администраторов: не установлены SPREADSHEET_ID или ADMINS_SHEET")
        return []
    
    try:
        client = get_google_sheets_client()
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        admins_sheet = spreadsheet.get_worksheet_by_id(int(ADMINS_SHEET))
        
        # Получаем все значения из первого столбца
        admin_ids_list = admins_sheet.col_values(1)
        
        # Убираем заголовок, если он есть
        if admin_ids_list and not admin_ids_list[0].isdigit():
            admin_ids_list = admin_ids_list[1:]
            
        return admin_ids_list
    except Exception as e:
        logging.error(f"Ошибка при загрузке списка администраторов: {e}")
        return []

def is_admin(user_id: int) -> bool:
    """
    Проверяет, является ли пользователь администратором.
    
    Args:
        user_id (int): ID пользователя
    
    Returns:
        bool: True, если пользователь администратор, иначе False
    """
    return str(user_id) in admin_ids

async def log_user_activity(user: types.User, action: str = "start"):
    """
    Записывает информацию о пользователе в Google Sheets.
    
    Args:
        user (types.User): Пользователь Telegram
        action (str): Действие пользователя (по умолчанию "start")
    """
    if not SPREADSHEET_ID or not USER_SHEET:
        logging.warning("Невозможно записать данные пользователя: не установлены SPREADSHEET_ID или USER_SHEET")
        return
    
    # Запускаем запись в таблицу в отдельной задаче, чтобы не блокировать основной поток
    asyncio.create_task(_log_user_to_sheets(user, action))

async def _log_user_to_sheets(user: types.User, action: str):
    """
    Внутренняя функция для записи информации о пользователе в Google Sheets.
    Выполняется в отдельной задаче.
    """
    try:
        # Выполняем блокирующие операции в отдельном потоке через run_in_executor
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: _update_user_sheet(user, action))
    except Exception as e:
        logging.error(f"Ошибка при записи данных пользователя в фоновом режиме: {e}")

def _update_user_sheet(user: types.User, action: str):
    """
    Синхронная функция для обновления данных пользователя в Google Sheets.
    Запускается через run_in_executor.
    """
    try:
        client = get_google_sheets_client()
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        user_sheet = spreadsheet.get_worksheet_by_id(int(USER_SHEET))
        
        # Проверяем, есть ли уже пользователь в таблице
        user_id = str(user.id)
        cell = user_sheet.find(user_id, in_column=1)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Формируем полное имя пользователя
        first_name = user.first_name or ""
        last_name = user.last_name or ""
        full_name = f"{first_name} {last_name}".strip()
        
        username = f"@{user.username}" if user.username else "Нет username"
        user_link = f"https://t.me/{user.username}" if user.username else f"tg://user?id={user.id}"
        
        if cell:
            # Если пользователь найден, обновляем счетчик запусков и имя
            row = cell.row
            count_cell = user_sheet.cell(row, 5)  # Теперь счетчик в 5-й колонке
            count = int(count_cell.value) if count_cell.value and count_cell.value.isdigit() else 0
            
            # Обновляем имя пользователя на случай, если оно изменилось
            user_sheet.update_cell(row, 2, full_name)
            
            if action == "start":
                count += 1
                user_sheet.update_cell(row, 5, count)  # Обновляем счетчик в 5-й колонке
        else:
            # Если пользователь не найден, добавляем новую запись
            user_sheet.append_row([
                user_id,           # 1. User ID
                full_name,         # 2. Полное имя
                user_link,         # 3. Ссылка на пользователя
                current_time,      # 4. Время первого запуска
                1 if action == "start" else 0  # 5. Счетчик запусков
            ])
    except Exception as e:
        logging.error(f"Ошибка при записи данных пользователя: {e}")

# Загружаем вопросы из таблицы
QUESTIONS_POOL = get_questions_from_google_sheets()

# Загружаем список администраторов
admin_ids = load_admin_ids()

start_keyboard = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Начать игру", callback_data="start_game")]
])

ask_question_keyboard = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Задать вопрос", callback_data="ask_question")]
])

next_question_keyboard = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Следующий вопрос", callback_data="ask_question")]
])

logging.basicConfig(level=logging.INFO)

@dp.message(Command("start"))
async def start_command(message: types.Message):
    """Обработчик команды /start. Сбрасывает состояние игры и предлагает начать заново."""
    user_data[message.chat.id] = {
        "players": [],
        "questions": [],
        "question_counts": {},
        "waiting_for_players": False
    }
    
    # Логируем активность пользователя
    await log_user_activity(message.from_user, "start")
    
    await message.answer("Намаскар! Я бот для игры «Ананда Марга — это я!»\n\nЧтобы играть нажми <b>Начать игру</b> 🙂\n\nКак играть, правила — /rules\nПодробнее об игре— /about\nПолучить карточки с вопросами для печати — /cards\nПоблагодарить автора игры — /donate", parse_mode="HTML", reply_markup=start_keyboard)

@dp.callback_query(lambda c: c.data == "start_game")
async def start_game(callback_query: types.CallbackQuery):
    """После нажатия 'Начать игру' бот просит ввести список участников."""
    chat_id = callback_query.message.chat.id
    user_data[chat_id]["waiting_for_players"] = True
    await callback_query.message.answer("Введите список участников, написав <b>каждое имя с новой строки</b> и отправьте.\n\nНапример:\n\n<i>Викрамадитья\nЮдистхира\nАлок\nДхармагупта</i>", parse_mode="HTML")

@dp.message(lambda message: user_data.get(message.chat.id, {}).get("waiting_for_players", False))
async def process_players(message: types.Message):
    """Обрабатывает ввод списка игроков только если бот в состоянии 'ждёт игроков'."""
    chat_id = message.chat.id
    players = message.text.split("\n")
    players = [p.strip() for p in players if p.strip()]

    if not players:
        await message.answer("Список участников пуст. Введите хотя бы одно имя.")
        return

    user_data[chat_id]["players"] = players
    user_data[chat_id]["questions"] = random.sample(QUESTIONS_POOL, len(QUESTIONS_POOL))
    user_data[chat_id]["question_counts"] = {player: 0 for player in players}
    user_data[chat_id]["waiting_for_players"] = False

    players_text = ", ".join(players)
    await message.answer(
        f"Начинаем! Участники игры: <i>{players_text}</i>\n\nНажмите <b>Задать вопрос</b>.", parse_mode="HTML",
        reply_markup=ask_question_keyboard
    )

@dp.callback_query(lambda c: c.data == "ask_question")
async def ask_question(callback_query: types.CallbackQuery):
    """Выдаёт случайный вопрос игроку."""
    chat_id = callback_query.message.chat.id
    data = user_data.get(chat_id)

    if not data or not data["players"]:
        await callback_query.message.answer("Ошибка: начните игру заново с /start")
        return

    if not data["questions"]:
        await callback_query.message.answer("Поздравляем, вопросы кончились! 😊", reply_markup=start_keyboard)
        return

    min_questions = min(data["question_counts"].values())
    candidates = [player for player, count in data["question_counts"].items() if count == min_questions]
    player = random.choice(candidates)

    data["question_counts"][player] += 1
    question = data["questions"].pop(0)

    await callback_query.message.answer(f"Вопрос для игрока <i>{player}</i>\n\n<b>{question}</b>", parse_mode="HTML", reply_markup=next_question_keyboard)

### 🔹 Команды меню
@dp.message(Command("rules"))
async def show_rules(message: types.Message):
    """Выводит правила игры."""
    await message.answer("📜 <b>Правила игры</b>\n\nВедущий выбирает в меню <b>Новая игра</b>, вводит имена игроков и нажимает кнопку <b>Задать вопрос</b>. Одному из игроков выдаётся первый вопрос. После ответа ведущий нажимает кнопку <b>Следующий вопрос</b>, и зачитывает вопрос для следующего участника игры. Участники могут получать и зачитывать вопросы самостоятельно, поочередно нажимая кнопку <b>Следующий вопрос</b>.\n\n<i><b>Главное условие игры:</b>\nПредоставить отвечающему возможность высказаться, не перебивая его и не дополняя его ответ своим мнением. Это игра — не дискуссия на заданную тему, мы не обсуждаем тему вопроса после или вовремя ответа участника. Мы предоставляем отвечающему возможность раскрыть себя, пространство для выражения своих мыслей и чувств, не давая им оценки.</i>\nВопросы выдаются случайно и в рамках одной игры не повторяются.\n\nВсе вопросы игры так или иначе связаны с АМ, то есть, свой ответ необходимо выстраивать, исходя из отношений с членами сообщества, Гуру, влиянием практик АМ на жизнь отвечающего и т.п.\n\nЕсли выпадает пустая карточка, значит участнику предоставляется возможность либо высказаться на любую тему, либо пропустить ход.\n\nМежду вопросами рекомендуется петь киртан.", parse_mode="HTML")

@dp.message(Command("about"))
async def show_about(message: types.Message):
    """Выводит информацию об игре."""
    await message.answer("ℹ <b>Об игре</b>\n\nИгра ориентирована только на лиц, имеющих инициацию в традиции «Ананда Марги», поскольку использует узкоспециальную терминологию. Задаваемые в ней вопросы могут быть неверно истолкованы лицами, не имеющими посвящения в указанную традицию.\n\n«Ананда Марга — это я!» — это игра, нацеленная на сплочение, объединение маргов, создание атмосферы доверия и единства. С помощью этой игры можно организовать знакомство, наладить взаимодействие, заинтересовать участников сообщества, снять напряженность в коммуникации.\n\nИгра может использоваться в трансформационных занятиях, тренингах и обучающих программах, как при индивидуальной работе, так и в группе. Это уникальный инструмент для сатсанга, создания настроения, возможности отрефлексировать жизнь в традиции, лучше понять свою роль в сообществе, увидеть новые возможности для раскрытия потенциала.", parse_mode="HTML")

@dp.message(Command("cards"))
async def show_cards(message: types.Message):
    """Выводит информацию о карточках (раздел в разработке)."""
    await message.answer("Упс, кажется, мы ещё не нарисовали карточки с вопросами для печати 🙂\n\nЕсли вы хотите помочь с дизайном, свяжитесь с разработчиком @dasarath_bro", parse_mode="HTML")

@dp.message(Command("donate"))
async def show_donate(message: types.Message):
    """Выводит информацию о донатах."""
    await message.answer("🙏 <b>Благодарность</b>\n\nПоблагодарить автора игры можно:\n— Переводом на карту Т-Банк <b>2200700942783597</b>\n— Или по ссылке https://www.tinkoff.ru/rm/r_zPXLAjkOMT.psqSQuKezK/eHvLB70230. \n\nТакже вы можете дать обратную связь или просто послать сердечный намаскар @Jayashrii_jane", parse_mode="HTML")

async def update_questions_cache():
    """
    Обновляет кеш вопросов из Google Sheets.
    
    Returns:
        list[str]: Обновленный список вопросов
    """
    global QUESTIONS_POOL
    try:
        new_questions = get_questions_from_google_sheets()
        QUESTIONS_POOL = new_questions
        return new_questions
    except Exception as e:
        logging.error(f"Ошибка при обновлении кеша вопросов: {e}")
        return QUESTIONS_POOL

@dp.message(Command("update"))
async def update_questions_command(message: types.Message):
    """
    Обработчик команды /update. Обновляет кеш вопросов из Google Sheets.
    Доступно только администраторам.
    """
    user_id = message.from_user.id
    
    # Сначала обновляем список администраторов, чтобы использовать актуальные данные
    global admin_ids
    admin_ids = load_admin_ids()
    
    if not is_admin(user_id):
        logging.warning(f"Попытка доступа к команде /update от неавторизованного пользователя: {user_id}")
        # Не отвечаем, чтобы скрыть команду
        return
    
    try:
        # Отправляем сообщение о начале обновления
        status_message = await message.answer("🔄 Обновление списка вопросов...")
        
        # Обновляем кеш вопросов
        questions = await update_questions_cache()
        
        # Отправляем сообщение об успешном обновлении
        await status_message.edit_text(f"✅ Список вопросов обновлен. Загружено {len(questions)} вопросов.")
        
    except Exception as e:
        logging.error(f"Ошибка при выполнении команды /update: {e}")
        await message.answer(f"❌ Ошибка при обновлении: {str(e)}")

async def on_startup(bot: Bot):
    """Запуск вебхука при старте."""
    await bot.set_webhook(WEBHOOK_URL)

async def keep_awake():
    """Периодический запрос к серверу, чтобы не засыпал (актуально для Render)."""
    while True:
        await asyncio.sleep(600)
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(PING_URL) as response:
                    logging.info(f"Ping response: {response.status}")
            except Exception as e:
                logging.error(f"Ping error: {e}")

async def main():
    """Основная функция запуска бота с вебхуком."""
    await bot.set_webhook(WEBHOOK_URL)
    app = web.Application()
    webhook_requests_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_requests_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 10000)
    await site.start()

    asyncio.create_task(keep_awake())
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
