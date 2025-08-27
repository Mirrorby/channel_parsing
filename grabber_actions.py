import os, datetime as dt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telethon import TelegramClient
from telethon.sessions import StringSession

# ==== Настройки каналов и фильтров ====
CHANNELS = [
  # впиши сюда каналы для чтения (как пользователь, ты должен в них состоять):
  # варианты: "@channel_username", "https://t.me/some_public_channel", или ID (например: -1001234567890)
  "@MELOCHOV",
  "@ABKS07",
  "@jjsbossj",
  "@toolsSADA"
]

INCLUDE = []  # например: ["скидка", "акция"] — если оставить пустым, ловим всё
EXCLUDE = []  # например: ["вакансия", "резюме"] — что исключать

# ==== Переменные окружения (из Secrets) ====
API_ID       = int(os.environ["API_ID"])
API_HASH     = os.environ["API_HASH"]
STRING_SESS  = os.environ["STRING_SESSION"]
GSHEET_TITLE = os.environ.get("GSHEET_TITLE", "Телеграм посты поставщиков")
SHEET_NAME   = os.environ.get("SHEET_NAME", "Posts")
GCP_JSON     = os.environ.get("GCP_JSON_PATH", "gcp_sa.json")

# ==== Google Sheets ====
def gs_sheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GCP_JSON, scope)
    gc = gspread.authorize(creds)
    sh = gc.open(GSHEET_TITLE)
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(SHEET_NAME, rows=2000, cols=9)
        ws.append_row(["Date","ChannelTitle","ChannelID","Username","MessageID","Link","MediaType","Text","LastIdKey"])
    return ws

def read_last_id(ws, chat_id):
    # ищем последнюю строку по ключу chat_id в 9-й колонке (LastIdKey)
    key = f"{chat_id}"
    cells = ws.findall(key, in_column=9)
    if cells:
        row = cells[-1].row
        mid = ws.cell(row, 5).value  # MessageID — 5-й столбец
        try:
            return int(mid)
        except:
            return 0
    return 0

def write_rows(ws, rows):
    if rows:
        ws.append_rows(rows, value_input_option="RAW")

def media_type(msg):
    if msg.photo: return "photo"
    if msg.video: return "video"
    if msg.document: return "document"
    if msg.audio: return "audio"
    if msg.voice: return "voice"
    return "text"

def link(username, mid):
    u = (username or "").lstrip("@")
    return f"https://t.me/{u}/{mid}" if u else ""

def passes_filters(text: str) -> bool:
    s = (text or "").lower()
    if INCLUDE and not any(k.lower() in s for k in INCLUDE):
        return False
    if EXCLUDE and any(k.lower() in s for k in EXCLUDE):
        return False
    return True

async def run():
    ws = gs_sheet()
    async with TelegramClient(StringSession(STRING_SESS), API_ID, API_HASH) as client:
        for ref in CHANNELS:
            chat = await client.get_entity(ref)
            last_id = read_last_id(ws, chat.id)
            new_rows = []
            async for msg in client.iter_messages(chat, min_id=last_id, reverse=True):
                text = msg.message or msg.caption or ""
                if not passes_filters(text):
                    continue
                row = [
                    dt.datetime.fromtimestamp(msg.date.timestamp()).strftime("%Y-%m-%d %H:%M:%S"),
                    getattr(chat,'title','') or getattr(chat,'username','') or str(chat.id),
                    chat.id,
                    f"@{getattr(chat,'username','')}" if getattr(chat,'username','') else "",
                    msg.id,
                    link(getattr(chat,'username',''), msg.id),
                    media_type(msg),
                    text,
                    f"{chat.id}"  # LastIdKey — для определения last_id
                ]
                new_rows.append(row)
            write_rows(ws, new_rows)

if __name__ == "__main__":
    import asyncio
    asyncio.run(run())
