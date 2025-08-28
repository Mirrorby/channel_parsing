import os, sys, traceback, datetime as dt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telethon import TelegramClient
from telethon.sessions import StringSession

# ============= Каналы и фильтры =============
CHANNELS = [
    # РЕКОМЕНДУЕМАЯ форма: @username или числовой id (-100...)
    # Можно оставить t.me/..., мы их нормализуем
    "@MELOCHOV",
    "@ABK507",
    "@jjsbossj",
    "@toolsSADA"
]
INCLUDE = []  # например: ["скидка","акция"]; пусто = не ограничиваем
EXCLUDE = []  # например: ["вакансия","резюме"]

# ============= Переменные окружения (Secrets) =============
def _env_required(name: str) -> str:
    v = os.environ.get(name, "")
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"ENV '{name}' is empty or missing")
    return str(v).strip()

def _env_optional(name: str, default: str) -> str:
    v = os.environ.get(name, default)
    return str(v).strip()

try:
    API_ID       = int(_env_required("API_ID"))
    API_HASH     = _env_required("API_HASH")
except Exception:
    print("[fatal] API_ID/API_HASH not set or invalid. Make sure Secrets API_ID (digits) and API_HASH (32 chars) exist.")
    raise

RAW_SESSION = _env_required("STRING_SESSION")
STRING_SESS = RAW_SESSION.strip()
print(f"[diag] STRING_SESSION length = {len(STRING_SESS)}")

GSHEET_TITLE = _env_optional("GSHEET_TITLE", "Telegram Posts Inbox")
SHEET_NAME   = _env_optional("SHEET_NAME", "Posts")
GCP_JSON     = _env_optional("GCP_JSON_PATH", "gcp_sa.json")

# ============= Google Sheets helpers =============
def gs_sheet():
    print(f"[diag] opening spreadsheet GSHEET_TITLE='{GSHEET_TITLE}', SHEET_NAME='{SHEET_NAME}'")
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
    key = f"{chat_id}"
    cells = ws.findall(key, in_column=9)  # LastIdKey
    if cells:
        row = cells[-1].row
        mid = ws.cell(row, 5).value  # MessageID
        try:
            return int(mid)
        except:
            return 0
    return 0

def write_rows(ws, rows):
    if not rows:
        return
    print(f"[diag] appending rows: {len(rows)}")
    ws.append_rows(rows, value_input_option="RAW")

# ============= Utils =============
def media_type(msg):
    if getattr(msg, "photo", None): return "photo"
    if getattr(msg, "video", None): return "video"
    if getattr(msg, "document", None): return "document"
    if getattr(msg, "audio", None): return "audio"
    if getattr(msg, "voice", None): return "voice"
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

def _normalize(ref: str) -> str:
    ref = str(ref).strip()
    if ref.startswith("https://t.me/"):
        ref = ref.split("/")[-1]
    if not (ref.startswith("@") or ref.startswith("-")):
        ref = "@" + ref
    return ref

# ============= Main run =============
async def run():
    try:
        ws = gs_sheet()

        # Подключаемся к Telegram по готовой сессии и сразу проверяем авторизацию
        async with TelegramClient(StringSession(STRING_SESS), API_ID, API_HASH) as client:
            me = await client.get_me()
            print(f"[diag] authorized as id={me.id}, username={getattr(me,'username','')}, phone={getattr(me,'phone','')}")

            for ref in CHANNELS:
                ref_norm = _normalize(ref)
                try:
                    chat = await client.get_entity(ref_norm)
                    print(f"[diag] processing chat: {getattr(chat,'title','') or getattr(chat,'username','') or chat.id} ({chat.id})")
                except Exception as e:
                    print(f"[warn] cannot resolve '{ref}' -> '{ref_norm}': {e}")
                    continue

                last_id = read_last_id(ws, chat.id)
                print(f"[diag] last_id for chat {chat.id}: {last_id}")

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
                        f"{chat.id}"  # LastIdKey
                    ]
                    new_rows.append(row)

                write_rows(ws, new_rows)

    except Exception as e:
        print("[fatal] Unhandled exception in run():", type(e).__name__, str(e))
        traceback.print_exc()
        # Принудительно non-zero код, чтобы Action подсветил ошибку
        sys.exit(1)

if __name__ == "__main__":
    import asyncio
    asyncio.run(run())
