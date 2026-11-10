import os
import io
import asyncio
import logging
from datetime import date, timedelta
from typing import Optional
from enum import IntEnum

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Query, Request, Response
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import uvicorn

from aiogram import Bot, Dispatcher, types, F
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import CommandStart, Command, BaseFilter
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import BufferedInputFile, BotCommand, BotCommandScopeDefault
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from PIL import Image, ImageDraw, ImageFont

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_HOST = os.getenv("DB_HOST", "postgres_db")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "cloudfarms_db")
DB_USER = os.getenv("DB_USER", "cf_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "cf_secure_password_123")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip()

if WEBAPP_URL and not WEBAPP_URL.startswith("https://"):
    logging.warning("⚠️ УВАГА: WEBAPP_URL має починатися з https://, інакше Telegram не відкриє Mini App!")

FARMS = {
    1: {"name": "Ферма №1 (с. Настасів)", "db_id": 12788, "has_br": True},
    7: {"name": "Ферма №7 (с. Старе місто)", "db_id": 12790, "has_br": False},
    14: {"name": "Ферма №14 (с. Зарожани)", "db_id": 12786, "has_br": False}
}

class FarmEnum(IntEnum):
    f1 = 1
    f7 = 7
    f14 = 14

class BroadcastStates(StatesGroup):
    waiting_for_message = State()

def get_db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        cursor_factory=RealDictCursor
    )

def ensure_bot_user_table():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_user (
                    telegram_id BIGINT PRIMARY KEY,
                    username VARCHAR(255),
                    first_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255),
                    language_code VARCHAR(10),
                    is_premium BOOLEAN NOT NULL DEFAULT FALSE,
                    is_bot BOOLEAN NOT NULL DEFAULT FALSE,
                    phone_number VARCHAR(32),
                    latitude NUMERIC(10, 7),
                    longitude NUMERIC(10, 7),
                    start_payload VARCHAR(255),
                    entry_point VARCHAR(50),
                    role VARCHAR(50) NOT NULL DEFAULT 'viewer',
                    assigned_farm_id INTEGER,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    is_bot_blocked BOOLEAN NOT NULL DEFAULT FALSE,
                    interactions_count INTEGER NOT NULL DEFAULT 0,
                    reports_generated_count INTEGER NOT NULL DEFAULT 0,
                    last_action VARCHAR(100),
                    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    client_ip INET,
                    client_platform VARCHAR(50),
                    user_agent TEXT,
                    telegram_app_version VARCHAR(50)
                );
                CREATE INDEX IF NOT EXISTS idx_bot_user_username
                    ON bot_user(username);
                CREATE INDEX IF NOT EXISTS idx_bot_user_last_seen
                    ON bot_user(last_seen_at);
                CREATE INDEX IF NOT EXISTS idx_bot_user_farm
                    ON bot_user(assigned_farm_id);
                CREATE INDEX IF NOT EXISTS idx_death_report_lookup
                    ON death(farm_id, location_id, death_date);
                CREATE INDEX IF NOT EXISTS idx_farrowing_report_lookup
                    ON farrowing(farm_id, farrowing_date);
            """)
        conn.commit()
    except Exception:
        conn.rollback()
        logging.exception("Не вдалося створити таблицю bot_user")
        raise
    finally:
        conn.close()

def save_or_update_user(
    user: types.User,
    action: str,
    start_payload: Optional[str] = None,
    assigned_farm_id: Optional[int] = None,
):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bot_user (
                    telegram_id, username, first_name, last_name,
                    language_code, is_premium, is_bot, start_payload,
                    entry_point, assigned_farm_id, interactions_count,
                    last_action, last_seen_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, NOW(), NOW())
                ON CONFLICT (telegram_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    language_code = EXCLUDED.language_code,
                    is_premium = EXCLUDED.is_premium,
                    is_bot = EXCLUDED.is_bot,
                    is_bot_blocked = FALSE,
                    assigned_farm_id = COALESCE(EXCLUDED.assigned_farm_id, bot_user.assigned_farm_id),
                    interactions_count = bot_user.interactions_count + 1,
                    reports_generated_count = bot_user.reports_generated_count
                        + CASE WHEN EXCLUDED.last_action IN ('location', 'refresh_report')
                               THEN 1 ELSE 0 END,
                    last_action = EXCLUDED.last_action,
                    last_seen_at = NOW(),
                    updated_at = NOW()
            """, (
                user.id,
                user.username,
                user.first_name or "",
                user.last_name,
                user.language_code,
                bool(user.is_premium),
                user.is_bot,
                start_payload,
                "bot",
                assigned_farm_id,
                action,
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        logging.exception("Не вдалося зберегти користувача Telegram %s", user.id)
        raise
    finally:
        conn.close()

# --- Робота з БД для адмін-панелі ---
def is_user_admin(telegram_id: int) -> bool:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT role FROM bot_user WHERE telegram_id = %s;", (telegram_id,))
            row = cur.fetchone()
            return bool(row and row.get("role") == "admin")
    finally:
        conn.close()

def get_admin_metrics() -> dict:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) AS total_users,
                    COUNT(*) FILTER (WHERE role = 'admin') AS admins_count,
                    COUNT(*) FILTER (WHERE is_active IS TRUE AND is_bot_blocked IS FALSE) AS active_recipients,
                    COUNT(*) FILTER (WHERE last_seen_at >= NOW() - INTERVAL '24 HOURS') AS active_today,
                    COALESCE(SUM(reports_generated_count), 0) AS total_reports,
                    COALESCE(SUM(interactions_count), 0) AS total_interactions
                FROM bot_user;
            """)
            return dict(cur.fetchone() or {})
    finally:
        conn.close()

def get_recent_users(limit: int = 10, offset: int = 0) -> list[dict]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    telegram_id, username, first_name, last_name,
                    role, is_active, is_bot_blocked, reports_generated_count, last_seen_at
                FROM bot_user
                ORDER BY last_seen_at DESC
                LIMIT %s OFFSET %s;
            """, (limit, offset))
            return cur.fetchall()
    finally:
        conn.close()

def get_user_by_id(telegram_id: int) -> Optional[dict]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM bot_user WHERE telegram_id = %s;", (telegram_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()

def update_user_role(telegram_id: int, new_role: str):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE bot_user 
                SET role = %s, updated_at = NOW() 
                WHERE telegram_id = %s;
            """, (new_role, telegram_id))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def mark_bot_blocked(telegram_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE bot_user 
                SET is_bot_blocked = TRUE, updated_at = NOW() 
                WHERE telegram_id = %s;
            """, (telegram_id,))
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

def get_broadcast_recipients(farm_id: Optional[int] = None) -> list[int]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            if farm_id is not None:
                cur.execute("""
                    SELECT telegram_id 
                    FROM bot_user 
                    WHERE is_active IS TRUE 
                      AND is_bot_blocked IS FALSE 
                      AND assigned_farm_id = %s;
                """, (farm_id,))
            else:
                cur.execute("""
                    SELECT telegram_id 
                    FROM bot_user 
                    WHERE is_active IS TRUE 
                      AND is_bot_blocked IS FALSE;
                """)
            return [row["telegram_id"] for row in cur.fetchall()]
    finally:
        conn.close()

class AdminOnlyFilter(BaseFilter):
    async def __call__(self, event: types.TelegramObject) -> bool:
        user = getattr(event, "from_user", None)
        if not user:
            return False
        return await asyncio.to_thread(is_user_admin, user.id)

# --- Логіка розрахунків ферм ---
def get_farrowing_locations(farm_num: int):
    farm_info = FARMS.get(farm_num)
    if not farm_info:
        return []
    
    conn = get_db()
    cur = conn.cursor()
    query = """
        SELECT id, name, locationnumber
        FROM location
        WHERE farm_id = %s
          AND (forbidfarrowing IS FALSE OR forbidfarrowing IS NULL)
          AND inhabitanttype_code = 'SOW'
          AND locationtype_id IN ('ST', 'HO', 'SE')
        ORDER BY name ASC;
    """
    cur.execute(query, (farm_info["db_id"],))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_death_totals(
    cur,
    farm_id: int,
    location_ids: tuple[int, ...],
    start_date: date,
):
    placeholders = ", ".join(["%s"] * len(location_ids))
    cur.execute(
        f"""
            SELECT
                location_id,
                (sow_id IS NULL) AS is_piglet,
                death_date::date AS death_day,
                SUM(amount) AS amount_sum,
                COUNT(id) AS row_count
            FROM death
            WHERE farm_id = %s
              AND location_id IN ({placeholders})
              AND death_date >= %s
            GROUP BY location_id, (sow_id IS NULL), death_date::date;
        """,
        (farm_id, *location_ids, start_date),
    )
    return cur.fetchall()

def calculate_batches_by_location(
    farm_num: int, 
    location_id: int, 
    include_babyroom: bool = True, 
    include_sows: bool = True
):
    farm_info = FARMS.get(farm_num)
    if not farm_info:
        return [], "", False
    
    db_farm_id = farm_info["db_id"]
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT id, name, locationnumber FROM location WHERE id = %s AND farm_id = %s;", (location_id, db_farm_id))
    loc_row = cur.fetchone()
    if not loc_row:
        cur.close()
        conn.close()
        return [], "", False

    location_display_name = loc_row["locationnumber"] or loc_row["name"]

    br_loc_id = None
    should_calc_br = farm_info["has_br"] and include_babyroom

    if should_calc_br:
        digits = "".join(filter(str.isdigit, loc_row["locationnumber"] or loc_row["name"]))
        if digits:
            cur.execute("""
                SELECT id FROM location
                WHERE farm_id = %s
                  AND inhabitanttype_code = 'PIGL'
                  AND (locationnumber ILIKE %s OR name ILIKE %s)
                LIMIT 1;
            """, (db_farm_id, f"%BR{digits}%", f"%BR{digits}%"))
            br_row = cur.fetchone()
            if br_row:
                br_loc_id = br_row["id"]

    cutoff_date = date.today() - timedelta(days=365)

    query_farrowings = """
        SELECT 
            f.id,
            f.farrowing_date::date as f_date,
            COALESCE(f.liveborn, 0) as liveborn,
            c.id as cycle_id
        FROM farrowing f
        JOIN cycle c ON f.cycle_id = c.id AND f.farm_id = c.farm_id
        WHERE f.farm_id = %s
          AND (c.farrow_location_id = %s OR f.cycle_id IN (
              SELECT id FROM cycle WHERE farrow_location_id = %s AND farm_id = %s
          ))
          AND f.farrowing_date::date >= %s
        ORDER BY f.farrowing_date ASC;
    """
    cur.execute(query_farrowings, (db_farm_id, location_id, location_id, db_farm_id, cutoff_date))
    farrowings = cur.fetchall()

    if not farrowings:
        cur.close()
        conn.close()
        return [], location_display_name, False

    batches = []
    current_batch = [farrowings[0]]

    for f in farrowings[1:]:
        prev_date = current_batch[-1]["f_date"]
        curr_date = f["f_date"]
        if (curr_date - prev_date).days > 6:
            batches.append(current_batch)
            current_batch = [f]
        else:
            current_batch.append(f)
    if current_batch:
        batches.append(current_batch)

    death_totals = {}
    if should_calc_br and br_loc_id:
        death_location_ids = (location_id, br_loc_id)
    else:
        death_location_ids = (location_id,)

    for death_row in get_death_totals(cur, db_farm_id, death_location_ids, cutoff_date):
        key = (death_row["location_id"], death_row["is_piglet"])
        death_totals.setdefault(key, []).append(death_row)

    results = []
    total_batches = len(batches)
    has_any_sow_deaths = False

    for idx, batch in enumerate(batches):
        start_d = batch[0]["f_date"]
        is_last_batch = (idx + 1 == total_batches)

        if not is_last_batch:
            next_start_d = batches[idx + 1][0]["f_date"]
            end_d = next_start_d - timedelta(days=1)
        else:
            last_farrow_d = batch[-1]["f_date"]
            today = date.today()
            end_d = today if today >= last_farrow_d else last_farrow_d

        days = (end_d - start_d).days + 1
        liveborn_sum = sum(item["liveborn"] for item in batch)
        sows_count = len(batch)
        per_sow = liveborn_sum / sows_count if sows_count > 0 else 0.0

        def count_deaths(location: int, is_piglet: bool) -> int:
            rows = [
                row for row in death_totals.get((location, is_piglet), [])
                if start_d <= row["death_day"] <= end_d
            ]
            amount_sum = sum(
                row["amount_sum"] for row in rows if row["amount_sum"] is not None
            )
            row_count = sum(row["row_count"] for row in rows)
            return int(amount_sum if any(row["amount_sum"] is not None for row in rows) else row_count)

        piglet_deaths = count_deaths(location_id, True)
        piglet_death_pct = (piglet_deaths / liveborn_sum * 100) if liveborn_sum > 0 else 0.0

        sow_deaths = 0
        sow_death_pct = 0.0
        if include_sows:
            sow_deaths = count_deaths(location_id, False)
            sow_death_pct = (sow_deaths / sows_count * 100) if sows_count > 0 else 0.0

            if sow_deaths > 0:
                has_any_sow_deaths = True

        br_deaths = None
        br_pct = None
        if should_calc_br and br_loc_id:
            br_deaths = count_deaths(br_loc_id, True)
            br_pct = (br_deaths / liveborn_sum * 100) if liveborn_sum > 0 else 0.0

        all_piglet_deaths = piglet_deaths + (br_deaths or 0)
        all_piglet_death_pct = (all_piglet_deaths / liveborn_sum * 100) if liveborn_sum > 0 else 0.0

        total_all_deaths = all_piglet_deaths + (sow_deaths if include_sows else 0)
        total_all_pct = (total_all_deaths / liveborn_sum * 100) if liveborn_sum > 0 else 0.0

        results.append({
            "start_date": start_d.strftime("%d.%m.%Y"),
            "end_date": end_d.strftime("%d.%m.%Y"),
            "days": days,
            "liveborn": liveborn_sum,
            "sows": sows_count,
            "per_sow": round(per_sow, 2),
            "piglet_deaths": piglet_deaths,
            "piglet_death_pct": round(piglet_death_pct, 2),
            "sow_deaths": sow_deaths if include_sows else 0,
            "sow_death_pct": round(sow_death_pct, 2) if include_sows else 0.0,
            "br": br_deaths if should_calc_br else None,
            "br_pct": round(br_pct, 2) if (should_calc_br and br_pct is not None) else None,
            "all_piglet_deaths": all_piglet_deaths,
            "all_piglet_death_pct": round(all_piglet_death_pct, 2),
            "total_all_deaths": total_all_deaths,
            "total_all_pct": round(total_all_pct, 2),
            "is_current": is_last_batch
        })

    cur.close()
    conn.close()
    return results, location_display_name, (has_any_sow_deaths and include_sows)

def get_cyrillic_font(size: int = 14):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "C:\\Windows\\Fonts\\arial.ttf",
        "C:\\Windows\\Fonts\\calibri.ttf"
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
    return ImageFont.load_default()

def render_table_image(data: list, show_babyroom: bool, show_sows: bool = False) -> io.BytesIO:
    headers = ["Початок", "Кінець", "Днів", "Живонародж.", "Свиноматок", "Порос./СМ", "Падіж пор.", "% Падежу"]
    
    if show_sows:
        headers.extend(["Падіж СМ", "% СМ"])

    if show_babyroom:
        headers.extend(["Падіж BR", "% BR", "Падіж пор. заг.", "Заг. % пор."])

    if show_sows or show_babyroom:
        headers.extend(["Всього пад.", "Всього %"])

    rows = []
    for r in data:
        row = [
            r["start_date"],
            r["end_date"],
            str(r["days"]),
            str(r["liveborn"]),
            str(r["sows"]),
            f"{r['per_sow']:.2f}",
            str(r["piglet_deaths"]),
            f"{r['piglet_death_pct']:.2f}%"
        ]
        
        if show_sows:
            row.extend([
                str(r["sow_deaths"]),
                f"{r['sow_death_pct']:.2f}%"
            ])

        if show_babyroom:
            row.extend([
                str(r["br"]) if r["br"] is not None else "-",
                f"{r['br_pct']:.2f}%" if r["br_pct"] is not None else "-",
                str(r["all_piglet_deaths"]),
                f"{r['all_piglet_death_pct']:.2f}%"
            ])

        if show_sows or show_babyroom:
            row.extend([
                str(r["total_all_deaths"]),
                f"{r['total_all_pct']:.2f}%"
            ])

        rows.append(row)

    font = get_cyrillic_font(13)
    font_header = get_cyrillic_font(13)
    row_height = 42
    padding_x = 14

    dummy_img = Image.new("RGB", (1, 1))
    dummy_draw = ImageDraw.Draw(dummy_img)

    col_widths = []
    for i, h in enumerate(headers):
        bbox_h = dummy_draw.textbbox((0, 0), h, font=font_header)
        max_w = bbox_h[2] - bbox_h[0]
        for r in rows:
            bbox_v = dummy_draw.textbbox((0, 0), r[i], font=font)
            max_w = max(max_w, bbox_v[2] - bbox_v[0])
        col_widths.append(max_w + padding_x * 2)

    width = sum(col_widths) + 50
    height = (len(rows) + 1) * row_height + 40

    img = Image.new("RGB", (width, height), color=(255, 255, 255))
    draw = ImageDraw.Draw(img)

    start_x = 25
    start_y = 20

    curr_x = start_x
    for i, header in enumerate(headers):
        w = col_widths[i]
        draw.rectangle([curr_x, start_y, curr_x + w, start_y + row_height], fill=(235, 240, 246), outline=(180, 190, 200), width=1)
        bbox = draw.textbbox((0, 0), header, font=font_header)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]
        draw.text((curr_x + (w - tw) / 2, start_y + (row_height - th) / 2 - 1), header, fill=(30, 41, 59), font=font_header)
        curr_x += w

    curr_y = start_y + row_height
    for row_idx, row in enumerate(rows):
        curr_x = start_x
        is_current_batch = data[row_idx].get("is_current", False)

        if is_current_batch:
            row_bg = (224, 242, 254)
            text_color = (3, 105, 161)
        elif row_idx % 2 == 0:
            row_bg = (255, 255, 255)
            text_color = (15, 23, 42)
        else:
            row_bg = (248, 250, 252)
            text_color = (15, 23, 42)

        for i, val in enumerate(row):
            w = col_widths[i]
            draw.rectangle([curr_x, curr_y, curr_x + w, curr_y + row_height], fill=row_bg, outline=(203, 213, 225), width=1)
            bbox = draw.textbbox((0, 0), val, font=font)
            tw = bbox[2] - bbox[0]
            th = bbox[3] - bbox[1]
            draw.text((curr_x + (w - tw) / 2, curr_y + (row_height - th) / 2 - 1), val, fill=text_color, font=font)
            curr_x += w
        curr_y += row_height

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf

# --- REST API ---
app = FastAPI(title="CloudFarms Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_telegram_iframe_headers(request: Request, call_next):
    response: Response = await call_next(request)
    if "x-frame-options" in response.headers:
        del response.headers["x-frame-options"]
    response.headers["Content-Security-Policy"] = "frame-ancestors 'self' https://web.telegram.org https://*.telegram.org https://*.t.me;"
    return response

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/")
def serve_home():
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"status": "ok", "message": f"static/index.html not found at {index_path}"}

@app.get("/api/farms")
def api_get_farms():
    return [{"id": k, "name": v["name"]} for k, v in FARMS.items()]

@app.get("/api/departments")
def api_get_departments(farm_id: FarmEnum = Query(...)):
    return get_farrowing_locations(farm_id.value)

@app.get("/api/report")
def api_report(
    farm_id: FarmEnum = Query(...), 
    location_id: int = Query(...),
    include_babyroom: bool = Query(True),
    include_sows: bool = Query(True)
):
    data, _, _ = calculate_batches_by_location(
        farm_id.value, 
        location_id, 
        include_babyroom=include_babyroom,
        include_sows=include_sows
    )
    return data

# --- Telegram Bot & FSM ---
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

async def setup_bot_commands():
    commands = [
        BotCommand(command="start", description="Головне меню / Вибір ферми"),
        BotCommand(command="report", description="Сформувати новий звіт"),
        BotCommand(command="admin", description="Панель адміністратора")
    ]
    await bot.set_my_commands(commands, scope=BotCommandScopeDefault())

async def setup_database():
    await asyncio.to_thread(ensure_bot_user_table)

def build_farms_keyboard(is_admin: bool = False):
    builder = InlineKeyboardBuilder()
    for f_num, f_data in FARMS.items():
        builder.button(text=f_data["name"], callback_data=f"farm_{f_num}")
    builder.adjust(1)
    
    if WEBAPP_URL and WEBAPP_URL.startswith("https://"):
        builder.row(types.InlineKeyboardButton(
            text="📱 Відкрити Web Mini App",
            web_app=types.WebAppInfo(url=WEBAPP_URL)
        ))
        
    if is_admin:
        builder.row(types.InlineKeyboardButton(
            text="🔐 Панель адміністратора",
            callback_data="admin_home"
        ))
        
    return builder.as_markup()

def build_dept_keyboard(farm_num: int, locations: list):
    builder = InlineKeyboardBuilder()
    for loc in locations:
        btn_text = f"{loc['locationnumber']} ({loc['name']})" if loc['locationnumber'] != loc['name'] else loc['name']
        builder.button(text=btn_text, callback_data=f"loc_{farm_num}_{loc['id']}_1_1")
    builder.adjust(2)
    builder.row(types.InlineKeyboardButton(text="⬅️ До вибору ферми", callback_data="back_farms"))
    return builder.as_markup()

def build_admin_main_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 Список користувачів", callback_data="admin_users_0")
    builder.button(text="📢 Створити розсилку", callback_data="admin_bc_audience")
    builder.button(text="🔄 Оновити дані", callback_data="admin_refresh")
    builder.button(text="⬅️ До вибору ферми", callback_data="back_farms")
    builder.adjust(1)
    return builder.as_markup()

def build_users_list_keyboard(users: list[dict], offset: int = 0, limit: int = 10):
    builder = InlineKeyboardBuilder()
    for u in users:
        role_mark = "👑" if u["role"] == "admin" else "👤"
        uname = f"@{u['username']}" if u.get("username") else str(u["telegram_id"])
        btn_title = f"{role_mark} {u.get('first_name', '')} ({uname})"
        builder.button(text=btn_title, callback_data=f"admin_view_{u['telegram_id']}_{offset}")

    builder.adjust(1)

    nav_buttons = []
    if offset >= limit:
        nav_buttons.append(types.InlineKeyboardButton(text="⬅️ Попередня", callback_data=f"admin_users_{offset - limit}"))
    if len(users) == limit:
        nav_buttons.append(types.InlineKeyboardButton(text="Наступна ➡️", callback_data=f"admin_users_{offset + limit}"))

    if nav_buttons:
        builder.row(*nav_buttons)
    builder.row(types.InlineKeyboardButton(text="⬅️ До адмін-панелі", callback_data="admin_home"))
    return builder.as_markup()

def build_user_card_keyboard(target_id: int, current_role: str, offset: int):
    builder = InlineKeyboardBuilder()
    if current_role == "admin":
        builder.button(text="🔻 Понизити до Viewer", callback_data=f"admin_setrole_{target_id}_viewer_{offset}")
    else:
        builder.button(text="👑 Призначити Admin", callback_data=f"admin_setrole_{target_id}_admin_{offset}")
    builder.button(text="⬅️ Назад до списку", callback_data=f"admin_users_{offset}")
    builder.adjust(1)
    return builder.as_markup()

def build_audience_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🌐 Всім користувачам", callback_data="admin_bc_target_all")
    for f_num, f_data in FARMS.items():
        builder.button(text=f"🏢 Тільки {f_data['name']}", callback_data=f"admin_bc_target_{f_num}")
    builder.button(text="❌ Скасувати", callback_data="admin_home")
    builder.adjust(1)
    return builder.as_markup()

def build_broadcast_confirm_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🚀 Підтвердити та відправити", callback_data="admin_bc_confirm")
    builder.button(text="❌ Скасувати", callback_data="admin_bc_cancel")
    builder.adjust(1)
    return builder.as_markup()

@dp.message(CommandStart())
@dp.message(Command("report"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    start_payload = None
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) == 2:
            start_payload = parts[1][:255]
    await asyncio.to_thread(
        save_or_update_user,
        message.from_user,
        "start" if message.text and message.text.startswith("/start") else "report",
        start_payload,
    )
    is_admin = await asyncio.to_thread(is_user_admin, message.from_user.id)
    await message.answer(
        "👋 **Аналітика опоросів та падежів**\n\nОберіть потрібну ферму для формування звіту:",
        reply_markup=build_farms_keyboard(is_admin=is_admin),
        parse_mode="Markdown"
    )

# --- Хендлери адмін-панелі ---
@dp.message(Command("admin"), AdminOnlyFilter())
async def cmd_admin(message: types.Message, state: FSMContext):
    await state.clear()
    await asyncio.to_thread(save_or_update_user, message.from_user, "admin_panel")
    metrics = await asyncio.to_thread(get_admin_metrics)
    text = (
        "🔐 **Панель адміністратора**\n\n"
        f"👥 **Всього користувачів:** `{metrics.get('total_users', 0)}`\n"
        f"👑 **Адміністраторів:** `{metrics.get('admins_count', 0)}`\n"
        f"📬 **Активних одержувачів:** `{metrics.get('active_recipients', 0)}`\n"
        f"🟢 **Активні за 24г:** `{metrics.get('active_today', 0)}`\n"
        f"📊 **Згенеровано звітів:** `{metrics.get('total_reports', 0)}`\n"
        f"💬 **Всього дій:** `{metrics.get('total_interactions', 0)}`"
    )
    await message.answer(text, reply_markup=build_admin_main_keyboard(), parse_mode="Markdown")

@dp.message(Command("admin"))
async def cmd_admin_denied(message: types.Message):
    await message.answer("⛔️ У вас немає доступу до панелі адміністратора.")

@dp.callback_query(F.data.in_(["admin_home", "admin_refresh"]), AdminOnlyFilter())
async def cb_admin_home(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await asyncio.to_thread(save_or_update_user, callback.from_user, "admin_panel")
    metrics = await asyncio.to_thread(get_admin_metrics)
    text = (
        "🔐 **Панель адміністратора**\n\n"
        f"👥 **Всього користувачів:** `{metrics.get('total_users', 0)}`\n"
        f"👑 **Адміністраторів:** `{metrics.get('admins_count', 0)}`\n"
        f"📬 **Активних одержувачів:** `{metrics.get('active_recipients', 0)}`\n"
        f"🟢 **Активні за 24г:** `{metrics.get('active_today', 0)}`\n"
        f"📊 **Згенеровано звітів:** `{metrics.get('total_reports', 0)}`\n"
        f"💬 **Всього дій:** `{metrics.get('total_interactions', 0)}`"
    )
    if callback.message.photo:
        await callback.message.answer(text, reply_markup=build_admin_main_keyboard(), parse_mode="Markdown")
        await callback.message.delete()
    else:
        await callback.message.edit_text(text, reply_markup=build_admin_main_keyboard(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_users_"), AdminOnlyFilter())
async def cb_admin_users(callback: types.CallbackQuery):
    offset = int(callback.data.split("_")[2])
    limit = 10
    users = await asyncio.to_thread(get_recent_users, limit, offset)

    if not users:
        await callback.answer("Користувачів більше немає.", show_alert=True)
        return

    text = f"👥 **Список користувачів** (показано {offset + 1}–{offset + len(users)}):"
    await callback.message.edit_text(text, reply_markup=build_users_list_keyboard(users, offset, limit), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_view_"), AdminOnlyFilter())
async def cb_admin_user_card(callback: types.CallbackQuery):
    _, _, target_id_str, offset_str = callback.data.split("_")
    target_id = int(target_id_str)
    offset = int(offset_str)

    user = await asyncio.to_thread(get_user_by_id, target_id)
    if not user:
        await callback.answer("Користувача не знайдено.", show_alert=True)
        return

    last_seen = user["last_seen_at"].strftime("%d.%m.%Y %H:%M") if user.get("last_seen_at") else "-"
    blocked_status = "⚠️ Заблокував бота" if user.get("is_bot_blocked") else "✅ Активний"
    text = (
        f"👤 **Профіль користувача:**\n\n"
        f"• **Telegram ID:** `{user['telegram_id']}`\n"
        f"• **Нікнейм:** @{user['username'] or '-'}\n"
        f"• **Ім'я:** {user['first_name']} {user.get('last_name') or ''}\n"
        f"• **Роль:** `{user['role']}`\n"
        f"• **Статус:** {blocked_status}\n"
        f"• **Остання дія:** `{user['last_action'] or '-'}`\n"
        f"• **Остання активність:** {last_seen}\n"
        f"• **Згенеровано звітів:** `{user['reports_generated_count']}`\n"
        f"• **Всього взаємодій:** `{user['interactions_count']}`"
    )
    await callback.message.edit_text(text, reply_markup=build_user_card_keyboard(target_id, user["role"], offset), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_setrole_"), AdminOnlyFilter())
async def cb_admin_set_role(callback: types.CallbackQuery):
    _, _, target_id_str, new_role, offset_str = callback.data.split("_")
    target_id = int(target_id_str)
    offset = int(offset_str)

    if target_id == callback.from_user.id and new_role != "admin":
        await callback.answer("⚠️ Ви не можете понизити власну роль!", show_alert=True)
        return

    await asyncio.to_thread(update_user_role, target_id, new_role)
    await callback.answer(f"Роль змінено на {new_role}!", show_alert=True)

    user = await asyncio.to_thread(get_user_by_id, target_id)
    last_seen = user["last_seen_at"].strftime("%d.%m.%Y %H:%M") if user.get("last_seen_at") else "-"
    blocked_status = "⚠️ Заблокував бота" if user.get("is_bot_blocked") else "✅ Активний"
    text = (
        f"👤 **Профіль користувача:**\n\n"
        f"• **Telegram ID:** `{user['telegram_id']}`\n"
        f"• **Нікнейм:** @{user['username'] or '-'}\n"
        f"• **Ім'я:** {user['first_name']} {user.get('last_name') or ''}\n"
        f"• **Роль:** `{user['role']}`\n"
        f"• **Статус:** {blocked_status}\n"
        f"• **Остання дія:** `{user['last_action'] or '-'}`\n"
        f"• **Остання активність:** {last_seen}\n"
        f"• **Згенеровано звітів:** `{user['reports_generated_count']}`\n"
        f"• **Всього взаємодій:** `{user['interactions_count']}`"
    )
    await callback.message.edit_text(text, reply_markup=build_user_card_keyboard(target_id, user["role"], offset), parse_mode="Markdown")

# --- Секція розсилки повідомлень (Broadcast) ---
@dp.callback_query(F.data == "admin_bc_audience", AdminOnlyFilter())
async def cb_admin_bc_audience(callback: types.CallbackQuery):
    text = "📢 **Створення розсилки**\n\nОберіть аудиторію для відправки повідомлення:"
    await callback.message.edit_text(text, reply_markup=build_audience_keyboard(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_bc_target_"), AdminOnlyFilter())
async def cb_admin_bc_target(callback: types.CallbackQuery, state: FSMContext):
    target_str = callback.data.replace("admin_bc_target_", "")
    target_farm_id = None if target_str == "all" else int(target_str)
    
    recipients = await asyncio.to_thread(get_broadcast_recipients, target_farm_id)
    count = len(recipients)
    
    if count == 0:
        await callback.answer("⚠️ Для цієї категорії немає активних отримувачів.", show_alert=True)
        return
        
    await state.update_data(target_farm_id=target_farm_id, recipients_count=count)
    await state.set_state(BroadcastStates.waiting_for_message)
    
    target_name = FARMS[target_farm_id]["name"] if target_farm_id else "Всі зареєстровані користувачі"
    text = (
        f"📢 **Розсилка для:** {target_name}\n"
        f"👥 **Одержувачів:** `{count}`\n\n"
        "✍️ **Надішліть наступним повідомленням те, що хочете розіслати.**\n"
        "Підтримуються: звичайний текст, форматування Markdown/HTML, фотографії, файли, відео з підписами.\n\n"
        "_Для скасування надішліть /cancel_"
    )
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Скасувати розсилку", callback_data="admin_bc_cancel")
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data == "admin_bc_cancel", AdminOnlyFilter())
@dp.message(Command("cancel"), AdminOnlyFilter())
async def cb_admin_bc_cancel(event: types.TelegramObject, state: FSMContext):
    await state.clear()
    text = "❌ Розсилку скасовано. Ви повернулися до адмін-панелі."
    if isinstance(event, types.CallbackQuery):
        await event.message.edit_text(text, reply_markup=build_admin_main_keyboard())
        await event.answer()
    else:
        await event.answer(text, reply_markup=build_admin_main_keyboard())

@dp.message(BroadcastStates.waiting_for_message, AdminOnlyFilter())
async def process_broadcast_content(message: types.Message, state: FSMContext):
    data = await state.get_data()
    target_farm_id = data.get("target_farm_id")
    recipients_count = data.get("recipients_count", 0)

    # Зберігаємо ID та чат повідомлення для копіювання
    await state.update_data(
        from_chat_id=message.chat.id,
        message_id=message.message_id
    )

    await message.answer("👁 **Попередній перегляд вашого оголошення:**")
    # Показуємо адміністратору точний вигляд повідомлення
    await message.copy_to(chat_id=message.chat.id)

    target_name = FARMS[target_farm_id]["name"] if target_farm_id else "Всі користувачі"
    confirm_text = (
        f"⚠️ **Підтвердження відправки**\n\n"
        f"• **Аудиторія:** {target_name}\n"
        f"• **Кількість одержувачів:** `{recipients_count}`\n\n"
        "Відправити це оголошення всім користувачам?"
    )
    await message.answer(confirm_text, reply_markup=build_broadcast_confirm_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "admin_bc_confirm", AdminOnlyFilter())
async def cb_admin_bc_execute(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    from_chat_id = data.get("from_chat_id")
    message_id = data.get("message_id")
    target_farm_id = data.get("target_farm_id")

    if not from_chat_id or not message_id:
        await callback.answer("⚠️ Помилка: дані повідомлення втрачено. Почніть спочатку.", show_alert=True)
        await state.clear()
        return

    recipients = await asyncio.to_thread(get_broadcast_recipients, target_farm_id)
    total_count = len(recipients)

    await callback.message.edit_text(f"⏳ **Розсилка розпочалася...**\nОдержувачів: `{total_count}`. Зачекайте завершення.")
    await callback.answer()

    success_count = 0
    blocked_count = 0
    error_count = 0

    for user_id in recipients:
        try:
            await bot.copy_message(
                chat_id=user_id,
                from_chat_id=from_chat_id,
                message_id=message_id
            )
            success_count += 1
            await asyncio.sleep(0.04)  # ~25 повідомлень на секунду для дотримання лімітів TG
        except TelegramForbiddenError:
            # Юзер заблокував бота або видалив акаунт
            blocked_count += 1
            await asyncio.to_thread(mark_bot_blocked, user_id)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id
                )
                success_count += 1
            except Exception:
                error_count += 1
        except Exception as e:
            logging.error(f"Помилка надсилання користувачу {user_id}: {e}")
            error_count += 1

    await state.clear()
    report_text = (
        "✅ **Розсилку успішно завершено!**\n\n"
        f"• **Усього адресатів:** `{total_count}`\n"
        f"• **Успішно доставлено:** `{success_count}`\n"
        f"• **Заблокували бота:** `{blocked_count}` (позначено в базі)\n"
        f"• **Помилок надсилання:** `{error_count}`"
    )
    await callback.message.answer(report_text, reply_markup=build_admin_main_keyboard(), parse_mode="Markdown")

# --- Основні Callback-хендлери бота ---
@dp.callback_query(F.data.startswith("farm_"))
async def on_farm_chosen(callback: types.CallbackQuery):
    farm_num = int(callback.data.split("_")[1])
    await asyncio.to_thread(
        save_or_update_user,
        callback.from_user,
        f"farm_{farm_num}",
        assigned_farm_id=farm_num,
    )
    locations = await asyncio.to_thread(get_farrowing_locations, farm_num)

    if not locations:
        await callback.answer("У базі не знайдено відділень опоросу для цієї ферми.", show_alert=True)
        return

    reply_markup = build_dept_keyboard(farm_num, locations)
    text = f"📍 **{FARMS[farm_num]['name']}**\nОберіть відділення опоросу:"

    if callback.message.photo:
        await callback.message.answer(text, reply_markup=reply_markup, parse_mode="Markdown")
        await callback.answer()
    else:
        await callback.message.edit_text(text, reply_markup=reply_markup, parse_mode="Markdown")

@dp.callback_query(F.data == "back_farms")
async def on_back_farms(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await asyncio.to_thread(save_or_update_user, callback.from_user, "back_farms")
    is_admin = await asyncio.to_thread(is_user_admin, callback.from_user.id)
    keyboard = build_farms_keyboard(is_admin=is_admin)

    if callback.message.photo:
        await callback.message.answer("Оберіть ферму:", reply_markup=keyboard)
        await callback.answer()
    else:
        await callback.message.edit_text("Оберіть ферму:", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("loc_"))
async def on_location_chosen(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    farm_num = int(parts[1])
    location_id = int(parts[2])
    
    br_flag = int(parts[3]) if len(parts) > 3 else 1
    sow_flag = int(parts[4]) if len(parts) > 4 else 1

    include_babyroom = (br_flag == 1)
    include_sows = (sow_flag == 1)

    await asyncio.to_thread(
        save_or_update_user,
        callback.from_user,
        "refresh_report" if len(parts) > 3 else "location",
        assigned_farm_id=farm_num,
    )
    await callback.answer("Формування звіту...")
    data, loc_name, _ = await asyncio.to_thread(
        calculate_batches_by_location,
        farm_num,
        location_id,
        include_babyroom,
        include_sows,
    )

    if not data:
        await callback.message.answer("⚠️ Не знайдено даних по опоросах для цього відділення.")
        return

    show_br = FARMS[farm_num]["has_br"] and include_babyroom
    img_buffer = await asyncio.to_thread(
        render_table_image,
        data,
        show_br,
        include_sows,
    )
    photo_file = BufferedInputFile(img_buffer.getvalue(), filename=f"report_{farm_num}_{location_id}.png")

    builder = InlineKeyboardBuilder()

    if FARMS[farm_num]["has_br"]:
        next_br_flag = 0 if include_babyroom else 1
        toggle_icon_br = "✅" if include_babyroom else "⬜"
        toggle_text_br = f"{toggle_icon_br} babyroom: {'Враховано' if include_babyroom else 'Вимкнено'}"
        builder.button(text=toggle_text_br, callback_data=f"loc_{farm_num}_{location_id}_{next_br_flag}_{sow_flag}")

    next_sow_flag = 0 if include_sows else 1
    toggle_icon_sow = "✅" if include_sows else "⬜"
    toggle_text_sow = f"{toggle_icon_sow} Свиноматки: {'Враховано' if include_sows else 'Вимкнено'}"
    builder.button(text=toggle_text_sow, callback_data=f"loc_{farm_num}_{location_id}_{br_flag}_{next_sow_flag}")

    builder.button(text="🔄 Оновити", callback_data=callback.data)
    builder.button(text="⬅️ Назад до відділень", callback_data=f"farm_{farm_num}")
    
    if FARMS[farm_num]["has_br"]:
        builder.adjust(1, 1, 2)
    else:
        builder.adjust(1, 2)

    caption_text = (
        f"📊 **Звіт по опоросах**\n"
        f"🏢 **Ферма:** {FARMS[farm_num]['name']}\n"
        f"🚪 **Відділення:** {loc_name}"
    )

    if callback.message.photo:
        try:
            await callback.message.edit_media(
                media=types.InputMediaPhoto(media=photo_file, caption=caption_text, parse_mode="Markdown"),
                reply_markup=builder.as_markup()
            )
        except TelegramBadRequest as error:
            if "message is not modified" not in str(error).lower():
                raise
            await callback.answer("Звіт уже актуальний.")
    else:
        await callback.message.answer_photo(
            photo=photo_file,
            caption=caption_text,
            reply_markup=builder.as_markup(),
            parse_mode="Markdown"
        )

async def main():
    await setup_database()
    await setup_bot_commands()
    
    polling_task = asyncio.create_task(dp.start_polling(bot))
    
    config = uvicorn.Config(app=app, host="0.0.0.0", port=8080, log_level="info")
    server = uvicorn.Server(config)
    try:
        await server.serve()
    finally:
        polling_task.cancel()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())