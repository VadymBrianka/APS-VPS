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
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import BufferedInputFile, BotCommand, BotCommandScopeDefault

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
        # % Всього рахується від живонароджених поросят. Прапорець
        # свиноматок додає їхній фактичний падіж, але не повинен змінювати
        # відсоток, якщо падежу свиноматок немає.
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

# Забезпечуємо роботу всередині Telegram WebApp IFrame (дозволяємо вбудовування)
@app.middleware("http")
async def add_telegram_iframe_headers(request: Request, call_next):
    response: Response = await call_next(request)
    # Видаляємо обмеження X-Frame-Options для iframe Telegram
    if "x-frame-options" in response.headers:
        del response.headers["x-frame-options"]
    response.headers["Content-Security-Policy"] = "frame-ancestors 'self' https://web.telegram.org https://*.telegram.org https://*.t.me;"
    return response

# Монтуємо папку зі статикою (перевіряємо шлях)
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

# --- Telegram Bot ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

async def setup_bot_commands():
    commands = [
        BotCommand(command="start", description="Головне меню / Вибір ферми"),
        BotCommand(command="report", description="Сформувати новий звіт")
    ]
    await bot.set_my_commands(commands, scope=BotCommandScopeDefault())

async def setup_database():
    await asyncio.to_thread(ensure_bot_user_table)

def build_farms_keyboard():
    builder = InlineKeyboardBuilder()
    for f_num, f_data in FARMS.items():
        builder.button(text=f_data["name"], callback_data=f"farm_{f_num}")
    builder.adjust(1)
    
    # Кнопка Mini App показується лише за наявності валідного HTTPS URL
    if WEBAPP_URL and WEBAPP_URL.startswith("https://"):
        builder.row(types.InlineKeyboardButton(
            text="📱 Відкрити Web Mini App",
            web_app=types.WebAppInfo(url=WEBAPP_URL)
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

@dp.message(CommandStart())
@dp.message(Command("report"))
async def cmd_start(message: types.Message):
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
    await message.answer(
        "👋 **Аналітика опоросів та падежів**\n\nОберіть потрібну ферму для формування звіту:",
        reply_markup=build_farms_keyboard(),
        parse_mode="Markdown"
    )

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
async def on_back_farms(callback: types.CallbackQuery):
    await asyncio.to_thread(save_or_update_user, callback.from_user, "back_farms")
    if callback.message.photo:
        await callback.message.answer("Оберіть ферму:", reply_markup=build_farms_keyboard())
        await callback.answer()
    else:
        await callback.message.edit_text("Оберіть ферму:", reply_markup=build_farms_keyboard())

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
    # Колонки свиноматок мають залежати від перемикача, а не від фактичної
    # наявності падежу. Інакше при нульовому падежі перемикання непомітне.
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
    # Запуск polling у фоні
    polling_task = asyncio.create_task(dp.start_polling(bot))
    
    # Запуск сервера uvicorn
    config = uvicorn.Config(app=app, host="0.0.0.0", port=8080, log_level="info")
    server = uvicorn.Server(config)
    try:
        await server.serve()
    finally:
        polling_task.cancel()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())