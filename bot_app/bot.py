import os
import io
import asyncio
import logging
from datetime import datetime
from typing import List, Optional
from enum import IntEnum

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import uvicorn

from aiogram import Bot, Dispatcher, types, F
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
WEBAPP_URL = os.getenv("WEBAPP_URL", "")

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

def calculate_batches_by_location(farm_num: int, location_id: int, include_babyroom: bool = True):
    farm_info = FARMS.get(farm_num)
    if not farm_info:
        return [], ""
    
    db_farm_id = farm_info["db_id"]
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT id, name, locationnumber FROM location WHERE id = %s AND farm_id = %s;", (location_id, db_farm_id))
    loc_row = cur.fetchone()
    if not loc_row:
        cur.close()
        conn.close()
        return [], ""

    location_display_name = loc_row["locationnumber"] or loc_row["name"]

    # Обчислюємо babyroom тільки якщо це Ферма №1 та користувач увімкнув прапорець
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
        ORDER BY f.farrowing_date ASC;
    """
    cur.execute(query_farrowings, (db_farm_id, location_id, location_id, db_farm_id))
    farrowings = cur.fetchall()

    if not farrowings:
        cur.close()
        conn.close()
        return [], location_display_name

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

    results = []
    for batch in batches:
        start_d = batch[0]["f_date"]
        end_d = batch[-1]["f_date"]
        days = (end_d - start_d).days + 1
        liveborn_sum = sum(item["liveborn"] for item in batch)
        sows_count = len(batch)
        per_sow = liveborn_sum / sows_count if sows_count > 0 else 0.0

        cur.execute("""
            SELECT COALESCE(SUM(amount), COUNT(id)) as deaths
            FROM death
            WHERE farm_id = %s
              AND location_id = %s
              AND death_date::date BETWEEN %s AND %s;
        """, (db_farm_id, location_id, start_d, end_d))
        death_row = cur.fetchone()
        deaths = death_row["deaths"] or 0
        death_pct = (deaths / liveborn_sum * 100) if liveborn_sum > 0 else 0.0

        br_deaths = None
        br_pct = None
        if should_calc_br and br_loc_id:
            cur.execute("""
                SELECT COALESCE(SUM(amount), COUNT(id)) as deaths
                FROM death
                WHERE farm_id = %s
                  AND location_id = %s
                  AND death_date::date BETWEEN %s AND %s;
            """, (db_farm_id, br_loc_id, start_d, end_d))
            br_row = cur.fetchone()
            br_deaths = br_row["deaths"] or 0
            br_pct = (br_deaths / liveborn_sum * 100) if liveborn_sum > 0 else 0.0

        all_deaths = deaths + (br_deaths or 0)
        all_death_pct = (all_deaths / liveborn_sum * 100) if liveborn_sum > 0 else 0.0

        results.append({
            "start_date": start_d.strftime("%d.%m.%Y"),
            "end_date": end_d.strftime("%d.%m.%Y"),
            "days": days,
            "liveborn": liveborn_sum,
            "sows": sows_count,
            "per_sow": round(per_sow, 2),
            "deaths": deaths,
            "death_pct": round(death_pct, 2),
            "br": br_deaths if should_calc_br else None,
            "br_pct": round(br_pct, 2) if (should_calc_br and br_pct is not None) else None,
            "all_deaths": all_deaths if should_calc_br else deaths,
            "all_death_pct": round(all_death_pct, 2) if should_calc_br else round(death_pct, 2)
        })

    cur.close()
    conn.close()
    return results, location_display_name

def get_cyrillic_font(size: int = 15):
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

def render_table_image(data: list, show_babyroom: bool) -> io.BytesIO:
    headers = ["З", "До", "Днів", "Поросят", "СМ", "На СМ", "Падіж", "% П"]
    if show_babyroom:
        headers.extend(["BR", "%BR", "ALL", "%ALL"])

    rows = []
    for r in data:
        row = [
            r["start_date"],
            r["end_date"],
            str(r["days"]),
            str(r["liveborn"]),
            str(r["sows"]),
            f"{r['per_sow']:.2f}",
            str(r["deaths"]),
            f"{r['death_pct']:.2f}"
        ]
        if show_babyroom:
            row.extend([
                str(r["br"]) if r["br"] is not None else "-",
                f"{r['br_pct']:.2f}" if r["br_pct"] is not None else "-",
                str(r["all_deaths"]),
                f"{r['all_death_pct']:.2f}"
            ])
        rows.append(row)

    font = get_cyrillic_font(15)
    row_height = 42
    padding_x = 18

    dummy_img = Image.new("RGB", (1, 1))
    dummy_draw = ImageDraw.Draw(dummy_img)

    col_widths = []
    for i, h in enumerate(headers):
        bbox_h = dummy_draw.textbbox((0, 0), h, font=font)
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
        draw.rectangle([curr_x, start_y, curr_x + w, start_y + row_height], fill=(240, 243, 246), outline=(60, 60, 60), width=1)
        bbox = draw.textbbox((0, 0), header, font=font)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]
        draw.text((curr_x + (w - tw) / 2, start_y + (row_height - th) / 2 - 2), header, fill=(0, 0, 0), font=font)
        curr_x += w

    curr_y = start_y + row_height
    for row_idx, row in enumerate(rows):
        curr_x = start_x
        row_bg = (255, 255, 255) if row_idx % 2 == 0 else (249, 250, 251)
        for i, val in enumerate(row):
            w = col_widths[i]
            draw.rectangle([curr_x, curr_y, curr_x + w, curr_y + row_height], fill=row_bg, outline=(60, 60, 60), width=1)
            bbox = draw.textbbox((0, 0), val, font=font)
            tw = bbox[2] - bbox[0]
            th = bbox[3] - bbox[1]
            draw.text((curr_x + (w - tw) / 2, curr_y + (row_height - th) / 2 - 2), val, fill=(15, 23, 42), font=font)
            curr_x += w
        curr_y += row_height

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf

# REST API
app = FastAPI(title="CloudFarms Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def serve_home():
    if os.path.exists("static/index.html"):
        return FileResponse("static/index.html")
    return {"status": "ok"}

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
    include_babyroom: bool = Query(True)
):
    data, _ = calculate_batches_by_location(farm_id.value, location_id, include_babyroom=include_babyroom)
    return data

# Telegram Bot
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

async def setup_bot_commands():
    commands = [
        BotCommand(command="start", description="Головне меню / Вибір ферми"),
        BotCommand(command="report", description="Сформувати новий звіт")
    ]
    await bot.set_my_commands(commands, scope=BotCommandScopeDefault())

def build_farms_keyboard():
    builder = InlineKeyboardBuilder()
    for f_num, f_data in FARMS.items():
        builder.button(text=f_data["name"], callback_data=f"farm_{f_num}")
    builder.adjust(1)
    if WEBAPP_URL:
        builder.row(types.InlineKeyboardButton(
            text="📱 Відкрити Web Mini App",
            web_app=types.WebAppInfo(url=WEBAPP_URL)
        ))
    return builder.as_markup()

def build_dept_keyboard(farm_num: int, locations: list):
    builder = InlineKeyboardBuilder()
    for loc in locations:
        btn_text = f"{loc['locationnumber']} ({loc['name']})" if loc['locationnumber'] != loc['name'] else loc['name']
        # За замовчуванням для ферми 1 babyroom увімкнено (1)
        builder.button(text=btn_text, callback_data=f"loc_{farm_num}_{loc['id']}_1")
    builder.adjust(2)
    builder.row(types.InlineKeyboardButton(text="⬅️ До вибору ферми", callback_data="back_farms"))
    return builder.as_markup()

@dp.message(CommandStart())
@dp.message(Command("report"))
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 **Аналітика опоросів та падежів**\n\nОберіть потрібну ферму для формування звіту:",
        reply_markup=build_farms_keyboard(),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("farm_"))
async def on_farm_chosen(callback: types.CallbackQuery):
    farm_num = int(callback.data.split("_")[1])
    locations = get_farrowing_locations(farm_num)

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
    # Отримуємо стан опції babyroom (за замовчуванням 1 - увімкнено)
    br_flag = int(parts[3]) if len(parts) > 3 else 1
    include_babyroom = (br_flag == 1)

    await callback.answer("Формування звіту...")
    data, loc_name = calculate_batches_by_location(farm_num, location_id, include_babyroom=include_babyroom)

    if not data:
        await callback.message.answer("⚠️ Не знайдено даних по опоросах для цього відділення.")
        return

    show_br = FARMS[farm_num]["has_br"] and include_babyroom
    img_buffer = render_table_image(data, show_babyroom=show_br)
    photo_file = BufferedInputFile(img_buffer.getvalue(), filename=f"report_{farm_num}_{location_id}.png")

    builder = InlineKeyboardBuilder()

    # Опціональний тумблер babyroom лише для Ферми №1
    if FARMS[farm_num]["has_br"]:
        next_br_flag = 0 if include_babyroom else 1
        toggle_icon = "✅" if include_babyroom else "⬜"
        toggle_text = f"{toggle_icon} babyroom: {'Враховано' if include_babyroom else 'Вимкнено'}"
        builder.button(text=toggle_text, callback_data=f"loc_{farm_num}_{location_id}_{next_br_flag}")

    builder.button(text="🔄 Оновити", callback_data=callback.data)
    builder.button(text="⬅️ Назад до відділень", callback_data=f"farm_{farm_num}")
    builder.adjust(1, 2)

    caption_text = (
        f"📊 **Звіт по опоросах**\n"
        f"🏢 **Ферма:** {FARMS[farm_num]['name']}\n"
        f"🚪 **Відділення:** {loc_name}"
    )

    # Якщо це перемикання галочки на вже існуючому фото — оновлюємо медіа, щоб не спамити
    if callback.message.photo:
        await callback.message.edit_media(
            media=types.InputMediaPhoto(media=photo_file, caption=caption_text, parse_mode="Markdown"),
            reply_markup=builder.as_markup()
        )
    else:
        await callback.message.answer_photo(
            photo=photo_file,
            caption=caption_text,
            reply_markup=builder.as_markup(),
            parse_mode="Markdown"
        )

async def main():
    await setup_bot_commands()
    asyncio.create_task(dp.start_polling(bot))
    config = uvicorn.Config(app=app, host="0.0.0.0", port=8080, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())