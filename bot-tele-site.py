import os
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from io import BytesIO

import feedparser
from bs4 import BeautifulSoup

# Telegram
import telegram

# Pillow + HTTP
from PIL import Image, ImageOps
import requests

# ====================
# CONFIG
# ====================
TZ = ZoneInfo("Africa/Casablanca")

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Website API upload
SITE_API_URL   = os.getenv("SITE_API_URL")    # e.g. https://exmple.com/api/new-article.php
SITE_API_TOKEN = os.getenv("SITE_API_TOKEN")  # optional bearer token

# Sources
CRUNCHYROLL_RSS_URL = "https://cr-news-api-service.prd.crunchyrollsvc.com/v1/ar-SA/rss"

# YouTube
CHANNEL_ID      = "UC1WGYjPeHHc_3nRXqbW3OcQ"
YOUTUBE_RSS_URL = f"https://www.youtube.com/feeds/videos.xml?channel_id={CHANNEL_ID}"

# State files (latest-only)
CRUNCHYROLL_LAST_FP_FILE = Path("last_crunchyroll_fp.txt")
YOUTUBE_LAST_ID_FILE     = Path("last_youtube_id.txt")

# Paths
DATA_BASE    = Path("data")            # data/YYYY/MM/DD-MM.json
GLOBAL_INDEX = Path("global_index")    # index_1.json, index_2.json, pagination.json, stats.json

# Global Index settings
GLOBAL_PAGE_SIZE = 500  # rotate after this many items per index file

# Logo overlay settings
LOGO_PATH = "logo.png"
LOGO_MIN_WIDTH_RATIO = 0.10  # 10% for small images
LOGO_MAX_WIDTH_RATIO = 0.20  # 20% for large images
LOGO_MARGIN = 10             # px margin from top-right

# Image processing limits
MAX_IMAGE_WIDTH  = 1280
MAX_IMAGE_HEIGHT = 1280
JPEG_QUALITY     = 85
WEBP_QUALITY     = 85
HTTP_TIMEOUT     = 25

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ====================
# Utils
# ====================
def now_local() -> datetime:
    return datetime.now(TZ)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def daily_path(dt: datetime) -> Path:
    y, m, d = dt.year, dt.month, dt.day
    out_dir = DATA_BASE / f"{y}" / f"{m:02d}"
    ensure_dir(out_dir)
    return out_dir / f"{d:02d}-{m:02d}.json"

def load_json_list(path: Path) -> list:
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception as e:
        logging.error(f"Failed reading {path}: {e}")
        return []

def save_json_list(path: Path, data: list):
    try:
        ensure_dir(path.parent)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Failed writing {path}: {e}")

def read_text_file(path: Path) -> str | None:
    try:
        if not path.exists():
            return None
        s = path.read_text(encoding="utf-8").strip()
        return s or None
    except Exception:
        return None

def write_text_file(path: Path, value: str):
    try:
        path.write_text((value or "").strip() + "\n", encoding="utf-8")
    except Exception as e:
        logging.error(f"Failed writing {path}: {e}")


# ====================
# RSS extraction helpers
# ====================
def extract_full_text(entry) -> str:
    """
    Full text without HTML:
    - prefer content:encoded (entry.content[0].value)
    - fallback to description
    """
    try:
        if hasattr(entry, "content") and entry.content and isinstance(entry.content, list):
            raw = entry.content[0].get("value") or ""
            if raw:
                return BeautifulSoup(raw, "html.parser").get_text(separator=" ", strip=True)
    except Exception:
        pass

    raw = getattr(entry, "description", "") or ""
    if raw:
        return BeautifulSoup(raw, "html.parser").get_text(separator=" ", strip=True)

    return ""

def extract_image(entry) -> str | None:
    # 1) media:thumbnail
    if hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
        try:
            return entry.media_thumbnail[0].get("url") or entry.media_thumbnail[0]["url"]
        except Exception:
            pass
    # 2) from content/description
    raw = ""
    try:
        if hasattr(entry, "content") and entry.content and isinstance(entry.content, list):
            raw = entry.content[0].get("value") or ""
    except Exception:
        pass
    if not raw:
        raw = getattr(entry, "description", "") or ""
    if raw:
        soup = BeautifulSoup(raw, "html.parser")
        img = soup.find("img")
        if img and img.has_attr("src"):
            return img["src"]
    return None

def extract_categories(entry) -> list:
    cats = []
    tags = getattr(entry, "tags", None)
    if tags:
        for t in tags:
            term = getattr(t, "term", None)
            if term:
                cats.append(str(term))
    return cats

def build_daily_record(entry) -> dict:
    """
    Daily record:
    - title
    - description_full (plain text)
    - image
    - categories
    """
    title = getattr(entry, "title", "") or ""
    description_full = extract_full_text(entry)
    image = extract_image(entry)
    categories = extract_categories(entry)
    return {
        "title": title,
        "description_full": description_full,
        "image": image,
        "categories": categories
    }

def get_entry_identity(entry) -> str:
    """Dedup fingerprint: title + image."""
    title = getattr(entry, "title", "") or ""
    image = extract_image(entry)
    return f"{title.strip()}|{(image or '').strip()}"


# ====================
# Image processing (logo + resize)
# ====================
def fetch_image(url: str) -> Image.Image | None:
    try:
        r = requests.get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        im = Image.open(BytesIO(r.content))
        im = ImageOps.exif_transpose(im)  # fix orientation
        return im.convert("RGBA")
    except Exception as e:
        logging.error(f"fetch_image failed for {url}: {e}")
        return None

def downscale_to_fit(im: Image.Image) -> Image.Image:
    w, h = im.size
    scale = min(
        (MAX_IMAGE_WIDTH / w) if w > 0 else 1,
        (MAX_IMAGE_HEIGHT / h) if h > 0 else 1,
        1
    )
    if scale < 1:
        new_w = max(1, int(w * scale))
        new_h = max(1, int(h * scale))
        im = im.resize((new_w, new_h), Image.LANCZOS)
    return im

def overlay_logo(im: Image.Image) -> Image.Image:
    """Overlay logo top-right with adaptive size."""
    if not Path(LOGO_PATH).exists():
        return im
    try:
        logo = Image.open(LOGO_PATH).convert("RGBA")
    except Exception as e:
        logging.error(f"Failed to open logo: {e}")
        return im

    pw, _ = im.size
    lw_ratio = LOGO_MIN_WIDTH_RATIO if pw < 600 else LOGO_MAX_WIDTH_RATIO
    lw = int(max(1, min(pw - 2 * LOGO_MARGIN, pw * lw_ratio)))
    ratio = lw / logo.width
    lh = int(max(1, logo.height * ratio))
    logo_resized = logo.resize((lw, lh), Image.LANCZOS)

    x = pw - lw - LOGO_MARGIN
    y = LOGO_MARGIN
    im.paste(logo_resized, (x, y), logo_resized)
    return im

def process_image_with_logo(url: str, out_format: str = "JPEG") -> BytesIO | None:
    """
    - download
    - exif transpose
    - downscale
    - overlay logo
    - export (JPEG or WEBP)
    """
    base = fetch_image(url)
    if base is None:
        return None

    base = downscale_to_fit(base)
    base = overlay_logo(base)

    out = BytesIO()

    fmt = out_format.upper().strip()
    if fmt == "WEBP":
        base.convert("RGB").save(out, format="WEBP", quality=WEBP_QUALITY, method=6)
    else:
        base.convert("RGB").save(out, format="JPEG", quality=JPEG_QUALITY, optimize=True)

    out.seek(0)
    return out


# ====================
# Website uploader
# ====================
def upload_article_to_site(
    title: str,
    description: str,
    published_time: str,
    image_webp: BytesIO | None,
) -> bool:
    """
    Sends multipart/form-data to SITE_API_URL:
      - title
      - description
      - time
      - image (webp file)
    Optional auth header:
      Authorization: Bearer SITE_API_TOKEN
    """
    if not SITE_API_URL:
        logging.warning("SITE_API_URL not set; skip upload.")
        return False

    headers = {}
    if SITE_API_TOKEN:
        headers["Authorization"] = f"Bearer {SITE_API_TOKEN}"

    data = {
        "title": title or "",
        "description": description or "",
        "time": published_time or "",
    }

    files = None
    if image_webp is not None:
        try:
            image_webp.seek(0)
        except Exception:
            pass
        files = {"image": ("article.webp", image_webp, "image/webp")}

    try:
        r = requests.post(
            SITE_API_URL,
            headers=headers,
            data=data,
            files=files,
            timeout=HTTP_TIMEOUT,
        )

        if r.status_code >= 400:
            logging.error(f"Upload failed: {r.status_code} {r.text[:500]}")
            return False

        logging.info(f"Upload OK: {r.status_code}")
        return True
    except Exception as e:
        logging.error(f"Upload exception: {e}")
        return False


# ====================
# Persist Daily (Crunchyroll) - ONLY ONE
# ====================
def save_single_news(entry):
    """
    Save ONLY 1 entry to today's JSON.
    Dedup by (title + image) within today's file.
    Return (record_or_none, day_path_str).
    """
    today = now_local()
    path = daily_path(today)
    existing = load_json_list(path)

    fp = get_entry_identity(entry)
    existing_fp = {f"{(x.get('title') or '').strip()}|{(x.get('image') or '').strip()}" for x in existing}

    if fp in existing_fp:
        return None, str(path)

    rec = build_daily_record(entry)
    existing.append(rec)
    save_json_list(path, existing)
    return rec, str(path)


# ====================
# Manifests (month/year)
# ====================
def update_month_manifest(dt: datetime):
    y, m = dt.year, dt.month
    month_dir = DATA_BASE / f"{y}" / f"{m:02d}"
    ensure_dir(month_dir)
    manifest_path = month_dir / "month_manifest.json"

    days = {}
    for p in sorted(month_dir.glob("*.json")):
        if p.name == "month_manifest.json":
            continue
        day_key = p.stem  # "DD-MM"
        days[day_key.split("-")[0]] = str(p.as_posix())

    manifest = {
        "year": str(y),
        "month": f"{m:02d}",
        "days": dict(sorted(days.items(), key=lambda kv: kv[0], reverse=True))
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

def update_year_manifest(dt: datetime):
    y = dt.year
    year_dir = DATA_BASE / f"{y}"
    ensure_dir(year_dir)
    manifest_path = year_dir / "year_manifest.json"

    months = {}
    for p in sorted(year_dir.glob("[0-1][0-9]")):
        m = p.name
        months[m] = f"{(p / 'month_manifest.json').as_posix()}"

    manifest = {
        "year": str(y),
        "months": dict(sorted(months.items(), key=lambda kv: kv[0], reverse=True))
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


# ====================
# Global Index (pagination + stats)
# ====================
def gi_paths():
    ensure_dir(GLOBAL_INDEX)
    pag_path  = GLOBAL_INDEX / "pagination.json"
    stats_path= GLOBAL_INDEX / "stats.json"
    return pag_path, stats_path

def gi_load_pagination():
    pag_path, _ = gi_paths()
    if not pag_path.exists():
        return {"total_articles": 0, "files": []}
    try:
        with open(pag_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"total_articles": 0, "files": []}

def gi_save_pagination(pag: dict):
    pag_path, _ = gi_paths()
    with open(pag_path, "w", encoding="utf-8") as f:
        json.dump(pag, f, ensure_ascii=False, indent=2)

def gi_save_stats(total_articles: int, added_today: int):
    _, stats_path = gi_paths()
    stats = {
        "total_articles": total_articles,
        "added_today": added_today,
        "last_update": now_local().isoformat()
    }
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

def convert_full_to_slim(records: list, source_path: str = None) -> list:
    """
    From daily records to slim records for global index:
    Keep: title, image, categories, path (data/...json#idx)
    """
    out = []
    for i, r in enumerate(records):
        path = f"{source_path}#{i}" if source_path else None
        out.append({
            "title": r.get("title"),
            "image": r.get("image"),
            "categories": r.get("categories") or [],
            "path": path
        })
    return out

def gi_append_records(new_records: list):
    if not new_records:
        return

    pag = gi_load_pagination()

    if not pag["files"]:
        first = GLOBAL_INDEX / "index_1.json"
        save_json_list(first, [])
        pag["files"].append("index_1.json")

    current_filename = pag["files"][-1]
    current_file = GLOBAL_INDEX / current_filename
    items = load_json_list(current_file)

    if len(items) >= GLOBAL_PAGE_SIZE:
        next_idx = len(pag["files"]) + 1
        current_filename = f"index_{next_idx}.json"
        current_file = GLOBAL_INDEX / current_filename
        save_json_list(current_file, [])
        pag["files"].append(current_filename)
        items = []

    items.extend(new_records)
    save_json_list(current_file, items)

    total = (pag.get("total_articles") or 0) + len(new_records)
    pag["total_articles"] = total

    gi_save_pagination(pag)
    gi_save_stats(total_articles=total, added_today=len(new_records))


# ====================
# Telegram Senders
# ====================
async def send_crunchyroll_one(bot: telegram.Bot, entry):
    rec = build_daily_record(entry)
    title = rec.get("title") or ""
    img_url = rec.get("image")

    if img_url:
        processed_jpg = process_image_with_logo(img_url, out_format="JPEG")
        try:
            if processed_jpg:
                await bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=processed_jpg, caption=title)
            else:
                await bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=img_url, caption=title)
            return
        except Exception as e:
            logging.error(f"Failed to send Crunchyroll photo: {e}")

    desc = rec.get("description_full") or ""
    text = f"📰 خبر جديد\n\n{title}"
    if desc:
        text += "\n\n" + (desc[:800] + "…" if len(desc) > 800 else desc)

    await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text)


async def send_youtube_latest_if_new(bot: telegram.Bot):
    """
    ONLY latest video:
    - if vid == last saved -> skip
    - else send & write last id
    """
    feed = feedparser.parse(YOUTUBE_RSS_URL)
    if not feed.entries:
        return

    entry = feed.entries[0]
    vid = getattr(entry, "yt_videoid", None) or getattr(entry, "id", None) or ""
    title = getattr(entry, "title", "") or ""
    url   = getattr(entry, "link", "") or ""

    last_vid = read_text_file(YOUTUBE_LAST_ID_FILE)
    if last_vid and vid and vid == last_vid:
        logging.info("YT: latest already sent. Skip.")
        return

    thumb = None
    if hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
        thumb = entry.media_thumbnail[0].get("url")

    caption = f"🎥 {title}\n{url}"
    try:
        if thumb:
            await bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=thumb, caption=caption)
        else:
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=caption)
    except Exception as e:
        logging.error(f"Failed to send YouTube: {e}")
        return

    write_text_file(YOUTUBE_LAST_ID_FILE, vid)
    logging.info("YT: sent latest & saved id.")


# ====================
# Main
# ====================
async def run():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("FATAL: TELEGRAM_TOKEN or TELEGRAM_CHAT_ID not set.")
        return

    bot = telegram.Bot(token=TELEGRAM_TOKEN)

    # 1) Crunchyroll: ONLY latest, ONLY if new -> send + save + index + upload to site
    news_feed = feedparser.parse(CRUNCHYROLL_RSS_URL)
    if news_feed.entries:
        latest = news_feed.entries[0]
        fp = get_entry_identity(latest)

        last_fp = read_text_file(CRUNCHYROLL_LAST_FP_FILE)
        if last_fp and fp == last_fp:
            logging.info("Crun: latest already processed/sent. Skip.")
        else:
            rec, day_path = save_single_news(latest)

            if rec is not None:
                # send telegram
                await send_crunchyroll_one(bot, latest)

                # upload to website (WebP)
                title = rec.get("title") or ""
                desc  = rec.get("description_full") or ""
                time_str = now_local().isoformat()

                img_url = rec.get("image")
                image_webp = process_image_with_logo(img_url, out_format="WEBP") if img_url else None

                upload_article_to_site(
                    title=title,
                    description=desc,
                    published_time=time_str,
                    image_webp=image_webp,
                )

                # manifests + global index
                today = now_local()
                update_month_manifest(today)
                update_year_manifest(today)

                slim = convert_full_to_slim([rec], day_path)
                gi_append_records(slim)

                write_text_file(CRUNCHYROLL_LAST_FP_FILE, fp)
                logging.info("Crun: sent & saved ONLY latest once.")
            else:
                # already in today's data, but still mark fp to prevent resend loop
                write_text_file(CRUNCHYROLL_LAST_FP_FILE, fp)
                logging.info("Crun: latest already in today's data; marked fp to avoid resend.")
    else:
        logging.warning("No entries in Crunchyroll feed.")

    # 2) YouTube: ONLY latest, ONLY if new
    await send_youtube_latest_if_new(bot)


if __name__ == "__main__":
    asyncio.run(run())
