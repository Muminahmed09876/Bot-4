#!/usr/bin/env python3
import os
import re
import aiohttp
import asyncio
import threading
from pathlib import Path
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.types import Message, BotCommand, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ParseMode
from PIL import Image
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata
import subprocess
import traceback
import json 
from flask import Flask, render_template_string
import requests
import time
import math
import logging
import yt_dlp
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# env
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "10000")) 
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME") 

TMP = Path("tmp")
TMP.mkdir(parents=True, exist_ok=True)

# state
USER_THUMBS = {}
TASKS = {}
SET_THUMB_REQUEST = set()
SUBSCRIBERS = set()
SET_CAPTION_REQUEST = set()
USER_CAPTIONS = {}
USER_COUNTERS = {}
EDIT_CAPTION_MODE = set()
USER_THUMB_TIME = {}
# সাময়িক ইমেইল জমা রাখার জন্য নতুন ডিকশনারি
USER_TEMP_EMAILS = {}

# --- STATE FOR AUDIO CHANGE ---
MKV_AUDIO_CHANGE_MODE = set()
PENDING_AUDIO_ORDERS = {} 
# ------------------------------

# --- NEW STATE FOR POST CREATION ---
CREATE_POST_MODE = set()
POST_CREATION_STATE = {} 

DEFAULT_POST_DATA = {
    'image_name': "Image Name",
    'genres': "",
    'season_list_raw': "1, 2" 
}
# ------------------------------------------------

# --- NEW STATE FOR BATCH CAPTION & QUEUE ---
BATCH_CAPTION_MODE = set()  # Users who turned 'on' batch mode
BATCH_DATA = {}            # {uid: [{'message': msg, 'file_info': info}, ...]}
BATCH_STATUS_MSG = {}      # {uid: message_id_of_status}

USER_QUEUES = {}           # {uid: asyncio.Queue()}
USER_WORKERS = {}          # {uid: asyncio.Task()}
USER_UPLOAD_LOCKS = {}     # {uid: asyncio.Lock()} - To ensure sequential uploads
# ------------------------------------------------

# --- YT-DLP STATE ---
YT_DATA = {} # Store info to handle callbacks
# --------------------

ADMIN_ID = int(os.getenv("ADMIN_ID", ""))
MAX_SIZE = 4 * 1024 * 1024 * 1024

# Updated workers to 1000 as requested
app = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=1000)
flask_app = Flask(__name__)

# ---- utilities ----
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def is_drive_url(url: str) -> bool:
    return "drive.google.com" in url or "docs.google.com" in url

def extract_drive_id(url: str) -> str:
    patterns = [
        r"/d/([a-zA-Z0-9_-]+)",
        r"id=([a-zA-Z0-9_-]+)",
        r"open\?id=([a-zA-Z0-9_-]+)",
        r"https://drive.google.com/file/d/([a-zA-Z0-9_-]+)/"
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None

def generate_new_filename(original_name: str) -> str:
    """Generates the new standardized filename while preserving the original extension."""
    BASE_NEW_NAME = "[@TA_HD_Anime] Telegram Channel"
    file_path = Path(original_name)
    file_ext = file_path.suffix.lower()
    
    file_ext = "." + file_ext.lstrip('.')
    
    if not file_ext or file_ext == '.':
        return BASE_NEW_NAME + ".mp4"
        
    return BASE_NEW_NAME + file_ext

def get_video_metadata(file_path: Path) -> dict:
    """Extracts duration, width, and height using FFprobe (with Hachoir fallback)."""
    data = {'duration': 0, 'width': 0, 'height': 0}
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-show_format", 
            str(file_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)
        metadata = json.loads(result.stdout)
        
        video_stream = None
        for stream in metadata.get('streams', []):
            if stream.get('codec_type') == 'video':
                video_stream = stream
                break
        
        if video_stream:
            data['width'] = int(video_stream.get('width', 0))
            data['height'] = int(video_stream.get('height', 0))
        
        duration_str = metadata.get('format', {}).get('duration')
        
        if not duration_str and video_stream:
            duration_str = video_stream.get('duration')
            
        if duration_str:
            try:
                data['duration'] = int(float(duration_str))
            except (ValueError, TypeError):
                logger.warning(f"Could not parse duration string: {duration_str}")
                data['duration'] = 0 
        
        if data['width'] == 0 or data['height'] == 0:
            raise Exception("FFprobe returned 0 dimensions, trying Hachoir")

    except Exception as e:
        logger.warning(f"FFprobe metadata extraction failed: {e}. Trying Hachoir fallback...")
        try:
            parser = createParser(str(file_path))
            if not parser:
                return data 
            with parser:
                h_metadata = extractMetadata(parser)
            if not h_metadata:
                return data 
            
            if h_metadata.has("duration") and data['duration'] == 0:
                data['duration'] = int(h_metadata.get("duration").total_seconds())
            if h_metadata.has("width") and data['width'] == 0:
                data['width'] = int(h_metadata.get("width"))
            if h_metadata.has("height") and data['height'] == 0:
                data['height'] = int(h_metadata.get("height"))
            logger.info(f"Hachoir fallback successful for {file_path}")
        except Exception as he:
            logger.error(f"Hachoir fallback ALSO failed: {he}")
    
    return data

def parse_time(time_str: str) -> int:
    total_seconds = 0
    parts = time_str.lower().split()
    for part in parts:
        if part.endswith('s'):
            total_seconds += int(part[:-1])
        elif part.endswith('m'):
            total_seconds += int(part[:-1]) * 60
        elif part.endswith('h'):
            total_seconds += int(part[:-1]) * 3600
    return total_seconds

def progress_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cancel ❌", callback_data="cancel_task")]])

def delete_caption_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Delete Caption 🗑️", callback_data="delete_caption")]])

def mode_check_keyboard(uid: int) -> InlineKeyboardMarkup:
    audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
    caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
    
    waiting_count = sum(1 for data in PENDING_AUDIO_ORDERS.values() if data['uid'] == uid)
    waiting_status = f" ({waiting_count}টি অর্ডার বাকি)" if waiting_count > 0 else ""
    
    keyboard = [
        [InlineKeyboardButton(f"MKV Audio Change Mode {audio_status}{waiting_status}", callback_data="toggle_audio_mode")],
        [InlineKeyboardButton(f"Edit Caption Mode {caption_status}", callback_data="toggle_caption_mode")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_audio_tracks_ffprobe(file_path: Path) -> list:
    """Uses ffprobe to get a list of audio streams with their index and title."""
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            str(file_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)
        metadata = json.loads(result.stdout)
        
        audio_tracks = []
        for stream in metadata.get('streams', []):
            if stream.get('codec_type') == 'audio':
                stream_index = stream.get('index') 
                title = stream.get('tags', {}).get('title', 'N/A')
                language = stream.get('tags', {}).get('language', 'und') 
                audio_tracks.append({
                    'stream_index': stream_index,
                    'title': title,
                    'language': language
                })
        return audio_tracks
    except Exception as e:
        logger.error(f"FFprobe error: {e}")
        return []

# --- NEW HELPER: Check if file has OPUS audio ---
def has_opus_audio(file_path: Path) -> bool:
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "a",
            "-show_entries", "stream=codec_name",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(file_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30)
        return "opus" in result.stdout.lower()
    except Exception as e:
        logger.error(f"Error checking OPUS audio: {e}")
        return False
# ------------------------------------------------

def format_size(bytes_size):
    if not bytes_size or bytes_size == 0:
        return "N/A"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(bytes_size, 1024)))
    p = math.pow(1024, i)
    s = round(bytes_size / p, 2)
    return "%s %s" % (s, size_name[i])

def generate_post_caption(data: dict) -> str:
    image_name = data.get('image_name', DEFAULT_POST_DATA['image_name'])
    genres = data.get('genres', DEFAULT_POST_DATA['genres'])
    season_list_raw = data.get('season_list_raw', DEFAULT_POST_DATA['season_list_raw'])

    season_entries = []
    
    parts = re.split(r'[,\s]+', season_list_raw.strip())
    parts = [p.strip() for p in parts if p.strip()]

    for part in parts:
        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                if start > end:
                    start, end = end, start
                for i in range(start, end + 1):
                    season_entries.append(f"**{image_name} Season {i:02d}**") 
            except ValueError:
                continue
        else:
            try:
                num = int(part)
                season_entries.append(f"**{image_name} Season {num:02d}**")
            except ValueError:
                continue

    unique_season_entries = list(dict.fromkeys(season_entries))
    if not unique_season_entries:
        unique_season_entries.append("**Coming Soon...**")
    elif unique_season_entries[-1] != "**Coming Soon...**" and unique_season_entries[0] != "**Coming Soon...**":
        unique_season_entries.append("**Coming Soon...**")
        
    season_text = "\n".join(unique_season_entries)

    base_caption = (
        f"**{image_name}**\n"
        f"**────────────────────**\n"
        f"**‣ Audio - Hindi Official**\n"
        f"**‣ Quality - 480p, 720p, 1080p**\n"
        f"**‣ Genres - {genres}**\n"
        f"**────────────────────**"
    )

    collapsible_text_parts = [
        f"> **{image_name} All Season List :-**", 
        "> " 
    ]
    
    for line in season_text.split('\n'):
        collapsible_text_parts.append(f"> {line}")
        collapsible_text_parts.append("> ") 
        
    if collapsible_text_parts and collapsible_text_parts[-1] == "> ":
        collapsible_text_parts.pop()
        
    collapsible_text = "\n".join(collapsible_text_parts)
    final_caption = f"{base_caption}\n\n{collapsible_text}"
    
    return final_caption


async def download_stream(resp, out_path: Path, message: Message = None, cancel_event: asyncio.Event = None):
    total = 0
    try:
        size = int(resp.headers.get("Content-Length", 0))
    except:
        size = 0
    chunk_size = 1024 * 1024
    try:
        with out_path.open("wb") as f:
            async for chunk in resp.content.iter_chunked(chunk_size):
                if cancel_event and cancel_event.is_set():
                    return False, "অপারেশন ব্যবহারকারী দ্বারা বাতিল করা হয়েছে।"
                if not chunk:
                    break
                if total > MAX_SIZE:
                    return False, "ফাইলের সাইজ 4GB এর বেশি হতে পারে না।"
                total += len(chunk)
                f.write(chunk)
    except Exception as e:
        return False, str(e)
    return True, None

async def download_url_generic(url: str, out_path: Path, message: Message = None, cancel_event: asyncio.Event = None):
    timeout = aiohttp.ClientTimeout(total=7200)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    connector = aiohttp.TCPConnector(limit=0, force_close=True)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as sess:
        try:
            async with sess.get(url, allow_redirects=True) as resp:
                if resp.status != 200:
                    return False, f"HTTP {resp.status}"
                return await download_stream(resp, out_path, message, cancel_event=cancel_event)
        except Exception as e:
            return False, str(e)

async def download_drive_file(file_id: str, out_path: Path, message: Message = None, cancel_event: asyncio.Event = None):
    base = f"https://drive.google.com/uc?export=download&id={file_id}"
    timeout = aiohttp.ClientTimeout(total=7200)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    connector = aiohttp.TCPConnector(limit=0, force_close=True)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as sess:
        try:
            async with sess.get(base, allow_redirects=True) as resp:
                if resp.status == 200 and "content-disposition" in (k.lower() for k in resp.headers.keys()):
                    return await download_stream(resp, out_path, message, cancel_event=cancel_event)
                text = await resp.text(errors="ignore")
                m = re.search(r"confirm=([0-9A-Za-z-_]+)", text)
                if m:
                    token = m.group(1)
                    download_url = f"https://drive.google.com/uc?export=download&confirm={token}&id={file_id}"
                    async with sess.get(download_url, allow_redirects=True) as resp2:
                        if resp2.status != 200:
                            return False, f"HTTP {resp2.status}"
                        return await download_stream(resp2, out_path, message, cancel_event=cancel_event)
                for k, v in resp.cookies.items():
                    if k.startswith("download_warning"):
                        token = v.value
                        download_url = f"https://drive.google.com/uc?export=download&confirm={token}&id={file_id}"
                        async with sess.get(download_url, allow_redirects=True) as resp2:
                            if resp2.status != 200:
                                return False, f"HTTP {resp2.status}"
                            return await download_stream(resp2, out_path, message, cancel_event=cancel_event)
                return False, "ডাউনলোডের জন্য Google Drive থেকে অনুমতি প্রয়োজন বা লিংক পাবলিক নয়।"
        except Exception as e:
            return False, str(e)

async def set_bot_commands():
    cmds = [
        BotCommand("start", "বট চালু/হেল্প"),
        BotCommand("upload_url", "URL থেকে ফাইল ডাউনলোড ও আপলোড (admin only)"),
        BotCommand("setthumb", "কাস্টম থাম্বনেইল সেট করুন (admin only)"),
        BotCommand("view_thumb", "আপনার থাম্বনেইল দেখুন (admin only)"),
        BotCommand("del_thumb", "আপনার থাম্বনেইল মুছে ফেলুন (admin only)"),
        BotCommand("set_caption", "কাস্টম ক্যাপশন সেট করুন (admin only)"),
        BotCommand("view_caption", "আপনার ক্যাপশন দেখুন (admin only)"),
        BotCommand("edit_caption_mode", "শুধু ক্যাপশন এডিট করুন (admin only)"),
        BotCommand("rename", "reply করা ভিডিও রিনেম করুন (admin only)"),
        BotCommand("mkv_video_audio_change", "MKV ভিডিওর অডিও ট্র্যাক পরিবর্তন (admin only)"),
        BotCommand("create_post", "নতুন পোস্ট তৈরি করুন (admin only)"), 
        BotCommand("mode_check", "বর্তমান মোড স্ট্যাটাস চেক করুন (admin only)"), 
        BotCommand("gen", "৬টি সাময়িক ইমেইল তৈরি করুন"),
        BotCommand("check", "ইমেইল ইনবক্স চেক করুন"),
        BotCommand("broadcast", "ব্রডকাস্ট (কেবল অ্যাডমিন)"),
        BotCommand("help", "সহায়িকা")
    ]
    try:
        await app.set_bot_commands(cmds)
    except Exception as e:
        logger.warning("Set commands error: %s", e)

async def sequential_upload_task(uid, client, message, tmp_path, renamed_file, status_msg_id, cancel_event):
    """Background task that waits for upload lock to ensure sequential uploads."""
    if uid not in USER_UPLOAD_LOCKS:
        USER_UPLOAD_LOCKS[uid] = asyncio.Lock()
    
    async with USER_UPLOAD_LOCKS[uid]:
        if cancel_event.is_set():
            if tmp_path.exists(): tmp_path.unlink()
            return
        # Now we possess the upload lock. Video 1 will hold this until done.
        # Video 2 (already downloaded) will wait here.
        await process_file_and_upload(client, message, tmp_path, original_name=renamed_file, messages_to_delete=[status_msg_id], cancel_event_passed=cancel_event)

# --- QUEUE WORKER ---
async def process_queue_handler(uid, client):
    """Worker function that processes tasks sequentially for a user."""
    queue = USER_QUEUES[uid]
    while not queue.empty():
        task_data = await queue.get()
        try:
            m = task_data.get('message')
            original_name = task_data.get('original_name')
            # NEW: Get the existing status message passed from the handler
            status_msg = task_data.get('status_msg') 
            
            # Start Processing
            file_info = m.video or m.document
            
            cancel_event = asyncio.Event()
            TASKS.setdefault(uid, []).append(cancel_event)

            # --- CHANGE: Do NOT send "Processing started" again. Just switch to Downloading. ---
            # Using the passed status_msg to edit directly.
            
            tmp_path = TMP / f"forwarded_{uid}_{int(datetime.now().timestamp())}_{original_name}"
            
            try:
                # 1. Download Phase (Sequential)
                if status_msg:
                    try:
                        await status_msg.edit("ডাউনলোড হচ্ছে...", reply_markup=progress_keyboard())
                    except: pass
                
                await m.download(file_name=str(tmp_path))
                
                if cancel_event.is_set():
                     if tmp_path.exists(): tmp_path.unlink()
                     TASKS[uid].remove(cancel_event)
                     continue

                try:
                    if status_msg:
                        await status_msg.edit("ডাউনলোড সম্পন্ন, Telegram-এ আপলোড হচ্ছে...", reply_markup=None)
                except Exception:
                    pass

                renamed_file = generate_new_filename(original_name)
                
                # 2. Upload Phase (Pipelined)
                asyncio.create_task(
                    sequential_upload_task(uid, client, m, tmp_path, renamed_file, status_msg.id if status_msg else None, cancel_event)
                )
            
            except Exception as e:
                logger.error(f"Queue Item Failed: {e}")
                if status_msg:
                    await status_msg.edit(f"Queue Error: {e}")
                else:
                    await m.reply_text(f"Queue Error for `{original_name}`: {e}")
                if tmp_path.exists():
                    tmp_path.unlink()
            finally:
                # We don't remove cancel event here because it is passed to upload task
                # It will be removed in process_file_and_upload
                pass

        except Exception as e:
            logger.error(f"Queue Loop Error: {e}")
        finally:
            queue.task_done()
    
    # Cleanup when queue is empty
    del USER_WORKERS[uid]
    del USER_QUEUES[uid]

# ---- handlers ----
@app.on_message(filters.command("start") & filters.private)
async def start_handler(c, m: Message):
    await set_bot_commands()
    SUBSCRIBERS.add(m.chat.id)
    text = (
        "Hi! আমি URL uploader bot.\n\n"
        "নোট: বটের অনেক কমান্ড শুধু অ্যাডমিন (owner) চালাতে পারবে।\n\n"
        "Commands:\n"
        "/upload_url <url> - URL থেকে ফাইল ডাউনলোড ও Telegram-এ আপলোড (admin only)\n"
        "/setthumb - একটি ছবি পাঠান, সেট হবে আপনার থাম্বনেইল (admin only)\n"
        "/view_thumb - আপনার থাম্বনেইল দেখুন (admin only)\n"
        "/del_thumb - আপনার থাম্বনেইল মুছে ফেলুন (admin only)\n"
        "/set_caption - একটি ক্যাপশন সেট করুন (admin only)\n"
        "/view_caption - আপনার ক্যাপশন দেখুন (admin only)\n"
        "/edit_caption_mode - শুধু ক্যাপশন এডিট করার মোড টগল করুন (admin only)\n"
        "/rename <newname.ext> - reply করা ভিডিও রিনেম করুন (admin only)\n"
        "/mkv_video_audio_change - MKV ভিডিওর অডিও ট্র্যাক পরিবর্তন মোড টগল করুন (admin only)\n"
        "/create_post - নতুন পোস্ট তৈরি করুন (admin only)\n" 
        "/mode_check - বর্তমান মোড স্ট্যাটাস চেক করুন এবং পরিবর্তন করুন (admin only)\n" 
        "/broadcast <text> - ব্রডকাস্ট (শুধুমাত্র অ্যাডমিন)\n"
        "/help - সাহায্য"
    )
    await m.reply_text(text)

@app.on_message(filters.command("help") & filters.private)
async def help_handler(c, m):
    await start_handler(c, m)

@app.on_message(filters.command("setthumb") & filters.private)
async def setthumb_prompt(c, m):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    uid = m.from_user.id
    if len(m.command) > 1:
        time_str = " ".join(m.command[1:])
        seconds = parse_time(time_str)
        if seconds > 0:
            USER_THUMB_TIME[uid] = seconds
            await m.reply_text(f"থাম্বনেইল তৈরির সময় সেট হয়েছে: {seconds} সেকেন্ড।")
        else:
            await m.reply_text("সঠিক ফরম্যাটে সময় দিন। উদাহরণ: `/setthumb 5s`, `/setthumb 1m`, `/setthumb 1m 30s`")
    else:
        SET_THUMB_REQUEST.add(uid)
        await m.reply_text("একটি ছবি পাঠান (photo) — সেট হবে আপনার থাম্বনেইল।")


@app.on_message(filters.command("view_thumb") & filters.private)
async def view_thumb_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    uid = m.from_user.id
    thumb_path = USER_THUMBS.get(uid)
    thumb_time = USER_THUMB_TIME.get(uid)
    
    if thumb_path and Path(thumb_path).exists():
        await c.send_photo(chat_id=m.chat.id, photo=thumb_path, caption="এটা আপনার সেভ করা থাম্বনেইল।")
    elif thumb_time:
        await m.reply_text(f"আপনার থাম্বনেইল তৈরির সময় সেট করা আছে: {thumb_time} সেকেন্ড।")
    else:
        await m.reply_text("আপনার কোনো থাম্বনেইল বা থাম্বনেইল তৈরির সময় সেভ করা নেই। /setthumb দিয়ে সেট করুন।")

@app.on_message(filters.command("del_thumb") & filters.private)
async def del_thumb_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    uid = m.from_user.id
    thumb_path = USER_THUMBS.get(uid)
    if thumb_path and Path(thumb_path).exists():
        try:
            Path(thumb_path).unlink()
        except Exception:
            pass
        USER_THUMBS.pop(uid, None)
    
    if uid in USER_THUMB_TIME:
        USER_THUMB_TIME.pop(uid)

    if not (thumb_path or uid in USER_THUMB_TIME):
        await m.reply_text("আপনার কোনো থাম্বনেইল সেভ করা নেই।")
    else:
        await m.reply_text("আপনার থাম্বনেইল/থাম্বনেইল তৈরির সময় মুছে ফেলা হয়েছে।")


@app.on_message(filters.photo & filters.private)
async def photo_handler(c, m: Message):
    if not is_admin(m.from_user.id):
        return
    uid = m.from_user.id

    # /gen কমান্ড: ৬টি নতুন ইমেইল তৈরি করবে এবং আগেরগুলো মুছে ফেলবে
@app.on_message(filters.command("gen") & filters.private)
async def gen_temp_emails(c, m: Message):
    if not is_admin(m.from_user.id): return
    uid = m.from_user.id
    
    # API থেকে ৬টি ইমেইল নেওয়া
    async with aiohttp.ClientSession() as session:
        async with session.get("https://www.1secmail.com/api/v1/?action=genAddrs&count=6") as resp:
            new_emails = await resp.json()
    
    # আগের ইমেইল ডিলিট করে নতুনগুলো সেভ করা
    USER_TEMP_EMAILS[uid] = new_emails
    
    text = "📧 **আপনার নতুন ৬টি সাময়িক ইমেইল:**\n\n"
    for i, email in enumerate(new_emails, 1):
        text += f"{i}. `{email}`\n"
    text += "\n⚠️ নতুন করে /gen দিলে এই ইমেইলগুলো আর কাজ করবে না।\n📥 চেক করতে: `/check [email]`"
    
    await m.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# /check কমান্ড: নির্দিষ্ট ইমেইলের ইনবক্স চেক করবে
@app.on_message(filters.command("check") & filters.private)
async def check_temp_email(c, m: Message):
    if not is_admin(m.from_user.id): return
    uid = m.from_user.id
    
    try:
        input_email = m.text.split()[1]
        
        # ইমেইলটি বর্তমান লিস্টে আছে কি না যাচাই
        if uid not in USER_TEMP_EMAILS or input_email not in USER_TEMP_EMAILS[uid]:
            await m.reply_text("❌ এই ইমেইলটি এখন আর সচল নেই। নতুন করে /gen দিন।")
            return

        user, domain = input_email.split('@')
        url = f"https://www.1secmail.com/api/v1/?action=getMessages&login={user}&domain={domain}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                msgs = await resp.json()
        
        if not msgs:
            await m.reply_text("📭 ইনবক্স খালি।")
        else:
            for msg in msgs:
                content_url = f"https://www.1secmail.com/api/v1/?action=readMessage&login={user}&domain={domain}&id={msg['id']}"
                async with session.get(content_url) as resp2:
                    content = await resp2.json()
                
                output = f"📩 **নতুন মেইল!**\n\n**থেকে:** {msg['from']}\n**বিষয়:** {msg['subject']}\n\n**বার্তা:**\n{content['textBody']}"
                await m.reply_text(output)
    except Exception:
        await m.reply_text("❗ সঠিক ফরম্যাট: `/check example@1secmail.com`")
    
    # --- NEW: Handle Create Post Mode ---
    if uid in CREATE_POST_MODE and uid in POST_CREATION_STATE and POST_CREATION_STATE[uid]['state'] == 'awaiting_image':
        
        state_data = POST_CREATION_STATE[uid]
        state_data['message_ids'].append(m.id) 
        
        out = TMP / f"post_img_{uid}.jpg"
        try:
            download_msg = await m.reply_text("ছবি ডাউনলোড হচ্ছে...")
            state_data['message_ids'].append(download_msg.id)
            
            await m.download(file_name=str(out))
            img = Image.open(out)
            img.thumbnail((1080, 1080)) 
            img = img.convert("RGB")
            img.save(out, "JPEG")
            
            state_data['image_path'] = str(out)
            state_data['state'] = 'awaiting_name_change'
            
            initial_caption = generate_post_caption(state_data['post_data'])
            
            post_msg = await c.send_photo(
                chat_id=m.chat.id, 
                photo=str(out), 
                caption=initial_caption, 
                parse_mode=ParseMode.MARKDOWN
            )
            state_data['post_message_id'] = post_msg.id 
            state_data['message_ids'].append(post_msg.id) 
            
            prompt_msg = await m.reply_text(
                f"✅ পোস্টের ছবি সেট হয়েছে।\n\n**এখন ছবির নামটি পরিবর্তন করুন।**\n"
                f"বর্তমান নাম: `{state_data['post_data']['image_name']}`\n"
                f"অনুগ্রহ করে শুধু **নামটি** পাঠান। উদাহরণ: `One Piece`"
            )
            state_data['message_ids'].append(prompt_msg.id)

        except Exception as e:
            logger.error(f"Post creation image error: {e}")
            await m.reply_text(f"ছবি সেভ করতে সমস্যা: {e}")
            CREATE_POST_MODE.discard(uid)
            POST_CREATION_STATE.pop(uid, None)
            if out.exists(): out.unlink(missing_ok=True)
        return
    # --- END NEW: Handle Create Post Mode ---
    
    if uid in SET_THUMB_REQUEST:
        SET_THUMB_REQUEST.discard(uid)
        out = TMP / f"thumb_{uid}.jpg"
        try:
            await m.download(file_name=str(out))
            img = Image.open(out)
            img.thumbnail((320, 320))
            img = img.convert("RGB")
            img.save(out, "JPEG")
            USER_THUMBS[uid] = str(out)
            USER_THUMB_TIME.pop(uid, None)
            await m.reply_text("আপনার থাম্বনেইল সেভ হয়েছে।")
        except Exception as e:
            await m.reply_text(f"থাম্বনেইল সেভ করতে সমস্যা: {e}")
    else:
        pass

# Handlers for caption
@app.on_message(filters.command("set_caption") & filters.private)
async def set_caption_prompt(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    SET_CAPTION_REQUEST.add(m.from_user.id)
    USER_COUNTERS.pop(m.from_user.id, None)
    
    await m.reply_text(
        "ক্যাপশন দিন। এখন আপনি এই কোডগুলো ব্যবহার করতে পারবেন:\n"
        "1. **নম্বর বৃদ্ধি:** `[01]`, `[(01)]` (নম্বর স্বয়ংক্রিয়ভাবে বাড়বে)\n"
        "2. **গুণমানের সাইকেল:** `[re (480p, 720p)]`\n"
        "3. **শর্তসাপেক্ষ টেক্সট (নতুন):** `[TEXT (XX)]` - যেমন: `[End (02)]`, `[hi (05)]` (যদি বর্তমান পর্বের নম্বর `XX` এর **সমান** হয়, তাহলে `TEXT` যোগ হবে)।"
    )

@app.on_message(filters.command("view_caption") & filters.private)
async def view_caption_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    uid = m.from_user.id
    caption = USER_CAPTIONS.get(uid)
    if caption:
        await m.reply_text(f"আপনার সেভ করা ক্যাপশন:\n\n`{caption}`", reply_markup=delete_caption_keyboard())
    else:
        await m.reply_text("আপনার কোনো ক্যাপশন সেভ করা নেই। /set_caption দিয়ে সেট করুন।")

@app.on_callback_query(filters.regex("delete_caption"))
async def delete_caption_cb(c, cb):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return
    if uid in USER_CAPTIONS:
        USER_CAPTIONS.pop(uid)
        USER_COUNTERS.pop(uid, None) 
        await cb.message.edit_text("আপনার ক্যাপশন মুছে ফেলা হয়েছে।")
    else:
        await cb.answer("আপনার কোনো ক্যাপশন সেভ করা নেই।", show_alert=True)

# Handler to toggle edit caption mode
@app.on_message(filters.command("edit_caption_mode") & filters.private)
async def toggle_edit_caption_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in EDIT_CAPTION_MODE:
        EDIT_CAPTION_MODE.discard(uid)
        # Clear batch if active
        if uid in BATCH_CAPTION_MODE:
            BATCH_CAPTION_MODE.discard(uid)
            BATCH_DATA.pop(uid, None)
            BATCH_STATUS_MSG.pop(uid, None)
        await m.reply_text("edit video caption mod **OFF**.\nএখন থেকে আপলোড করা ভিডিওর রিনেম ও থাম্বনেইল পরিবর্তন হবে, এবং সেভ করা ক্যাপশন যুক্ত হবে।")
    else:
        EDIT_CAPTION_MODE.add(uid)
        await m.reply_text("edit video caption mod **ON**.\nএখন থেকে শুধু সেভ করা ক্যাপশন ভিডিওতে যুক্ত হবে। ভিডিওর নাম এবং থাম্বনেইল একই থাকবে।\n\n**New Feature:** ফাইল আইডি সেভ মোড অন করতে `on` লিখুন। অফ করতে `off` লিখুন।")

# --- HANDLER: /mkv_video_audio_change ---
@app.on_message(filters.command("mkv_video_audio_change") & filters.private)
async def toggle_audio_change_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in MKV_AUDIO_CHANGE_MODE:
        MKV_AUDIO_CHANGE_MODE.discard(uid)
        await m.reply_text("MKV অডিও পরিবর্তন মোড **অফ** করা হয়েছে।")
    else:
        MKV_AUDIO_CHANGE_MODE.add(uid)
        await m.reply_text("MKV অডিও পরিবর্তন মোড **অন** করা হয়েছে। এখন আপনি একটি **MKV ফাইল** অথবা অন্য কোনো **ভিডিও ফাইল** পাঠান।\n(এই মোড ম্যানুয়ালি অফ না করা পর্যন্ত চালু থাকবে।)")

# --- NEW HANDLER: /create_post ---
@app.on_message(filters.command("create_post") & filters.private)
async def toggle_create_post_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in CREATE_POST_MODE:
        CREATE_POST_MODE.discard(uid)
        if uid in POST_CREATION_STATE:
            state_data = POST_CREATION_STATE.pop(uid)
            try:
                if state_data.get('image_path'):
                    Path(state_data['image_path']).unlink(missing_ok=True)
                messages_to_delete = state_data.get('message_ids', [])
                post_id = state_data.get('post_message_id')
                if post_id and post_id in messages_to_delete:
                    messages_to_delete.remove(post_id) 
                if messages_to_delete:
                    await c.delete_messages(m.chat.id, messages_to_delete)
            except Exception as e:
                logger.warning(f"Post mode OFF cleanup error: {e}")
                
        await m.reply_text("Create Post Mode **অফ** করা হয়েছে।")
    else:
        CREATE_POST_MODE.add(uid)
        POST_CREATION_STATE[uid] = {
            'image_path': None, 
            'message_ids': [m.id], 
            'state': 'awaiting_image', 
            'post_data': DEFAULT_POST_DATA.copy(),
            'post_message_id': None
        }
        await m.reply_text("Create Post Mode **অন** করা হয়েছে।\nএকটি ছবি (**Photo**) পাঠান যা পোস্টের ইমেজ হিসেবে ব্যবহার হবে।")
# ---------------------------------------------


# --- NEW HANDLER: /mode_check ---
@app.on_message(filters.command("mode_check") & filters.private)
async def mode_check_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
    caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
    
    waiting_count = sum(1 for data in PENDING_AUDIO_ORDERS.values() if data['uid'] == uid)
    waiting_status_text = f"{waiting_count}টি ফাইল ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে।" if waiting_count > 0 else "কোনো ফাইল অপেক্ষা করছে না।"
    
    status_text = (
        "🤖 **বর্তমান মোড স্ট্যাটাস:**\n\n"
        f"1. **MKV Audio Change Mode:** `{audio_status}`\n"
        f"   - *কাজ:* ফরওয়ার্ড/ডাউনলোড করা MKV/ভিডিও ফাইলের অডিও ট্র্যাক অর্ডার পরিবর্তন করে। (ম্যানুয়ালি অফ না করা পর্যন্ত ON থাকবে)\n"
        f"   - *স্ট্যাটাস:* {waiting_status_text}\n\n"
        f"2. **Edit Caption Mode:** `{caption_status}`\n"
        f"   - *কাজ:* ফরওয়ার্ড করা ভিডিওর রিনেম বা থাম্বনেইল পরিবর্তন না করে শুধু সেভ করা ক্যাপশন যুক্ত করে।\n\n"
        "নিচের বাটনগুলিতে ক্লিক করে মোড পরিবর্তন করুন।"
    )
    
    await m.reply_text(status_text, reply_markup=mode_check_keyboard(uid), parse_mode=ParseMode.MARKDOWN)

# --- NEW CALLBACK: Mode Toggle Buttons ---
@app.on_callback_query(filters.regex("toggle_(audio|caption)_mode"))
async def mode_toggle_callback(c: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return

    action = cb.data
    
    if action == "toggle_audio_mode":
        if uid in MKV_AUDIO_CHANGE_MODE:
            MKV_AUDIO_CHANGE_MODE.discard(uid)
            message = "MKV Audio Change Mode OFF."
        else:
            MKV_AUDIO_CHANGE_MODE.add(uid)
            message = "MKV Audio Change Mode ON."
            
    elif action == "toggle_caption_mode":
        if uid in EDIT_CAPTION_MODE:
            EDIT_CAPTION_MODE.discard(uid)
            message = "Edit Caption Mode OFF."
        else:
            EDIT_CAPTION_MODE.add(uid)
            message = "Edit Caption Mode ON."
            
    try:
        audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
        caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
        
        waiting_count = sum(1 for data in PENDING_AUDIO_ORDERS.values() if data['uid'] == uid)
        waiting_status_text = f"{waiting_count}টি ফাইল ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে।" if waiting_count > 0 else "কোনো ফাইল অপেক্ষা করছে না।"

        status_text = (
            "🤖 **বর্তমান মোড স্ট্যাটাস:**\n\n"
            f"1. **MKV Audio Change Mode:** `{audio_status}`\n"
            f"   - *কাজ:* ফরওয়ার্ড/ডাউনলোড করা MKV/ভিডিও ফাইলের অডিও ট্র্যাক অর্ডার পরিবর্তন করে। (ম্যানুয়ালি অফ না করা পর্যন্ত ON থাকবে)\n"
            f"   - *স্ট্যাটাস:* {waiting_status_text}\n\n"
            f"2. **Edit Caption Mode:** `{caption_status}`\n"
            f"   - *কাজ:* ফরওয়ার্ড করা ভিডিওর রিনেম বা থাম্বনেইল পরিবর্তন না করে শুধু সেভ করা ক্যাপশন যুক্ত করে।\n\n"
            "নিচের বাটনগুলিতে ক্লিক করে মোড পরিবর্তন করুন।"
        )
        
        await cb.message.edit_text(status_text, reply_markup=mode_check_keyboard(uid), parse_mode=ParseMode.MARKDOWN)
        await cb.answer(message, show_alert=True)
    except Exception as e:
        logger.error(f"Callback edit error: {e}")
        await cb.answer(message, show_alert=True)


@app.on_message(filters.text & filters.private)
async def text_handler(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return
    text = m.text.strip()
    
    # --- BATCH CAPTION COMMANDS (NEW) ---
    if uid in EDIT_CAPTION_MODE:
        if text.lower() == "on":
            BATCH_CAPTION_MODE.add(uid)
            BATCH_DATA[uid] = []
            await m.reply_text("Batch Caption Mode ON. এখন ভিডিও ফরওয়ার্ড করলে ফাইল আইডি সেভ হবে।")
            return
        elif text.lower() == "off":
            BATCH_CAPTION_MODE.discard(uid)
            BATCH_DATA.pop(uid, None)
            BATCH_STATUS_MSG.pop(uid, None)
            await m.reply_text("Batch Caption Mode OFF. এখন ভিডিও ফরওয়ার্ড করলে সরাসরি ক্যাপশন পরিবর্তন হবে।")
            return
        elif text.lower() == "ok":
            if uid in BATCH_CAPTION_MODE and uid in BATCH_DATA and BATCH_DATA[uid]:
                items = BATCH_DATA[uid]
                await m.reply_text(f"Processing started for {len(items)} items...")
                
                # Process strictly in order (Insertion order matches user forward order)
                # Since we appended, [0] is oldest, [-1] is newest.
                
                for item in items:
                    msg_obj = item['message']
                    file_info_obj = item['file_info']
                    await handle_caption_only_upload_with_file(c, msg_obj, file_info_obj)
                    # Small delay to ensure order in TG network slightly
                    await asyncio.sleep(0.5)
                
                # Cleanup
                BATCH_DATA[uid] = []
                # Remove status msg
                if uid in BATCH_STATUS_MSG:
                    try:
                        await c.delete_messages(m.chat.id, BATCH_STATUS_MSG[uid])
                    except: pass
                    BATCH_STATUS_MSG.pop(uid, None)
                
                # মেসেজ পাঠানো
                complete_msg = await m.reply_text("Batch processing complete.")
                
                # সরাসরি অপেক্ষা করে ডিলিট করা
                async def auto_delete():
                    await asyncio.sleep(5) # ৫ সেকেন্ড অপেক্ষা
                    try:
                        await complete_msg.delete()
                    except:
                        pass
                
                # এটি টাস্ক হিসেবে রান করুন
                asyncio.ensure_future(auto_delete())
                
            else:
                await m.reply_text("Batch list is empty or mode is not ON.")
            return
    # ------------------------------------

    # Handle set caption request
    if uid in SET_CAPTION_REQUEST:
        SET_CAPTION_REQUEST.discard(uid)
        USER_CAPTIONS[uid] = text
        USER_COUNTERS.pop(uid, None) 
        await m.reply_text("আপনার ক্যাপশন সেভ হয়েছে। এখন থেকে আপলোড করা ভিডিওতে এই ক্যাপশন ব্যবহার হবে।")
        return

    # --- Handle audio order input (MODIFIED) ---
    if m.reply_to_message and m.reply_to_message.id in PENDING_AUDIO_ORDERS:
        prompt_message_id = m.reply_to_message.id
        file_data = PENDING_AUDIO_ORDERS.get(prompt_message_id)
        
        if file_data['uid'] != uid:
             await m.reply_text("আপনি এই ফাইলের জন্য অর্ডার দিতে পারবেন না।")
             return

        tracks = file_data['tracks']
        try:
            # Parse input like "1,3" or "2"
            new_order_str = [x.strip() for x in text.split(',') if x.strip()]
            num_tracks_in_file = len(tracks)
            
            # --- UPDATED VALIDATION: Allow any subset ---
            if not new_order_str:
                 await m.reply_text("আপনাকে অন্তত একটি ট্র্যাক নম্বর দিতে হবে।")
                 return

            new_stream_map = []
            valid_user_indices = list(range(1, num_tracks_in_file + 1))
            
            for user_track_num_str in new_order_str:
                user_track_num = int(user_track_num_str) 
                if user_track_num not in valid_user_indices:
                     await m.reply_text(f"ভুল ট্র্যাক নম্বর: {user_track_num}। ট্র্যাক নম্বরগুলো হতে হবে: {', '.join(map(str, valid_user_indices))}")
                     return
                
                stream_index_to_map = tracks[user_track_num - 1]['stream_index']
                new_stream_map.append(f"0:{stream_index_to_map}") 
            # --------------------------------------------

            asyncio.create_task(
                handle_audio_remux(
                    c, m, file_data['path'], 
                    file_data['original_name'], 
                    new_stream_map, 
                    messages_to_delete=[prompt_message_id, m.id]
                )
            )

            PENDING_AUDIO_ORDERS.pop(prompt_message_id, None) 
            return

        except ValueError:
            await m.reply_to_message.reply_text("ভুল ফরম্যাট। কমা-সেপারেটেড সংখ্যা দিন। উদাহরণ: `1,3`")
            return
        except Exception as e:
            logger.error(f"Audio remux preparation error: {e}")
            await m.reply_to_message.reply_text(f"অডিও পরিবর্তন প্রক্রিয়া শুরু করতে সমস্যা: {e}")
            
            try: Path(file_data['path']).unlink(missing_ok=True)
            except Exception: pass
            PENDING_AUDIO_ORDERS.pop(prompt_message_id, None)
            return
    # -----------------------------------------------------

    # --- NEW: Handle Post Creation Editing Steps ---
    if uid in CREATE_POST_MODE and uid in POST_CREATION_STATE:
        state_data = POST_CREATION_STATE[uid]
        state_data['message_ids'].append(m.id) 
        
        current_state = state_data['state']
        
        if current_state == 'awaiting_name_change':
            if not text:
                prompt_msg = await m.reply_text("নাম খালি রাখা যাবে না। সঠিক নামটি দিন।")
                state_data['message_ids'].append(prompt_msg.id)
                return
            
            state_data['post_data']['image_name'] = text
            state_data['state'] = 'awaiting_genres_add'
            
            new_caption = generate_post_caption(state_data['post_data'])
            try:
                await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"Edit caption error in name change: {e}")
                await m.reply_text("ক্যাপশন এডিট করতে সমস্যা হয়েছে। প্রক্রিয়া বাতিল করা হচ্ছে। /create_post দিয়ে মোড অফ করুন।")
                return

            prompt_msg = await m.reply_text(
                f"✅ ছবির নাম সেট হয়েছে: `{text}`\n\n**এখন Genres যোগ করুন।**\n"
                f"উদাহরণ: `Comedy, Romance, Action`"
            )
            state_data['message_ids'].append(prompt_msg.id)
            
        elif current_state == 'awaiting_genres_add':
            state_data['post_data']['genres'] = text 
            state_data['state'] = 'awaiting_season_list'
            
            new_caption = generate_post_caption(state_data['post_data'])
            try:
                await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"Edit caption error in genres add: {e}")
                await m.reply_text("ক্যাপশন এডিট করতে সমস্যা হয়েছে। প্রক্রিয়া বাতিল করা হচ্ছে। /create_post দিয়ে মোড অফ করুন।")
                return

            prompt_msg = await m.reply_text(
                f"✅ Genres সেট হয়েছে: `{text}`\n\n**এখন Season List পরিবর্তন করুন।**\n"
                f"Change Season List এর মানে \"{state_data['post_data']['image_name']}\" Season 01 কয়টি add করব?\n"
                f"ফরম্যাট: সিজন নম্বর অথবা রেঞ্জ কমা বা স্পেস-সেপারেটেড দিন।\n"
                f"উদাহরণ:\n"
                f"‣ `1` (Season 01)\n"
                f"‣ `1-2` (Season 01 থেকে Season 02)\n"
                f"‣ `1-2 4-5` বা `1-2, 4-5` (Season 01-02 এবং 04-05)"
            )
            state_data['message_ids'].append(prompt_msg.id)
            
        elif current_state == 'awaiting_season_list':
            if not text.strip():
                state_data['post_data']['season_list_raw'] = ""
            else:
                state_data['post_data']['season_list_raw'] = text
            
            new_caption = generate_post_caption(state_data['post_data'])
            try:
                await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"Edit caption error in season list: {e}")
                await m.reply_text("ক্যাপশন এডিট করতে সমস্যা হয়েছে। প্রক্রিয়া বাতিল করা হচ্ছে। /create_post দিয়ে মোড অফ করুন।")
                return

            all_messages = state_data.get('message_ids', [])
            post_id = state_data.get('post_message_id')
            if post_id and post_id in all_messages:
                all_messages.remove(post_id) 
            if all_messages:
                try:
                    await c.delete_messages(m.chat.id, all_messages)
                except Exception as e:
                    logger.warning(f"Error deleting post creation messages: {e}")
            
            image_path = state_data['image_path']
            if image_path and Path(image_path).exists():
                Path(image_path).unlink(missing_ok=True)
            
            CREATE_POST_MODE.discard(uid)
            POST_CREATION_STATE.pop(uid, None)
            
            await m.reply_text("✅ পোস্ট তৈরি সফলভাবে সম্পন্ন হয়েছে এবং সমস্ত অতিরিক্ত বার্তা মুছে ফেলা হয়েছে।")
            return
    # --- END NEW: Handle Post Creation Editing Steps ---


    if text.startswith("http://") or text.startswith("https://"):
        asyncio.create_task(handle_url_download_and_upload(c, m, text))
    
@app.on_message(filters.command("upload_url") & filters.private)
async def upload_url_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not m.command or len(m.command) < 2:
        await m.reply_text("ব্যবহার: /upload_url <url>\nউদাহরণ: /upload_url https://example.com/file.mp4")
        return
    url = m.text.split(None, 1)[1].strip()
    asyncio.create_task(handle_url_download_and_upload(c, m, url))

async def handle_url_download_and_upload(c: Client, m: Message, url: str):
    uid = m.from_user.id
    
    # Try generic fallback if it looks like a direct link/drive to avoid yt-dlp delay if possible
    # But user requested buttons, so we use yt-dlp first.
    
    try:
        # Don't send "Analyzing..." just go straight to options if fast, 
        # but to be safe and responsive, send a temp message
        status_msg = await m.reply_text("Searching formats...", reply_markup=progress_keyboard())

        ydl_opts = {'noplaylist': True, 'quiet': True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info = await asyncio.to_thread(ydl.extract_info, url, download=False)
            except Exception as e:
                # If yt-dlp fails, try generic fallback
                if is_drive_url(url):
                     await download_and_process_generic(c, m, url, status_msg)
                     return
                else:
                     await status_msg.edit(f"URL Extract Error: {e}")
                     return

        formats = info.get('formats', [])
        valid_formats = []
        for f in formats:
            if f.get('vcodec') != 'none' or f.get('height'): 
                valid_formats.append(f)
        
        valid_formats.sort(key=lambda x: x.get('height', 0) or 0, reverse=True)
        
        buttons = []
        # Best Quality
        key_best = f"ytdl_{uid}_best"
        YT_DATA[key_best] = {'url': url, 'format_id': 'bestvideo+bestaudio/best', 'info': info, 'msg_id': status_msg.id}
        buttons.append([InlineKeyboardButton(f"Best Quality", callback_data=key_best)])

        seen_res = set()
        for f in valid_formats:
            h = f.get('height')
            if not h: continue
            res_str = f"{h}p"
            if res_str in seen_res: continue
            seen_res.add(res_str)
            
            ext = f.get('ext', 'mp4')
            key = f"ytdl_{uid}_{f['format_id']}"
            YT_DATA[key] = {'url': url, 'format_id': f['format_id'], 'info': info, 'msg_id': status_msg.id}
            
            # Button text without size as requested
            btn_text = f"{res_str} | {ext}"
            buttons.append([InlineKeyboardButton(btn_text, callback_data=key)])
        
        # Add MP3 Button
        key_mp3 = f"ytdl_{uid}_mp3"
        YT_DATA[key_mp3] = {'url': url, 'format_id': 'bestaudio/best', 'info': info, 'msg_id': status_msg.id, 'is_audio': True}
        buttons.append([InlineKeyboardButton("🎵 MP3 (Audio Only)", callback_data=key_mp3)])

        buttons.append([InlineKeyboardButton("Cancel ❌", callback_data="cancel_task")])
        
        await status_msg.edit(
            f"**{info.get('title', 'Video')}**\nSelect Quality:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        
    except Exception as e:
        logger.error(f"YTDL Error: {e}")
        try:
            await status_msg.edit(f"Error: {e}")
        except:
             await m.reply_text(f"Error: {e}")

@app.on_callback_query(filters.regex(r"^ytdl_"))
async def ytdl_callback(c: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    key = cb.data
    data = YT_DATA.get(key)
    
    if not data:
        await cb.answer("Data expired.", show_alert=True)
        return
        
    await cb.answer("Download started...")
    
    url = data['url']
    fmt = data['format_id']
    info = data['info']
    msg_id = data['msg_id']
    is_audio = data.get('is_audio', False)
    
    try:
        await c.edit_message_reply_markup(cb.message.chat.id, msg_id, reply_markup=None)
    except: pass
    
    status_msg = await c.get_messages(cb.message.chat.id, msg_id)
    
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    try:
        title = info.get('title', 'video')
        timestamp = int(datetime.now().timestamp())
        
        # Audio handling logic
        if is_audio:
            out_tmpl = str(TMP / f"dl_{uid}_{timestamp}.%(ext)s")
            ydl_opts = {
                'format': 'bestaudio/best',
                'outtmpl': out_tmpl,
                'quiet': True,
                'noplaylist': True,
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
            }
        else:
            out_tmpl = str(TMP / f"dl_{uid}_{timestamp}.%(ext)s")
            # Force merge with best audio if not 'best' generic
            # data['format_id'] is usually just the video stream ID (e.g. '137')
            # We want '137+bestaudio' to ensure audio presence
            
            final_fmt = fmt
            if fmt != 'bestvideo+bestaudio/best':
                final_fmt = f"{fmt}+bestaudio/best"
                
            ydl_opts = {
                'format': final_fmt,
                'outtmpl': out_tmpl,
                'quiet': True,
                'noplaylist': True,
                'merge_output_format': 'mkv' # Ensure container can hold streams
            }

        # Progress hook for cancellation
        def ytdl_progress_hook(d):
            if cancel_event.is_set():
                raise Exception("Download cancelled by user")

        ydl_opts['progress_hooks'] = [ytdl_progress_hook]
        
        await status_msg.edit(f"Downloading `{title}`...", reply_markup=progress_keyboard())
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            await asyncio.to_thread(ydl.download, [url])
            
        # Search for file
        expected_prefix = f"dl_{uid}_{timestamp}"
        found_file = None
        for f in TMP.iterdir():
            if f.name.startswith(expected_prefix):
                found_file = f
                break
        
        if not found_file:
            await status_msg.edit(f"Download failed (file not found).")
            return
            
        await status_msg.edit("Download complete. Uploading...", reply_markup=progress_keyboard())
        
        safe_title = re.sub(r"[\\/*?\"<>|:]", "_", title)
        final_filename = f"{safe_title}{found_file.suffix}"
        
        asyncio.create_task(
            sequential_upload_task(uid, c, cb.message, found_file, final_filename, status_msg.id, cancel_event)
        )
        
    except Exception as e:
        if "cancelled" in str(e).lower():
             await status_msg.edit("Download Cancelled.")
        else:
             await status_msg.edit(f"Download Error: {e}")
    finally:
        # Don't remove cancel event yet if upload task took it, but if we crashed here, cleanup
        pass

async def download_and_process_generic(c, m, url, status_msg):
    """Fallback for direct URLs or Google Drive if YT-DLP fails/skips"""
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    try:
        fname = url.split("/")[-1].split("?")[0] or f"download_{int(datetime.now().timestamp())}"
        safe_name = re.sub(r"[\\/*?\"<>|:]", "_", fname)

        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        if not any(safe_name.lower().endswith(ext) for ext in video_exts):
            safe_name += ".mp4"

        tmp_in = TMP / f"dl_{uid}_{int(datetime.now().timestamp())}_{safe_name}"
        ok, err = False, None
        
        if is_drive_url(url):
            fid = extract_drive_id(url)
            if not fid:
                await status_msg.edit("Google Drive ID not found.")
                TASKS[uid].remove(cancel_event)
                return
            ok, err = await download_drive_file(fid, tmp_in, status_msg, cancel_event=cancel_event)
        else:
            ok, err = await download_url_generic(url, tmp_in, status_msg, cancel_event=cancel_event)

        if not ok:
            await status_msg.edit(f"Download Failed: {err}")
            if tmp_in.exists(): tmp_in.unlink()
            TASKS[uid].remove(cancel_event)
            return

        await status_msg.edit("Download complete. Uploading...", reply_markup=None)
        renamed_file = generate_new_filename(safe_name)
        
        asyncio.create_task(
            sequential_upload_task(uid, c, m, tmp_in, renamed_file, status_msg.id, cancel_event)
        )
    except Exception as e:
        await status_msg.edit(f"Error: {e}")
    finally:
        pass # Task cleanup in upload

async def handle_caption_only_upload(c: Client, m: Message):
    """Wrapper for handling caption change from message object"""
    file_info = m.video or m.document
    await handle_caption_only_upload_with_file(c, m, file_info)

async def handle_caption_only_upload_with_file(c: Client, m: Message, file_info):
    uid = m.from_user.id
    caption_to_use = USER_CAPTIONS.get(uid)
    if not caption_to_use:
        await m.reply_text("ক্যাপশন এডিট মোড চালু আছে কিন্তু কোনো সেভ করা ক্যাপশন নেই। /set_caption দিয়ে ক্যাপশন সেট করুন।")
        return

    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    try:
        status_msg = await m.reply_text("ক্যাপশন এডিট করা হচ্ছে...", reply_markup=progress_keyboard())
    except Exception:
        status_msg = await m.reply_text("ক্যাপশন এডিট করা হচ্ছে...", reply_markup=progress_keyboard())
    
    try:
        source_message = m
        
        if not file_info:
            try:
                await status_msg.edit("এটি একটি ভিডিও বা ডকুমেন্ট ফাইল নয়।")
            except Exception:
                await m.reply_text("এটি একটি ভিডিও বা ডকুমেন্ট ফাইল নয়।")
            return
        
        final_caption = process_dynamic_caption(uid, caption_to_use)
        
        if file_info.file_id:
            try:
                if source_message.video or (file_info and getattr(file_info, 'duration', 0) > 0): # Treat as video
                    await c.send_video(
                        chat_id=m.chat.id,
                        video=file_info.file_id,
                        caption=final_caption,
                        thumb=file_info.thumbs[0].file_id if file_info.thumbs else None,
                        duration=file_info.duration,
                        width=file_info.width,       
                        height=file_info.height,     
                        supports_streaming=True,
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await c.send_document(
                        chat_id=m.chat.id,
                        document=file_info.file_id,
                        file_name=file_info.file_name,
                        caption=final_caption,
                        thumb=file_info.thumbs[0].file_id if file_info.thumbs else None,
                        parse_mode=ParseMode.MARKDOWN
                    )
                try:
                    await status_msg.delete() # SILENT SUCCESS
                except Exception:
                    pass
            except Exception as e:
                try:
                    await status_msg.edit(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
                except Exception:
                    await m.reply_text(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
                return
        else:
            try:
                await status_msg.edit("ফাইলের ফাইল আইডি পাওয়া যায়নি।", reply_markup=None)
            except Exception:
                await m.reply_text("ফাইলের ফাইল আইডি পাওয়া যায়নি।", reply_markup=None)
            return

    except Exception as e:
        traceback.print_exc()
        try:
            await status_msg.edit(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
        except Exception:
            await m.reply_text(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

@app.on_message(filters.private & (filters.video | filters.document))
async def forwarded_file_or_direct_file(c: Client, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return

    # --- Check for MKV Audio Change Mode first ---
    if uid in MKV_AUDIO_CHANGE_MODE:
        await handle_audio_change_file(c, m)
        return
    # -------------------------------------------------

    # Fallback to existing logic (Forwarded/direct file for rename/re-upload logic)

    # Check if the user is in edit caption mode
    if uid in EDIT_CAPTION_MODE and m.forward_date: 
        
        # --- NEW BATCH LOGIC ---
        if uid in BATCH_CAPTION_MODE:
            file_info = m.video or m.document
            if not file_info: 
                return
            
            if uid not in BATCH_DATA: 
                BATCH_DATA[uid] = []
            
            # Save relevant data. We store the message object to reply to/extract data from later
            BATCH_DATA[uid].append({
                'message': m,
                'file_info': file_info
            })
            
            count = len(BATCH_DATA[uid])
            status_text = f"{count} টি ভিডিও এর file id save করা হয়েছে।"
            
            # Update or Send Status Message
            if uid in BATCH_STATUS_MSG:
                try:
                    # মেসেজ এডিট হলে সেটি অটো ডিলিট করা কঠিন, তাই আমরা আগেরটি ডিলিট করে নতুন পাঠাতে পারি
                    # অথবা সরাসরি এডিট করতে পারেন:
                    await c.edit_message_text(m.chat.id, BATCH_STATUS_MSG[uid], status_text)
                except Exception:
                    # If edit fails (e.g. deleted), send new
                    msg = await m.reply_text(status_text)
                    BATCH_STATUS_MSG[uid] = msg.id
                    
                    # ১৫ সেকেন্ড পর ডিলিট করার লজিক
                    await asyncio.sleep(15)
                    try:
                        await msg.delete()
                    except:
                        pass
            else:
                msg = await m.reply_text(status_text)
                BATCH_STATUS_MSG[uid] = msg.id
                
                # ১৫ সেকেন্ড পর ডিলিট করার লজিক
                await asyncio.sleep(15)
                try:
                    await msg.delete()
                    # ডিলিট হয়ে গেলে ডিকশনারি থেকে আইডি মুছে ফেলা ভালো
                    if uid in BATCH_STATUS_MSG:
                        del BATCH_STATUS_MSG[uid]
                except:
                    pass
            return
        # ------------------------

        await handle_caption_only_upload(c, m)
        return

    if m.forward_date:
        # --- SEQUENTIAL QUEUE LOGIC START ---
        
        file_info = m.video or m.document
        
        if file_info and file_info.file_name:
            original_name = file_info.file_name
        elif m.video:
            original_name = f"video_{file_info.file_unique_id}.mp4"
        else:
            original_name = f"file_{file_info.file_unique_id}"

        # Setup Queue
        if uid not in USER_QUEUES:
            USER_QUEUES[uid] = asyncio.Queue()
        
        # Send initial message immediately
        try:
            status_msg = await m.reply_text(f"Queue: Processing started for `{original_name}`...", reply_markup=progress_keyboard())
        except:
            status_msg = None

        await USER_QUEUES[uid].put({
            'message': m,
            'original_name': original_name,
            'status_msg': status_msg # Pass msg to worker
        })
        
        # Start Worker if not running
        if uid not in USER_WORKERS or USER_WORKERS[uid].done():
             USER_WORKERS[uid] = asyncio.create_task(process_queue_handler(uid, c))
        else:
             pass
        # --- SEQUENTIAL QUEUE LOGIC END ---
        
    else:
        pass

# --- HANDLER FUNCTION: Handle file in audio change mode ---
async def handle_audio_change_file(c: Client, m: Message):
    uid = m.from_user.id
    file_info = m.video or m.document
    
    if not file_info:
        await m.reply_text("এটি একটি ভিডিও ফাইল নয়।")
        return
    
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    tmp_path = None
    status_msg = None
    try:
        original_name = file_info.file_name or f"video_{file_info.file_unique_id}.mkv"
        if not '.' in original_name:
            original_name += '.mkv'
            
        tmp_path = TMP / f"audio_change_{uid}_{int(datetime.now().timestamp())}_{original_name}"
        
        status_msg = await m.reply_text("অডিও ট্র্যাক বিশ্লেষণের জন্য ফাইল ডাউনলোড করা হচ্ছে...", reply_markup=progress_keyboard())
        await m.download(file_name=str(tmp_path))
        
        audio_tracks = await asyncio.to_thread(get_audio_tracks_ffprobe, tmp_path)
        
        if not audio_tracks:
            await status_msg.edit("এই ভিডিওতে কোনো অডিও ট্র্যাক পাওয়া যায়নি বা FFprobe চলতে পারেনি।")
            tmp_path.unlink(missing_ok=True)
            return

        if len(audio_tracks) == 1:
            await status_msg.edit("ফাইলটিতে ১টি অডিও ট্র্যাক রয়েছে। স্বয়ংক্রিয়ভাবে রিমাক্স করা হচ্ছে...", reply_markup=progress_keyboard())
            
            stream_index = audio_tracks[0]['stream_index']
            new_stream_map = [f"0:{stream_index}"]
            
            asyncio.create_task(
                handle_audio_remux(
                    c, m, tmp_path, 
                    original_name, 
                    new_stream_map, 
                    messages_to_delete=[status_msg.id]
                )
            )
            
            return 

        track_list_text = "ফাইলের অডিও ট্র্যাকসমূহ:\n\n"
        for i, track in enumerate(audio_tracks, 1):
            track_list_text += f"{i}. **Stream Index:** {track['stream_index']}, **Language:** {track['language']}, **Title:** {track['title']}\n"
            
        track_list_text += (
            "\n**অডিও অর্ডার দিতে এই মেসেজটিতে রিপ্লাই করে** কমা-সেপারেটেড সংখ্যায় আপনার ট্র্যাক নম্বরগুলো দিন।\n"
            "যেমন: `1,3` দিলে ১ এবং ৩ নম্বর ট্র্যাক থাকবে। `2` দিলে শুধু ২ নম্বর ট্র্যাক থাকবে। বাকিগুলো মুছে যাবে।\n"
        )
            
        track_list_text += (
            "\nঅডিও পরিবর্তন না করতে চাইলে, এই মেসেজের `Cancel` বাটনটি ব্যবহার করুন অথবা `/mkv_video_audio_change` লিখে মোড অফ করুন।"
        )
        
        await status_msg.edit(track_list_text, reply_markup=progress_keyboard()) 
        
        PENDING_AUDIO_ORDERS[status_msg.id] = {
            'uid': uid,
            'path': tmp_path, 
            'original_name': original_name,
            'tracks': audio_tracks
        }
        
    except Exception as e:
        logger.error(f"Audio track analysis error: {e}")
        if status_msg:
            await status_msg.edit(f"অডিও ট্র্যাক বিশ্লেষণে সমস্যা: {e}")
        else:
            await m.reply_text(f"অডিও ট্র্যাক বিশ্লেষণে সমস্যা: {e}")
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

# --- HANDLER FUNCTION: Handle audio remux ---
async def handle_audio_remux(c: Client, m: Message, in_path: Path, original_name: str, new_stream_map: list, messages_to_delete: list = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    out_name = generate_new_filename(original_name)
    if not out_name.lower().endswith(".mkv"):
        out_name = Path(out_name).stem + ".mkv"
    
    asyncio.create_task(
        sequential_remux_upload_task(uid, c, m, in_path, out_name, new_stream_map, messages_to_delete, cancel_event)
    )

async def sequential_remux_upload_task(uid, c, m, in_path, out_name, new_stream_map, messages_to_delete, cancel_event):
    if uid not in USER_UPLOAD_LOCKS:
        USER_UPLOAD_LOCKS[uid] = asyncio.Lock()
    
    async with USER_UPLOAD_LOCKS[uid]:
        if cancel_event.is_set():
             if in_path.exists(): in_path.unlink()
             return

        out_path = TMP / f"remux_{uid}_{int(datetime.now().timestamp())}_{out_name}"
        
        map_args = ["-map", "0:v", "-map", "0:s?", "-map", "0:d?"] 
        for stream_index in new_stream_map:
            map_args.extend(["-map", stream_index])
            
        cmd = [
            "ffmpeg",
            "-i", str(in_path),
            "-disposition:a", "0",            
            *map_args,
            "-disposition:a:0", "default",
            "-metadata:s:a", "title=[@TA_HD_Anime] Telegram Channel", 
            "-c", "copy",
            "-metadata", "handler_name=", 
            str(out_path)
        ]

        status_msg = None
        if messages_to_delete:
             pass

        try:
            status_msg = await m.reply_text("অডিও ট্র্যাক অর্ডার পরিবর্তন করা হচ্ছে (Remuxing)...", reply_markup=progress_keyboard())
            
            result = await asyncio.to_thread(
                subprocess.run,
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=3600
            )
            
            if result.returncode != 0:
                logger.error(f"FFmpeg Remux failed: {result.stderr}")
                out_path.unlink(missing_ok=True)
                raise Exception(f"FFmpeg Remux ব্যর্থ হয়েছে। ত্রুটি: {result.stderr[:500]}...")

            if not out_path.exists() or out_path.stat().st_size == 0:
                raise Exception("পরিবর্তিত ফাইলটি পাওয়া যায়নি বা শূন্য আকারের।")

            await status_msg.edit("অডিও পরিবর্তন সম্পন্ন, ফাইল আপলোড করা হচ্ছে...", reply_markup=progress_keyboard())
            
            all_messages_to_delete = messages_to_delete if messages_to_delete else []
            all_messages_to_delete.append(status_msg.id)

            await process_file_and_upload(c, m, out_path, original_name=out_name, messages_to_delete=all_messages_to_delete, cancel_event_passed=cancel_event) 

        except Exception as e:
            logger.error(f"Audio remux process error: {e}")
            try:
                if status_msg:
                    await status_msg.edit(f"অডিও পরিবর্তন প্রক্রিয়া ব্যর্থ: {e}")
                else:
                    await m.reply_text(f"অডিও পরিবর্তন প্রক্রিয়া ব্যর্থ: {e}")
            except Exception:
                pass
        finally:
            try:
                in_path.unlink(missing_ok=True)
                if out_path.exists(): out_path.unlink(missing_ok=True)
                TASKS[uid].remove(cancel_event)
            except Exception:
                pass


@app.on_message(filters.command("rename") & filters.private)
async def rename_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই।")
        return
    if not m.reply_to_message or not (m.reply_to_message.video or m.reply_to_message.document):
        await m.reply_text("ভিডিও/ডকুমেন্ট ফাইলের reply দিয়ে এই কমান্ড দিন।\nUsage: /rename new_name.mp4")
        return
    if len(m.command) < 2:
        await m.reply_text("নতুন ফাইল নাম দিন। উদাহরণ: /rename new_video.mp4")
        return
    new_name = m.text.split(None, 1)[1].strip()
    new_name = re.sub(r"[\\/*?\"<>|:]", "_", new_name)
    
    await m.reply_text(f"ভিডিও রিনেম করা হবে: {new_name}\n(রিনেম করতে reply করা ফাইলটি পুনরায় ডাউনলোড করে আপলোড করা হবে)")

    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    try:
        status_msg = await m.reply_text("রিনেমের জন্য ফাইল ডাউনলোড করা হচ্ছে...", reply_markup=progress_keyboard())
    except Exception:
        status_msg = await m.reply_text("রিনেমের জন্য ফাইল ডাউনলোড করা হচ্ছে...", reply_markup=progress_keyboard())
    tmp_out = TMP / f"rename_{uid}_{int(datetime.now().timestamp())}_{new_name}"
    try:
        await m.reply_to_message.download(file_name=str(tmp_out))
        try:
            await status_msg.edit("ডাউনলোড সম্পন্ন, এখন নতুন নাম দিয়ে আপলোড হচ্ছে...", reply_markup=None)
        except Exception:
            await m.reply_text("ডাউনলোড সম্পন্ন, এখন নতুন নাম দিয়ে আপলোড হচ্ছে...", reply_markup=None)
        
        # Use sequential upload logic
        asyncio.create_task(
            sequential_upload_task(uid, c, m, tmp_out, new_name, status_msg.id, cancel_event)
        )
    except Exception as e:
        await m.reply_text(f"রিনেম ত্রুটি: {e}")
    finally:
        # Cancel event cleanup handled in upload task
        pass

@app.on_callback_query(filters.regex("cancel_task"))
async def cancel_task_cb(c, cb):
    uid = cb.from_user.id
    prompt_message_id = cb.message.id

    if prompt_message_id in PENDING_AUDIO_ORDERS:
        file_data = PENDING_AUDIO_ORDERS.pop(prompt_message_id)
        if file_data['uid'] == uid:
            try:
                Path(file_data['path']).unlink(missing_ok=True)
            except Exception:
                pass
            
            # Find and set specific events if possible, but for simplicity we trigger user events
            for ev in list(TASKS.get(uid, [])):
                try: ev.set()
                except: pass

            await cb.answer("অডিও পরিবর্তন প্রক্রিয়া বাতিল করা হয়েছে।", show_alert=True)
            try:
                await cb.message.delete()
            except Exception:
                pass
            return
    
    # Trigger cancellation for tasks
    if uid in TASKS and TASKS[uid]:
        count = 0
        for ev in list(TASKS[uid]):
            try:
                ev.set()
                count += 1
            except:
                pass
        
        await cb.answer(f"{count}টি অপারেশন বাতিল করা হয়েছে।", show_alert=True)
        try:
            await cb.message.delete()
        except Exception:
            pass
    else:
        await cb.answer("কোনো অপারেশন চলছে না।", show_alert=True)

async def generate_video_thumbnail(video_path: Path, thumb_path: Path, timestamp_sec: int = 1):
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i", str(video_path),
            "-ss", str(timestamp_sec),
            "-vframes", "1",
            "-vf", "scale=320:-1",
            str(thumb_path)
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return thumb_path.exists() and thumb_path.stat().st_size > 0
    except Exception as e:
        logger.warning("Thumbnail generate error: %s", e)
        return False


def process_dynamic_caption(uid, caption_template):
    if uid not in USER_COUNTERS:
        USER_COUNTERS[uid] = {'uploads': 0, 'episode_numbers': {}, 'dynamic_counters': {}, 're_options_count': 0}

    USER_COUNTERS[uid]['uploads'] += 1

    quality_match = re.search(r"\[re\s*\((.*?)\)\]", caption_template)
    if quality_match:
        options_str = quality_match.group(1)
        options = [opt.strip() for opt in options_str.split(',')]
        
        if not USER_COUNTERS[uid]['re_options_count']:
            USER_COUNTERS[uid]['re_options_count'] = len(options)
        
        current_index = (USER_COUNTERS[uid]['uploads'] - 1) % len(options)
        current_quality = options[current_index]
        
        caption_template = caption_template.replace(quality_match.group(0), current_quality)

        if (USER_COUNTERS[uid]['uploads'] - 1) % USER_COUNTERS[uid]['re_options_count'] == 0 and USER_COUNTERS[uid]['uploads'] > 1:
            for key in USER_COUNTERS[uid]['dynamic_counters']:
                USER_COUNTERS[uid]['dynamic_counters'][key]['value'] += 1
    elif USER_COUNTERS[uid]['uploads'] > 1: 
        for key in USER_COUNTERS[uid].get('dynamic_counters', {}):
             USER_COUNTERS[uid]['dynamic_counters'][key]['value'] += 1


    counter_matches = re.findall(r"\[\s*(\(?\d+\)?)\s*\]", caption_template)
    
    if USER_COUNTERS[uid]['uploads'] == 1:
        for match in counter_matches:
            has_paren = match.startswith('(') and match.endswith(')')
            clean_match = re.sub(r'[()]', '', match)
            USER_COUNTERS[uid]['dynamic_counters'][match] = {'value': int(clean_match), 'has_paren': has_paren}
    
    for match, data in USER_COUNTERS[uid]['dynamic_counters'].items():
        value = data['value']
        has_paren = data['has_paren']
        
        original_num_len = len(re.sub(r'[()]', '', match))
        formatted_value = f"{value:0{original_num_len}d}"

        final_value = f"({formatted_value})" if has_paren else formatted_value
        
        caption_template = re.sub(re.escape(f"[{match}]"), final_value, caption_template)


    current_episode_num = 0
    if USER_COUNTERS[uid].get('dynamic_counters'):
        current_episode_num = min(data['value'] for data in USER_COUNTERS[uid]['dynamic_counters'].values())

    conditional_matches = re.findall(r"\[([a-zA-Z0-9\s]+)\s*\((.*?)\)\]", caption_template)

    for match in conditional_matches:
        text_to_add = match[0].strip() 
        target_num_str = re.sub(r'[^0-9]', '', match[1]).strip() 

        placeholder = re.escape(f"[{match[0].strip()} ({match[1].strip()})]")
        
        try:
            target_num = int(target_num_str)
        except ValueError:
            caption_template = re.sub(placeholder, "", caption_template)
            continue
        
        if current_episode_num == target_num:
            caption_template = re.sub(placeholder, text_to_add, caption_template)
        else:
            caption_template = re.sub(placeholder, "", caption_template)

    return "**" + "\n".join(caption_template.splitlines()) + "**"


async def process_file_and_upload(c: Client, m: Message, in_path: Path, original_name: str = None, messages_to_delete: list = None, cancel_event_passed: asyncio.Event = None):
    uid = m.from_user.id
    # Use passed cancel event or create new one (though logically should be passed)
    cancel_event = cancel_event_passed
    if not cancel_event:
        cancel_event = asyncio.Event()
        TASKS.setdefault(uid, []).append(cancel_event)
    
    upload_path = in_path
    temp_thumb_path = None
    final_caption_template = USER_CAPTIONS.get(uid)
    status_msg = None 

    try:
        # Check cancel early
        if cancel_event.is_set():
             raise Exception("Cancelled")

        input_name = in_path.name
        target_name = original_name or input_name
        
        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        audio_exts = {".mp3", ".m4a", ".flac", ".wav", ".aac"}
        
        is_video_file = bool(m.video) or any(input_name.lower().endswith(ext) for ext in video_exts)
        is_audio_file = any(input_name.lower().endswith(ext) for ext in audio_exts)
        
        if is_video_file:
            # Check conditions
            is_mp4_container = input_name.lower().endswith(".mp4")
            is_mkv_container = input_name.lower().endswith(".mkv")
            has_opus = has_opus_audio(in_path)
            
            # Determine final extension
            if is_mp4_container:
                if has_opus:
                    final_ext = ".mkv" # MP4 + OPUS -> MKV
                else:
                    final_ext = ".mp4" # MP4 (No OPUS) -> MP4
            elif is_mkv_container:
                final_ext = ".mkv" # MKV -> MKV
            else:
                final_ext = ".mkv" # Others -> MKV
            
            # Adjust target_name extension
            target_stem = Path(target_name).stem
            target_name = target_stem + final_ext
            
            # Prepare for processing
            processed_path = TMP / f"proc_{uid}_{int(datetime.now().timestamp())}_{target_name}"
            
            try:
                status_text = "ভিডিও প্রসেস করা হচ্ছে (Metadata & Format Check)..."
                if messages_to_delete:
                     # Try to edit existing msg if possible, but msg object not passed here, just ID
                     pass 
                status_msg = await m.reply_text(status_text, reply_markup=progress_keyboard())
            except Exception:
                status_msg = await m.reply_text(status_text, reply_markup=progress_keyboard())
            
            if messages_to_delete:
                messages_to_delete.append(status_msg.id)
            else:
                messages_to_delete = [status_msg.id]

            # --- FFmpeg Command for Processing ---
            cmd = [
                "ffmpeg",
                "-i", str(in_path),
                "-map", "0", # Copy all streams
                "-c", "copy", # Copy codec (fast)
                "-metadata:s:a", "title=[@TA_HD_Anime] Telegram Channel", # Set audio title
                "-metadata", "handler_name=",
                str(processed_path)
            ]
            
            if cancel_event.is_set(): raise Exception("Cancelled")
            
            result = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, check=False, timeout=3600)
            
            if result.returncode == 0 and processed_path.exists() and processed_path.stat().st_size > 0:
                upload_path = processed_path
            else:
                logger.warning(f"Processing failed: {result.stderr}. Uploading original.")
                pass

        if is_video_file:
            thumb_path = USER_THUMBS.get(uid)
            if not thumb_path:
                temp_thumb_path = TMP / f"thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
                thumb_time_sec = USER_THUMB_TIME.get(uid, 1) 
                ok = await generate_video_thumbnail(upload_path, temp_thumb_path, timestamp_sec=thumb_time_sec)
                if ok:
                    thumb_path = str(temp_thumb_path)

        try:
            if status_msg:
                await status_msg.edit("আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
            else:
                status_msg = await m.reply_text("আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
        except Exception:
             status_msg = await m.reply_text("আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
             
        if messages_to_delete:
            if status_msg.id not in messages_to_delete:
                messages_to_delete.append(status_msg.id)
        else:
            messages_to_delete = [status_msg.id]


        if cancel_event.is_set():
            raise Exception("Cancelled")
        
        video_metadata = get_video_metadata(upload_path) if (is_video_file and upload_path.exists()) else {'duration': 0, 'width': 0, 'height': 0}
        duration_sec = video_metadata.get('duration', 0)
        width_px = video_metadata.get('width', 0)
        height_px = video_metadata.get('height', 0)
        
        caption_to_use = f"**{target_name}**" # Default to Bold Filename
        if final_caption_template:
            caption_to_use = process_dynamic_caption(uid, final_caption_template)

        upload_attempts = 3
        last_exc = None
        for attempt in range(1, upload_attempts + 1):
            try:
                if cancel_event.is_set(): raise Exception("Cancelled")

                # Progress callback for upload cancellation checking
                async def progress(current, total):
                    if cancel_event.is_set():
                        c.stop_transmission()

                if is_video_file:
                    await c.send_video(
                        chat_id=m.chat.id,
                        video=str(upload_path),
                        caption=caption_to_use,
                        thumb=thumb_path,
                        duration=duration_sec,
                        width=width_px,
                        height=height_px,
                        supports_streaming=True,
                        file_name=target_name, 
                        parse_mode=ParseMode.MARKDOWN,
                        progress=progress
                    )
                elif is_audio_file:
                     await c.send_audio(
                        chat_id=m.chat.id,
                        audio=str(upload_path),
                        file_name=target_name,
                        caption=caption_to_use,
                        parse_mode=ParseMode.MARKDOWN,
                        progress=progress
                    )
                else:
                    await c.send_document(
                        chat_id=m.chat.id,
                        document=str(upload_path),
                        file_name=target_name,
                        caption=caption_to_use,
                        parse_mode=ParseMode.MARKDOWN,
                        progress=progress
                    )
                
                if messages_to_delete:
                    try:
                        await c.delete_messages(chat_id=m.chat.id, message_ids=messages_to_delete)
                    except Exception:
                        pass
                
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                if "Cancelled" in str(e):
                    break
                logger.warning("Upload attempt %s failed: %s", attempt, e)
                await asyncio.sleep(2 * attempt)
        
        if last_exc:
            msg_text = "অপারেশন বাতিল করা হয়েছে।" if "Cancelled" in str(last_exc) else f"আপলোড ব্যর্থ: {last_exc}"
            if status_msg:
                await status_msg.edit(msg_text, reply_markup=None)
            else:
                await m.reply_text(msg_text, reply_markup=None)

    except Exception as e:
        msg_text = "অপারেশন বাতিল করা হয়েছে।" if "Cancelled" in str(e) else f"আপলোডে ত্রুটি: {e}"
        if status_msg:
            await status_msg.edit(msg_text)
        else:
            await m.reply_text(msg_text)
    finally:
        try:
            if upload_path != in_path and upload_path.exists():
                upload_path.unlink()
            if in_path.exists():
                in_path.unlink()
            if temp_thumb_path and Path(temp_thumb_path).exists():
                Path(temp_thumb_path).unlink()
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd_no_reply(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই।")
        return
    if not m.reply_to_message:
        await m.reply_text("ব্রডকাস্ট করতে যেকোনো মেসেজে (ছবি, ভিডিও বা টেক্সট) **রিপ্লাই করে** এই কমান্ড দিন।")
        return

@app.on_message(filters.command("broadcast") & filters.private & filters.reply)
async def broadcast_cmd_reply(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই।")
        return
    
    source_message = m.reply_to_message
    if not source_message:
        await m.reply_text("ব্রডকাস্ট করতে যেকোনো মেসেজে রিপ্লাই করে এই কমান্ড দিন।")
        return

    await m.reply_text(f"ব্রডকাস্ট শুরু হচ্ছে {len(SUBSCRIBERS)} সাবস্ক্রাইবারে...", quote=True)
    failed = 0
    sent = 0
    for chat_id in list(SUBSCRIBERS):
        if chat_id == m.chat.id:
            continue
        try:
            await c.forward_messages(chat_id=chat_id, from_chat_id=source_message.chat.id, message_ids=source_message.id)
            sent += 1
            await asyncio.sleep(0.08)
        except Exception as e:
            failed += 1
            logger.warning("Broadcast to %s failed: %s", chat_id, e)

    await m.reply_text(f"ব্রডকাস্ট শেষ। পাঠানো: {sent}, ব্যর্থ: {failed}")

@flask_app.route('/')
def home():
    html_content = """
    <!DOCTYPE-html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Bot Status</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                background-color: #f0f2f5;
                color: #333;
                text-align: center;
                padding-top: 50px;
            }
            .container {
                background-color: #fff;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                display: inline-block;
            }
            h1 {
                color: #28a745;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>TA File Share Bot is running! ✅</h1>
            <p>This page confirms that the bot's web server is active.</p>
        </div>
    </body>
    </html>
    """
    return render_template_string(html_content)

def ping_service():
    if not RENDER_EXTERNAL_HOSTNAME:
        print("Render URL is not set. Ping service is disabled.")
        return

    url = f"http://{RENDER_EXTERNAL_HOSTNAME}"
    while True:
        try:
            response = requests.get(url, timeout=10)
            print(f"Pinged {url} | Status Code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error pinging {url}: {e}")
        time.sleep(600)

def run_flask_and_ping():
    flask_thread = threading.Thread(target=lambda: flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False))
    flask_thread.start()
    ping_thread = threading.Thread(target=ping_service)
    ping_thread.start()
    print("Flask and Ping services started.")

async def periodic_cleanup():
    while True:
        try:
            now = datetime.now()
            for p in TMP.iterdir():
                try:
                    if p.is_file():
                        if now - datetime.fromtimestamp(p.stat().st_mtime) > timedelta(days=3):
                            p.unlink()
                except Exception:
                    pass
        except Exception:
            pass
        await asyncio.sleep(3600)

# --- ২. ইমেইল জেনারেশন ফাংশন (/gen) ---
@app.on_message(filters.command("gen") & filters.private)
async def gen_temp_emails(c, m: Message):
    uid = m.from_user.id

if __name__ == "__main__":
    print("Bot চালু হচ্ছে... Flask and Ping threads start করা হচ্ছে, তারপর Pyrogram চালু হবে।")
    t = threading.Thread(target=run_flask_and_ping, daemon=True)
    t.start()
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(periodic_cleanup())
    except RuntimeError:
        pass
    app.run()
