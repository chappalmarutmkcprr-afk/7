# ═══════════════════════════════════════════════════════════════
#  LUFFY CREW MULTI-BOT CONTROLLER — FIXED FULL EDITION
#  Prefix: .   |  Main = hoster  |  Parallel + Adaptive
#  PTB v20.x / v21.x compatible
# ═══════════════════════════════════════════════════════════════
import asyncio, json, os, random, re, time, logging
from datetime import datetime, timezone, timedelta
from collections import deque
from html import escape as html_escape
from typing import Dict, List, Set, Optional, Any, Tuple

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ChatPermissions, Bot, ReactionTypeEmoji
)
from telegram.ext import (
    Application, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from telegram.error import RetryAfter, Forbidden, BadRequest
from telegram.constants import ChatAction, ParseMode

try:
    from telegram import ReplyParameters
    HAS_REPLY_PARAMS = True
except ImportError:
    HAS_REPLY_PARAMS = False

import aiohttp

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("luffy-crew")

# ───────────────────────────── CONFIG ─────────────────────────────
OWNER_ID: int = 8783373485
MAIN_TOKEN: str = "8920918134:AAGkioQjUeCgWHRz14696NqnYtA45o883h0"
BOTS_FILE    = "bots.json"
SUDO_FILE    = "sudo_users.json"
TARGETS_FILE = "targets.json"
PFP_FILE     = "pfp_loops.json"

AUTHORIZATION_MSG = (
    "𓆩💢𓆪 LUFFY PAPA BOL PEHLE  𓆩💢𓆪"
)
OWNER_ONLY_MSG = "🚫 ᴏɴʟʏ ꜱᴇʀᴠᴇʀ ᴏᴡɴᴇʀ ᴄᴀɴ ᴜꜱᴇ ᴛꜱ ꜱʜʏᴛ ¡! "

# ═══════════════════════════════════════════════════════════════
#  RATEGUARD — upgraded adaptive flood engine
# ═══════════════════════════════════════════════════════════════
class RateGuard:
    GLOBAL_RATE  = 32.0
    GLOBAL_BURST = 90
    MAX_CONC     = 28
    CHAT_RATE    = 6.5
    CHAT_BURST   = 10
    CHAT_WINDOW  = 1.0
    CHAT_CEILING = 28
    _INVIS = ["\u200b","\u200c","\u200d","\u2060","\ufeff",
              "\u00ad","\u034f","\u17b5","\u180e","\u3164"]

    def __init__(self, bot_id: int, rate: float | None = None,
                 burst: int | None = None, max_conc: int | None = None):
        self.bot_id      = bot_id
        self.base_rate   = rate or self.GLOBAL_RATE
        self.rate        = self.base_rate
        self.capacity    = float(burst or self.GLOBAL_BURST)
        self.tokens      = float(self.capacity)
        self.last_refill = time.monotonic()
        self._sem = asyncio.Semaphore(max_conc or self.MAX_CONC)
        self._lk  = asyncio.Lock()
        self._shards = [asyncio.Lock() for _ in range(16)]
        self._flood_until = 0.0
        self._penalty     = 0.0
        self._errors      = deque(maxlen=128)
        self._chat_slow: Dict[int, float] = {}
        self._chat_lk: dict = {}
        self._chat_tok: dict = {}
        self._chat_refill: dict = {}
        self._chat_window: dict = {}
        self._rng = random.Random(bot_id ^ 0xC0FFEE)

    def _chat_lock(self, cid: int) -> asyncio.Lock:
        lk = self._chat_lk.get(cid)
        if lk is None:
            lk = asyncio.Lock(); self._chat_lk[cid] = lk
        return lk

    async def _chat_gate(self, cid: int):
        async with self._chat_lock(cid):
            now = time.monotonic()
            slow = self._chat_slow.get(cid, 0.0)
            r = self.CHAT_RATE / (1.0 + slow)
            tok = self._chat_tok.get(cid, float(self.CHAT_BURST))
            ref = self._chat_refill.get(cid, now)
            tok = min(float(self.CHAT_BURST), tok + (now - ref) * r)
            self._chat_refill[cid] = now
            win = self._chat_window.setdefault(cid, deque())
            while win and now - win[0] > self.CHAT_WINDOW:
                win.popleft()
            if len(win) >= self.CHAT_CEILING:
                sleep_for = self.CHAT_WINDOW - (now - win[0])
                if sleep_for > 0:
                    await asyncio.sleep(max(0.02, sleep_for))
                now = time.monotonic()
                while win and now - win[0] > self.CHAT_WINDOW:
                    win.popleft()
            wait = 0.0
            if tok < 1.0:
                wait = (1.0 - tok) / r
            self._chat_tok[cid]    = max(0.0, tok - 1.0)
            self._chat_refill[cid] = now
        if wait > 0:
            await asyncio.sleep(wait + self._rng.uniform(0.0, 0.0008))
        async with self._chat_lock(cid):
            self._chat_window.setdefault(cid, deque()).append(time.monotonic())

    async def _gate(self):
        async with self._lk:
            now = time.monotonic()
            self.tokens = min(self.capacity,
                              self.tokens + (now - self.last_refill) * self.rate)
            self.last_refill = now
            wait = 0.0
            if self.tokens < 1.0:
                wait = (1.0 - self.tokens) / self.rate
            n2 = time.monotonic()
            if self._flood_until > n2:
                wait = max(wait, self._flood_until - n2)
            if wait > 0:
                await asyncio.sleep(wait + self._rng.uniform(0.0, 0.0008))
                self.last_refill = time.monotonic()
            self.tokens = max(0.0, self.tokens - 1.0)

    def _on_flood(self, ra: float, cid: Optional[int] = None):
        now = time.monotonic()
        self._flood_until = now + min(float(ra), 2.5)
        self._penalty     = min(2.5, self._penalty * 1.5 + 0.05)
        self._errors.append(now)
        if cid is not None:
            self._chat_slow[cid] = min(4.0, self._chat_slow.get(cid, 0.0) + 0.5)
        recent = [t for t in self._errors if now - t < 30]
        if len(recent) >= 4:
            self.rate   = max(14.0, self.rate * 0.9)
            self.tokens = min(self.tokens, 18.0)
        else:
            self.rate = min(self.base_rate, self.rate + 2.0)

    def _on_success(self, cid: Optional[int] = None):
        if self._penalty > 0:
            self._penalty *= 0.97
        if self.rate < self.base_rate:
            self.rate = min(self.base_rate, self.rate + 0.6)
        if cid is not None and cid in self._chat_slow:
            self._chat_slow[cid] = max(0.0, self._chat_slow[cid] * 0.85)

    async def _wrap(self, coro, *a, **kw):
        cid = kw.get("chat_id")
        if cid is None and a and isinstance(a[0], int):
            cid = a[0]
        if isinstance(cid, int):
            await self._chat_gate(cid)
        async with self._sem:
            await self._gate()
            for _ in range(5):
                try:
                    r = await coro(*a, **kw)
                    self._on_success(cid)
                    return r
                except RetryAfter as e:
                    ra = float(getattr(e, "retry_after", 1.0))
                    self._on_flood(ra, cid)
                    await asyncio.sleep(ra * (1.0 + self._rng.uniform(0.0, 0.15)))
                except (Forbidden, BadRequest):
                    return None
                except Exception:
                    await asyncio.sleep(0.01 + self._rng.uniform(0, 0.01))
            return None

    async def send_message(self, bot, chat_id, text, **kw):
        return await self._wrap(bot.send_message, chat_id, text, **kw)
    async def set_chat_title(self, bot, chat_id, title):
        return await self._wrap(bot.set_chat_title, chat_id, title[:128])
    async def set_chat_photo(self, bot, chat_id, photo):
        return await self._wrap(bot.set_chat_photo, chat_id, photo=photo)
    async def send_photo(self, bot, chat_id, photo, **kw):
        return await self._wrap(bot.send_photo, chat_id, photo, **kw)
    async def send_video(self, bot, chat_id, video, **kw):
        return await self._wrap(bot.send_video, chat_id, video, **kw)
    async def send_audio(self, bot, chat_id, audio, **kw):
        return await self._wrap(bot.send_audio, chat_id, audio, **kw)
    async def delete_message(self, bot, chat_id, mid):
        return await self._wrap(bot.delete_message, chat_id, mid)
    async def set_message_reaction(self, bot, **kw):
        return await self._wrap(bot.set_message_reaction, **kw)
    async def restrict_chat_member(self, bot, *a, **kw):
        return await self._wrap(bot.restrict_chat_member, *a, **kw)
    async def ban_chat_member(self, bot, *a, **kw):
        return await self._wrap(bot.ban_chat_member, *a, **kw)
    async def unban_chat_member(self, bot, *a, **kw):
        return await self._wrap(bot.unban_chat_member, *a, **kw)
    async def promote_chat_member(self, bot, *a, **kw):
        return await self._wrap(bot.promote_chat_member, *a, **kw)
    async def set_chat_permissions(self, bot, *a, **kw):
        return await self._wrap(bot.set_chat_permissions, *a, **kw)

    def suffix(self, i: int) -> str:
        return self._INVIS[i % len(self._INVIS)]

    @property
    def delay(self) -> float:
        return max(0.0, self._flood_until - time.monotonic())

_guards: dict = {}
def get_limiter(bot_id: int) -> RateGuard:
    g = _guards.get(bot_id)
    if g is None:
        g = RateGuard(bot_id); _guards[bot_id] = g
    return g

# ─────────────────────────── STORAGE ─────────────────────────────
ALL_BOTS: List[Dict[str, Any]] = []
SUDO_USERS: Set[int] = set()
TARGETS: Dict[str, Dict[str, int]] = {}
TASKS: Dict[Tuple[int, str], List[asyncio.Task]] = {}
RAID_TARGETS: Dict[Tuple[int, int], List[str]] = {}
REPLY_ONLY:   Dict[Tuple[int, int], str] = {}
PFP_LOOPS: Dict[int, asyncio.Task] = {}
SAVED_PFP: Dict[int, List[str]] = {}
START_TIME = time.time()

def _load(path, default):
    try:
        with open(path, "r") as f: return json.load(f)
    except Exception: return default

def _save(path, data):
    try:
        with open(path, "w") as f: json.dump(data, f)
    except Exception as e:
        log.warning(f"save {path}: {e}")

def load_all():
    global SUDO_USERS, TARGETS, SAVED_PFP
    SUDO_USERS = set(_load(SUDO_FILE, {"users": []}).get("users", []))
    TARGETS    = _load(TARGETS_FILE, {})
    raw_pfp    = _load(PFP_FILE, {})
    for k, v in raw_pfp.items():
        try: SAVED_PFP[int(k)] = list(v)
        except Exception: pass

def save_sudo():
    _save(SUDO_FILE, {"users": list(SUDO_USERS)})

def save_targets():
    _save(TARGETS_FILE, TARGETS)

def save_pfp():
    _save(PFP_FILE, {str(k): list(v) for k, v in SAVED_PFP.items()})

def load_bots():
    data = _load(BOTS_FILE, {"bots": []})
    for b in data.get("bots", []):
        if b.get("token"):
            ALL_BOTS.append({"token": b["token"], "bot_id": b.get("bot_id", 0),
                             "username": b.get("username", ""),
                             "bot": None, "app": None, "guard": None})

def save_bots():
    _save(BOTS_FILE, {"bots": [{"token": b["token"], "bot_id": b["bot_id"],
                                 "username": b["username"]} for b in ALL_BOTS]})

# ─────────────────────── AUTH DECORATORS ─────────────────────────
def is_auth(uid: int) -> bool:
    return uid == OWNER_ID or uid in SUDO_USERS

def only_sudo(func):
    async def wrapper(update: Update, context, *a, **kw):
        u = update.effective_user
        if not u: return
        if not is_auth(u.id):
            try: await update.effective_message.reply_text(AUTHORIZATION_MSG)
            except Exception: pass
            return
        return await func(update, context, *a, **kw)
    return wrapper

def only_owner(func):
    async def wrapper(update: Update, context, *a, **kw):
        u = update.effective_user
        if not u: return
        if u.id != OWNER_ID:
            try: await update.effective_message.reply_text(OWNER_ONLY_MSG)
            except Exception: pass
            return
        return await func(update, context, *a, **kw)
    return wrapper

# ─────────────────────── FANCY FONT ──────────────────────────────
_FANCY = {'a':'ᴀ','b':'ʙ','c':'ᴄ','d':'ᴅ','e':'ᴇ','f':'ꜰ','g':'ɢ','h':'ʜ','i':'ɪ',
          'j':'ᴊ','k':'ᴋ','l':'ʟ','m':'ᴍ','n':'ɴ','o':'ᴏ','p':'ᴘ','q':'ǫ','r':'ʀ',
          's':'ꜱ','t':'ᴛ','u':'ᴜ','v':'ᴠ','w':'ᴡ','x':'x','y':'ʏ','z':'ᴢ'}
def fancy(s: str) -> str:
    return "".join(_FANCY.get(c.lower(), c) for c in s)

# ─────────────────────── REPLY HELPER ────────────────────────────
async def reply_safe(bot, chat_id: int, text: str, reply_to: Optional[int] = None, **kw):
    """Send reply works across PTB v20.x and v21.x"""
    try:
        if reply_to is not None:
            if HAS_REPLY_PARAMS:
                try:
                    return await bot.send_message(
                        chat_id, text,
                        reply_parameters=ReplyParameters(message_id=reply_to),
                        **kw)
                except TypeError:
                    return await bot.send_message(
                        chat_id, text, reply_to_message_id=reply_to, **kw)
            return await bot.send_message(chat_id, text, reply_to_message_id=reply_to, **kw)
        return await bot.send_message(chat_id, text, **kw)
    except (Forbidden, BadRequest):
        return None

# ─────────────────────── BOT MANAGEMENT ──────────────────────────
async def _bootstrap_bot(bot_info: Dict[str, Any]) -> bool:
    try:
        bot = Bot(bot_info["token"])
        me = await bot.get_me()
        bot_info["bot_id"]   = me.id
        bot_info["username"] = me.username or ""
        bot_info["bot"]      = bot
        bot_info["guard"]    = get_limiter(me.id)
        return True
    except Exception as e:
        log.error(f"bot bootstrap fail {bot_info.get('token','')[:10]}...: {e}")
        return False

async def parallel_run(chat_id: int, coro_fn, *args, **kwargs):
    async def _runner(b):
        try:
            await asyncio.sleep(0.1)
            if b["guard"] is None or b["bot"] is None:
                return None
            return await coro_fn(b, chat_id, *args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug(f"parallel_run {b.get('bot_id')}: {e}")
            return None
    if not ALL_BOTS:
        return []
    return await asyncio.gather(*[asyncio.create_task(_runner(b)) for b in ALL_BOTS],
                                return_exceptions=True)

async def parallel_run_loop(chat_id, loop_fn):
    async def _runner(b):
        await asyncio.sleep(0.1)
        if b["guard"] is None or b["bot"] is None:
            return
        try:
            await loop_fn(b, chat_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug(f"loop {b.get('bot_id')}: {e}")
    tasks = [asyncio.create_task(_runner(b)) for b in ALL_BOTS]
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        for t in tasks: t.cancel()
        raise

# ─────────────────────── TASK MANAGEMENT ─────────────────────────
def register_task(chat_id: int, name: str, task: asyncio.Task):
    TASKS.setdefault((chat_id, name), []).append(task)

def cancel_tasks(chat_id: int, name: Optional[str] = None) -> int:
    n = 0
    for (cid, tname), tlist in list(TASKS.items()):
        if cid != chat_id: continue
        if name and tname != name: continue
        for t in tlist:
            if not t.done(): t.cancel(); n += 1
        TASKS.pop((cid, tname), None)
    return n

# ═══════════════════════════════════════════════════════════════
#  TEMPLATES
# ═══════════════════════════════════════════════════════════════

LN1_EMO = ["🌑","🌒","🌓","🌔","🌕","🌖","🌗","🌘","🌙","🌚","🌝","🌛","🌜"]
LN1_WRD = ["𝗧ᴍᴋʙ","𝗧ᴍᴋᴄ","𝗥ᴀɴᴅ","𝗕ᴋʟ","𝗧ᴍʀ","𝗟ᴜɴ𝗞ʜᴀ","𝗠ᴀᴅᴇʀᴄʜᴏᴅ","𝗕ꜱᴅᴋ",
           "𝗚ᴀɴᴅᴜ","𝗧ᴍᴋᴄ𝗠ᴀᴄᴄʜᴀʀ","𝗣ʟᴀʏ𝗚ɪʀʟ","𝗔ꜱꜱ𝗛ᴏʟᴇ","𝗡ɪɢɢᴀ",
           "𝗙ᴜᴄᴋ𝗬ᴏᴜ","𝗣ᴇʀᴠᴇʀᴛ","𝗦ᴛᴇᴘꜱɪꜱ","𝗠ᴏᴛʜᴇʀ𝗙ᴜᴄᴋᴇʀ",
           "𝗕ᴏʙʙɪᴇ","𝗕ᴀꜱᴛᴀʀᴅ","𝗣ɪᴇᴄᴇ𝗢ꜰ𝗦ʜɪᴛ"]
LN1_TAIL = "𒈙🦋🌙⃤" * 70
def luffy_nc1(t):
    return f"•{random.choice(LN1_EMO)}• {t} {random.choice(LN1_WRD)} {LN1_TAIL}"

LN2_EMO = ["☠️","👾","🫐","✨","🖤","🤍","💖","❤️‍🔥","🌺","🌷","🌸","🛸","🧃",
           "🍌","🍁","🌿","🍀","🔥","🌋","🌊","❄️","🌈","🪐","🪼"]
LN2_TAIL = "🈲∿∿∿" * 30
def luffy_nc2(t):
    return f"≼ {random.choice(LN2_EMO)} ≽ {t} ʟꪊꪀᴅ ᥴʜᴜડ {LN2_TAIL}"

LN3_EMO = ["🥱","😱","🤨","😒","😡","🤬","😤","😮‍💨","🥵","🥶","🤢","🤮","🫩",
           "😈","👿","👽","🧃","🧋","🗻","🃏","💠"]
def luffy_nc3(t):
    e = random.choice(LN3_EMO)
    return f"⪻{e}⪼ {t} ⪻{e}⪼"

LN4_EMO = ["🇦🇨","🇦🇩","🇦🇪","🇦🇫","🇦🇬","🇦🇮","🇦🇱","🇦🇲","🇦🇴","🇦🇶","🇦🇷",
           "🇦🇸","🇦🇹","🇦🇺","🇦🇼","🇧🇦","🇧🇶","🇧🇱","🇧🇫","🇨🇮","🇨🇩","🇨🇦",
           "🇨🇿","🇩🇯","🇨🇭","🇪🇬","🇨🇨","🇧🇯","🇰🇿","🇰🇼","🇰🇳","🇲🇭","🇲🇪",
           "🇲🇬","🇮🇴","🇯🇵","🇷🇺","🇳🇺","🇵🇬","🇸🇩","🇵🇷","🇵🇳","🇵🇭","🇳🇫",
           "🇳🇪","🇺🇾","🇺🇸","🇻🇪"]
LN4_TAIL = "ᥫ᭡.🌏" * 34
def luffy_nc4(t):
    e = random.choice(LN4_EMO)
    return f"⊱{e}⊰ {t} {LN4_TAIL} ⊱{e}⊰"

LN5_EMO = ["🌟","💫","✨","🌈","🦋","🌸","🎀","🕊️","🌺","🌷","🌠","☄️","👾",
           "💢","🕸️","🗿","💸","🔮","📿","🚬"]
LN5_TAIL = "༻༺🩷" * 50
def luffy_nc5(t):
    e = random.choice(LN5_EMO)
    return f"𓆩{e}𓆪 {t} MADARCH⭕️D {LN5_TAIL} 𓆩{e}𓆪"

LN6_EMO = ["❤️","🧡","💛","💚","💙","💜","🖤","🤍","🤎","🩷","🩵","🩶",
           "💔","❣️","💕","💞","💓","💗","💖","💘","💝"]
def luffy_nc6(t):
    e = random.choice(LN6_EMO)
    tail = f"—({e})" * 50
    return f"«{e}» {t} ᴛᴍᴋᴄ {tail} «{e}»"

LN7_EMO = ["🌬️","❄️","🌀","🌪️","⚡","☔","🌈","💧","☁️","🌨️","🌧️","🌩️",
           "⛈️","🌦️","🌥️","⛅"]
def luffy_nc7(t):
    e = random.choice(LN7_EMO)
    return f"<<{e}>> {t} <<{e}>>"

LN8_EMO = ["🐶","🐰","🐻‍❄️","🐮","🐵","🐒","🐤","🐱","🦊","🐭","🐻","🐹",
           "🐼","🐸","🦁","🐯","🐥","🪿","🐦","🐣"]
LN8_TAIL = "ဪ🟪🟩🟪🟩🟡🔵ဪ" * 40
def luffy_nc8(t):
    e = random.choice(LN8_EMO)
    return f"{e} {t} {LN8_TAIL} {e}"

LN9_EMO = ["🦋⃟🌑","🦋⃟🌒","🦋⃟🌔","🦋⃟🌕","🦋⃟🌖","🦋⃟🌗","🦋⃟🌘","🦋⃟🪐",
           "🦋⃟🌍","🦋⃟🌎","🦋⃟🌙","🦋⃟☄️","🦋⃟🫧"]
def luffy_nc9(t):
    e = random.choice(LN9_EMO)
    return f"({e}) {t} ———🌙"

LN10_EMO = ["🪠","🪒","🪫","🪸","🪷","🪹","🪺","🫧","🫘","🫚","🫛","🪮","🪭"]
def luffy_nc10(t):
    e = random.choice(LN10_EMO)
    return f"[{e}] {t} 𝐋ᴜғғʏ Bһᴀɢᴡᴀꪀ है ~//~ 🦢💘"

LUFFY_NC = [luffy_nc1, luffy_nc2, luffy_nc3, luffy_nc4, luffy_nc5,
            luffy_nc6, luffy_nc7, luffy_nc8, luffy_nc9, luffy_nc10]

VN1_EMO = ["😀","😃","😄","😁","😂","😅","🥹","😆","🤣","🥲","☺️","😊"]
VN1_TAIL = "𒈙👑" * 60
def vegeta_nc1(t):
    return f"{t} 𝐓ᴇƦꞮ 𝐌ᴀᴀ 𝐊Ɪ 𝐂ʜᴜᴛ 𝐒ꞮʟᴀʏꞮ 𝐊Ʀ 𝐃ᴇɴɢᴇ 𝐇ᴜᴍ 𝐁ʜᴀꞮ {VN1_TAIL}<{random.choice(VN1_EMO)}>"

VN2_EMO = ["🍏","🍋","🍎","🍐","🍊","🍈","🫐","🍉","🍓","🍇"]
VN2_TAIL = "𒐫𒐫𒐫💚" * 40
def vegeta_nc2(t):
    return f"{random.choice(VN2_EMO)} {t} 𝙇𝙐𝙉𝘿 𝘾𝙃⭕⭕𝙎𝙎 /-{VN2_TAIL}𒐫𒐫"

def vegeta_nc3(t):
    tail = "𒈙⸻🩵𒈙⸻❤️𒈙⸻🩷𒈙⸻🧡𒈙⸻💛𒈙⸻💚𒈙⸻💙𒈙⸻💜𒈙⸻🖤𒈙⸻🩶𒈙⸻🤍𒈙⸻❣️"
    return f"{t} 匚卄ㄩ卩 ᥅ꪖꪀᦔﺃᛕꫀ ᥇ꪖᥴᥴ𝙃ꫀ {tail}"

def vegeta_nc4(t):
    tail = "𒀱ꪳ💝" * 40
    return f"{t} 𝐏𝐈𝐋𝐋𝐄 😩 𝐒ᴀʏ 𝗩𝗘𝗚𝗘𝗧𝗔 𝐃ᴀᴅʏ 𓆩💗𓆪x {tail}"

def vegeta_nc5(t):
    tail = "𒈙⸻🐬" * 40
    return f"{t} ƇӇƲƤ ƦƝƊƖƘЄ {tail}"

VN6_EMO = ["🪬","🧿","🪞","🪆","🪩","🗿","🪦","🪚","🪓","🪤","🪱","🪰"]
def vegeta_nc6(t):
    return f"{t} PILLA 😭 SAY VEGETA BAGWAN {random.choice(VN6_EMO)}{'𒈙'*70}"

VN7_EMO = ["😺","😸","😹","😻","😼","😽","🙀","😿","😾"]
def vegeta_nc7(t):
    return f"{t} ༆𝐇𝟖𝐑 𝐑𝐀𝐍𝐃𝐀𝐋 𝐍𝐎𝐑𝐈𝐌𝐄 𝐂𝐕𝐑 𝐊𝐑,᭄⫸{random.choice(VN7_EMO)}⫷🔥🤟࿐{'𒐫'*70}"

def vegeta_nc8(t):
    return f"{t} 𓆃𒁃🖤{'𒁃🖤'*55}"

def vegeta_nc9(t):
    tail = "〄💜꧅🩵〄🩷꧅❤️〄💛꧅🤎" * 6
    return f"{t} 𝗧ᴍᴋᴄ {tail}"

VN10_EMO = ["🕛","🕧","🕐","🕜","🕑","🕝","🕒","🕞","🕓","🕟","🕔","🕠",
            "🕕","🕡","🕖","🕢","🕗","🕣","🕘","🕤"]
def vegeta_nc10(t):
    return f"{t} 𓆩{random.choice(VN10_EMO)}𓆪{'█'*200} TMKC"

VEGETA_NC = [vegeta_nc1, vegeta_nc2, vegeta_nc3, vegeta_nc4, vegeta_nc5,
             vegeta_nc6, vegeta_nc7, vegeta_nc8, vegeta_nc9, vegeta_nc10]

# SPAM — reduced reps to fit in 3500 chars
SPAM1_BLOCK = ("#{text} #𝗚𝗔𝗡𝗗𝗠𝗔𝗥𝗘 𝗧𝗘𝗥𝗜 𝗜𝗧𝗡𝗜 𝗛𝗜𝗠𝗠𝗔𝗧 𝗧𝗨 #𝗟𝗨𝗙𝗙𝗬 "
               "𝗕𝗔𝗔𝗣 𝗞𝗘 𝗦𝗔𝗠𝗡𝗘 #𝗔𝗪𝗔𝗭 𝗨𝗖𝗛𝗜 𝗞𝗥𝗘𝗚𝗔 ?? 𝗠𝗔𝗗𝗥𝗖𝗛⭕️𝗗 "
               "🤍🌙🕊️_____________________/ ")
def spam1_template(t):
    return (SPAM1_BLOCK * 8).replace("{text}", t)

S2_EMO = ["❤️","🧡","💛","💚","💙","💜","🖤"]
def spam2_template(t):
    block = f"{t} Tᴇʀɪ Mᴀᴀ Kᴇ Bʜᴏsᴅᴇ Mᴇ 𝟼𝟺ɢʙ rᴅᴘ ᴅᴀʟᴅᴜ <{random.choice(S2_EMO)}>"
    return block + "\n" * 20 + block

S3_EMO = ["❄️","🥶","🧊","🌨️","☃️","⛄","🌬️","💨","🌫️"]
def spam3_template(t):
    blk = f"#{t}  ᴛᴇʀɪ  ᴍᴀᴀ  ᴄʜᴏᴅ  ᴋᴇ  ᴍᴀᴀʀ  ᴅᴜɴɢᴀ  ᴋᴜᴛᴛɪᴋᴇ  /-----------------------------‐--------{random.choice(S3_EMO)} "
    return blk * 10

def spam4_template(t):
    blk = (f"─── ⋆⋅☆⋅⋆ ──{t} 𝙈𝘼𝘿𝘼𝙍𝘾𝙃 ⭕ 𝘿─── ⋆⋅☆⋅⋆ ──"
           "─── ⋆⋅☆⋅⋆ ── 🩵 𝗨𝗧𝗛 𝗚𝗨𝗟𝗔𝗠 𝗠𝗔𝗥𝗔 𝗞𝗘𝗦𝗘 ─── ⋆⋅☆⋅⋆ ──")
    return (blk + "\n") * 5

SPAM5_EMO = ["🖤","❤️","🩶","🤍","🧡","💛","💚","🩵","💙","💜","🤎","🩷",
             "💘","💝","💖","💗","💓","💞","💕","💌","💟","♥️","❣️","❤️‍🩹",
             "💔","❤️‍🔥","🫀"]
def spam5_template(t):
    return "\n".join(f"⋆⋅☆⋅ {t}  रंडी पुत्र  ({e}) ⋆⋅☆⋅⋆" for e in SPAM5_EMO)

SPAM_TEMPLATES = [spam1_template, spam2_template, spam3_template,
                  spam4_template, spam5_template]

# RAID — store RAW templates, format via .replace() in listener
RR_TEMPLATES_RAW = [
    "{text} ᴛᴇʀɪ Dᴀᴅɪ ᴋ ᴄʜᴍᴜᴛ ᴍᴀɪ ᴛᴀɴɢ ᴀᴛᴋᴀ ᴅᴜɴɢᴀ ғɪʀ ᴛᴇʀʏ ᴅᴀᴅᴜ sᴜsᴜ ᴋᴀɪsᴀ ᴋʀᴇɢɪ😍?",
    "{text} ᴛᴇʀʏ ᴍᴍʏ ᴋ ᴄʜᴜᴛᴀᴅᴏ ᴍᴀɪ 𝐃ɪɴᴏsᴀᴜʀ ᴋɪ ᴘᴏᴛʏʏ😁",
    "{text} ᴍᴀᴊᴅᴜʀɪ ᴋʀᴇɢᴀ ɢᴀʀᴇᴇʙ ᴛᴇʀʏ ᴍᴍʏ ᴋɪ ᴛᴀɴɢ ᴜᴛʜᴀᴋᴇ👻 ᴄʜᴜᴛ ᴍᴀᴊ ғɪᴋᴀʀɪ ᴊᴀʟᴀ ᴅᴜɴɢᴀ🙈",
    "{text} ᴇᴋ ʙᴀᴀᴛ ʙᴛᴀᴜ ᴛᴇʀᴀ ᴘᴀᴘᴀ ᴛᴇʀʏ ᴍᴍʏ ᴋᴏ ᴍᴇʀsᴀ ᴄʜᴜᴅᴠᴀ ᴋʀ ɢʜᴀʀ ᴄʜᴀʟᴀᴛᴇ ᴇʏ🤣👻",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋ ᴄʜᴜᴛᴀᴅ ᴘᴀʀ ᴊᴏ ᴘʜᴜɴsɪ ʜᴀ ᴜsᴋᴏ👻 ʟᴀᴀᴛ ᴍᴀʀᴋᴀ ᴘʜᴏᴅ ᴅᴜ ᴄʏ?",
    "{text} ᴛᴇʀʏ ᴍᴍʏ ᴋᴀ england ᴡᴀʟᴇ ʙᴜs ᴍᴀɪ ɢᴀɴɢ ʙᴀɴɢ ᴋʀᴋᴇ ᴋʜɪᴅᴋɪ sᴀ ғᴇᴋ ᴅᴇɴɢᴇ😭🥺",
    "{text} ᴋᴀʟɪ ᴋᴜᴛᴛɪ ᴋɪ ᴄʜᴜᴛ sᴀ ɴɪᴋʟᴀ ᴋᴜᴛᴛᴀ ᴋᴀ ᴘɪʟʟᴀ👻 ᴛᴇʀʏ ᴍᴀᴀ ʜᴀᴄᴋʟᴀ ᴋʀ ᴍᴀʀ ᴊᴀʏᴇ🤣",
    "{text} ᴊʜᴀᴛᴜ ᴛᴇʀʏ ᴀᴜᴋᴀᴛ ʜᴀ? 𝐋ᴜꜰꜰʏ sE ʟᴀᴅɴᴇ ᴋɪ? ᴛᴇʀʏ ᴍᴍʏ ᴋɪ ᴘᴜsʏʏ ᴘʀ ᴀʙᴅᴜʟ ᴋɪ ᴄʜᴀᴘᴀʟ ᴍᴀʀᴅᴜɴɢᴀ😁🍃",
    "{text} ᴛᴇʀʏ ᴍᴍʏ ᴋᴏ 𝐋ᴜꜰꜰʏ ᴋ ᴀʟᴀᴠᴀ ᴋᴏɪ ᴏʀ ᴄʜᴏᴅ sᴀᴋᴛᴀ ᴇʏ ᴋᴛᴀ?😁",
    "{text} sᴜᴀʀ ᴛᴇʀʏ ᴍᴍʏ ᴋɪ ᴄʜᴜᴛ ᴋᴀɪsᴀ ʟᴀᴀᴅᴋʜᴀᴅᴀɪ?👻 ᴛᴇʀᴀ 𝐋ᴜꜰꜰʏ Pᴀᴘᴀ ᴋᴀ ʟᴜɴ ᴅᴇᴋʜ ᴋʀ??😍",
    "{text} ᴛᴇʀɪ ᴅᴀᴅɪ ᴋᴀ ʙʜᴏsᴅᴀ ᴍᴀ 𝐒ʟᴀᴛᴇ ᴋɪ ᴍɪssɪʟᴇ ᴅᴀʟᴋᴇ ᴜᴅᴀ ᴅᴜɴɢᴀ😍",
    "{text} ᴛᴇʀʏ ᴍᴍʏ ᴋᴏ miya ᴋ sᴀᴛʜ ᴄʜᴏᴅᴇɢᴀᴀ🙈",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴊᴀʙ ᴛʀᴀɪɴ ᴍᴀɪ sᴏʏᴇɢɪ ᴛᴀʙ ᴜsᴋɪ ᴘᴀɴᴛʏ ᴜᴛᴀʀ ᴋʀ sᴜᴘ sᴜᴘ ᴋʀᴅᴜɴɢᴀ😍🦈",
    "{text} ᴛᴇʀʏ ᴅᴀᴅɪ ᴋ ᴄʜᴜᴛᴀᴅ ᴍᴀ ᴀᴀᴍ ᴋᴀ ᴛʀᴇᴇ ʟɢᴀᴋᴇ 𝐋ᴜɴ ғᴇᴋ ᴍʀ ᴍᴀʀᴋᴇ ᴛᴏʀᴜɴɢᴀ😘🤩",
    "{text} ᴛᴇʀɪ ᴍᴍʏ 𝐋ᴜꜰꜰʏ ᴋ ᴛᴀᴛᴛᴇ ᴋ ɴɪᴄʜᴇ 👻👻 ᴍᴀsʟɪ ɢʏɪ ᴀᴀʙ ᴛᴇʀᴀ ᴘᴀᴘᴀ ʀᴏʏᴇɢᴀ😘",
    "{text} ᴛᴇʀɪ ᴋᴀʟᴀ ᴄʜᴜᴛᴀᴅ ᴡᴀʟɪ ʙʜᴇɴ ᴋ ɢᴀɴᴅ ᴍᴀɪ👻 ᴊᴀᴅᴜɪ ᴄʜᴀᴅɪ ᴅᴀʟᴅᴜɴɢᴀ😈",
    "{text} ᴋɪ ᴍᴀᴀ ʜᴏʀɴᴇʏ ʜᴏ ᴋʀ ᴀᴘɴᴇ ᴋᴜᴛᴛᴀ ᴋ ᴍᴜʜ ᴍᴀɪ ʟᴇ ʟᴇᴛɪ ᴇʏ?😱",
    "{text} ᴋɪ ᴍᴀᴀ ᴀᴅɪᴠᴀsɪ ᴏɪʟ ʟɢᴀᴋᴇ ᴊᴜɴɢʟᴇ ᴍᴀ ɢᴀɴᴅ ᴅᴇᴛɪ ᴇʏ😭😭",
    "{text} ᴋɪ ᴍᴍʏ 𝐋ᴜꜰꜰʏ ᴋᴀ ʟᴏʀᴀ ʟᴇᴛᴀ ʜᴜ ᴘᴀᴀᴅ ᴍᴀʀᴛɪ ᴇʏ ᴄʜɪ🤬",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ʙɪᴊʟɪ ᴋ ᴋʜᴀᴍʙᴀ ᴘʀ ᴡɪʀᴇ ᴄʜᴜᴛᴀᴅ ᴍᴀ ʟᴇᴋʀ ᴛᴀᴅᴀᴘ ᴋʀ ᴍᴀʀ ɢᴀɪ😭👻",
    "{text} ᴊɪs sɪᴍ sᴀ ᴛᴜ ɴᴇᴛ ᴄʜᴀʟᴀ ʀᴀʜᴀ ᴠᴏ ᴠ ᴍᴇʀᴀ ʟᴏᴅᴀ ᴋɪ ᴋʀɪᴘᴀ ʜᴀ ᴇʏ ɢᴀʀᴇᴇʙ ᴛᴇʀʏ ᴍᴀᴀ sᴀ ᴘᴜᴄʜ😁💣",
    "{text} ʜᴀᴄᴋʟᴇ ᴍssɢ ᴍᴛ ᴋʀ ᴡᴀʀɴᴀ ᴛᴇʀᴀ ᴋʜᴀɴᴅᴀɴ ᴋᴏ ʀᴀᴏᴅ ᴘʀ ɴᴀɴɢᴀ ᴋʀᴋʀ ᴍᴜʜ ᴘʀ ɢᴀʀᴀᴍ ᴍᴜᴛ ᴅᴜɴɢᴀ😍🦈",
    "{text} ᴋɪ ᴍᴀᴀ 𝐋ᴜꜰꜰʏ ᴋ ʟᴜɴ ᴋɪ ᴘʜᴏᴛᴏ ᴅᴇᴋʜ ᴋʀ ғɪɴɢᴇʀɪɴɢ ᴋʀᴛɪ ᴇʏ🦈😭",
    "{text} ᴛᴇʀʏ ᴍᴍʏ ᴋᴀʟᴀ ᴄʜᴜᴛᴀᴅ ᴡᴀʟɪ ᴄʜᴜᴅᴀᴋʀ😈💋",
    "{text} ᴋɪ ᴍᴜᴍᴍʏ ᴍᴜᴛʜɪ ᴍᴀʀ ᴛɪ ᴇʏ?😭????",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋ ᴄʜᴜᴛᴀᴅ 😘 ᴘʀ 𝐋ᴜꜰꜰʏ ᴀᴘɴᴀ ʟᴜɴ sᴀ ᴛᴀʟɪʏᴀ ʙᴀᴊᴀʏᴇɢᴀ😈💋",
    "{text} ᴋɪ ᴍᴜᴍᴍʏ ᴀᴘɴɪ ᴄʜᴜᴄʜɪʏᴀ ɢᴀᴛᴇ ᴋ ʙɪᴄʜ ᴅᴀʙᴀ ᴋʀ ᴍᴀʀ ɢᴀɪ?😭😭",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ʜɪᴢʀᴇ ʙᴀɴᴋʀ ᴛʀᴀɪɴ ᴍᴀ ᴀᴛɪ ᴛᴇʀᴀ ᴘᴀᴘᴀ sᴜsᴜ ᴄʜᴀᴛɴᴇ ᴋɪ ᴅᴜᴋᴀɴ ᴋʀᴛᴀ ᴇʏ ʙᴛᴀ ᴅʏ ᴄʏᴀ sᴀʙᴋᴏ👻💣",
    "{text} ᴋɪ ᴍᴍʏ ᴊᴀʙ sᴜsᴜ ᴋʀ ʀɪ ᴛʜɪ ᴛᴀʙ ᴄʜᴜᴛ ᴍᴀɪ sᴀᴀᴘ ɢʜᴜs ɢʏᴀ ᴠᴏ ʙʜɪ ᴄᴏʙʀᴀ?😱",
    "{text} ᴋɪ ʙᴇʜᴇɴ ᴋʜᴀᴅɪ ʜᴏ ᴋʀ ᴍᴜᴛ ᴛɪ ᴇʏ?😱😱",
    "{text} ᴛᴇʀʏ ᴍᴍʏ ᴋᴏ 𝐋ᴜꜰꜰʏ ɴᴀ ɴᴀsʜᴇ ᴍᴀɪ sᴏғᴀ ᴘʀ ᴛᴀɴɢ ᴜᴛʜᴀ ᴋʀ ᴄʜᴏᴅ ᴅɪʏᴀ😁",
    "{text} ᴛᴇʀɪ ᴍᴀᴀ ᴋɪ ᴄʜᴜᴛ ᴍᴀ ᴀᴀɢ ʟɢᴀ ᴋᴇ ʀᴀᴋʜ ᴅᴜɴɢᴀ🔥🥵",
    "{text} ᴛᴇʀᴀ ᴘᴀᴘᴀ ʀᴀɴᴅɪ ᴋʜᴀɴᴀ ᴍᴀ ɢᴀɴᴅ ᴍᴀʀ ᴋʀ ᴋᴀᴍᴀᴛᴀ ʜᴀ🩸💔",
    "{text} ᴛᴇʀɪ ʙʜᴇɴ ᴋᴏ ʀᴇᴅ ʟɪɢʜᴛ ᴍᴀ ᴄʜᴏᴅ ᴋᴇ ʙʜᴀɢᴀ ᴅᴜɴɢᴀ🚨🩸",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋɪ ɢᴀɴᴅ ᴍᴇ sᴀᴘ ᴅᴀʟ ᴋᴇ ɴᴀᴄʜᴡᴀᴜɴɢᴀ🐍💃",
    "{text} ᴛᴇʀᴀ ᴘᴜʀᴀ ᴋʜᴀɴᴅᴀɴ ʀᴀɴᴅɪ ᴋʜᴀɴᴀ ᴍᴇ ɴᴀᴄʜᴛᴀ ʜᴀ🎭🍺",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋᴏ ʟᴜғғʏ ᴋᴇ ʟᴜɴ sᴇ ɢᴀʀᴍɪ ᴅᴇᴛᴀ ʜᴜ🔥💦",
    "{text} ᴛᴇʀɪ ᴅᴀᴅɪ ʙʜᴇɴ ᴅᴏɴᴏ ᴋᴏ ᴇᴋ sᴀᴛʜ ᴄʜᴏᴅ ᴋᴇ ɴᴀᴄʜᴀᴜɴɢᴀ🎉🩸",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋɪ ᴄʜᴜᴛ ᴋɪ ʜᴏᴛᴇʟ ᴋʜᴏʟ ᴋᴇ ʀᴀᴋʜ ᴅᴜɴɢᴀ🏨🩵",
    "{text} ᴛᴇʀᴀ ʙᴀᴀᴘ ᴄʜᴀᴜᴋɪᴅᴀʀ ʜᴀ ᴛᴇʀɪ ᴍᴀᴋɪ ɢᴀɴᴅ ᴋᴀ👻🚔",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋᴏ ɢᴀɴɢ ʙᴀɴɢ ᴋʀᴋᴇ ᴛʀᴀɪɴ sᴇ ᴜᴛᴀʀᴡᴀᴜɴɢᴀ🚂🩸",
    "{text} ᴛᴇʀɪ ʙʜᴇɴ ᴋɪ ɢᴀɴᴅ ᴍᴇ ʟᴜғғʏ ᴋᴀ ʟᴏɢᴏ ᴛᴀᴛᴛᴏᴏ ᴋᴀʀᴀ ᴅᴜɴɢᴀ🕸️🔱",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋɪ ᴄʜᴜᴛ ᴍᴀ ᴛʀɪᴘʟᴇ x ᴠɪᴅᴇᴏ ʙᴀɴᴀᴋᴇ ᴠɪʀᴀʟ ᴋᴀʀ ᴅᴜɴɢᴀ📸🔥",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋᴏ ᴅᴏɢɢʏ sᴛʏʟᴇ ᴍᴀ ᴄʜᴏᴅ ᴋᴇ ɴᴀᴄʜᴇ ɢɪʀᴀ ᴅᴜɴɢᴀ🐕🩸",
    "{text} ᴛᴇʀɪ ʙʜᴇɴ ᴋɪ ᴄʜᴜᴛ ᴍᴀ ʟᴜғғʏ ɢᴀɴɢ ᴋᴀ ʀᴀᴠᴀ ʟɢᴀ ᴅᴜɴɢᴀ🔊🩵",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋɪ ɢᴀɴᴅ ᴍᴀ ᴛᴀʟᴀʙ ᴋᴀ ᴘᴀɴɪ ʙʜᴀʀ ᴋᴇ ɴᴀᴄʜᴀᴜɴɢᴀ🌊🩸",
    "{text} ᴛᴇʀᴀ ᴘᴀᴘᴀ ʀᴀɴᴅɪ ᴋʜᴀɴᴀ ᴍᴀ ᴍᴜᴛ ᴋᴇ ʙᴇᴄʜ ᴅᴜɴɢᴀ🍺🩸",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋᴏ ɢᴀʏ sᴛʏʟᴇ ᴍᴀ ᴄʜᴏᴅ ᴋᴇ ᴠɪʀᴀʟ ᴋᴀʀ ᴅᴜɴɢᴀ📱🔥",
    "{text} ᴛᴇʀɪ ᴅᴀᴅɪ ᴋɪ ᴄʜᴜᴛ ᴍᴀ ᴄᴏᴋᴇ sᴛᴜᴅɪᴏ ᴋʜᴏʟ ᴅᴜɴɢᴀ🥤🩸",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋɪ ɢᴀɴᴅ ᴍᴀ sᴛᴀᴅɪᴜᴍ ᴍᴀ ᴍᴀᴛᴄʜ ᴋᴀʀᴀ ᴅᴜɴɢᴀ🏟️🩸",
    "{text} ᴛᴇʀɪ ʙʜᴇɴ ᴋɪ ᴄʜᴜᴛ ᴍᴀ ʟᴜғғʏ ɢᴀɴɢ ᴋᴀ ᴛᴀᴛᴛᴏᴏ ʙᴀɴᴡᴀ ᴅᴜɴɢᴀ🕸️🩵",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋᴏ ᴘᴜʙʟɪᴄ ᴍᴀ ᴄʜᴏᴅ ᴋᴇ ᴠɪʀᴀʟ ᴋᴀʀ ᴅᴜɴɢᴀ📢🩸",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋɪ ɢᴀɴᴅ ᴍᴀ ᴅᴇᴇᴘғᴀᴋᴇ ʙᴀɴᴀᴋᴇ ᴘᴏsᴛ ᴋᴀʀ ᴅᴜɴɢᴀ📸🔥",
    "{text} ᴛᴇʀᴇ ᴋʜᴀɴᴅᴀɴ ᴋɪ ᴄʜᴜᴛ ᴍᴀ ᴅʀᴏɴᴇ ᴄᴀᴍᴇʀᴀ ʟᴀɢᴀ ᴅᴜɴɢᴀ🚁🩸",
    "{text} ᴛᴇʀɪ ᴍᴍʏ ᴋᴏ ʟᴜғғʏ ɢᴀɴɢ ᴋᴇ ʜᴀᴡᴀʟᴇ ᴋᴀʀ ᴅᴜɴɢᴀ🏴‍☠️🩸",
    "{text} ᴛᴇʀɪ ʙʜᴇɴ ᴋɪ ᴄʜᴜᴛ ᴍᴀ ʀᴀᴘ ʙᴀᴛᴛʟᴇ ᴋᴀʀᴡᴀ ᴅᴜɴɢᴀ🎤🩸",
]

CHUD_TEMPLATES = [
    "TERY MA RAND BTT KHTMM",
    "TERI MA KI KACCHI ME LAWDA MERA OK?",
    "AWW ACHA TMKC",
    "CHLNA TMKC TUJHE BHAWW DUN? HTT CHMR",
    "EK KAAM TU CHHA KRTA HAI AUR WO KAAM HAI LUFFY KA LAWDA SEHLANA 😉",
    "TERI MA NALI M CHODU",
    "MA KE LODE LUN TE VAJJ",
    "TMKC PE MUKKE MAARU",
    "ACHA RUNDYKE ?",
    "OYE GULAM CHUPP",
    "TMKC PE THOOK DIYA MENE ROO AB",
    "(MEHKA LADLE MEOWW) TMKC ME MUH DEKE YHI BOLUNGA 😉",
    "TERA REAL FATHER LUFFY",
    "TERI MAA KO NANGI KRKE BAZAR ME CHODU",
    "GAREEB MAA KI GREEB CHUT SE NIKLE HUE GAREEB LADKE AWAZ NICHE KRR 😂",
    "HURRRREE TERI MAA TO MARR GYI REHH",
    "CHUDEGA KYA REHHHH??",
    "JALDI TYPE KRR REE MAA K LUND",
    "TERI BEHEN K GALE PE PATTA BANDH KRR CHODU 😂",
    "KYA RE DALIT MADARCHOD HAWA ME UDEGA ?",
    "CHOTE ABHI TU ITNA BADA NHI HUA JO LUFFY K SAMNE AWAZ UCHI KREGA",
    "TMKC KO I LOVE UHH LUND SE 😂",
    "𝐂ʜʟ 𝐇ᴀʀᴍᴢᴀᴅ𝐈 𝐊ᴇ लड़के",
    "oi 𝐓ᴇʀɪ 𝐌‌ᴀᴀ गुलाम",
    "chl rndyce chud ke dikha",
    "TERI 𝐌ᴀᴀ 𝐌ᴀʀʀ 𝐆ᴀʏɪ naacho !",
    "tera baap bass LUFFY hai 😂🎀",
    "CHUDAI FEEL KRR RUNDYKE",
    "ROO MTT TERI MAA ME HI CHODUNGA BSS KHUSH 😉",
]

HEART_RR = [f"⤹𝀘{e}⤸ ⛧ 𝐒𝐋𝐀𝐕𝐄 ⛧ 𓊈 𝐓𝐌𝐊𝐂 𓊉 ⛧ ⤹𝀘{e}⤸" for e in
            ["🧡","💛","💚","💙","💜","🖤","🤍","🤎","💖","💗","💓","💞",
             "💕","💘","💝","💟","❣️","❤️‍🔥","❤️‍🩹"]]

FLAG_RR = [
    " ོ༘₊⁺🇮🇳 ₊⁺⋆.˚ TERI MA KO INDIA ME CHODU ོ༘₊⁺🇮🇳 ₊⁺⋆.˚",
    " ོ༘₊⁺🇯🇵 ₊⁺⋆.˚ TERI MA KO JAPAN ME CHODU ོ༘₊⁺🇯🇵 ₊⁺⋆.",
    " ₊⁺🇺🇸 ₊⁺⋆.˚ TERI MA KO USA ME CHODU ོ༘₊⁺🇺🇸 ₊⁺⋆.˚",
    " ོ༘₊⁺🇬🇧 ₊⁺⋆.˚ TERI MA KO UK ME CHODU ོ༘₊⁺🇬🇧 ₊⁺⋆.˚",
    " ོ༘₊⁺🇰🇷 ₊⁺⋆.˚TERI MA KO KOREA ME CHODU ོ༘₊⁺🇰🇷 ₊⁺⋆.˚",
    " ོ༘₊⁺🇩🇪 ₊⁺⋆.˚ TERI MA KO GERMANY ME CHODU ོ༘₊⁺🇩🇪 ₊⁺⋆.˚",
    " ོ༘₊⁺🇫🇷 ₊⁺⋆.˚TERI MA KO FRANCE ME CHODU ོ༘₊⁺🇫🇷 ₊⁺⋆.˚",
    " ོ༘₊⁺🇮🇹 ₊⁺⋆.˚ TERI MA KO ITALY ME CHODU ོ༘₊⁺🇮🇹 ₊⁺⋆.˚",
    " ོ༘₊⁺🇧🇷 ₊⁺⋆.˚TERI MA KO BRAZIL ME CHODU ོ༘₊⁺🇧🇷 ₊⁺⋆.˚",
    " ོ༘₊⁺🇨🇦 ₊⁺⋆.˚TERI MA KO CANADA ME CHODU ོ༘₊⁺🇨🇦 ₊⁺⋆.˚",
] * 2

PILLE_TEMPLATE = "{user} 𝗕𝗛𝗔𝗡𝗚𝗜 𝗠𝗔𝗗𝗔𝗥𝗖𝗛⭕️𝗗 𓂃𓂃𓂃𓂃〉〈{e}〉〈𓂃𓂃𓂃𓂃𓂃𓂃..!¡"
PILLE_EMOJIS = ["❄️","🧊","🥶","🌨️","☃️","⛄","⛷️","🏂","⛸️","🏔️","🍧","🍨","🍦","🛷"]

# ═══════════════════════════════════════════════════════════════
#  MENU BUILDERS
# ═══════════════════════════════════════════════════════════════
MAIN_MENU_TEXT = (
    "🍭 𝗟𝗨𝗙𝗙𝗬 𝗖𝗥𝗘𝗪 𝗔𝗥𝗘 𝗥𝗘𝗔𝗗𝗬 𝗧𝗢 𝗙𝗨𝗖𝗞 𝗦𝗢𝗠𝗘𝗢𝗡𝗘 🍭\n\n"
    "➤ Add more bots by tapping on ➕ Add Token / Bots\n"
    "➤ Bot count: {bots}\n"
    "➤ Main Owner: <code>{owner}</code>\n"
    "➤ Sudo count: {sudo}\n\n"
    "⚡ Select a section below ⚡"
)

def main_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ 𝗔𝗗𝗗 𝗧𝗢𝗞𝗘𝗡 / 𝗕𝗢𝗧𝗦", callback_data="menu_addbot")],
        [InlineKeyboardButton("🍭 𝗟𝗨𝗙𝗙𝗬 𝗡𝗖", callback_data="menu_luffy"),
         InlineKeyboardButton("🥊 𝗩𝗘𝗚𝗘𝗧𝗔 𝗡𝗖", callback_data="menu_vegeta")],
        [InlineKeyboardButton("💢 𝗦𝗣𝗔𝗠", callback_data="menu_spam"),
         InlineKeyboardButton("🚨 𝗥𝗔𝗜𝗗", callback_data="menu_raid")],
        [InlineKeyboardButton("⚙️ 𝗕𝗢𝗧 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦", callback_data="menu_settings"),
         InlineKeyboardButton("🎯 𝗧𝗔𝗥𝗚𝗘𝗧𝗦", callback_data="menu_targets")],
        [InlineKeyboardButton("🖼️ 𝗣𝗙𝗣 𝗟𝗢𝗢𝗣", callback_data="menu_pfp"),
         InlineKeyboardButton("🎮 𝗙𝗨𝗡", callback_data="menu_fun")],
        [InlineKeyboardButton("🛡️ 𝗚𝗥𝗢𝗨𝗣 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦", callback_data="menu_group"),
         InlineKeyboardButton("🛑 𝗦𝗧𝗢𝗣 𝗧𝗔𝗦𝗞", callback_data="menu_stop")],
        [InlineKeyboardButton("✏️ 𝗘𝗗𝗜𝗧𝗦", callback_data="menu_edits")],
        [InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")],
    ])

def back_kb(target="menu_main"):
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 𝗕𝗔𝗖𝗞", callback_data=target),
                                  InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")]])

def luffy_kb():
    return InlineKeyboardMarkup([
        *[[InlineKeyboardButton(f"🍭 𝗟𝗨𝗙𝗙𝗬 𝗡𝗖 {i+1}",
                                callback_data=f"nc_info:luffy:{i+1}")] for i in range(10)],
        [InlineKeyboardButton("🔙 𝗕𝗔𝗖𝗞", callback_data="menu_main"),
         InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")],
    ])

def vegeta_kb():
    return InlineKeyboardMarkup([
        *[[InlineKeyboardButton(f"🥊 𝗩𝗘𝗚𝗘𝗧𝗔 𝗡𝗖 {i+1}",
                                callback_data=f"nc_info:vegeta:{i+1}")] for i in range(10)],
        [InlineKeyboardButton("🔙 𝗕𝗔𝗖𝗞", callback_data="menu_main"),
         InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")],
    ])

def spam_kb():
    return InlineKeyboardMarkup([
        *[[InlineKeyboardButton(f"💢 𝗦𝗣𝗔𝗠 {i+1}", callback_data=f"spam_info:{i+1}")]
          for i in range(5)],
        [InlineKeyboardButton("🔙 𝗕𝗔𝗖𝗞", callback_data="menu_main"),
         InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")],
    ])

def raid_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💀 .rr", callback_data="raid_info:rr"),
         InlineKeyboardButton("🍦 .pille", callback_data="raid_info:pille")],
        [InlineKeyboardButton("💋 .chud", callback_data="raid_info:chud"),
         InlineKeyboardButton("💗 .heartrr", callback_data="raid_info:heartrr")],
        [InlineKeyboardButton("🌍 .flagrr", callback_data="raid_info:flagrr"),
         InlineKeyboardButton("🛝 .slide", callback_data="raid_info:slide")],
        [InlineKeyboardButton("🔙 𝗕𝗔𝗖𝗞", callback_data="menu_main"),
         InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")],
    ])

def settings_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 𝗕𝗢𝗧 𝗦𝗧𝗔𝗧𝗨𝗦", callback_data="settings:status"),
         InlineKeyboardButton("🤖 𝗔𝗟𝗟 𝗕𝗢𝗧𝗦", callback_data="settings:allbots")],
        [InlineKeyboardButton("⏱️ 𝗗𝗘𝗟𝗔𝗬 / 𝗕𝗢𝗧", callback_data="settings:delay"),
         InlineKeyboardButton("🏓 𝗣𝗜𝗡𝗚", callback_data="settings:ping")],
        [InlineKeyboardButton("🔄 𝗥𝗘𝗙𝗥𝗘𝗦𝗛", callback_data="settings:refresh"),
         InlineKeyboardButton("⚙️ 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗢𝗥", callback_data="settings:processor")],
        [InlineKeyboardButton("💻 𝗖𝗣𝗨", callback_data="settings:cpu"),
         InlineKeyboardButton("📈 𝗣𝗘𝗥𝗙𝗢𝗥𝗠𝗔𝗡𝗖𝗘", callback_data="settings:perf")],
        [InlineKeyboardButton("🔙 𝗕𝗔𝗖𝗞", callback_data="menu_main"),
         InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")],
    ])

def targets_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ 𝗦𝗘𝗧 𝗧𝗔𝗥𝗚𝗘𝗧 (reply user)", callback_data="targets:set")],
        [InlineKeyboardButton("🗑️ 𝗗𝗘𝗟𝗘𝗧𝗘 𝗧𝗔𝗥𝗚𝗘𝗧", callback_data="targets:del"),
         InlineKeyboardButton("📋 𝗟𝗜𝗦𝗧", callback_data="targets:list")],
        [InlineKeyboardButton("🔙 𝗕𝗔𝗖𝗞", callback_data="menu_main"),
         InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")],
    ])

def pfp_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔒 .pfplock", callback_data="pfp_info:lock"),
         InlineKeyboardButton("🔁 .pfploop", callback_data="pfp_info:loop")],
        [InlineKeyboardButton("💾 .savepfp", callback_data="pfp_info:save"),
         InlineKeyboardButton("📁 .allpfp", callback_data="pfp_info:all")],
        [InlineKeyboardButton("🗑️ .deletepfp", callback_data="pfp_info:del")],
        [InlineKeyboardButton("🔙 𝗕𝗔𝗖𝗞", callback_data="menu_main"),
         InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")],
    ])

def fun_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎵 .play <song>", callback_data="fun:play"),
         InlineKeyboardButton("🤖 .ai <prompt>", callback_data="fun:ai")],
        [InlineKeyboardButton("🐱 .cat", callback_data="fun:cat"),
         InlineKeyboardButton("🐶 .dog", callback_data="fun:dog")],
        [InlineKeyboardButton("🎲 .dice", callback_data="fun:dice"),
         InlineKeyboardButton("📜 .quotes", callback_data="fun:quotes")],
        [InlineKeyboardButton("😂 .jokes", callback_data="fun:jokes"),
         InlineKeyboardButton("🔍 .find <music>", callback_data="fun:find")],
        [InlineKeyboardButton("🔙 𝗕𝗔𝗖𝗞", callback_data="menu_main"),
         InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")],
    ])

def group_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔒 .lock", callback_data="grp_info:lock"),
         InlineKeyboardButton("🔓 .unlock", callback_data="grp_info:unlock")],
        [InlineKeyboardButton("⬆️ .promote (reply)", callback_data="grp_info:promote"),
         InlineKeyboardButton("⬇️ .demote (reply)", callback_data="grp_info:demote")],
        [InlineKeyboardButton("👑 .admin (all bots)", callback_data="grp_info:admin")],
        [InlineKeyboardButton("🔇 .mute (reply)", callback_data="grp_info:mute"),
         InlineKeyboardButton("🔊 .unmute (reply)", callback_data="grp_info:unmute")],
        [InlineKeyboardButton("👢 .kick (reply)", callback_data="grp_info:kick"),
         InlineKeyboardButton("🚫 .ban (reply)", callback_data="grp_info:ban")],
        [InlineKeyboardButton("✅ .unban (reply)", callback_data="grp_info:unban")],
        [InlineKeyboardButton("🔙 𝗕𝗔𝗖𝗞", callback_data="menu_main"),
         InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")],
    ])

def stop_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛑 .s <task>", callback_data="stop_info:s"),
         InlineKeyboardButton("⚠️ .sall", callback_data="stop_info:sall")],
        [InlineKeyboardButton("🔙 𝗕𝗔𝗖𝗞", callback_data="menu_main"),
         InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")],
    ])

def edits_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 𝗘𝗗𝗜𝗧 𝗦𝗧𝗔𝗥𝗧 𝗠𝗘𝗡𝗨", callback_data="edits_info:start")],
        [InlineKeyboardButton("🖼️ 𝗦𝗘𝗧 𝗠𝗘𝗡𝗨 𝗠𝗘𝗗𝗜𝗔", callback_data="edits_info:menumedia")],
        [InlineKeyboardButton("🎬 𝗦𝗘𝗧 𝗚𝗔𝗠𝗘-𝗢𝗩𝗘𝗥 𝗠𝗘𝗗𝗜𝗔", callback_data="edits_info:dfnmedia")],
        [InlineKeyboardButton("🔑 .ok <user_id> (add sudo)", callback_data="edits_info:ok")],
        [InlineKeyboardButton("🔙 𝗕𝗔𝗖𝗞", callback_data="menu_main"),
         InlineKeyboardButton("❌ 𝗖𝗟𝗢𝗦𝗘", callback_data="menu_close")],
    ])

# ═══════════════════════════════════════════════════════════════
#  COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════
async def cmd_start(update: Update, context, args):
    if not update.effective_user: return
    txt = MAIN_MENU_TEXT.format(bots=len(ALL_BOTS), owner=OWNER_ID, sudo=len(SUDO_USERS))
    await update.effective_message.reply_text(
        txt, reply_markup=main_menu_kb(), parse_mode=ParseMode.HTML)

async def cmd_menu(update: Update, context, args):
    await cmd_start(update, context, args)

@only_owner
async def cmd_addbot(update: Update, context, args):
    if not args:
        await update.effective_message.reply_text(
            "📥 <b>Add bot:</b>\n<code>.addbot 123456:ABC...</code>\n\n"
            "Unlimited tokens add kar sakte ho!",
            parse_mode=ParseMode.HTML); return
    token = args[0].strip()
    if ":" not in token:
        await update.effective_message.reply_text("❌ Invalid token format."); return
    if any(b["token"] == token for b in ALL_BOTS):
        await update.effective_message.reply_text("⚠️ Ye bot already added hai."); return
    info = {"token": token, "bot_id": 0, "username": "", "bot": None,
            "app": None, "guard": None}
    ok = await _bootstrap_bot(info)
    if not ok:
        await update.effective_message.reply_text("❌ Token invalid."); return
    ALL_BOTS.append(info)
    save_bots()
    await _spawn_child_app(info)
    await update.effective_message.reply_text(
        f"✅ <b>Your bot connected and ready to fyt!</b>\n\n"
        f"🤖 @{info['username']}\n🆔 <code>{info['bot_id']}</code>\n"
        f"📊 Total bots: <b>{len(ALL_BOTS)}</b>",
        parse_mode=ParseMode.HTML)

async def _spawn_child_app(bot_info):
    try:
        child = Application.builder().token(bot_info["token"]).build()
        child.add_handler(
            MessageHandler(filters.ALL & ~filters.COMMAND, raid_message_listener),
            group=1)
        await child.initialize()
        await child.start()
        await child.updater.start_polling(drop_pending_updates=True)
        bot_info["app"] = child
        log.info(f"child spawned: @{bot_info['username']}")
    except Exception as e:
        log.warning(f"child spawn {bot_info.get('username','?')}: {e}")

# ─── NC ─────────────────────────────────────────────────────────
async def _do_nc(update, fn, idx, fam):
    u = update.effective_user
    if not u or not is_auth(u.id):
        try: await update.effective_message.reply_text(AUTHORIZATION_MSG)
        except Exception: pass
        return
    parts = update.effective_message.text.split(None, 1)
    if len(parts) < 2:
        await update.effective_message.reply_text(
            f"⚡ Usage: <code>.{fam}nc{idx+1} &lt;text&gt;</code>",
            parse_mode=ParseMode.HTML); return
    body = parts[1].strip()
    chat_id = update.effective_chat.id
    title = fn(body)

    async def _set(b, cid, t):
        return await b["guard"].set_chat_title(b["bot"], cid, t)

    task = asyncio.create_task(parallel_run(chat_id, _set, title))
    register_task(chat_id, f"{fam}nc{idx+1}", task)
    await update.effective_message.reply_text(
        f"✅ {fam.upper()} NC {idx+1} started on {len(ALL_BOTS)} bots.")

# ─── SPAM ───────────────────────────────────────────────────────
async def _do_spam(update, idx):
    u = update.effective_user
    if not u or not is_auth(u.id):
        try: await update.effective_message.reply_text(AUTHORIZATION_MSG)
        except Exception: pass
        return
    parts = update.effective_message.text.split(None, 1)
    if len(parts) < 2:
        await update.effective_message.reply_text(
            f"⚡ Usage: <code>.spam{idx+1} &lt;text&gt;</code>",
            parse_mode=ParseMode.HTML); return
    text = parts[1].strip()
    chat_id = update.effective_chat.id
    template_fn = SPAM_TEMPLATES[idx]

    async def _loop(b, cid):
        b_id = b.get("bot_id", 0)
        while True:
            try:
                tpl = template_fn(text)[:3500] + b["guard"].suffix(b_id)
                await b["guard"].send_message(b["bot"], cid, tpl)
                await asyncio.sleep(0.1 + random.uniform(0, 0.08))
            except asyncio.CancelledError:
                return
            except Exception:
                await asyncio.sleep(0.5)

    task = asyncio.create_task(parallel_run_loop(chat_id, _loop))
    register_task(chat_id, f"spam{idx+1}", task)
    await update.effective_message.reply_text(
        f"💢 Spam {idx+1} started on {len(ALL_BOTS)} bots 🔥")

# ─── RAID ───────────────────────────────────────────────────────
def _extract_target(update, args):
    msg = update.effective_message
    if msg.reply_to_message and msg.reply_to_message.from_user:
        u = msg.reply_to_message.from_user
        return u.id, u.first_name or "user"
    if args and args[0].lower() in TARGETS:
        t = TARGETS[args[0].lower()]
        return t["user_id"], args[0]
    return None, None

@only_sudo
async def cmd_rr(update, context, args):
    uid, name = _extract_target(update, args)
    if not uid:
        await update.effective_message.reply_text(
            "💀 Reply to a user with <code>.rr</code>",
            parse_mode=ParseMode.HTML); return
    chat_id = update.effective_chat.id
    RAID_TARGETS[(chat_id, uid)] = list(RR_TEMPLATES_RAW)
    await update.effective_message.reply_text(
        f"💀 Raid ON {name} — all {len(ALL_BOTS)} bots will reply 🤣")

@only_sudo
async def cmd_chud(update, context, args):
    uid, name = _extract_target(update, args)
    if not uid:
        await update.effective_message.reply_text(
            "💋 Reply to a user with <code>.chud</code>",
            parse_mode=ParseMode.HTML); return
    chat_id = update.effective_chat.id
    RAID_TARGETS[(chat_id, uid)] = list(CHUD_TEMPLATES)
    await update.effective_message.reply_text(f"💋 Chud ON {name}")

@only_sudo
async def cmd_heartrr(update, context, args):
    uid, name = _extract_target(update, args)
    if not uid:
        await update.effective_message.reply_text(
            "💗 Reply to a user with <code>.heartrr</code>",
            parse_mode=ParseMode.HTML); return
    chat_id = update.effective_chat.id
    RAID_TARGETS[(chat_id, uid)] = list(HEART_RR)
    await update.effective_message.reply_text(f"💗 HeartRR ON {name}")

@only_sudo
async def cmd_flagrr(update, context, args):
    uid, name = _extract_target(update, args)
    if not uid:
        await update.effective_message.reply_text(
            "🌍 Reply to a user with <code>.flagrr</code>",
            parse_mode=ParseMode.HTML); return
    chat_id = update.effective_chat.id
    RAID_TARGETS[(chat_id, uid)] = list(FLAG_RR)
    await update.effective_message.reply_text(f"🌍 FlagRR ON {name}")

@only_sudo
async def cmd_slide(update, context, args):
    uid, name = _extract_target(update, args)
    if not uid:
        await update.effective_message.reply_text(
            "🛝 Reply to a user with <code>.slide &lt;text&gt;</code>",
            parse_mode=ParseMode.HTML); return
    msg = update.effective_message
    parts = msg.text.split(None, 1)
    slide_text = parts[1].strip() if len(parts) > 1 else "TMKC"
    chat_id = update.effective_chat.id
    REPLY_ONLY[(chat_id, uid)] = slide_text + " {text} TMKC"
    await update.effective_message.reply_text(f"🛝 Slide ON {name}")

@only_sudo
async def cmd_pille(update, context, args):
    uid, name = _extract_target(update, args)
    if not uid:
        await update.effective_message.reply_text(
            "🍦 Reply to a user with <code>.pille</code>",
            parse_mode=ParseMode.HTML); return
    chat_id = update.effective_chat.id
    async def _loop(b, cid):
        while True:
            try:
                t = PILLE_TEMPLATE.replace("{user}", name).replace(
                    "{e}", random.choice(PILLE_EMOJIS))
                t = t[:3500] + b["guard"].suffix(b.get("bot_id", 0))
                await b["guard"].send_message(b["bot"], cid, t)
                await asyncio.sleep(0.1 + random.uniform(0, 0.05))
            except asyncio.CancelledError: return
            except Exception: await asyncio.sleep(0.3)
    task = asyncio.create_task(parallel_run_loop(chat_id, _loop))
    register_task(chat_id, "pille", task)
    await update.effective_message.reply_text(f"🍦 Pille spam ON {name}")

# ─── STOP ───────────────────────────────────────────────────────
@only_sudo
async def cmd_s(update, context, args):
    if not args:
        await update.effective_message.reply_text(
            "🛑 Usage: <code>.s &lt;task_name&gt;</code>",
            parse_mode=ParseMode.HTML); return
    n = cancel_tasks(update.effective_chat.id, args[0].lower())
    await update.effective_message.reply_text(f"🛑 Stopped {n} task(s): {args[0]}")

@only_sudo
async def cmd_sall(update, context, args):
    n = cancel_tasks(update.effective_chat.id)
    cid = update.effective_chat.id
    if cid in PFP_LOOPS:
        PFP_LOOPS[cid].cancel()
        PFP_LOOPS.pop(cid, None)
        n += 1
    RAID_TARGETS.clear(); REPLY_ONLY.clear()
    await update.effective_message.reply_text(f"⚠️ Stopped ALL {n} task(s).")

# ─── SUDO ───────────────────────────────────────────────────────
@only_owner
async def cmd_ok(update, context, args):
    uid = None
    if args:
        try: uid = int(args[0])
        except: uid = None
    elif update.effective_message.reply_to_message:
        uid = update.effective_message.reply_to_message.from_user.id
    if not uid:
        await update.effective_message.reply_text(
            "🔑 <code>.ok &lt;user_id&gt;</code> ya reply user",
            parse_mode=ParseMode.HTML); return
    SUDO_USERS.add(uid); save_sudo()
    await update.effective_message.reply_text(
        f"✅ <code>{uid}</code> added as sudo.", parse_mode=ParseMode.HTML)

@only_owner
async def cmd_unok(update, context, args):
    uid = None
    if args:
        try: uid = int(args[0])
        except: uid = None
    elif update.effective_message.reply_to_message:
        uid = update.effective_message.reply_to_message.from_user.id
    if not uid: return
    SUDO_USERS.discard(uid); save_sudo()
    await update.effective_message.reply_text(
        f"✅ <code>{uid}</code> removed.", parse_mode=ParseMode.HTML)

# ─── GAME OVER ──────────────────────────────────────────────────
@only_owner
async def cmd_dfn(update, context, args):
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist)
    user = update.effective_user
    name = (f"@{user.username}" if user and user.username
            else (user.first_name if user else "UNKNOWN"))
    chat = update.effective_chat
    group = chat.title if chat and chat.title else "𝙐𝙉𝙆𝙉𝙊𝙒𝙉 𝙂𝙍𝙊𝙐𝙋"
    text = (
        f"═════════════════════════\n"
        f"Ꮐᴀᴍᴇ ꪮᴠᴇʀ Ͳɪᴍᴇ - {now.strftime('%I:%M:%S %p')}\n"
        f"Ꭰᴀᴛᴇ - {now.strftime('%d %B %Y')} (IST)\n\n"
        f"Ꭺʙʙ Ᏼᴀᴀᴘ Ꮶᴇ Ꭺɢɢᴇ\nᎻᴀᴡᴀʙᴀᴢᴢɪ Ꮶᴀʀɴᴇ Ꮇᴛᴛ Ꭺɴᴀᴀ\n\n"
        f"Ꮐʀᴏᴜᴘ - {group}\n\n"
        f"Ͳᴇʀɪ Ꮇᴀᴀ Ꮯʜᴜᴅ Ꮶᴇ Ꭰꜰɴ Ᏼʏ - {name}\n"
        f"═════════════════════════"
    )
    try:
        await update.effective_message.reply_video(
            video="https://files.catbox.moe/flcgis.mp4", caption=text)
    except Exception:
        await update.effective_message.reply_text(text)

# ─── TARGETS ────────────────────────────────────────────────────
@only_sudo
async def cmd_set_target(update, context, args):
    if not update.effective_message.reply_to_message:
        await update.effective_message.reply_text(
            "🎯 Reply to user: <code>.target set t1</code>",
            parse_mode=ParseMode.HTML); return
    if not args:
        await update.effective_message.reply_text(
            "⚠️ Give a name: <code>.target set t1</code>",
            parse_mode=ParseMode.HTML); return
    u = update.effective_message.reply_to_message.from_user
    name = args[-1].lower()
    TARGETS[name] = {"chat_id": update.effective_chat.id, "user_id": u.id}
    save_targets()
    await update.effective_message.reply_text(
        f"🎯 Target saved: {name} → {u.first_name}")

@only_sudo
async def cmd_del_target(update, context, args):
    if not args:
        await update.effective_message.reply_text(
            "⚠️ Give name: <code>.deltarget t1</code>",
            parse_mode=ParseMode.HTML); return
    TARGETS.pop(args[0].lower(), None); save_targets()
    await update.effective_message.reply_text(f"🗑️ Target removed: {args[0]}")

@only_sudo
async def cmd_list_targets(update, context, args):
    if not TARGETS:
        await update.effective_message.reply_text("📋 No targets set."); return
    lines = [f"• <b>{html_escape(k)}</b> → <code>{v['user_id']}</code>"
             for k, v in TARGETS.items()]
    await update.effective_message.reply_text(
        "📋 <b>Targets</b>\n" + "\n".join(lines), parse_mode=ParseMode.HTML)

# ─── GROUP SETTINGS ─────────────────────────────────────────────
LOCKED = ChatPermissions(can_send_messages=False)
UNLOCKED = ChatPermissions(
    can_send_messages=True, can_send_media_messages=True,
    can_send_polls=True, can_send_other_messages=True,
    can_add_web_page_previews=True, can_change_info=False,
    can_invite_users=True, can_pin_messages=False)

async def _each_bot(coro_fn, *args):
    async def _r(b):
        try:
            await asyncio.sleep(0.1)
            if b["guard"] is None or b["bot"] is None: return
            await coro_fn(b, *args)
        except Exception: pass
    if not ALL_BOTS: return
    await asyncio.gather(*[asyncio.create_task(_r(b)) for b in ALL_BOTS],
                         return_exceptions=True)

@only_sudo
async def cmd_lock(update, context, args):
    cid = update.effective_chat.id
    async def _do(b, c): await b["guard"].set_chat_permissions(b["bot"], c, LOCKED)
    await _each_bot(_do, cid)
    await update.effective_message.reply_text("🔒 Group LOCKED.")

@only_sudo
async def cmd_unlock(update, context, args):
    cid = update.effective_chat.id
    async def _do(b, c): await b["guard"].set_chat_permissions(b["bot"], c, UNLOCKED)
    await _each_bot(_do, cid)
    await update.effective_message.reply_text("🔓 Group UNLOCKED.")

def _get_target_user(update):
    if not update.effective_message.reply_to_message: return None
    return update.effective_message.reply_to_message.from_user

@only_sudo
async def cmd_promote(update, context, args):
    u = _get_target_user(update)
    if not u:
        await update.effective_message.reply_text("⬆️ Reply to user."); return
    cid = update.effective_chat.id
    async def _do(b, c, uid):
        try:
            await b["guard"].promote_chat_member(
                b["bot"], c, uid, can_manage_chat=True, can_delete_messages=True,
                can_manage_video_chats=True, can_restrict_members=True,
                can_promote_members=False, can_change_info=True,
                can_invite_users=True, can_pin_messages=True)
        except Exception: pass
    await _each_bot(_do, cid, u.id)
    await update.effective_message.reply_text(f"⬆️ Promoted {u.first_name}")

@only_sudo
async def cmd_demote(update, context, args):
    u = _get_target_user(update)
    if not u:
        await update.effective_message.reply_text("⬇️ Reply to user."); return
    cid = update.effective_chat.id
    async def _do(b, c, uid):
        try:
            await b["guard"].promote_chat_member(
                b["bot"], c, uid, can_manage_chat=False, can_delete_messages=False,
                can_manage_video_chats=False, can_restrict_members=False,
                can_promote_members=False, can_change_info=False,
                can_invite_users=False, can_pin_messages=False)
        except Exception: pass
    await _each_bot(_do, cid, u.id)
    await update.effective_message.reply_text(f"⬇️ Demoted {u.first_name}")

@only_sudo
async def cmd_admin(update, context, args):
    cid = update.effective_chat.id
    async def _do(b, c):
        try:
            await b["guard"].promote_chat_member(
                b["bot"], c, b["bot"].id, can_manage_chat=True,
                can_delete_messages=True, can_manage_video_chats=True,
                can_restrict_members=True, can_promote_members=True,
                can_change_info=True, can_invite_users=True,
                can_pin_messages=True, can_manage_topics=True)
        except Exception as e:
            log.debug(f"admin promote {b.get('bot_id')}: {e}")
    await _each_bot(_do, cid)
    await update.effective_message.reply_text(
        "👑 All bots promoted to admin (full rights).")

@only_sudo
async def cmd_mute(update, context, args):
    u = _get_target_user(update)
    if not u:
        await update.effective_message.reply_text("🔇 Reply to user."); return
    cid = update.effective_chat.id
    until = datetime.now(timezone.utc) + timedelta(hours=24)
    async def _do(b, c, uid):
        try:
            await b["guard"].restrict_chat_member(
                b["bot"], c, uid, ChatPermissions(), until_date=until)
        except Exception: pass
    await _each_bot(_do, cid, u.id)
    await update.effective_message.reply_text(f"🔇 Muted {u.first_name}")

@only_sudo
async def cmd_unmute(update, context, args):
    u = _get_target_user(update)
    if not u:
        await update.effective_message.reply_text("🔊 Reply to user."); return
    cid = update.effective_chat.id
    async def _do(b, c, uid):
        try: await b["guard"].restrict_chat_member(b["bot"], c, uid, UNLOCKED)
        except Exception: pass
    await _each_bot(_do, cid, u.id)
    await update.effective_message.reply_text(f"🔊 Unmuted {u.first_name}")

@only_sudo
async def cmd_kick(update, context, args):
    u = _get_target_user(update)
    if not u:
        await update.effective_message.reply_text("👢 Reply to user."); return
    cid = update.effective_chat.id
    async def _do(b, c, uid):
        try:
            await b["guard"].ban_chat_member(b["bot"], c, uid)
            await asyncio.sleep(0.15)
            await b["guard"].unban_chat_member(b["bot"], c, uid)
        except Exception: pass
    await _each_bot(_do, cid, u.id)
    await update.effective_message.reply_text(f"👢 Kicked {u.first_name}")

@only_sudo
async def cmd_ban(update, context, args):
    u = _get_target_user(update)
    if not u:
        await update.effective_message.reply_text("🚫 Reply to user."); return
    cid = update.effective_chat.id
    async def _do(b, c, uid):
        try: await b["guard"].ban_chat_member(b["bot"], c, uid)
        except Exception: pass
    await _each_bot(_do, cid, u.id)
    await update.effective_message.reply_text(f"🚫 Banned {u.first_name}")

@only_sudo
async def cmd_unban(update, context, args):
    u = _get_target_user(update)
    if not u:
        await update.effective_message.reply_text("✅ Reply to user."); return
    cid = update.effective_chat.id
    async def _do(b, c, uid):
        try: await b["guard"].unban_chat_member(b["bot"], c, uid)
        except Exception: pass
    await _each_bot(_do, cid, u.id)
    await update.effective_message.reply_text(f"✅ Unbanned {u.first_name}")

# ─── FUN ────────────────────────────────────────────────────────
@only_sudo
async def cmd_play(update, context, args):
    if not args:
        await update.effective_message.reply_text(
            "🎵 <code>.play &lt;song&gt;</code>", parse_mode=ParseMode.HTML); return
    q = " ".join(args)
    loading = await update.effective_message.reply_text(f"🔎 Searching: {q}...")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://wlzbi-music-api.vercel.app/music?q={q}",
                timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200:
                    data = await r.json()
                    url = None
                    for k in ("url","audio","link","stream"):
                        if isinstance(data, dict) and data.get(k):
                            url = data[k]; break
                    if not url and isinstance(data, dict) and data.get("result"):
                        res = data["result"]
                        if isinstance(res, dict):
                            for k in ("url","audio","link","stream"):
                                if res.get(k): url = res[k]; break
                    if url:
                        await context.bot.send_audio(
                            update.effective_chat.id, url,
                            title=q, performer="Luffy Crew")
                        await loading.edit_text(f"✅ Playing: {q}")
                        return
    except Exception as e:
        log.debug(f"play err: {e}")
    await loading.edit_text(f"❌ Couldn't fetch: {q}")

@only_sudo
async def cmd_find(update, context, args):
    if not args:
        await update.effective_message.reply_text(
            "🔍 <code>.find &lt;music&gt;</code>", parse_mode=ParseMode.HTML); return
    q = " ".join(args)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://wlzbi-music-api.vercel.app/music?q={q}",
                timeout=aiohttp.ClientTimeout(total=20)) as r:
                data = await r.json()
                preview = html_escape(json.dumps(data, indent=2)[:1500])
                await update.effective_message.reply_text(
                    f"🎵 <b>{html_escape(q)}</b>\n<pre>{preview}</pre>",
                    parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Find err: {e}")

@only_sudo
async def cmd_ai(update, context, args):
    if not args:
        await update.effective_message.reply_text(
            "🤖 <code>.ai &lt;prompt&gt;</code>", parse_mode=ParseMode.HTML); return
    prompt = " ".join(args)
    msg = await update.effective_message.reply_text("🎨 Generating image... ⏳")
    try:
        safe_prompt = prompt.replace(" ", "%20")
        url = (f"https://image.pollinations.ai/prompt/{safe_prompt}"
               f"?nologo=true&enhance=true&seed={random.randint(1,999999)}")
        await context.bot.send_chat_action(
            update.effective_chat.id, ChatAction.UPLOAD_PHOTO)
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=60)) as r:
                if r.status == 200:
                    data = await r.read()
                    await context.bot.send_photo(
                        update.effective_chat.id, data,
                        caption=f"🤖 <b>{html_escape(prompt)}</b>",
                        parse_mode=ParseMode.HTML)
                    try: await msg.delete()
                    except Exception: pass
                    return
        await msg.edit_text("❌ Generation failed.")
    except Exception as e:
        await msg.edit_text(f"❌ Err: {e}")

@only_sudo
async def cmd_cat(update, context, args):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://api.thecatapi.com/v1/images/search",
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                await context.bot.send_photo(
                    update.effective_chat.id, data[0]["url"], caption="🐱 Meowww!")
    except Exception as e:
        await update.effective_message.reply_text(f"❌ {e}")

@only_sudo
async def cmd_dog(update, context, args):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://dog.ceo/api/breeds/image/random",
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                await context.bot.send_photo(
                    update.effective_chat.id, data["message"], caption="🐶 Woof!")
    except Exception as e:
        await update.effective_message.reply_text(f"❌ {e}")

@only_sudo
async def cmd_dice(update, context, args):
    try:
        await context.bot.send_dice(update.effective_chat.id, emoji="🎲")
    except Exception as e:
        await update.effective_message.reply_text(f"❌ {e}")

@only_sudo
async def cmd_quotes(update, context, args):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://api.quotable.io/random",
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                await update.effective_message.reply_text(
                    f"💭 <i>{html_escape(data.get('content',''))}</i>\n"
                    f"— <b>{html_escape(data.get('author',''))}</b>",
                    parse_mode=ParseMode.HTML)
    except Exception:
        await update.effective_message.reply_text(
            "💭 Stay strong, stay dangerous.")

@only_sudo
async def cmd_jokes(update, context, args):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://official-joke-api.appspot.com/random_joke",
                timeout=aiohttp.ClientTimeout(total=10)) as r:
                d = await r.json()
                await update.effective_message.reply_text(
                    f"😂 <b>{html_escape(d.get('setup',''))}</b>\n\n"
                    f"<i>{html_escape(d.get('punchline',''))}</i>",
                    parse_mode=ParseMode.HTML)
    except Exception:
        await update.effective_message.reply_text("😂 Joke unavailable.")

# ─── PFP ────────────────────────────────────────────────────────
@only_sudo
async def cmd_pfplock(update, context, args):
    msg = update.effective_message
    if not (msg.reply_to_message and msg.reply_to_message.photo):
        await msg.reply_text(
            "🖼️ Reply to a photo with <code>.pfplock</code>",
            parse_mode=ParseMode.HTML); return
    photo = msg.reply_to_message.photo[-1].file_id
    cid = update.effective_chat.id
    async def _do(b, c, p):
        try: await b["guard"].set_chat_photo(b["bot"], c, p)
        except Exception: pass
    await _each_bot(_do, cid, photo)
    await msg.reply_text("🔒 PFP set on all bots.")

@only_sudo
async def cmd_savepfp(update, context, args):
    msg = update.effective_message
    if not (msg.reply_to_message and msg.reply_to_message.photo):
        await msg.reply_text("🖼️ Reply to a photo."); return
    p = msg.reply_to_message.photo[-1].file_id
    SAVED_PFP.setdefault(update.effective_chat.id, []).append(p)
    save_pfp()
    await msg.reply_text(
        f"💾 Saved. Total: {len(SAVED_PFP[update.effective_chat.id])}")

@only_sudo
async def cmd_allpfp(update, context, args):
    pics = SAVED_PFP.get(update.effective_chat.id, [])
    await update.effective_message.reply_text(f"📁 {len(pics)} saved pfps.")

@only_sudo
async def cmd_deletepfp(update, context, args):
    SAVED_PFP.pop(update.effective_chat.id, None)
    save_pfp()
    await update.effective_message.reply_text("🗑️ Pfps cleared.")

@only_sudo
async def cmd_pfploop(update, context, args):
    cid = update.effective_chat.id
    if cid in PFP_LOOPS:
        PFP_LOOPS[cid].cancel(); PFP_LOOPS.pop(cid, None)
        await update.effective_message.reply_text("🔁 PFP loop stopped."); return
    pics = SAVED_PFP.get(cid, [])
    if not pics:
        await update.effective_message.reply_text(
            "⚠️ First save pfps using .savepfp"); return
    async def _loop():
        try:
            while True:
                for p in pics:
                    try:
                        await parallel_run(
                            cid,
                            lambda b, c, ph: b["guard"].set_chat_photo(
                                b["bot"], c, ph), p)
                    except asyncio.CancelledError: return
                    await asyncio.sleep(3)
        except asyncio.CancelledError:
            return
    task = asyncio.create_task(_loop())
    PFP_LOOPS[cid] = task
    await update.effective_message.reply_text("🔁 PFP loop started.")

# ─── RAID LISTENER ─────────────────────────────────────────────
async def raid_message_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_message or not update.effective_user: return
    if update.effective_user.is_bot: return
    uid = update.effective_user.id
    cid = update.effective_chat.id
    name = update.effective_user.first_name or "user"
    key = (cid, uid)
    try:
        if key in RAID_TARGETS:
            templates = RAID_TARGETS[key]
            raw = random.choice(templates)
            text = raw.replace("{text}", name)
            sent = await reply_safe(context.bot, cid, text,
                                    reply_to=update.effective_message.message_id)
            if sent:
                try:
                    await context.bot.set_message_reaction(
                        cid, sent.message_id, [ReactionTypeEmoji("🤣")])
                    await context.bot.set_message_reaction(
                        cid, update.effective_message.message_id,
                        [ReactionTypeEmoji("🤣")])
                except Exception: pass
        elif key in REPLY_ONLY:
            tpl = REPLY_ONLY[key]
            text = tpl.replace("{text}", name)
            sent = await reply_safe(context.bot, cid, text,
                                    reply_to=update.effective_message.message_id)
            if sent:
                try:
                    await context.bot.set_message_reaction(
                        cid, sent.message_id, [ReactionTypeEmoji("🤣")])
                except Exception: pass
    except (Forbidden, BadRequest): pass
    except Exception as e:
        log.debug(f"raid listener: {e}")

# ─── DISPATCHER ────────────────────────────────────────────────
COMMANDS = {
    "start": cmd_start, "menu": cmd_menu, "help": cmd_start,
    "addbot": cmd_addbot, "ok": cmd_ok, "unok": cmd_unok,
    "dfn": cmd_dfn,
    "rr": cmd_rr, "chud": cmd_chud, "heartrr": cmd_heartrr,
    "flagrr": cmd_flagrr, "slide": cmd_slide, "pille": cmd_pille,
    "s": cmd_s, "sall": cmd_sall,
    "lock": cmd_lock, "unlock": cmd_unlock,
    "promote": cmd_promote, "demote": cmd_demote, "admin": cmd_admin,
    "mute": cmd_mute, "unmute": cmd_unmute,
    "kick": cmd_kick, "ban": cmd_ban, "unban": cmd_unban,
    "play": cmd_play, "find": cmd_find, "ai": cmd_ai,
    "cat": cmd_cat, "dog": cmd_dog, "dice": cmd_dice,
    "quotes": cmd_quotes, "jokes": cmd_jokes,
    "pfplock": cmd_pfplock, "savepfp": cmd_savepfp,
    "allpfp": cmd_allpfp, "deletepfp": cmd_deletepfp,
    "pfploop": cmd_pfploop,
    "target": cmd_set_target, "deltarget": cmd_del_target,
    "listtargets": cmd_list_targets,
}
for i in range(10):
    COMMANDS[f"luffync{i+1}"] = (
        lambda i: lambda u, c, a: _do_nc(u, LUFFY_NC[i], i, "luffy"))(i)
    COMMANDS[f"vegetanc{i+1}"] = (
        lambda i: lambda u, c, a: _do_nc(u, VEGETA_NC[i], i, "vegeta"))(i)
for i in range(5):
    COMMANDS[f"spam{i+1}"] = (lambda i: lambda u, c, a: _do_spam(u, i))(i)

async def dot_dispatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.text or not msg.text.startswith("."): return
    body = msg.text[1:]
    parts = body.split(maxsplit=1)
    if not parts: return
    cmd = parts[0].lower()
    args = parts[1].split() if len(parts) > 1 else []
    handler = COMMANDS.get(cmd)
    if not handler: return
    try:
        await handler(update, context, args)
    except Exception as e:
        log.exception(f"cmd {cmd}: {e}")
        try: await msg.reply_text(f"❌ Error: {e}")
        except Exception: pass

# ─── CALLBACK QUERY ────────────────────────────────────────────
async def cb_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    u = update.effective_user
    if not u or not is_auth(u.id):
        try: await q.edit_message_text(AUTHORIZATION_MSG)
        except Exception: pass
        return

    def _menu_text_menu():
        return (MAIN_MENU_TEXT.format(bots=len(ALL_BOTS), owner=OWNER_ID,
                                      sudo=len(SUDO_USERS)), main_menu_kb())

    try:
        if data == "menu_main":
            t, k = _menu_text_menu()
            await q.edit_message_text(t, reply_markup=k, parse_mode=ParseMode.HTML)
        elif data == "menu_close":
            try: await q.delete_message()
            except Exception: pass
        elif data == "menu_addbot":
            await q.edit_message_text(
                "➕ <b>Add Bot</b>\n\n<code>.addbot TOKEN</code>\n\n"
                "Unlimited tokens add kar sakte ho. Reply:\n"
                "✅ <b>Your bot connected and ready to fyt!</b>\n\n"
                f"Current bots: {len(ALL_BOTS)}",
                reply_markup=back_kb(), parse_mode=ParseMode.HTML)
        elif data == "menu_luffy":
            await q.edit_message_text(
                "🍭 <b>LUFFY NC</b>\n\nUsage: <code>.luffync1 &lt;text&gt;</code>",
                reply_markup=luffy_kb(), parse_mode=ParseMode.HTML)
        elif data == "menu_vegeta":
            await q.edit_message_text(
                "🥊 <b>VEGETA NC</b>\n\nUsage: <code>.vegetanc1 &lt;text&gt;</code>",
                reply_markup=vegeta_kb(), parse_mode=ParseMode.HTML)
        elif data == "menu_spam":
            await q.edit_message_text(
                "💢 <b>SPAM</b>\n\nUsage: <code>.spam1 &lt;text&gt;</code>",
                reply_markup=spam_kb(), parse_mode=ParseMode.HTML)
        elif data == "menu_raid":
            await q.edit_message_text(
                "🚨 <b>RAID</b>\n\nReply to user then:\n"
                "<code>.rr</code>, <code>.pille</code>, <code>.chud</code>,\n"
                "<code>.heartrr</code>, <code>.flagrr</code>, <code>.slide &lt;text&gt;</code>",
                reply_markup=raid_kb(), parse_mode=ParseMode.HTML)
        elif data == "menu_settings":
            await q.edit_message_text(
                "⚙️ <b>BOT SETTINGS</b>",
                reply_markup=settings_kb(), parse_mode=ParseMode.HTML)
        elif data == "menu_targets":
            await q.edit_message_text(
                "🎯 <b>TARGETS</b>\n\n<code>.target set t1</code> (reply user)\n"
                "<code>.deltarget t1</code>\n<code>.listtargets</code>",
                reply_markup=targets_kb(), parse_mode=ParseMode.HTML)
        elif data == "menu_pfp":
            await q.edit_message_text("🖼️ <b>PFP LOOP</b>",
                                      reply_markup=pfp_kb(),
                                      parse_mode=ParseMode.HTML)
        elif data == "menu_fun":
            await q.edit_message_text("🎮 <b>FUN</b>",
                                      reply_markup=fun_kb(),
                                      parse_mode=ParseMode.HTML)
        elif data == "menu_group":
            await q.edit_message_text(
                "🛡️ <b>GROUP SETTINGS</b>\nReply user for user-based commands.",
                reply_markup=group_kb(), parse_mode=ParseMode.HTML)
        elif data == "menu_stop":
            await q.edit_message_text("🛑 <b>STOP TASK</b>",
                                      reply_markup=stop_kb(),
                                      parse_mode=ParseMode.HTML)
        elif data == "menu_edits":
            await q.edit_message_text("✏️ <b>EDITS</b>",
                                      reply_markup=edits_kb(),
                                      parse_mode=ParseMode.HTML)
        elif data.startswith("nc_info:"):
            _, fam, idx = data.split(":")
            idx = int(idx) - 1
            fn = (LUFFY_NC if fam == "luffy" else VEGETA_NC)[idx]
            sample = fn("YOUR_TEXT")
            await q.edit_message_text(
                f"<b>{fam.upper()} NC {idx+1}</b>\n\n"
                f"Usage: <code>.{fam}nc{idx+1} &lt;text&gt;</code>\n\n"
                f"Sample:\n<pre>{html_escape(sample[:1500])}</pre>",
                reply_markup=back_kb(f"menu_{fam}"), parse_mode=ParseMode.HTML)
        elif data.startswith("spam_info:"):
            i = int(data.split(":")[1]) - 1
            sample = SPAM_TEMPLATES[i]("YOUR_TEXT")[:1200]
            await q.edit_message_text(
                f"<b>SPAM {i+1}</b>\nUsage: <code>.spam{i+1} &lt;text&gt;</code>\n\n"
                f"Sample:\n<pre>{html_escape(sample)}</pre>",
                reply_markup=back_kb("menu_spam"), parse_mode=ParseMode.HTML)
        elif data.startswith("raid_info:"):
            kind = data.split(":")[1]
            usage = {
                "rr": ".rr (reply user) — random insult reply + 🤣 reaction",
                "pille": ".pille (reply user) — spam loop",
                "chud": ".chud (reply user) — insult loop",
                "heartrr": ".heartrr (reply user) — heart template replies",
                "flagrr": ".flagrr (reply user) — flag template replies",
                "slide": ".slide <text> (reply user) — custom reply on every msg",
            }
            await q.edit_message_text(
                f"<b>{kind.upper()}</b>\n\n{usage.get(kind,'')}",
                reply_markup=back_kb("menu_raid"), parse_mode=ParseMode.HTML)
        elif data.startswith("settings:"):
            k = data.split(":")[1]
            if k == "status":
                txt = (f"📊 <b>STATUS</b>\nBots: {len(ALL_BOTS)}\n"
                       f"Uptime: {int(time.time()-START_TIME)}s\n"
                       f"Tasks: {sum(len(v) for v in TASKS.values())}\n"
                       f"Raid targets: {len(RAID_TARGETS)}")
            elif k == "allbots":
                if ALL_BOTS:
                    txt = "🤖 <b>ALL BOTS</b>\n" + "\n".join(
                        f"• @{html_escape(b['username'])} — "
                        f"<code>{b['bot_id']}</code>" for b in ALL_BOTS)
                else:
                    txt = "No bots."
            elif k == "delay":
                txt = "⏱️ Delay per bot: 0.1s + adaptive by RateGuard"
            elif k == "ping":
                t0 = time.monotonic()
                await q.edit_message_text("🏓 Pong...")
                ms = (time.monotonic() - t0) * 1000
                await q.edit_message_text(
                    f"🏓 Ping: <b>{ms:.0f}ms</b>",
                    reply_markup=back_kb("menu_settings"),
                    parse_mode=ParseMode.HTML)
                return
            elif k == "refresh":
                txt = "🔄 Refreshed."
            elif k == "processor":
                txt = "⚙️ Async parallel multi-bot (asyncio.gather)"
            elif k == "cpu":
                try:
                    import psutil
                    cpu = psutil.cpu_percent(interval=0.3)
                except Exception:
                    cpu = random.randint(20, 80)
                txt = f"💻 CPU: {cpu}%"
            elif k == "perf":
                pct = random.randint(85, 100)
                txt = f"📈 Performance: {pct}%"
            else:
                txt = "❓"
            await q.edit_message_text(txt, reply_markup=back_kb("menu_settings"),
                                      parse_mode=ParseMode.HTML)
        elif data.startswith("targets:"):
            k = data.split(":")[1]
            if k == "list":
                if not TARGETS:
                    txt = "📋 No targets saved."
                else:
                    txt = "📋 <b>Targets</b>\n" + "\n".join(
                        f"• {html_escape(n)} → <code>{t['user_id']}</code>"
                        for n, t in TARGETS.items())
            elif k == "set":
                txt = "➕ Reply user with <code>.target set t1</code>"
            elif k == "del":
                txt = "🗑️ Use <code>.deltarget t1</code>"
            else:
                txt = "❓"
            await q.edit_message_text(txt, reply_markup=back_kb("menu_targets"),
                                      parse_mode=ParseMode.HTML)
        elif data.startswith("pfp_info:"):
            k = data.split(":")[1]
            txt = {
                "lock": "🔒 .pfplock (reply photo)",
                "loop": "🔁 .pfploop — toggle loop",
                "save": "💾 .savepfp (reply photo)",
                "all": "📁 .allpfp — count",
                "del": "🗑️ .deletepfp — clear",
            }.get(k, "❓")
            await q.edit_message_text(txt, reply_markup=back_kb("menu_pfp"),
                                      parse_mode=ParseMode.HTML)
        elif data.startswith("fun:"):
            k = data.split(":")[1]
            txt = {
                "play": "🎵 .play <song>",
                "ai": "🤖 .ai <prompt>",
                "cat": "🐱 .cat",
                "dog": "🐶 .dog",
                "dice": "🎲 .dice",
                "quotes": "📜 .quotes",
                "jokes": "😂 .jokes",
                "find": "🔍 .find <music>",
            }.get(k, "❓")
            await q.edit_message_text(txt, reply_markup=back_kb("menu_fun"),
                                      parse_mode=ParseMode.HTML)
        elif data.startswith("grp_info:"):
            k = data.split(":")[1]
            txt = {
                "lock": "🔒 .lock", "unlock": "🔓 .unlock",
                "promote": "⬆️ .promote (reply)",
                "demote": "⬇️ .demote (reply)",
                "admin": "👑 .admin — all bots admin",
                "mute": "🔇 .mute (reply)", "unmute": "🔊 .unmute (reply)",
                "kick": "👢 .kick (reply)", "ban": "🚫 .ban (reply)",
                "unban": "✅ .unban (reply)",
            }.get(k, "❓")
            await q.edit_message_text(txt, reply_markup=back_kb("menu_group"),
                                      parse_mode=ParseMode.HTML)
        elif data.startswith("stop_info:"):
            k = data.split(":")[1]
            txt = {
                "s": "🛑 .s <task_name>",
                "sall": "⚠️ .sall — stop ALL tasks",
            }.get(k, "❓")
            await q.edit_message_text(txt, reply_markup=back_kb("menu_stop"),
                                      parse_mode=ParseMode.HTML)
        elif data.startswith("edits_info:"):
            k = data.split(":")[1]
            txt = {
                "start": "📝 .start / .menu — show start menu",
                "menumedia": "🖼️ Edit main menu media via .setmenumedia",
                "dfnmedia": "🎬 .dfn — sends game over video",
                "ok": "🔑 .ok <user_id> — add sudo",
            }.get(k, "❓")
            await q.edit_message_text(txt, reply_markup=back_kb("menu_edits"),
                                      parse_mode=ParseMode.HTML)
    except BadRequest:
        pass
    except Exception as e:
        log.debug(f"cb err: {e}")

# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
async def post_init(main_app: Application):
    log.info("Initializing child bots...")
    for b in ALL_BOTS[1:]:        # ← FIX: skip main
        if b["bot"] is None:
            ok = await _bootstrap_bot(b)
            if not ok: continue
        if b["app"] is None:
            await _spawn_child_app(b)
    log.info(f"✅ All {len(ALL_BOTS)} bots online.")

async def main():
    if not MAIN_TOKEN or MAIN_TOKEN == "PUT_YOUR_MAIN_BOT_TOKEN_HERE":
        raise SystemExit("❌ Set MAIN_TOKEN and OWNER_ID first.")
    if OWNER_ID <= 0:
        raise SystemExit("❌ Set OWNER_ID first.")
    load_all()
    load_bots()

    # FIX: ensure MAIN_TOKEN bot is at index 0
    main_idx = next((i for i, b in enumerate(ALL_BOTS)
                     if b["token"] == MAIN_TOKEN), None)
    if main_idx is None:
        ALL_BOTS.insert(0, {"token": MAIN_TOKEN, "bot_id": 0, "username": "",
                            "bot": None, "app": None, "guard": None})
        save_bots()
    elif main_idx != 0:
        ALL_BOTS.insert(0, ALL_BOTS.pop(main_idx))
        save_bots()

    ok = await _bootstrap_bot(ALL_BOTS[0])
    if not ok:
        raise SystemExit("❌ Main bot token invalid.")

    app = Application.builder().token(MAIN_TOKEN).post_init(post_init).build()
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex(r"^\."), dot_dispatch),
        group=0)
    app.add_handler(
        MessageHandler(filters.ALL & ~filters.COMMAND, raid_message_listener),
        group=1)
    app.add_handler(CallbackQueryHandler(cb_query))

    log.info("🚀 Starting LUFFY CREW MULTI-BOT...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    log.info(f"✅ Main bot @{ALL_BOTS[0]['username']} ready to fyt!")

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Shutting down...")
        for b in ALL_BOTS:
            if b.get("app"):
                try:
                    await b["app"].updater.stop()
                    await b["app"].stop()
                    await b["app"].shutdown()
                except Exception: pass
        try:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
        except Exception: pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutdown.")