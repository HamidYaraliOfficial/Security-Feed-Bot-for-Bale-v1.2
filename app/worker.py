# -*- coding: utf-8 -*-
"""Background engine: fetch feeds -> queue -> build full post (+translation) -> send to Bale -> delete from queue."""
import json
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from . import composer, config, fetcher
from .bale import BaleClient, BaleError, explain, is_fatal
from .db import Store
from .translate import Translator

MAX_QUEUE = 200          # safety cap (posts are kept with full text until sent); oldest overflow is dropped
MAX_TRIES = 3            # after 3 failed attempts a post is dropped (so the DB never clogs)
PART_DELAY = 1.2         # seconds between the messages of one post
MAX_TR_DEFER = 6         # after 6 cycles of failing translation a piece is sent in its original language
TR_MAX_WAIT = 90         # seconds to keep retrying a piece before postponing the post to the next cycle


class TranslationDeferred(Exception):
    """Translation is temporarily impossible; the post resumes from the same piece in the next cycle."""


class Engine:
    def __init__(self):
        self.store = Store(config.DATA_DIR / "bot.db")
        self.logs = deque(maxlen=300)
        self.stop_evt = threading.Event()
        self.wake_evt = threading.Event()
        self.thread = None
        self.cache = {}
        self._tr = None
        self._tr_key = None
        self._token = ""
        self._secret2 = ""
        self.status = {"running": False, "state": "متوقف", "last_cycle": None, "next_cycle": None,
                       "last_summary": "", "last_error": ""}

    # ------------------------------------------------------------ utilities
    def log(self, msg, level="info"):
        for secret in (self._token, self._secret2):
            if secret:
                msg = msg.replace(secret, "***")
        stamp = datetime.now().strftime("%H:%M:%S")
        self.logs.append({"t": stamp, "level": level, "msg": msg})
        try:
            print(f"[{stamp}] {level.upper():5} {msg}", flush=True)
        except Exception:
            pass

    def snapshot(self):
        st = dict(self.status)
        st.update(self.store.stats())
        st["sent_total"] = int(self.store.get_meta("sent_total", 0) or 0)
        return st

    def translator(self, cfg):
        if not cfg["translate_enabled"]:
            return None
        key = (cfg["translate_provider"], cfg["libretranslate_url"], cfg["libretranslate_key"], cfg["mymemory_email"],
               cfg["fetch_proxy"], cfg["lingva_url"], cfg["google_api_key"], cfg["translate_delay_sec"])
        if key != self._tr_key:
            self._tr, self._tr_key = Translator(cfg), key
            self._tr.sleep = self.stop_evt.wait          # pressing Stop interrupts any waiting
            self._tr.stopped = self.stop_evt.is_set
        return self._tr

    # ------------------------------------------------------------ lifecycle
    def start(self):
        if self.thread and self.thread.is_alive():
            return False
        self.stop_evt.clear()
        self.wake_evt.clear()
        self.thread = threading.Thread(target=self._loop, name="engine", daemon=True)
        self.thread.start()
        return True

    def stop(self):
        self.stop_evt.set()
        self.wake_evt.set()
        if self.thread:
            self.thread.join(timeout=8)
        self.status.update(running=False, state="متوقف", next_cycle=None)
        self.log("ربات متوقف شد.")

    def run_now(self):
        self.wake_evt.set()

    @property
    def running(self):
        return bool(self.thread and self.thread.is_alive())

    def _loop(self):
        self.status.update(running=True, state="در حال اجرا")
        self.log("ربات شروع به کار کرد.")
        while not self.stop_evt.is_set():
            try:
                self.run_cycle()
            except Exception as e:      # never let the loop die
                self.log(f"خطای غیرمنتظره: {type(e).__name__}: {e}", "error")
            if self.stop_evt.is_set():
                break
            wait = config.load()["interval_minutes"] * 60
            self.status["next_cycle"] = time.time() + wait
            self.wake_evt.clear()
            self.wake_evt.wait(wait)
        self.status.update(running=False, state="متوقف", next_cycle=None)

    # ------------------------------------------------------------ one cycle
    def run_cycle(self):
        cfg = config.load()
        self._token, self._secret2 = cfg["bale_token"], cfg["google_api_key"] or cfg["libretranslate_key"]
        if not cfg["bale_token"] or not cfg["channel_id"]:
            self.status.update(state="تنظیمات ناقص (توکن یا کانال)", last_error="توکن یا آیدی کانال وارد نشده است.")
            self.log("توکن یا آیدی کانال وارد نشده؛ این دوره رد شد.", "warn")
            return
        self.status.update(state="در حال دریافت منابع…")
        added, failed = self.ingest(cfg)
        self.status["state"] = "در حال ارسال…"
        sent = self.dispatch(cfg)
        pruned = self.store.prune(cfg["seen_retention_days"], MAX_QUEUE)
        if pruned:
            self.log(f"{pruned} مورد قدیمی از صف حذف شد (سقف صف {MAX_QUEUE}).", "warn")
        self.status.update(
            state="در حال اجرا (منتظر دوره بعد)", last_cycle=time.time(),
            last_summary=f"{added} مطلب جدید، {sent} ارسال، {failed} منبع ناموفق")
        self.log(f"پایان دوره: {added} مطلب جدید در صف، {sent} ارسال شد، {failed} منبع ناموفق.")

    # ------------------------------------------------------------ fetching
    def ingest(self, cfg):
        sources = config.active_sources(cfg)
        if not sources:
            self.log("هیچ منبعی انتخاب نشده است.", "warn")
            return 0, 0
        first_run = self.store.get_meta("initialized") != "1"
        skip = first_run and cfg["skip_backlog_first_run"]
        cutoff = time.time() - cfg["max_age_hours"] * 3600

        def work(src):
            try:
                content = fetcher.fetch_feed(fetcher.new_session(cfg["fetch_proxy"]), src["url"], self.cache)
                return src, ([] if content is None else fetcher.parse_feed(content, src)), None
            except Exception as e:
                return src, [], fetcher.describe_error(e)

        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(work, sources))

        added, failures = 0, []
        for src, items, err in results:
            if err:
                failures.append(f"{src['name']} ({err})")
                continue
            for it in items:
                if it["published_ts"] < cutoff or self.store.is_known(it["uid"]):
                    continue
                if not fetcher.passes_filters(it, cfg["include_keywords"], cfg["exclude_keywords"]):
                    continue
                if skip:
                    self.store.mark_seen(it["uid"])
                elif self.store.add_pending(it):
                    added += 1
        if first_run:
            self.store.set_meta("initialized", "1")
            if skip:
                self.log("اولین اجرا: مطالب موجود فعلی نادیده گرفته شد؛ فقط مطالب جدید ارسال می‌شود.")
        if failures:
            self.log("منابع ناموفق: " + "، ".join(failures), "warn")
        return added, len(failures)

    # ------------------------------------------------------------ sending
    def dispatch(self, cfg):
        rows = self.store.pending(cfg["max_posts_per_cycle"])
        if not rows:
            return 0
        client = BaleClient(cfg["bale_token"], cfg["bale_api_base"], cfg["bale_proxy"])
        session = fetcher.new_session(cfg["fetch_proxy"])
        sent = 0
        for row in rows:
            if self.stop_evt.is_set():
                break
            self.status["state"] = f"در حال ارسال: {row['title'][:40]}"
            try:
                done = self._process_row(client, session, cfg, row)
            except TranslationDeferred as e:
                if self.stop_evt.is_set():
                    break
                self.status["last_error"] = str(e)
                self.log(f"ترجمه موقتاً ممکن نیست؛ «{row['title'][:40]}» از همان تکه در دورهٔ بعد ادامه می‌یابد. ({e})", "warn")
                break
            except BaleError as e:
                if e.network or e.code == 429:
                    self.status["last_error"] = explain(e)
                    self.log("ارسال متوقف شد (مشکل اتصال یا محدودیت نرخ؛ دورهٔ بعد ادامه می‌یابد): " + explain(e), "error")
                    break
                if is_fatal(e):
                    self.status["last_error"] = explain(e)
                    self.log("خطای تنظیمات: " + explain(e), "error")
                    break
                tries = self.store.bump(row["id"])
                self.log(f"ارسال «{row['title'][:50]}» ناموفق ({e.description}) تلاش {tries}/{MAX_TRIES}", "warn")
                if tries >= MAX_TRIES:
                    self.store.finish(row["id"], row["uid"])
                    self.log("این مطلب پس از چند تلاش حذف شد.", "warn")
                continue
            if not done:                 # stop requested in the middle of a post; it resumes later
                break
            # success -> delete the whole post (text, plan) from DB, keep only the tiny 'seen' marker
            self.store.finish(row["id"], row["uid"])
            self.store.set_meta("sent_total", int(self.store.get_meta("sent_total", 0) or 0) + 1)
            self.status["last_error"] = ""
            sent += 1
            self.stop_evt.wait(cfg["delay_between_posts_sec"])
        return sent

    def _load_plan(self, row):
        plan = json.loads(row["plan"])
        if isinstance(plan, list):                       # plans written by older versions
            plan = {"v": 2, "stage": "send", "parts": plan}
        return plan

    def _process_row(self, client, session, cfg, row):
        """1) build the post once (stored as 'plan')  2) if needed translate it piece by piece (every result is saved)
        3) send: header -> FULL original text -> FULL Persian translation. Resumes after any interruption."""
        if row.get("plan"):
            plan = self._load_plan(row)
        else:
            try:
                plan, info = composer.build_plan(row, cfg, session)
            except Exception as e:
                self.log(f"ساخت متن کامل ناموفق ({type(e).__name__}); فقط خلاصه ارسال می‌شود.", "warn")
                plan, info = composer.fallback_plan(row, cfg), {}
            self.store.save_plan(row["id"], json.dumps(plan, ensure_ascii=False))
            row["progress"], row["tr_defer"] = 0, 0
            note = " | ترجمهٔ تکه‌به‌تکه" if info.get("translate") else ""
            self.log(f"آماده‌سازی: «{row['title'][:50]}» → {len(plan['parts'])} بخش، {info.get('images', 0)} عکس{note}")
        if plan["stage"] == "translate":
            plan = self._translate_plan(cfg, row, plan)          # may raise TranslationDeferred
        parts = plan["parts"]
        for i in range(int(row.get("progress") or 0), len(parts)):
            if self.stop_evt.is_set():
                return False
            self._send_part_retry(client, session, cfg, parts[i])
            self.store.set_progress(row["id"], i + 1)
            if i < len(parts) - 1:
                self.stop_evt.wait(PART_DELAY)
        self.log(f"ارسال شد: {row['title'][:70]} ({len(parts)} پیام)")
        return True

    def _translate(self, cfg, row, text):
        """Translate one piece. -> ('ok'|'same'|'failed', text). Raises TranslationDeferred when services are down."""
        tr = self.translator(cfg)
        if tr is None:
            return "same", text
        gave_up = int(row.get("tr_defer") or 0) >= MAX_TR_DEFER
        out, st = tr.translate(text, patient=not gave_up, max_wait=TR_MAX_WAIT)
        if st == "ok":
            if row.get("tr_defer"):
                self.store.reset_defer(row["id"])
                row["tr_defer"] = 0
            return "ok", out
        if st == "same":
            return "same", text
        if self.stop_evt.is_set():
            raise TranslationDeferred("توقف درخواست شد")
        why = "؛ ".join(tr.errors.values()) or "نامشخص"
        if gave_up:
            self.log(f"ترجمهٔ یک تکه ممکن نشد؛ متن اصلی آن در جای خودش گذاشته می‌شود ({why})", "warn")
            return "failed", text
        row["tr_defer"] = self.store.bump_defer(row["id"])
        raise TranslationDeferred(why)

    def _translate_plan(self, cfg, row, plan):
        """Translate title + every piece (one request each, saved immediately so nothing is redone after a pause).
        When everything is done the plan becomes the final list of messages."""
        save = lambda: self.store.update_plan(row["id"], json.dumps(plan, ensure_ascii=False), progress=0)
        if plan.get("tr_title") and "title_done" not in plan:
            st, out = self._translate(cfg, row, plan["hd"]["title"])
            plan["title_fa"] = out if st == "ok" else ""
            plan["title_failed"] = st == "failed"
            plan["title_done"] = True
            save()
        todo = [p for p in plan["parts"] if p["k"] == "text" and p.get("tr") and "fa" not in p and not p.get("failed")]
        for n, part in enumerate(todo, 1):
            if self.stop_evt.is_set():
                raise TranslationDeferred("توقف درخواست شد")
            st, out = self._translate(cfg, row, part["text"])
            if st == "ok":
                part["fa"] = out
            elif st == "same":
                part["fa"], part["same"] = out, True
            else:
                part["failed"] = True
            save()
            if n < len(todo):
                self.status["state"] = f"ترجمه: {n}/{len(todo)} تکه — {row['title'][:30]}"
        final = {"v": 2, "stage": "send", "parts": composer.finalize(plan)}
        self.store.update_plan(row["id"], json.dumps(final, ensure_ascii=False), progress=0)
        row["progress"] = 0
        self.log(f"ترجمه کامل شد؛ ابتدا متن اصلی و سپس ترجمهٔ فارسی ارسال می‌شود ({len(final['parts'])} پیام)")
        return final

    def _send_part_retry(self, client, session, cfg, part):
        try:
            self._send_part(client, session, cfg, part)
        except BaleError as e:
            if e.code == 429:
                wait = max(1, min(int(e.retry_after or 10), 120))
                self.log(f"محدودیت نرخ بله؛ {wait} ثانیه صبر می‌کنم.", "warn")
                self.stop_evt.wait(wait)
                self._send_part(client, session, cfg, part)
            else:
                raise

    def _send_part(self, client, session, cfg, part):
        chat = cfg["channel_id"]
        if part["k"] == "text":
            return self._send_text_safe(client, chat, part["text"])
        return self._send_photo_or_text(client, session, chat, part["url"], part.get("caption", ""))

    def _send_photo_or_text(self, client, session, chat, url, caption):
        img = fetcher.download_image(session, url) if url else None
        if img:
            tried_jpeg = False
            while True:
                try:
                    client.send_photo(chat, img[0], img[1], caption)
                    return
                except BaleError as e:
                    if e.network or e.code in (401, 403, 429) or is_fatal(e):
                        raise
                    conv = None if tried_jpeg else fetcher.to_jpeg(img[0])
                    tried_jpeg = True
                    if conv is None:
                        self.log(f"ارسال تصویر نشد ({e.description})", "warn")
                        break
                    img = conv                                    # retry once as JPEG
        if caption:                      # image unavailable: the header/text must still reach the channel
            self._send_text_safe(client, chat, caption)

    def _send_text_safe(self, client, chat, text, depth=0):
        try:
            client.send_message(chat, text)
        except BaleError as e:
            if e.network or e.code in (401, 403, 429) or is_fatal(e):
                raise
            if e.code == 400 and len(text) >= 400 and depth < 3:      # maybe too long for Bale: send in two halves
                mid = len(text) // 2
                cut = max(text.rfind("\n\n", 0, mid + 200), text.rfind(" ", 0, mid + 200))
                cut = cut if cut > 100 else mid
                self._send_text_safe(client, chat, text[:cut].strip(), depth + 1)
                self._send_text_safe(client, chat, text[cut:].strip(), depth + 1)
            else:
                self.log(f"یک پیام رد شد و نادیده گرفته شد: {e.description}", "warn")
