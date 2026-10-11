
import os
import re
import json
import time
import uuid
import hmac
import hashlib
import logging
import threading
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, request, redirect

# ============================================================
# GLOBALNAIRA ENGINE — FREE-SOURCE EDITION
# Five-stage discovery and report pipeline.
# No mandatory OpenAI API key or paid AI dependency.
# ============================================================

APP_NAME = "GlobalNaira Engine"
APP_VERSION = "1.0.0-free-sources"

PORT = int(os.getenv("PORT", "10000"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
DB_PATH = DATA_DIR / "globalnaira.sqlite3"

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
PAYSTACK_SECRET = os.getenv("PAYSTACK_SECRET", "").strip()
REPORT_PRICE_NAIRA = max(
    0, int(os.getenv("REPORT_PRICE_NAIRA", "25000"))
)

SCAN_INTERVAL_SECONDS = max(
    300, int(os.getenv("SCAN_INTERVAL_SECONDS", "3600"))
)
REQUEST_TIMEOUT_SECONDS = max(
    5, int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
)
DISCOVERY_QUERY = os.getenv(
    "DISCOVERY_QUERY",
    "small business grants procurement contracts business opportunities"
)
MAX_ITEMS_PER_SOURCE = max(
    5, min(50, int(os.getenv("MAX_ITEMS_PER_SOURCE", "15")))
)
DISABLE_BACKGROUND_PIPELINE = (
    os.getenv("DISABLE_BACKGROUND_PIPELINE", "false").lower()
    in ("1", "true", "yes", "on")
)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("globalnaira")

app = Flask(__name__)
wsgi_app = app

pipeline_lock = threading.Lock()
pipeline_thread = None
pipeline_started = False

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "GlobalNairaEngine/1.0 (public-source research)",
    "Accept": "application/json",
})


# -------------------------- UTILITIES ------------------------

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def safe_json(value):
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def parse_json(value, fallback=None):
    try:
        return json.loads(value)
    except Exception:
        return fallback if fallback is not None else {}


def clean_text(value, limit=1000):
    if value is None:
        return ""
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text[:limit]


def valid_http_url(url):
    return isinstance(url, str) and url.startswith(("https://", "http://"))


def http_get_json(url, params=None):
    response = SESSION.get(
        url,
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


# -------------------------- DATABASE -------------------------

@contextmanager
def db_connect():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(
        str(DB_PATH),
        timeout=30,
        check_same_thread=False,
    )
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def init_db():
    with db_connect() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                report_json TEXT NOT NULL
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fingerprint TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                url TEXT,
                source TEXT,
                category TEXT,
                score REAL DEFAULT 0,
                discovered_at TEXT NOT NULL,
                raw_json TEXT
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                reference TEXT PRIMARY KEY,
                email TEXT NOT NULL,
                amount_kobo INTEGER NOT NULL,
                status TEXT NOT NULL,
                report_id TEXT NOT NULL,
                access_token_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                paid_at TEXT,
                gateway_response TEXT,
                FOREIGN KEY(report_id) REFERENCES reports(id)
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS engine_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_opportunities_score
            ON opportunities(score DESC)
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_status
            ON orders(status)
        """)


def set_state(key, value):
    with db_connect() as db:
        db.execute("""
            INSERT INTO engine_state(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
        """, (key, str(value), utc_now()))


def get_state(key, default=None):
    with db_connect() as db:
        row = db.execute(
            "SELECT value FROM engine_state WHERE key=?",
            (key,),
        ).fetchone()
    return row["value"] if row else default


# ----------------------- TELEGRAM ALERTS ---------------------

def send_telegram(message):
    if not TELEGRAM_TOKEN or not ADMIN_CHAT_ID:
        return False

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": ADMIN_CHAT_ID,
                "text": clean_text(message, 3900),
                "disable_web_page_preview": True,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return True
    except Exception as exc:
        log.warning("Telegram notification failed: %s", exc)
        return False


# -------------------- ROBOT 1: DISCOVERY ---------------------
# Queries free public sources. These records are research leads,
# not confirmed income, guaranteed grants, or verified contracts.

def discover_gdelt():
    query = DISCOVERY_QUERY.strip()
    if not query:
        return []

    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": MAX_ITEMS_PER_SOURCE,
        "sort": "HybridRel",
    }

    try:
        data = http_get_json(url, params)
        items = data.get("articles", []) if isinstance(data, dict) else []
        results = []

        for item in items:
            title = clean_text(item.get("title"), 300)
            link = item.get("url", "")
            if not title or not valid_http_url(link):
                continue
            results.append({
                "title": title,
                "description": clean_text(
                    item.get("seendate", "") + " " +
                    item.get("domain", ""),
                    1000,
                ),
                "url": link,
                "source": "GDELT public news index",
                "category": "news_or_opportunity",
                "raw": item,
            })
        return results
    except Exception as exc:
        log.warning("GDELT discovery failed: %s", exc)
        return []


def discover_data_gov():
    url = "https://catalog.data.gov/api/3/action/package_search"
    params = {
        "q": DISCOVERY_QUERY,
        "rows": MAX_ITEMS_PER_SOURCE,
    }

    try:
        data = http_get_json(url, params)
        result = data.get("result", {}) if isinstance(data, dict) else {}
        packages = result.get("results", []) if isinstance(result, dict) else []
        records = []

        for item in packages:
            title = clean_text(item.get("title"), 300)
            link = item.get("url") or (
                "https://catalog.data.gov/dataset/" +
                str(item.get("name", ""))
            )
            if not title or not valid_http_url(link):
                continue
            records.append({
                "title": title,
                "description": clean_text(item.get("notes"), 1500),
                "url": link,
                "source": "Data.gov catalogue",
                "category": "public_dataset",
                "raw": item,
            })
        return records
    except Exception as exc:
        log.warning("Data.gov discovery failed: %s", exc)
        return []


def discover_world_bank():
    # The World Bank API provides public development indicators.
    # These records are background research, not commercial leads.
    url = (
        "https://api.worldbank.org/v2/country/all/"
        "indicator/NY.GDP.MKTP.CD"
    )
    params = {
        "format": "json",
        "per_page": MAX_ITEMS_PER_SOURCE,
        "date": "2022:2025",
    }

    try:
        data = http_get_json(url, params)
        rows = data[1] if isinstance(data, list) and len(data) > 1 else []
        records = []

        for item in rows:
            country = (item.get("country") or {}).get("value", "")
            year = item.get("date", "")
            value = item.get("value")
            if not country:
                continue
            title = f"World Bank GDP indicator: {country} ({year})"
            records.append({
                "title": title,
                "description": (
                    f"Public GDP indicator value: {value}. "
                    "Use the original source for definitions and context."
                ),
                "url": (
                    "https://data.worldbank.org/indicator/NY.GDP.MKTP.CD"
                ),
                "source": "World Bank Open Data",
                "category": "economic_context",
                "raw": item,
            })
        return records
    except Exception as exc:
        log.warning("World Bank discovery failed: %s", exc)
        return []


def robot_1_discover():
    records = []
    # Keep individual source failures from stopping other sources.
    for source_function in (
        discover_gdelt,
        discover_data_gov,
        discover_world_bank,
    ):
        try:
            records.extend(source_function())
        except Exception:
            log.exception("Discovery source unexpectedly failed")
    log.info("Robot 1 discovered %s source records", len(records))
    return records


# -------------------- ROBOT 2: NORMALIZE ---------------------

def robot_2_normalize(records):
    normalized = []
    seen = set()

    for record in records:
        title = clean_text(record.get("title"), 300)
        url = clean_text(record.get("url"), 1500)
        if not title or not valid_http_url(url):
            continue

        fingerprint = hashlib.sha256(
            (title.lower() + "|" + url.lower()).encode("utf-8")
        ).hexdigest()

        if fingerprint in seen:
            continue
        seen.add(fingerprint)

        normalized.append({
            "fingerprint": fingerprint,
            "title": title,
            "description": clean_text(record.get("description"), 2000),
            "url": url,
            "source": clean_text(record.get("source"), 200),
            "category": clean_text(record.get("category"), 100),
            "raw": record.get("raw", {}),
            "discovered_at": utc_now(),
        })

    log.info("Robot 2 normalized %s records", len(normalized))
    return normalized


# --------------------- ROBOT 3: SCORING ----------------------

POSITIVE_TERMS = {
    "grant": 3,
    "funding": 3,
    "tender": 3,
    "procurement": 3,
    "contract": 3,
    "small business": 3,
    "supplier": 2,
    "request for proposal": 3,
    "rfp": 2,
    "business opportunity": 3,
    "training": 1,
    "support programme": 2,
    "entrepreneur": 2,
    "award": 1,
}

def robot_3_score(records):
    scored = []

    for item in records:
        text = (
            item["title"] + " " + item["description"] + " " +
            item["category"]
        ).lower()

        score = 0.0
        matched_terms = []
        for term, weight in POSITIVE_TERMS.items():
            if term in text:
                score += weight
                matched_terms.append(term)

        # A source-specific relevance score, not a probability of success.
        if item["source"] == "Data.gov catalogue":
            score += 0.5
        elif item["source"] == "GDELT public news index":
            score += 0.5
        elif item["source"] == "World Bank Open Data":
            score += 0.25

        item["score"] = round(min(score, 20.0), 2)
        item["matched_terms"] = matched_terms
        scored.append(item)

    scored.sort(key=lambda row: row["score"], reverse=True)
    log.info("Robot 3 scored %s records", len(scored))
    return scored


# -------------------- ROBOT 4: PERSIST -----------------------

def robot_4_persist(records):
    inserted = 0
    updated = 0

    with db_connect() as db:
        for item in records:
            cursor = db.execute("""
                INSERT INTO opportunities(
                    fingerprint, title, description, url, source,
                    category, score, discovered_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fingerprint) DO UPDATE SET
                    title=excluded.title,
                    description=excluded.description,
                    url=excluded.url,
                    source=excluded.source,
                    category=excluded.category,
                    score=excluded.score,
                    raw_json=excluded.raw_json
            """, (
                item["fingerprint"],
                item["title"],
                item["description"],
                item["url"],
                item["source"],
                item["category"],
                item["score"],
                item["discovered_at"],
                safe_json(item.get("raw", {})),
            ))
            if cursor.rowcount:
                updated += 1

        row = db.execute(
            "SELECT COUNT(*) AS n FROM opportunities"
        ).fetchone()

    log.info(
        "Robot 4 saved records; database contains %s records",
        row["n"],
    )
    return {"processed": len(records), "written": updated, "total": row["n"]}


# --------------------- ROBOT 5: REPORT -----------------------

def robot_5_build_report(records, persistence):
    report_id = str(uuid.uuid4())
    top_items = []

    for item in records[:50]:
        top_items.append({
            "title": item["title"],
            "description": item["description"],
            "url": item["url"],
            "source": item["source"],
            "category": item["category"],
            "score": item["score"],
            "matched_terms": item.get("matched_terms", []),
        })

    report = {
        "report_id": report_id,
        "generated_at": utc_now(),
        "engine": APP_NAME,
        "version": APP_VERSION,
        "discovery_mode": "free_public_sources",
        "source_count": len({
            item.get("source") for item in records
        }),
        "records_discovered": len(records),
        "records_included": len(top_items),
        "database": persistence,
        "opportunities": top_items,
        "limitations": [
            "Public-source discovery does not prove that an offer is genuine.",
            "Scores are keyword relevance scores, not success probabilities.",
            "Check eligibility, deadlines, fees, and official source pages.",
            "No earnings or funding outcome is guaranteed.",
            "Some public APIs may be temporarily unavailable or rate-limited.",
        ],
    }

    with db_connect() as db:
        db.execute("""
            INSERT INTO reports(id, created_at, status, report_json)
            VALUES (?, ?, ?, ?)
        """, (
            report_id,
            report["generated_at"],
            "ready",
            safe_json(report),
        ))

    set_state("last_pipeline_at", report["generated_at"])
    set_state("last_report_id", report_id)
    set_state("last_discovered_count", len(records))

    log.info("Robot 5 generated report %s", report_id)
    return report


# -------------------- PIPELINE CONTROL -----------------------

def run_pipeline():
    if not pipeline_lock.acquire(blocking=False):
        return {
            "status": "already_running",
            "message": "A pipeline run is already in progress.",
        }

    started = time.monotonic()
    try:
        log.info("Starting five-robot pipeline")
        raw = robot_1_discover()
        normalized = robot_2_normalize(raw)
        scored = robot_3_score(normalized)
        persistence = robot_4_persist(scored)
        report = robot_5_build_report(scored, persistence)

        set_state("last_pipeline_status", "success")
        set_state("last_pipeline_duration_seconds", round(
            time.monotonic() - started, 2
        ))

        send_telegram(
            f"{APP_NAME}: pipeline completed.\n"
            f"Records discovered: {len(scored)}\n"
            f"Report ID: {report['report_id']}"
        )
        return {
            "status": "success",
            "report_id": report["report_id"],
            "records_discovered": len(scored),
            "duration_seconds": round(time.monotonic() - started, 2),
        }

    except Exception as exc:
        log.exception("Pipeline failed")
        set_state("last_pipeline_status", "failed")
        set_state("last_pipeline_error", str(exc)[:500])
        send_telegram(f"{APP_NAME}: pipeline failed: {str(exc)[:500]}")
        return {"status": "error", "error": str(exc)[:500]}
    finally:
        pipeline_lock.release()


def background_loop():
    log.info(
        "Background pipeline loop started; interval=%ss",
        SCAN_INTERVAL_SECONDS,
    )
    while True:
        try:
            run_pipeline()
        except Exception:
            log.exception("Unexpected background-loop failure")
        time.sleep(SCAN_INTERVAL_SECONDS)


def start_pipeline():
    global pipeline_thread, pipeline_started

    if DISABLE_BACKGROUND_PIPELINE:
        log.info("Background pipeline disabled by environment variable")
        return

    if pipeline_started:
        return

    pipeline_thread = threading.Thread(
        target=background_loop,
        name="globalnaira-pipeline",
        daemon=True,
    )
    pipeline_thread.start()
    pipeline_started = True


# ---------------------- REPORT ACCESS ------------------------

def hash_token(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def latest_report_snapshot():
    with db_connect() as db:
        row = db.execute("""
            SELECT id, report_json FROM reports
            WHERE status='ready'
            ORDER BY created_at DESC LIMIT 1
        """).fetchone()

    if row:
        return row["id"], parse_json(row["report_json"])

    # A paid report can still be purchased before the first scan.
    empty_id = str(uuid.uuid4())
    empty_report = {
        "report_id": empty_id,
        "generated_at": utc_now(),
        "engine": APP_NAME,
        "version": APP_VERSION,
        "discovery_mode": "free_public_sources",
        "source_count": 0,
        "records_discovered": 0,
        "records_included": 0,
        "database": {},
        "opportunities": [],
        "notice": (
            "No scan has completed yet. Run the pipeline and create "
            "a new report to populate this report."
        ),
    }

    with db_connect() as db:
        db.execute("""
            INSERT INTO reports(id, created_at, status, report_json)
            VALUES (?, ?, ?, ?)
        """, (empty_id, utc_now(), "ready", safe_json(empty_report)))

    return empty_id, empty_report


# ------------------------ PAYSTACK ---------------------------

def paystack_headers():
    return {
        "Authorization": f"Bearer {PAYSTACK_SECRET}",
        "Content-Type": "application/json",
    }


def verify_paystack_transaction(reference):
    if not PAYSTACK_SECRET:
        raise RuntimeError("PAYSTACK_SECRET is not configured.")

    response = requests.get(
        "https://api.paystack.co/transaction/verify/"
        + requests.utils.quote(reference, safe=""),
        headers=paystack_headers(),
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()

    if not payload.get("status"):
        raise RuntimeError(
            clean_text(payload.get("message"), 300)
            or "Paystack verification failed."
        )

    return payload.get("data", {})


@app.post("/api/payments/initialize")
def initialize_payment():
    if not PAYSTACK_SECRET:
        return jsonify({
            "error": "Payments are not configured. Set PAYSTACK_SECRET."
        }), 503

    body = request.get_json(silent=True) or {}
    email = clean_text(body.get("email"), 254).lower()

    if not email or "@" not in email:
        return jsonify({"error": "Provide a valid email address."}), 400

    # The public API does not accept a customer-supplied amount.
    amount_kobo = REPORT_PRICE_NAIRA * 100
    if amount_kobo <= 0:
        return jsonify({"error": "Report price is not configured."}), 503

    report_id, _report = latest_report_snapshot()
    reference = "GNE_" + uuid.uuid4().hex
    access_token = uuid.uuid_urlsafe(32) if hasattr(uuid, "uuid_urlsafe") else None
    # Use a cryptographically secure token, not a predictable UUID.
    import secrets
    access_token = secrets.token_urlsafe(32)
    token_hash = hash_token(access_token)

    with db_connect() as db:
        db.execute("""
            INSERT INTO orders(
                reference, email, amount_kobo, status, report_id,
                access_token_hash, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            reference,
            email,
            amount_kobo,
            "pending",
            report_id,
            token_hash,
            utc_now(),
        ))

    callback_url = (
        f"{PUBLIC_BASE_URL}/paystack/callback?"
        + urlencode({"token": access_token})
    ) if PUBLIC_BASE_URL else None

    if not callback_url:
        with db_connect() as db:
            db.execute(
                "DELETE FROM orders WHERE reference=?",
                (reference,),
            )
        return jsonify({
            "error": "PUBLIC_BASE_URL must be set to your Render service URL."
        }), 503

    payload = {
        "email": email,
        "amount": amount_kobo,
        "reference": reference,
        "callback_url": callback_url,
        "metadata": {
            "report_id": report_id,
            "product": APP_NAME,
        },
    }

    try:
        response = requests.post(
            "https://api.paystack.co/transaction/initialize",
            headers=paystack_headers(),
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        result = response.json()

        if not result.get("status"):
            raise RuntimeError(
                clean_text(result.get("message"), 300)
                or "Paystack could not initialize the transaction."
            )

        return jsonify({
            "status": "initialized",
            "reference": reference,
            "authorization_url": result["data"]["authorization_url"],
            "access_token": access_token,
            "report_id": report_id,
            "amount_naira": REPORT_PRICE_NAIRA,
            "message": "Complete payment at the Paystack checkout URL.",
        }), 200

    except Exception as exc:
        log.exception("Payment initialization failed")
        with db_connect() as db:
            db.execute(
                "UPDATE orders SET status='initialization_failed' "
                "WHERE reference=?",
                (reference,),
            )
        return jsonify({
            "error": "Payment initialization failed.",
            "detail": str(exc)[:300],
        }), 502


def mark_order_paid(reference, gateway_data):
    with db_connect() as db:
        order = db.execute(
            "SELECT * FROM orders WHERE reference=?",
            (reference,),
        ).fetchone()

        if not order:
            return False, "Unknown payment reference."

        if order["status"] == "paid":
            # Idempotent: do not rotate the customer's access token.
            return True, "Payment was already verified."

        expected_amount = int(order["amount_kobo"])
        actual_amount = int(gateway_data.get("amount") or 0)
        currency = str(gateway_data.get("currency") or "").upper()
        gateway_status = str(gateway_data.get("status") or "").lower()

        if gateway_status != "success":
            return False, "Paystack transaction is not successful."
        if actual_amount != expected_amount:
            return False, "The verified payment amount does not match."
        if currency != "NGN":
            return False, "Unexpected payment currency."
        if str(gateway_data.get("reference", "")) != reference:
            return False, "The verified reference does not match."

        db.execute("""
            UPDATE orders SET status='paid', paid_at=?, gateway_response=?
            WHERE reference=?
        """, (utc_now(), safe_json(gateway_data), reference))

    return True, "Payment verified."


@app.get("/paystack/callback")
def paystack_callback():
    reference = clean_text(request.args.get("reference"), 150)
    token = clean_text(request.args.get("token"), 200)

    if not reference:
        return "Missing payment reference.", 400

    try:
        gateway_data = verify_paystack_transaction(reference)
        ok, message = mark_order_paid(reference, gateway_data)
        if not ok:
            return f"Payment not confirmed: {message}", 400

        # A token is supplied in the callback URL and must match the order.
        with db_connect() as db:
            order = db.execute(
                "SELECT access_token_hash FROM orders WHERE reference=?",
                (reference,),
            ).fetchone()

        if not order or not token:
            return "Payment verified, but report token is missing.", 400
        if not hmac.compare_digest(
            order["access_token_hash"], hash_token(token)
        ):
            return "Invalid report token.", 403

        if PUBLIC_BASE_URL:
            return redirect(f"{PUBLIC_BASE_URL}/report/{token}", code=302)
        return "Payment verified. Your report token is ready."

    except Exception as exc:
        log.exception("Paystack callback failed")
        return f"Unable to verify payment: {str(exc)[:300]}", 502


@app.post("/paystack/webhook")
def paystack_webhook():
    if not PAYSTACK_SECRET:
        return jsonify({"error": "Payments are not configured."}), 503

    signature = request.headers.get("x-paystack-signature", "")
    raw_body = request.get_data()

    expected_signature = hmac.new(
        PAYSTACK_SECRET.encode("utf-8"),
        raw_body,
        hashlib.sha512,
    ).hexdigest()

    if not signature or not hmac.compare_digest(
        signature, expected_signature
    ):
        return jsonify({"error": "Invalid webhook signature."}), 401

    event = request.get_json(silent=True) or {}
    if event.get("event") != "charge.success":
        return jsonify({"status": "ignored"}), 200

    data = event.get("data") or {}
    reference = clean_text(data.get("reference"), 150)
    if not reference:
        return jsonify({"error": "Missing transaction reference."}), 400

    try:
        # Verify directly with Paystack instead of trusting webhook fields.
        verified = verify_paystack_transaction(reference)
        ok, message = mark_order_paid(reference, verified)
        if not ok:
            return jsonify({"error": message}), 400
        return jsonify({"status": "verified"}), 200
    except Exception as exc:
        log.exception("Paystack webhook verification failed")
        return jsonify({"error": str(exc)[:300]}), 502


@app.get("/report/<token>")
def get_paid_report(token):
    token_hash = hash_token(token)

    with db_connect() as db:
        order = db.execute("""
            SELECT report_id, status, access_token_hash
            FROM orders WHERE access_token_hash=?
            ORDER BY created_at DESC LIMIT 1
        """, (token_hash,)).fetchone()

        if not order:
            return jsonify({"error": "Report token not found."}), 404
        if order["status"] != "paid":
            return jsonify({"error": "Payment has not been verified."}), 403

        report = db.execute(
            "SELECT report_json FROM reports WHERE id=?",
            (order["report_id"],),
        ).fetchone()

    if not report:
        return jsonify({"error": "The report could not be found."}), 404

    return jsonify(parse_json(report["report_json"])), 200


# -------------------------- WEB API --------------------------

@app.get("/")
def index():
    return jsonify({
        "name": APP_NAME,
        "version": APP_VERSION,
        "status": "online",
        "description": (
            "Five-stage public-source discovery and reporting engine."
        ),
        "health": "/health",
        "endpoints": {
            "health": "GET /health",
            "run_pipeline": "POST /api/pipeline/run",
            "latest_report": "GET /api/reports/latest",
            "initialize_payment": "POST /api/payments/initialize",
            "payment_callback": "GET /paystack/callback",
            "paystack_webhook": "POST /paystack/webhook",
            "paid_report": "GET /report/<token>",
        },
    })


@app.get("/health")
def health():
    try:
        with db_connect() as db:
            db.execute("SELECT 1").fetchone()
        db_status = "ok"
    except Exception as exc:
        db_status = f"error: {str(exc)[:150]}"

    return jsonify({
        "status": "ok" if db_status == "ok" else "degraded",
        "name": APP_NAME,
        "version": APP_VERSION,
        "database": db_status,
        "discovery_provider": "free_public_sources",
        "paystack_configured": bool(PAYSTACK_SECRET),
        "telegram_configured": bool(TELEGRAM_TOKEN and ADMIN_CHAT_ID),
        "background_pipeline_disabled": DISABLE_BACKGROUND_PIPELINE,
        "last_pipeline_at": get_state("last_pipeline_at"),
        "last_pipeline_status": get_state("last_pipeline_status"),
        "last_discovered_count": get_state("last_discovered_count", "0"),
    }), 200 if db_status == "ok" else 503


@app.post("/api/pipeline/run")
def api_run_pipeline():
    # Protect this endpoint with an admin secret in production.
    # A secret is required so the public cannot trigger repeated scans.
    admin_secret = os.getenv("ADMIN_API_SECRET", "").strip()
    supplied = request.headers.get("x-admin-secret", "")

    if not admin_secret:
        return jsonify({
            "error": "Set ADMIN_API_SECRET to enable manual pipeline runs."
        }), 503

    if not hmac.compare_digest(supplied, admin_secret):
        return jsonify({"error": "Unauthorized."}), 401

    result = run_pipeline()
    code = 200 if result.get("status") in (
        "success", "already_running"
    ) else 500
    return jsonify(result), code


@app.get("/api/reports/latest")
def api_latest_report():
    with db_connect() as db:
        row = db.execute("""
            SELECT id, created_at, status, report_json FROM reports
            ORDER BY created_at DESC LIMIT 1
        """).fetchone()

    if not row:
        return jsonify({
            "status": "no_report",
            "message": "No pipeline report has been generated yet.",
        }), 404

    report = parse_json(row["report_json"])
    return jsonify({
        "id": row["id"],
        "created_at": row["created_at"],
        "status": row["status"],
        "report": report,
    })


# --------------------------- START ---------------------------

init_db()

# The DISABLE_BACKGROUND_PIPELINE variable allows controlled startup.
# Gunicorn should run one worker to avoid duplicate background loops.
start_pipeline()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
