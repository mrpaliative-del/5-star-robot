
import os
import re
import json
import time
import uuid
import hmac
import hashlib
import logging
import secrets
import sqlite3
import threading
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, request, redirect

# ============================================================
# GLOBALNAIRA ENGINE — FREE PUBLIC-SOURCE EDITION
# Five robots | SQLite | Paystack | Optional Telegram
# ============================================================

APP_NAME = "GlobalNaira Engine"
APP_VERSION = "1.1.0-free-sources"

PORT = int(os.getenv("PORT", "10000"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/globalnaira"))
DB_PATH = DATA_DIR / "globalnaira.sqlite3"

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
PAYSTACK_SECRET = os.getenv("PAYSTACK_SECRET", "").strip()
REPORT_PRICE_NAIRA = max(0, int(os.getenv("REPORT_PRICE_NAIRA", "25000")))

SCAN_INTERVAL_SECONDS = max(
    300, int(os.getenv("SCAN_INTERVAL_SECONDS", "3600"))
)
REQUEST_TIMEOUT_SECONDS = max(
    5, int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
)
MAX_ITEMS_PER_SOURCE = max(
    5, min(50, int(os.getenv("MAX_ITEMS_PER_SOURCE", "15")))
)
DISCOVERY_QUERY = os.getenv(
    "DISCOVERY_QUERY",
    "small business grants procurement contracts business opportunities"
)
DISABLE_BACKGROUND_PIPELINE = (
    os.getenv("DISABLE_BACKGROUND_PIPELINE", "false").lower()
    in ("1", "true", "yes", "on")
)

ADMIN_API_SECRET = os.getenv("ADMIN_API_SECRET", "").strip()
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

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "GlobalNairaEngine/1.1 public-source-research",
    "Accept": "application/json",
})

pipeline_lock = threading.Lock()
pipeline_started = False
pipeline_thread = None


# ============================================================
# UTILITIES
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def clean_text(value, limit=1000):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()[:limit]


def safe_json(value):
    return json.dumps(value, ensure_ascii=False, default=str)


def parse_json(value, fallback=None):
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return fallback if fallback is not None else {}


def valid_http_url(url):
    return isinstance(url, str) and url.startswith(("https://", "http://"))


def get_json(url, params=None):
    response = SESSION.get(
        url, params=params, timeout=REQUEST_TIMEOUT_SECONDS
    )
    response.raise_for_status()
    return response.json()


# ============================================================
# SQLITE DATABASE
# ============================================================

@contextmanager
def db_connect():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(
        str(DB_PATH), timeout=30, check_same_thread=False
    )
    db.row_factory = sqlite3.Row

    try:
        db.execute("PRAGMA busy_timeout=30000")
        db.execute("PRAGMA foreign_keys=ON")
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


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


# ============================================================
# OPTIONAL TELEGRAM NOTIFICATIONS
# ============================================================

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


# ============================================================
# ROBOT 1 — DISCOVERY
# Active sources: Grants.gov and World Bank.
# GDELT is deliberately disabled for now due to HTTP 429.
# ============================================================

def discover_grants_gov():
    """Retrieve public US federal grant opportunity records."""
    url = "https://api.grants.gov/v1/api/search2"

    payload = {
        "keyword": DISCOVERY_QUERY,
        "oppStatuses": "posted|forecasted",
        "rows": MAX_ITEMS_PER_SOURCE,
        "startRecordNum": 0,
    }

    try:
        response = SESSION.post(
            url,
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()

        if data.get("errorcode") not in (0, "0", None):
            log.warning(
                "Grants.gov API error: %s",
                data.get("msg", "Unknown API error"),
            )
            return []

        result = data.get("data") or {}
        opportunities = result.get("oppHits") or []

        records = []

        for item in opportunities:
            title = clean_text(item.get("title"), 300)
            opportunity_id = clean_text(item.get("id"), 100)

            if not title or not opportunity_id:
                continue

            # Official search page is used instead of guessing
            # an individual detail-page URL format.
            official_url = (
                "https://www.grants.gov/search-results"
                "?keywords=" + requests.utils.quote(title)
            )

            records.append({
                "title": title,
                "description": clean_text(
                    "Agency: " + str(item.get("agencyName") or "Unknown")
                    + "; Status: " + str(item.get("oppStatus") or "Unknown")
                    + "; Opens: " + str(item.get("openDate") or "Unknown")
                    + "; Closes: " + str(item.get("closeDate") or "Unknown")
                    + "; Opportunity ID: " + opportunity_id,
                    1500,
                ),
                "url": official_url,
                "source": "Grants.gov",
                "category": "federal_grant",
                "raw": item,
            })

        log.info(
            "Grants.gov returned %s usable opportunities", len(records)
        )
        return records

    except Exception as exc:
        log.warning("Grants.gov discovery failed: %s", exc)
        return []


def discover_world_bank():
    """Retrieve public GDP indicators for economic context."""
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
        data = get_json(url, params)
        rows = data[1] if isinstance(data, list) and len(data) > 1 else []
        records = []

        for item in rows:
            country = (item.get("country") or {}).get("value", "")
            year = item.get("date", "")
            value = item.get("value")

            if not country:
                continue

            records.append({
                "title": f"World Bank GDP indicator: {country} ({year})",
                "description": (
                    f"Public GDP indicator value: {value}. "
                    "Use the original source for context and definitions."
                ),
                "url": (
                    "https://data.worldbank.org/indicator/NY.GDP.MKTP.CD"
                ),
                "source": "World Bank Open Data",
                "category": "economic_context",
                "raw": item,
            })

        log.info(
            "World Bank returned %s usable records", len(records)
        )
        return records

    except Exception as exc:
        log.warning("World Bank discovery failed: %s", exc)
        return []


def robot_1_discover():
    records = []

    # GDELT is excluded temporarily because the supplied logs
    # show HTTP 429 Too Many Requests.
    sources = (
        discover_grants_gov,
        discover_world_bank,
    )

    for source_function in sources:
        try:
            records.extend(source_function())
        except Exception:
            log.exception(
                "Unexpected source error: %s",
                source_function.__name__,
            )

    log.info(
        "Robot 1 discovered %s total source records", len(records)
    )
    return records


# ============================================================
# ROBOT 2 — NORMALIZATION AND DEDUPLICATION
# ============================================================

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
            "description": clean_text(
                record.get("description"), 2000
            ),
            "url": url,
            "source": clean_text(record.get("source"), 200),
            "category": clean_text(record.get("category"), 100),
            "raw": record.get("raw", {}),
            "discovered_at": utc_now(),
        })

    log.info("Robot 2 normalized %s records", len(normalized))
    return normalized


# ============================================================
# ROBOT 3 — RELEVANCE SCORING
# Scores are keyword matches, NOT success probabilities.
# ============================================================

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
    "entrepreneur": 2,
    "award": 1,
}


def robot_3_score(records):
    scored = []

    for item in records:
        searchable = (
            item["title"] + " " +
            item["description"] + " " +
            item["category"]
        ).lower()

        score = 0.0
        matches = []

        for term, weight in POSITIVE_TERMS.items():
            if term in searchable:
                score += weight
                matches.append(term)

        if item["source"] == "Grants.gov":
            score += 1.0
        elif item["source"] == "World Bank Open Data":
            score += 0.25

        item["score"] = round(min(score, 20.0), 2)
        item["matched_terms"] = matches
        scored.append(item)

    scored.sort(key=lambda row: row["score"], reverse=True)
    log.info("Robot 3 scored %s records", len(scored))
    return scored


# ============================================================
# ROBOT 4 — DATABASE PERSISTENCE
# ============================================================

def robot_4_persist(records):
    with db_connect() as db:
        for item in records:
            db.execute("""
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
                    discovered_at=excluded.discovered_at,
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

        row = db.execute(
            "SELECT COUNT(*) AS total FROM opportunities"
        ).fetchone()

    result = {
        "processed": len(records),
        "total": row["total"],
    }

    log.info(
        "Robot 4 persisted records; database total=%s", row["total"]
    )
    return result


# ============================================================
# ROBOT 5 — REPORT GENERATION
# ============================================================

def robot_5_build_report(records, persistence):
    report_id = str(uuid.uuid4())

    opportunities = []
    for item in records[:50]:
        opportunities.append({
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
            item["source"] for item in records
        }),
        "records_discovered": len(records),
        "records_included": len(opportunities),
        "database": persistence,
        "opportunities": opportunities,
        "limitations": [
            "Source records are research leads, not verified earnings.",
            "World Bank records are economic context, not grant offers.",
            "Keyword scores are not probabilities of success.",
            "Check eligibility, deadlines, and official source pages.",
            "No funding or profit outcome is guaranteed.",
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


# ============================================================
# PIPELINE CONTROL
# ============================================================

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

        duration = round(time.monotonic() - started, 2)

        set_state("last_pipeline_status", "success")
        set_state("last_pipeline_duration_seconds", duration)

        send_telegram(
            f"{APP_NAME}: pipeline completed.\n"
            f"Records discovered: {len(scored)}\n"
            f"Report ID: {report['report_id']}"
        )

        return {
            "status": "success",
            "report_id": report["report_id"],
            "records_discovered": len(scored),
            "duration_seconds": duration,
        }

    except Exception as exc:
        log.exception("Pipeline failed")
        set_state("last_pipeline_status", "failed")
        set_state("last_pipeline_error", str(exc)[:500])

        send_telegram(
            f"{APP_NAME}: pipeline failed: {str(exc)[:500]}"
        )

        return {
            "status": "error",
            "error": str(exc)[:500],
        }

    finally:
        pipeline_lock.release()


def background_loop():
    log.info(
        "Background pipeline loop started; interval=%s seconds",
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
        log.info("Background pipeline disabled by configuration")
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


# ============================================================
# REPORT HELPERS
# ============================================================

def hash_token(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def latest_report_snapshot():
    with db_connect() as db:
        row = db.execute("""
            SELECT id, report_json FROM reports
            WHERE status='ready'
            ORDER BY created_at DESC
            LIMIT 1
        """).fetchone()

    if row:
        return row["id"], parse_json(row["report_json"])

    report_id = str(uuid.uuid4())

    empty_report = {
        "report_id": report_id,
        "generated_at": utc_now(),
        "engine": APP_NAME,
        "version": APP_VERSION,
        "discovery_mode": "free_public_sources",
        "source_count": 0,
        "records_discovered": 0,
        "records_included": 0,
        "database": {},
        "opportunities": [],
        "notice": "No scan has completed yet.",
    }

    with db_connect() as db:
        db.execute("""
            INSERT INTO reports(id, created_at, status, report_json)
            VALUES (?, ?, ?, ?)
        """, (
            report_id, utc_now(), "ready", safe_json(empty_report)
        ))

    return report_id, empty_report


# ============================================================
# PAYSTACK PAYMENT HELPERS
# ============================================================

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


def mark_order_paid(reference, gateway_data):
    with db_connect() as db:
        order = db.execute(
            "SELECT * FROM orders WHERE reference=?",
            (reference,),
        ).fetchone()

        if not order:
            return False, "Unknown payment reference."

        if order["status"] == "paid":
            return True, "Payment was already verified."

        expected_amount = int(order["amount_kobo"])
        actual_amount = int(gateway_data.get("amount") or 0)
        currency = str(gateway_data.get("currency") or "").upper()
        gateway_status = str(
            gateway_data.get("status") or ""
        ).lower()
        gateway_reference = str(
            gateway_data.get("reference") or ""
        )

        if gateway_status != "success":
            return False, "Paystack transaction is not successful."

        if actual_amount != expected_amount:
            return False, "Payment amount does not match."

        if currency != "NGN":
            return False, "Unexpected payment currency."

        if gateway_reference != reference:
            return False, "Payment reference does not match."

        db.execute("""
            UPDATE orders
            SET status='paid', paid_at=?, gateway_response=?
            WHERE reference=?
        """, (utc_now(), safe_json(gateway_data), reference))

    return True, "Payment verified."


# ============================================================
# HTTP ROUTES
# ============================================================

@app.get("/")
def index():
    return jsonify({
        "name": APP_NAME,
        "version": APP_VERSION,
        "status": "online",
        "discovery_provider": "free_public_sources",
        "health": "/health",
        "endpoints": {
            "health": "GET /health",
            "manual_pipeline": "POST /api/pipeline/run",
            "latest_report_metadata": "GET /api/reports/latest",
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
        db_status = "error: " + str(exc)[:150]

    return jsonify({
        "status": "ok" if db_status == "ok" else "degraded",
        "name": APP_NAME,
        "version": APP_VERSION,
        "database": db_status,
        "discovery_provider": "Grants.gov + World Bank",
        "gdelt_enabled": False,
        "paystack_configured": bool(PAYSTACK_SECRET),
        "telegram_configured": bool(
            TELEGRAM_TOKEN and ADMIN_CHAT_ID
        ),
        "background_pipeline_disabled": DISABLE_BACKGROUND_PIPELINE,
        "last_pipeline_at": get_state("last_pipeline_at"),
        "last_pipeline_status": get_state("last_pipeline_status"),
        "last_discovered_count": get_state(
            "last_discovered_count", "0"
        ),
    }), 200 if db_status == "ok" else 503


@app.post("/api/pipeline/run")
def api_run_pipeline():
    if not ADMIN_API_SECRET:
        return jsonify({
            "error": "ADMIN_API_SECRET is not configured."
        }), 503

    supplied = request.headers.get("x-admin-secret", "")

    if not hmac.compare_digest(supplied, ADMIN_API_SECRET):
        return jsonify({"error": "Unauthorized."}), 401

    result = run_pipeline()
    code = (
        200 if result.get("status") in ("success", "already_running")
        else 500
    )
    return jsonify(result), code


@app.get("/api/reports/latest")
def api_latest_report():
    # Public endpoint returns metadata only, not the paid report.
    with db_connect() as db:
        row = db.execute("""
            SELECT id, created_at, status, report_json
            FROM reports
            ORDER BY created_at DESC
            LIMIT 1
        """).fetchone()

    if not row:
        return jsonify({
            "status": "no_report",
            "message": "No report has been generated yet.",
        }), 404

    report = parse_json(row["report_json"])

    return jsonify({
        "id": row["id"],
        "created_at": row["created_at"],
        "status": row["status"],
        "records_discovered": report.get("records_discovered", 0),
        "records_included": report.get("records_included", 0),
        "message": "Purchase access to retrieve the full report.",
    })


@app.post("/api/payments/initialize")
def initialize_payment():
    if not PAYSTACK_SECRET:
        return jsonify({
            "error": "PAYSTACK_SECRET is not configured."
        }), 503

    if not PUBLIC_BASE_URL:
        return jsonify({
            "error": "PUBLIC_BASE_URL must contain your Render URL."
        }), 503

    body = request.get_json(silent=True) or {}
    email = clean_text(body.get("email"), 254).lower()

    if not email or "@" not in email:
        return jsonify({
            "error": "Provide a valid email address."
        }), 400

    amount_kobo = REPORT_PRICE_NAIRA * 100

    if amount_kobo <= 0:
        return jsonify({"error": "Invalid report price."}), 503

    report_id, _report = latest_report_snapshot()
    reference = "GNE_" + uuid.uuid4().hex
    access_token = secrets.token_urlsafe(32)
    token_hash = hash_token(access_token)

    callback_url = (
        PUBLIC_BASE_URL + "/paystack/callback?"
        + urlencode({"token": access_token})
    )

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
                or "Paystack initialization failed."
            )

        return jsonify({
            "status": "initialized",
            "reference": reference,
            "authorization_url": (
                result["data"]["authorization_url"]
            ),
            "access_token": access_token,
            "report_id": report_id,
            "amount_naira": REPORT_PRICE_NAIRA,
        })

    except Exception as exc:
        log.exception("Paystack initialization failed")
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


@app.get("/paystack/callback")
def paystack_callback():
    reference = clean_text(request.args.get("reference"), 150)
    token = clean_text(request.args.get("token"), 200)

    if not reference or not token:
        return "Missing payment reference or access token.", 400

    try:
        gateway_data = verify_paystack_transaction(reference)
        ok, message = mark_order_paid(reference, gateway_data)

        if not ok:
            return "Payment not confirmed: " + message, 400

        with db_connect() as db:
            order = db.execute(
                "SELECT access_token_hash FROM orders WHERE reference=?",
                (reference,),
            ).fetchone()

        if not order or not hmac.compare_digest(
            order["access_token_hash"], hash_token(token)
        ):
            return "Invalid report access token.", 403

        return redirect(
            PUBLIC_BASE_URL + "/report/" + token,
            code=302,
        )

    except Exception as exc:
        log.exception("Paystack callback failed")
        return "Unable to verify payment: " + str(exc)[:300], 502


@app.post("/paystack/webhook")
def paystack_webhook():
    if not PAYSTACK_SECRET:
        return jsonify({
            "error": "PAYSTACK_SECRET is not configured."
        }), 503

    signature = request.headers.get("x-paystack-signature", "")
    raw_body = request.get_data()

    expected = hmac.new(
        PAYSTACK_SECRET.encode("utf-8"),
        raw_body,
        hashlib.sha512,
    ).hexdigest()

    if not signature or not hmac.compare_digest(signature, expected):
        return jsonify({"error": "Invalid webhook signature."}), 401

    event = request.get_json(silent=True) or {}

    if event.get("event") != "charge.success":
        return jsonify({"status": "ignored"}), 200

    data = event.get("data") or {}
    reference = clean_text(data.get("reference"), 150)

    if not reference:
        return jsonify({"error": "Missing reference."}), 400

    try:
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
            SELECT report_id, status
            FROM orders
            WHERE access_token_hash=?
            ORDER BY created_at DESC
            LIMIT 1
        """, (token_hash,)).fetchone()

        if not order:
            return jsonify({"error": "Report token not found."}), 404

        if order["status"] != "paid":
            return jsonify({
                "error": "Payment has not been verified."
            }), 403

        report_row = db.execute(
            "SELECT report_json FROM reports WHERE id=?",
            (order["report_id"],),
        ).fetchone()

    if not report_row:
        return jsonify({"error": "Report not found."}), 404

    return jsonify(parse_json(report_row["report_json"])), 200


# ============================================================
# STARTUP
# ============================================================

init_db()
start_pipeline()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
