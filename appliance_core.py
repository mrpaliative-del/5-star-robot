
import os
import re
import json
import time
import uuid
import hmac
import hashlib
import asyncio
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
from telegram import Bot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("GlobalNairaEngine")

# ============================================================
# CONFIGURATION
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
ADMIN_CHAT_ID_RAW = os.getenv("ADMIN_CHAT_ID", "").strip()
PAYSTACK_SECRET = os.getenv("PAYSTACK_SECRET", "").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
SCAN_TARGETS_RAW = os.getenv("SCAN_TARGETS", "").strip()

SCAN_INTERVAL = max(60, int(os.getenv("SCAN_INTERVAL_SECONDS", "300")))
REQUEST_TIMEOUT = max(3, min(30, int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))))
REPORT_PRICE_NAIRA = max(100, int(os.getenv("REPORT_PRICE_NAIRA", "25000")))

DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/global-naira-engine"))
REPORT_DIR = DATA_DIR / "reports"
DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

try:
    ADMIN_CHAT_ID = int(ADMIN_CHAT_ID_RAW) if ADMIN_CHAT_ID_RAW else None
except ValueError as exc:
    raise RuntimeError("ADMIN_CHAT_ID must be a numeric Telegram chat ID.") from exc

bot = Bot(token=TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None

SCAN_TARGETS = []
for target in SCAN_TARGETS_RAW.split(","):
    target = target.strip()
    if not target:
        continue
    parsed = urlparse(target)
    if (
        parsed.scheme in ("http", "https")
        and parsed.hostname
        and not parsed.username
        and not parsed.password
    ):
        SCAN_TARGETS.append(target)
    else:
        logger.warning("Ignoring invalid scan target.")

app = Flask(__name__)
wsgi_app = app

stop_event = threading.Event()
pipeline_thread = None
startup_lock = threading.Lock()
started = False
status_lock = threading.Lock()

status = {
    "started": False,
    "thread_alive": False,
    "last_scan_at": None,
    "last_error": None,
    "scans_completed": 0,
    "reports_created": 0,
}

# ============================================================
# SHARED HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_text(value):
    return hashlib.sha256(
        value.encode("utf-8", errors="replace")
    ).hexdigest()


def save_json(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    temporary.replace(path)


def extract_emails(text):
    emails = re.findall(
        r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b",
        text,
    )
    return sorted(set(emails))[:20]


def fetch_page(target):
    if target not in SCAN_TARGETS:
        raise ValueError("URL is not in the configured target allowlist.")

    parsed = urlparse(target)
    if parsed.hostname in ("localhost", "127.0.0.1", "::1"):
        raise ValueError("Local targets are not permitted.")

    with requests.get(
        target,
        timeout=REQUEST_TIMEOUT,
        allow_redirects=False,
        stream=True,
        headers={
            "User-Agent": "GlobalNaira-AuthorizedSiteReporter/1.0"
        },
    ) as response:
        chunks = []
        size = 0
        limit = 500_000

        for chunk in response.iter_content(chunk_size=16384):
            if not chunk:
                continue
            remaining = limit - size
            if remaining <= 0:
                break
            chunks.append(chunk[:remaining])
            size += min(len(chunk), remaining)
            if size >= limit:
                break

        content = b"".join(chunks).decode(
            response.encoding or "utf-8",
            errors="replace",
        )

        if "html" in response.headers.get("Content-Type", "").lower():
            soup = BeautifulSoup(content, "html.parser")
            title = soup.title.get_text(" ", strip=True) if soup.title else ""
            for element in soup(["script", "style", "noscript"]):
                element.decompose()
            text = soup.get_text(" ", strip=True)
        else:
            title = ""
            text = content

        return {
            "target_url": target,
            "http_status": response.status_code,
            "content_type": response.headers.get("Content-Type", ""),
            "page_title": title[:300],
            "content_excerpt": text[:4000],
            "content_sha256": sha256_text(content),
            "emails_found_unverified": extract_emails(text),
            "checked_at": utc_now(),
            "note": (
                "Informational page report only. No vulnerability or fix "
                "has been independently verified."
            ),
        }


# ============================================================
# ROBOT 1: DISCOVERY
# ============================================================

async def robot_1_discovery(q12, loop_stop):
    logger.info("Robot 1 started.")

    while not loop_stop.is_set():
        for target in SCAN_TARGETS:
            if loop_stop.is_set():
                break

            try:
                result = await asyncio.to_thread(fetch_page, target)
                result["job_id"] = str(uuid.uuid4())
                await q12.put(result)
                logger.info("Discovery completed for %s", target)

            except Exception as exc:
                logger.warning("Discovery failed: %s", exc)
                with status_lock:
                    status["last_error"] = str(exc)[:500]

            await asyncio.sleep(1)

        await asyncio.sleep(SCAN_INTERVAL)


# ============================================================
# ROBOT 2: CONTACT CONTEXT
# ============================================================

async def robot_2_lead_context(q12, q23):
    logger.info("Robot 2 started.")

    while True:
        item = await q12.get()
        try:
            item["contact_context"] = {
                "emails": item.get("emails_found_unverified", []),
                "verified": False,
            }
            await q23.put(item)
        except Exception:
            logger.exception("Robot 2 failed.")
        finally:
            q12.task_done()


# ============================================================
# ROBOT 3: INTEGRITY PROOF
# ============================================================

async def robot_3_proof(q23, q34):
    logger.info("Robot 3 started.")

    while True:
        item = await q23.get()
        try:
            canonical = json.dumps(
                item, sort_keys=True, ensure_ascii=False
            )
            item["integrity"] = {
                "algorithm": "SHA-256",
                "digest": sha256_text(canonical),
                "created_at": utc_now(),
            }
            await q34.put(item)
        except Exception:
            logger.exception("Robot 3 failed.")
        finally:
            q23.task_done()


# ============================================================
# ROBOT 4: REPORT GENERATOR
# ============================================================

async def robot_4_report_builder(q34, q45):
    logger.info("Robot 4 started.")

    while True:
        item = await q34.get()
        try:
            report_id = item["job_id"]

            report = {
                "report_id": report_id,
                "created_at": utc_now(),
                "report_type": "informational_page_snapshot",
                "target_url": item["target_url"],
                "http_status": item["http_status"],
                "content_type": item["content_type"],
                "page_title": item["page_title"],
                "content_sha256": item["content_sha256"],
                "integrity": item["integrity"],
                "contact_context": item["contact_context"],
                "content_excerpt": item["content_excerpt"],
                "limitations": [
                    "This is not a security audit.",
                    "No vulnerability has been verified.",
                    "No technical fix has been tested.",
                    "Extracted email addresses are unverified.",
                ],
            }

            report_path = REPORT_DIR / f"{report_id}.json"
            save_json(report_path, report)

            item["report_id"] = report_id
            item["report_path"] = str(report_path)
            item["report_sha256"] = sha256_text(
                report_path.read_text(encoding="utf-8")
            )

            await q45.put(item)

            with status_lock:
                status["reports_created"] += 1

            logger.info("Report created: %s", report_id)

        except Exception:
            logger.exception("Robot 4 failed.")
        finally:
            q34.task_done()


# ============================================================
# ROBOT 5: DELIVERY INDEX AND TELEGRAM NOTIFICATION
# ============================================================

async def robot_5_delivery(q45):
    logger.info("Robot 5 started.")

    while True:
        item = await q45.get()
        try:
            logger.info(
                "Report ready: id=%s sha256=%s",
                item["report_id"],
                item["report_sha256"],
            )

            if bot and ADMIN_CHAT_ID:
                try:
                    await bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=(
                            "GlobalNaira report created\n"
                            f"Report ID: {item['report_id']}\n"
                            f"Target: {item['target_url']}\n"
                            f"HTTP status: {item['http_status']}\n"
                            "Type: informational report"
                        ),
                    )
                except Exception:
                    logger.exception("Telegram notification failed.")

            with status_lock:
                status["scans_completed"] += 1
                status["last_scan_at"] = utc_now()

        except Exception:
            logger.exception("Robot 5 failed.")
        finally:
            q45.task_done()


# ============================================================
# ASYNC PIPELINE LIFECYCLE
# ============================================================

async def run_pipeline(loop_stop):
    q12 = asyncio.Queue(maxsize=20)
    q23 = asyncio.Queue(maxsize=20)
    q34 = asyncio.Queue(maxsize=20)
    q45 = asyncio.Queue(maxsize=20)

    tasks = [
        asyncio.create_task(robot_1_discovery(q12, loop_stop)),
        asyncio.create_task(robot_2_lead_context(q12, q23)),
        asyncio.create_task(robot_3_proof(q23, q34)),
        asyncio.create_task(robot_4_report_builder(q34, q45)),
        asyncio.create_task(robot_5_delivery(q45)),
    ]

    try:
        while not loop_stop.is_set():
            await asyncio.sleep(1)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def pipeline_main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop_stop = asyncio.Event()

    def watch_shutdown():
        stop_event.wait()
        try:
            loop.call_soon_threadsafe(loop_stop.set)
        except RuntimeError:
            pass

    threading.Thread(
        target=watch_shutdown,
        daemon=True,
    ).start()

    with status_lock:
        status["started"] = True
        status["thread_alive"] = True

    try:
        loop.run_until_complete(run_pipeline(loop_stop))
    except Exception as exc:
        logger.exception("Pipeline terminated.")
        with status_lock:
            status["last_error"] = str(exc)[:500]
    finally:
        with status_lock:
            status["thread_alive"] = False
        loop.close()


def start_pipeline_once():
    global pipeline_thread, started

    with startup_lock:
        if started:
            return
        started = True

        if not SCAN_TARGETS:
            logger.warning(
                "SCAN_TARGETS is empty. No pages will be scanned."
            )

        pipeline_thread = threading.Thread(
            target=pipeline_main,
            name="global-naira-pipeline",
            daemon=True,
        )
        pipeline_thread.start()


# ============================================================
# FLASK / GUNICORN ENDPOINTS
# ============================================================

@app.get("/")
def index():
    return jsonify({
        "service": "GlobalNaira Engine",
        "status": "ok",
        "health": "/health",
    })


@app.get("/health")
def health():
    with status_lock:
        current = dict(status)

    return jsonify({
        "service": "GlobalNaira Engine",
        "status": "ok" if current["thread_alive"] else "starting_or_stopped",
        "pipeline": current,
        "configured_targets": len(SCAN_TARGETS),
        "payments_configured": bool(PAYSTACK_SECRET and PUBLIC_BASE_URL),
        "timestamp": utc_now(),
    }), (200 if current["thread_alive"] else 503)


@app.get("/api/reports/<report_id>")
def get_report(report_id):
    try:
        uuid.UUID(report_id)
    except ValueError:
        return jsonify({"error": "Invalid report ID"}), 400

    path = REPORT_DIR / f"{report_id}.json"
    if not path.is_file():
        return jsonify({"error": "Report not found"}), 404

    return jsonify(json.loads(path.read_text(encoding="utf-8")))


# ============================================================
# PAYSTACK INITIALIZATION AND VERIFICATION
# ============================================================

def paystack_headers():
    return {
        "Authorization": f"Bearer {PAYSTACK_SECRET}",
        "Content-Type": "application/json",
    }


@app.post("/api/payments/initialize")
def initialize_payment():
    if not PAYSTACK_SECRET or not PUBLIC_BASE_URL:
        return jsonify({
            "error": "Configure PAYSTACK_SECRET and PUBLIC_BASE_URL."
        }), 503

    body = request.get_json(silent=True) or {}
    email = str(body.get("email", "")).strip().lower()
    report_id = str(body.get("report_id", "")).strip()

    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        return jsonify({"error": "A valid customer email is required."}), 400

    try:
        uuid.UUID(report_id)
    except ValueError:
        return jsonify({"error": "A valid report_id is required."}), 400

    if not (REPORT_DIR / f"{report_id}.json").is_file():
        return jsonify({"error": "Report not found."}), 404

    reference = "gn_" + uuid.uuid4().hex
    amount_kobo = REPORT_PRICE_NAIRA * 100

    payload = {
        "email": email,
        "amount": amount_kobo,
        "currency": "NGN",
        "reference": reference,
        "callback_url": f"{PUBLIC_BASE_URL}/payment/callback",
        "metadata": {
            "report_id": report_id,
            "service": "Informational page report",
        },
    }

    try:
        response = requests.post(
            "https://api.paystack.co/transaction/initialize",
            headers=paystack_headers(),
            json=payload,
            timeout=20,
        )
        result = response.json()
    except (requests.RequestException, ValueError):
        logger.exception("Paystack initialization failed.")
        return jsonify({"error": "Payment provider unavailable."}), 502

    if (
        response.status_code >= 400
        or not result.get("status")
        or not result.get("data", {}).get("authorization_url")
    ):
        return jsonify({
            "error": "Payment initialization failed.",
            "provider_message": result.get("message", "Unknown error"),
        }), 502

    order = {
        "reference": reference,
        "email": email,
        "report_id": report_id,
        "amount_kobo": amount_kobo,
        "currency": "NGN",
        "status": "initialized",
        "created_at": utc_now(),
    }

    save_json(DATA_DIR / f"payment_{reference}.json", order)

    return jsonify({
        "status": "initialized",
        "reference": reference,
        "authorization_url": result["data"]["authorization_url"],
        "amount_naira": REPORT_PRICE_NAIRA,
        "currency": "NGN",
    }), 201


def verify_transaction(reference, order):
    response = requests.get(
        "https://api.paystack.co/transaction/verify/"
        + requests.utils.quote(reference, safe=""),
        headers=paystack_headers(),
        timeout=20,
    )
    result = response.json()

    if response.status_code >= 400 or not result.get("status"):
        raise RuntimeError("Paystack verification failed.")

    transaction = result.get("data") or {}

    if (
        transaction.get("status") != "success"
        or transaction.get("reference") != reference
        or int(transaction.get("amount", -1)) != int(order["amount_kobo"])
        or transaction.get("currency") != order["currency"]
        or str(transaction.get("customer", {}).get("email", "")).lower()
        != order["email"]
    ):
        raise ValueError("Payment does not match the expected order.")

    order["status"] = "paid_verified"
    order["verified_at"] = utc_now()
    save_json(DATA_DIR / f"payment_{reference}.json", order)
    return order


@app.get("/payment/callback")
def payment_callback():
    if not PAYSTACK_SECRET:
        return jsonify({"error": "Payments are not configured."}), 503

    reference = request.args.get("reference", "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{8,100}", reference):
        return jsonify({"error": "Invalid reference."}), 400

    order_path = DATA_DIR / f"payment_{reference}.json"
    if not order_path.is_file():
        return jsonify({"error": "Order not found."}), 404

    try:
        order = json.loads(order_path.read_text(encoding="utf-8"))
        verified = verify_transaction(reference, order)

        return jsonify({
            "status": "paid_verified",
            "reference": reference,
            "report_id": verified["report_id"],
            "message": (
                "Payment verified. This endpoint does not provide an "
                "authenticated report download."
            ),
        })
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 402
    except Exception:
        logger.exception("Payment verification error.")
        return jsonify({"error": "Unable to verify payment."}), 502


@app.post("/api/payments/webhook")
def paystack_webhook():
    if not PAYSTACK_SECRET:
        return jsonify({"error": "Payments are not configured."}), 503

    raw_body = request.get_data(cache=False)
    signature = request.headers.get("x-paystack-signature", "")
    expected = hmac.new(
        PAYSTACK_SECRET.encode("utf-8"),
        raw_body,
        hashlib.sha512,
    ).hexdigest()

    if not signature or not hmac.compare_digest(signature, expected):
        return jsonify({"error": "Invalid signature."}), 401

    try:
        event = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return jsonify({"error": "Invalid JSON."}), 400

    if event.get("event") != "charge.success":
        return jsonify({"status": "ignored"}), 200

    transaction = event.get("data") or {}
    reference = str(transaction.get("reference", ""))

    if not re.fullmatch(r"[A-Za-z0-9_-]{8,100}", reference):
        return jsonify({"error": "Invalid reference."}), 400

    order_path = DATA_DIR / f"payment_{reference}.json"
    if not order_path.is_file():
        return jsonify({"status": "unmatched_event"}), 200

    try:
        order = json.loads(order_path.read_text(encoding="utf-8"))
        verify_transaction(reference, order)
        return jsonify({"status": "accepted"}), 200
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception:
        logger.exception("Webhook verification failed.")
        return jsonify({"error": "Webhook processing failed."}), 500


# Start the background pipeline once in this Gunicorn worker.
# Keep Gunicorn workers=1 unless pipeline execution is moved to a separate service.
if os.getenv("DISABLE_BACKGROUND_PIPELINE", "").lower() not in {
    "1", "true", "yes"
}:
    start_pipeline_once()
