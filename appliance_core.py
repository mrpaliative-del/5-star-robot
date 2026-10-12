import os, re, json, time, uuid, hmac, hashlib, logging, secrets, sqlite3, threading
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timezone, date
from urllib.parse import urlencode, quote
import requests
from flask import Flask, jsonify, request, redirect, abort

APP_NAME = "GlobalNaira Engine"
APP_VERSION = "2.3.0-nigerian-eligibility"
PORT = int(os.getenv("PORT", "10000"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/globalnaira"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "globalnaira.sqlite3"
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
PAYSTACK_SECRET = os.getenv("PAYSTACK_SECRET", "")
REPORT_PRICE_NAIRA = max(100, int(os.getenv("REPORT_PRICE_NAIRA", "25000")))
SCAN_INTERVAL_SECONDS = max(300, int(os.getenv("SCAN_INTERVAL_SECONDS", "3600")))
REQUEST_TIMEOUT_SECONDS = max(5, int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15")))
MAX_ITEMS_PER_SOURCE = min(50, max(5, int(os.getenv("MAX_ITEMS_PER_SOURCE", "15"))))
DISCOVERY_QUERY = os.getenv("DISCOVERY_QUERY", "small business grants procurement contracts business opportunities")
DISABLE_BACKGROUND_PIPELINE = os.getenv("DISABLE_BACKGROUND_PIPELINE", "false").lower() in {"1", "true", "yes"}
ADMIN_API_SECRET = os.getenv("ADMIN_API_SECRET", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(APP_NAME)
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1_000_000
session = requests.Session()
session.headers.update({"User-Agent": "GlobalNairaEngine/2.0 (+opportunity discovery; contact administrator)"})
pipeline_lock = threading.Lock()

@contextmanager
def db_conn():
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA busy_timeout=30000")
    try:
        yield con
    finally:
        con.close()

def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def init_db():
    with db_conn() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript('''
        CREATE TABLE IF NOT EXISTS reports (
          id TEXT PRIMARY KEY, created_at TEXT NOT NULL, status TEXT NOT NULL,
          report_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS opportunities (
          id TEXT PRIMARY KEY, fingerprint TEXT UNIQUE NOT NULL, title TEXT NOT NULL,
          description TEXT NOT NULL, url TEXT NOT NULL, source TEXT NOT NULL,
          category TEXT NOT NULL, score REAL NOT NULL, status_label TEXT NOT NULL,
          deadline TEXT, country TEXT, eligibility TEXT NOT NULL,
          discovered_at TEXT NOT NULL, raw_json TEXT NOT NULL,
          last_seen_at TEXT, eligibility_status TEXT NOT NULL DEFAULT 'unverified',
          score_reasons TEXT NOT NULL DEFAULT '[]'
        );
        CREATE TABLE IF NOT EXISTS orders (
          reference TEXT PRIMARY KEY, email TEXT NOT NULL, amount_kobo INTEGER NOT NULL,
          status TEXT NOT NULL, report_id TEXT NOT NULL, access_token_hash TEXT NOT NULL,
          created_at TEXT NOT NULL, paid_at TEXT, gateway_response TEXT,
          FOREIGN KEY(report_id) REFERENCES reports(id)
        );
        CREATE TABLE IF NOT EXISTS engine_state (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_opportunities_score ON opportunities(score DESC);
        CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
        ''')

    # Safe additive migration for databases created by older releases.
    with db_conn() as c:
        existing = {row["name"] for row in c.execute("PRAGMA table_info(opportunities)").fetchall()}
        migrations = {
            "status_label": "TEXT NOT NULL DEFAULT 'status_unverified'",
            "deadline": "TEXT",
            "country": "TEXT",
            "eligibility": "TEXT NOT NULL DEFAULT 'Eligibility not verified'",
            "last_seen_at": "TEXT",
            "eligibility_status": "TEXT NOT NULL DEFAULT 'unverified'",
            "score_reasons": "TEXT NOT NULL DEFAULT '[]'",
        }
        for column, declaration in migrations.items():
            if column not in existing:
                c.execute(f"ALTER TABLE opportunities ADD COLUMN {column} {declaration}")
        c.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_opportunities_source_seen ON opportunities(source,last_seen_at)")


def state_set(key, value):
    with db_conn() as c:
        c.execute("INSERT INTO engine_state(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at", (key, str(value), utcnow()))

def state_get(key, default=None):
    with db_conn() as c:
        row = c.execute("SELECT value FROM engine_state WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default

def safe_text(value, limit=5000):
    if value is None: return ""
    return re.sub(r"\s+", " ", str(value)).strip()[:limit]

def valid_http_url(url):
    return isinstance(url, str) and url.startswith(("https://", "http://")) and len(url) < 3000

def fingerprint(item):
    key = (safe_text(item.get("title"), 500).lower() + "|" + safe_text(item.get("url"), 2000).lower()).encode()
    return hashlib.sha256(key).hexdigest()

def date_status(item, today=None):
    """Conservative status label: never infer current eligibility from a keyword."""
    today = today or date.today()
    raw = safe_text(item.get("deadline"), 80)
    status = safe_text(item.get("status"), 100).lower()
    if any(word in status for word in ("forecast", "anticipated")):
        return "forecasted"
    if any(word in status for word in ("closed", "archived", "cancelled", "canceled", "withdrawn")):
        return "closed"
    if raw:
        try:
            # ISO dates and ISO datetimes are supported; malformed values stay unverified.
            d = date.fromisoformat(raw[:10])
            if d < today:
                return "expired"
        except (ValueError, TypeError):
            return "status_unverified"
    if status in {"posted", "open", "active", "opportunity"}:
        return "posted_unverified_eligibility" if raw else "posted_deadline_unconfirmed"
    return "status_unverified"


def eligibility_state(item):
    """Eligibility is unverified unless an authoritative review explicitly verifies it."""
    explicit = safe_text(item.get("eligibility_status"), 40).lower()
    if explicit in {"verified_eligible", "verified_ineligible", "unverified", "review_required"}:
        return explicit
    text = safe_text(item.get("eligibility"), 1000).lower()
    if "not applicable" in text and item.get("category") == "economic_context":
        return "not_applicable"
    return "unverified"


def fingerprint(item):
    # Prefer stable source IDs when available; fallback remains deterministic.
    source_id = safe_text(item.get("source_id"), 200)
    source = safe_text(item.get("source"), 100).lower()
    if source_id:
        key = (source + "|id|" + source_id).encode()
    else:
        key = (source + "|" + safe_text(item.get("title"), 500).lower() + "|" + safe_text(item.get("url"), 2000).lower()).encode()
    return hashlib.sha256(key).hexdigest()

def discover_grants_gov():
    url = "https://api.grants.gov/v1/api/search2"
    payload = {"keyword": DISCOVERY_QUERY, "oppStatuses": "posted|forecasted", "rows": MAX_ITEMS_PER_SOURCE, "startRecordNum": 0}
    try:
        r = session.post(url, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        r.raise_for_status()
        data = r.json()
        hits = data.get("data", {}).get("oppHits", []) if isinstance(data, dict) else []
        items = []
        for h in hits[:MAX_ITEMS_PER_SOURCE]:
            title = safe_text(h.get("title") or h.get("opportunityTitle") or "Untitled Grants.gov opportunity", 500)
            opp_id = safe_text(h.get("id") or h.get("oppId") or h.get("opportunityId"), 100)
            close = safe_text(h.get("closeDate") or h.get("close_date"), 40)
            status = safe_text(h.get("oppStatus") or h.get("status") or "unverified", 80)
            agency = safe_text(h.get("agencyName") or h.get("agency") or "Not supplied", 300)
            detail_url = "https://www.grants.gov/search-results-detail/" + quote(opp_id, safe="") if opp_id else "https://www.grants.gov/search-grants"
            items.append({"title": title, "description": f"Agency: {agency}. Status reported by Grants.gov: {status}. Closing date: {close or 'not supplied'}. Verify all terms on the official record.", "url": detail_url, "source": "Grants.gov", "source_id": opp_id, "category": "grant_opportunity", "status": status, "deadline": close, "country": "United States", "eligibility": "US federal opportunity; eligibility for Nigerian applicants is unverified and may be restricted by the official notice.", "eligibility_status": "unverified", "raw": h})
        return items, {"ok": True, "count": len(items)}
    except Exception as e:
        log.warning("Grants.gov discovery failed: %s", type(e).__name__)
        return [], {"ok": False, "error": type(e).__name__}

def discover_world_bank():
    # Context only: GDP observations are not grants, contracts, or funding offers.
    url = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD"
    try:
        r = session.get(url, params={"format": "json", "per_page": MAX_ITEMS_PER_SOURCE, "date": "2022:2025"}, timeout=REQUEST_TIMEOUT_SECONDS)
        r.raise_for_status(); data = r.json()
        rows = data[1] if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list) else []
        items = []
        for row in rows[:MAX_ITEMS_PER_SOURCE]:
            country = safe_text((row.get("country") or {}).get("value") or "Unspecified country", 200)
            year = safe_text(row.get("date"), 10)
            value = row.get("value")
            if value is None: continue
            title = f"Economic context: GDP — {country} ({year})"
            items.append({"title": title, "description": f"World Bank GDP observation for {country}, year {year}, value {value}. This is economic context, not a funding opportunity.", "url": "https://data.worldbank.org/indicator/NY.GDP.MKTP.CD", "source": "World Bank", "source_id": f"{country}|{year}", "category": "economic_context", "status": "historical_indicator", "deadline": "", "country": country, "eligibility": "Not applicable — economic indicator only", "eligibility_status": "not_applicable", "raw": row})
        return items, {"ok": True, "count": len(items)}
    except Exception as e:
        log.warning("World Bank discovery failed: %s", type(e).__name__)
        return [], {"ok": False, "error": type(e).__name__}

def classify_applicant_and_geography(item):
    """Heuristic tags only; never treat text matching as verified eligibility."""
    text_value = (safe_text(item.get("title"), 500) + " " +
                  safe_text(item.get("description"), 5000) + " " +
                  safe_text(item.get("eligibility"), 1000)).lower()
    if item.get("category") == "economic_context":
        return ["not_applicable"], "context_only"

    individual_terms = ("student", "individual", "researcher", "early career", "fellowship", "scholarship")
    business_terms = ("small business", "startup", "start-up", "sme", "company", "enterprise", "for-profit", "entrepreneur")
    has_individual = any(t in text_value for t in individual_terms)
    has_business = any(t in text_value for t in business_terms)
    if has_individual and has_business:
        applicant_types = ["individual", "business"]
    elif has_individual:
        applicant_types = ["individual"]
    elif has_business:
        applicant_types = ["business"]
    else:
        applicant_types = ["unknown"]

    country = safe_text(item.get("country"), 200).lower()
    if country in {"nigeria", "nigerian"}:
        geography = "nigeria_only_or_nigeria_based"
    elif country in {"united states", "usa", "us"}:
        geography = "united_states_source_non_us_eligibility_unverified"
    elif any(t in text_value for t in ("international applicants", "all countries", "worldwide", "global applicants")):
        geography = "international_claim_unverified"
    else:
        geography = "geographic_scope_unverified"
    return applicant_types, geography


def normalize_item(item):
    title = safe_text(item.get("title"), 500)
    url = safe_text(item.get("url"), 2500)
    if not title or not valid_http_url(url):
        return None
    applicant_types, geographic_scope = classify_applicant_and_geography(item)
    return {
        "title": title,
        "description": safe_text(item.get("description"), 5000),
        "url": url,
        "source": safe_text(item.get("source") or "Unknown", 100),
        "source_id": safe_text(item.get("source_id") or item.get("id") or item.get("oppId") or item.get("opportunityId"), 200),
        "category": safe_text(item.get("category") or "unclassified", 100),
        "status": safe_text(item.get("status") or "unverified", 100),
        "deadline": safe_text(item.get("deadline"), 80),
        "country": safe_text(item.get("country") or "Not identified", 200),
        "eligibility": safe_text(item.get("eligibility") or "Eligibility not verified", 1000),
        "eligibility_status": eligibility_state(item),
        "applicant_types": applicant_types,
        "geographic_scope": geographic_scope,
        "funding_quality_status": "context_only" if item.get("category") == "economic_context" else "requires_official_verification",
        "raw": item.get("raw", {}) if isinstance(item.get("raw", {}), (dict, list)) else {},
    }


def score_item(item):
    """Transparent heuristic relevance score, not an award probability."""
    text_value = (item["title"] + " " + item["description"]).lower()
    terms = {"grant": 1.0, "funding": 1.0, "small business": 1.2, "procurement": 1.0,
             "tender": 1.0, "contract": 0.8, "award": 0.5, "application": 0.2}
    matched = [term for term in terms if term in text_value]
    reasons = ["Matched terms: " + ", ".join(matched)] if matched else ["No funding-related terms matched"]
    status = date_status(item)
    eligibility = eligibility_state(item)
    score = sum(terms[term] for term in matched)
    if item["category"] == "economic_context":
        score = 0.0
        reasons = ["Economic context only; not a funding offer"]
    else:
        if status in {"expired", "closed"}:
            score *= 0.05; reasons.append("Reduced sharply: opportunity appears expired or closed")
        elif status == "forecasted":
            score *= 0.35; reasons.append("Reduced: forecast, not necessarily accepting applications")
        elif status in {"status_unverified", "posted_deadline_unconfirmed"}:
            score *= 0.55; reasons.append("Reduced: status or deadline needs confirmation")
        elif status == "posted_unverified_eligibility":
            score *= 0.7; reasons.append("Reduced: posted status does not establish applicant eligibility")
        if eligibility in {"unverified", "review_required"}:
            score *= 0.75; reasons.append("Reduced: applicant eligibility is unverified")
        elif eligibility == "verified_ineligible":
            score = 0.0; reasons.append("Not ranked: eligibility marked verified ineligible")
        if item["source"] == "Grants.gov":
            score += 0.2
            reasons.append("Official source record; eligibility still requires review")
        if item.get("deadline") and status not in {"status_unverified"}:
            reasons.append("A deadline value is published; verify it on the official record")
    return round(min(max(score, 0.0), 10.0), 3), matched, status, reasons, eligibility

def send_telegram(message):
    if not TELEGRAM_TOKEN or not ADMIN_CHAT_ID: return
    try:
        session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json={"chat_id": ADMIN_CHAT_ID, "text": message[:3500]}, timeout=REQUEST_TIMEOUT_SECONDS).raise_for_status()
    except Exception as e: log.warning("Telegram notification failed: %s", type(e).__name__)

def run_pipeline():
    if not pipeline_lock.acquire(blocking=False):
        return {"status": "already_running"}
    started = time.time()
    scan_at = utcnow()
    try:
        grants, grants_meta = discover_grants_gov()
        wb, wb_meta = discover_world_bank()
        source_results = {"Grants.gov": (grants, grants_meta), "World Bank": (wb, wb_meta)}
        successful_sources = {name for name, (_, meta) in source_results.items() if meta.get("ok") is True}
        failed_sources = {name: meta for name, (_, meta) in source_results.items() if meta.get("ok") is not True}
        if not successful_sources:
            elapsed = round(time.time() - started, 2)
            state_set("last_pipeline_status", "error")
            state_set("last_pipeline_at", scan_at)
            state_set("last_pipeline_count", 0)
            state_set("last_pipeline_seconds", elapsed)
            state_set("last_pipeline_error", "all_sources_failed")
            return {"status": "error", "sources": {k: v[1] for k, v in source_results.items()}, "seconds": elapsed}

        raw_items = grants + wb
        normalized = [x for x in (normalize_item(i) for i in raw_items) if x]
        ranked = []
        seen_by_source = {name: set() for name in successful_sources}
        for item in normalized:
            score, matched, status, reasons, eligibility = score_item(item)
            item.update({"score": score, "matched_terms": matched, "status_label": status,
                         "score_reasons": reasons, "eligibility_status": eligibility,
                         "fingerprint": fingerprint(item), "last_seen_at": scan_at})
            if item["source"] in seen_by_source:
                seen_by_source[item["source"]].add(item["fingerprint"])
            ranked.append(item)

        with db_conn() as c:
            for item in ranked:
                c.execute("""INSERT INTO opportunities(
                    id,fingerprint,title,description,url,source,category,score,status_label,deadline,country,
                    eligibility,discovered_at,raw_json,last_seen_at,eligibility_status,score_reasons
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(fingerprint) DO UPDATE SET
                    title=excluded.title,description=excluded.description,url=excluded.url,source=excluded.source,
                    category=excluded.category,score=excluded.score,status_label=excluded.status_label,
                    deadline=excluded.deadline,country=excluded.country,eligibility=excluded.eligibility,
                    discovered_at=excluded.discovered_at,raw_json=excluded.raw_json,
                    last_seen_at=excluded.last_seen_at,eligibility_status=excluded.eligibility_status,
                    score_reasons=excluded.score_reasons""",
                    (str(uuid.uuid4()), item["fingerprint"], item["title"], item["description"], item["url"],
                     item["source"], item["category"], item["score"], item["status_label"], item["deadline"],
                     item["country"], item["eligibility"], scan_at, json.dumps(item.get("raw", {}), ensure_ascii=False),
                     scan_at, item["eligibility_status"], json.dumps(item["score_reasons"], ensure_ascii=False)))

            # Only mark records stale for a source that completed successfully.
            for source in successful_sources:
                seen = seen_by_source.get(source, set())
                existing = c.execute("SELECT fingerprint FROM opportunities WHERE source=?", (source,)).fetchall()
                for row in existing:
                    if row["fingerprint"] not in seen:
                        c.execute("UPDATE opportunities SET status_label='stale' WHERE fingerprint=? AND status_label NOT IN ('expired','closed')", (row["fingerprint"],))
            c.execute("DELETE FROM opportunities WHERE category='grant_opportunity' AND status_label IN ('expired','closed') AND COALESCE(last_seen_at,discovered_at) < datetime('now','-180 days')")
            total_grants = c.execute("SELECT COUNT(*) FROM opportunities WHERE category='grant_opportunity'").fetchone()[0]
            total_context = c.execute("SELECT COUNT(*) FROM opportunities WHERE category='economic_context'").fetchone()[0]
            rows = c.execute("SELECT * FROM opportunities ORDER BY CASE WHEN category='grant_opportunity' THEN 0 ELSE 1 END, score DESC, COALESCE(last_seen_at,discovered_at) DESC LIMIT 100").fetchall()
            all_items = [dict(r) for r in rows]
            grants_only = [x for x in all_items if x["category"] == "grant_opportunity"]
            context_only = [x for x in all_items if x["category"] == "economic_context"]
            report_id = str(uuid.uuid4())
            pipeline_status = "success" if not failed_sources else "partial_success"
            report = {
                "report_id": report_id, "generated_at": scan_at, "engine": APP_NAME, "version": APP_VERSION,
                "pipeline_status": pipeline_status,
                "summary": {
                    "raw_records_received_this_run": len(raw_items),
                    "records_normalized_this_run": len(normalized),
                    "records_discovered_this_run": len(normalized),
                    "grant_opportunities_in_database": total_grants,
                    "economic_context_records_in_database": total_context,
                    "grant_records_included_in_report": len(grants_only),
                    "context_records_included_in_report": len(context_only),
                    "posted_status_records_in_report": sum(x["status_label"] in {"posted_unverified_eligibility", "posted_deadline_unconfirmed"} for x in grants_only),
                    "forecasted_records_in_report": sum(x["status_label"] == "forecasted" for x in grants_only),
                    "stale_records_in_report": sum(x["status_label"] == "stale" for x in grants_only),
                    "expired_or_closed_records_in_report": sum(x["status_label"] in {"expired", "closed"} for x in grants_only),
                    "sources": {name: meta for name, (_, meta) in source_results.items()},
                    "failed_sources": failed_sources,
                },
                "grant_opportunities": [{k:x.get(k) for k in ["title","description","url","source","category","score","status_label","deadline","country","eligibility","eligibility_status","last_seen_at"]} | {"score_reasons": json.loads(x.get("score_reasons") or "[]"), "applicant_types": classify_applicant_and_geography(x)[0], "geographic_scope": classify_applicant_and_geography(x)[1], "funding_quality_status": "requires_official_verification"} for x in grants_only[:50]],
                "economic_context": [{k:x.get(k) for k in ["title","description","url","source","category","score","status_label","country"]} for x in context_only[:20]],
                "scoring_note": "Scores are heuristic relevance rankings, not probabilities of approval, eligibility, or payment.",
                "limitations": [
                    "A posted status does not establish that applications remain open or that you qualify.",
                    "Eligibility is unverified unless the official notice has been reviewed and explicitly confirms it.",
                    "Grants.gov is a US federal source; non-US applicant eligibility may be restricted.",
                    "Applicant-type and geographic tags are heuristic triage labels, not verified eligibility decisions.",
                    "This version prioritizes both individuals and businesses, and Nigeria plus international opportunities; the current live discovery connectors still determine which records are actually found.",
                    "No opportunity is marked eligible for a Nigerian applicant unless a future authoritative review explicitly verifies it.",
                    "World Bank economic indicators are context only, not grants or funding offers.",
                    "A record missing from a successful source refresh is marked stale; a failed source is not used to stale its records.",
                    "Always verify the official notice, deadline, eligibility, terms, and authenticity before applying."
                ]
            }
            c.execute("INSERT INTO reports(id,created_at,status,report_json) VALUES(?,?,?,?)", (report_id, scan_at, "ready", json.dumps(report, ensure_ascii=False)))

        elapsed = round(time.time() - started, 2)
        state_set("last_pipeline_status", pipeline_status)
        state_set("last_pipeline_at", scan_at)
        state_set("last_pipeline_count", len(normalized))
        state_set("last_pipeline_seconds", elapsed)
        state_set("last_pipeline_sources_failed", json.dumps(failed_sources))
        send_telegram(f"{APP_NAME}: scan {pipeline_status}. New normalized records: {len(normalized)}; grants in database: {total_grants}; context records: {total_context}; failed sources: {len(failed_sources)}.")
        return {"status": pipeline_status, "report_id": report_id, "discovered": len(normalized), "raw_records": len(raw_items), "grants_in_database": total_grants, "economic_context_in_database": total_context, "failed_sources": failed_sources, "seconds": elapsed}
    except Exception as e:
        log.exception("Pipeline failed")
        state_set("last_pipeline_status", "error")
        state_set("last_pipeline_at", scan_at)
        state_set("last_pipeline_error", type(e).__name__)
        return {"status": "error", "error": type(e).__name__}
    finally:
        pipeline_lock.release()

def background_loop():
    while True:
        try: run_pipeline()
        except Exception: log.exception("Background pipeline loop failure")
        time.sleep(SCAN_INTERVAL_SECONDS)

def start_pipeline():
    if DISABLE_BACKGROUND_PIPELINE: log.info("Background pipeline disabled by configuration"); return
    t = threading.Thread(target=background_loop, name="globalnaira-pipeline", daemon=True)
    t.start()

def hash_token(token): return hashlib.sha256(token.encode()).hexdigest()
def valid_email(email):
    email = safe_text(email, 254)
    return bool(re.fullmatch(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+", email)) and ".." not in email

def verify_paystack_transaction(reference):
    if not PAYSTACK_SECRET: raise RuntimeError("PAYSTACK_SECRET is not configured")
    r = session.get("https://api.paystack.co/transaction/verify/" + reference, headers={"Authorization": "Bearer " + PAYSTACK_SECRET}, timeout=REQUEST_TIMEOUT_SECONDS)
    r.raise_for_status(); payload = r.json()
    if not payload.get("status") or not isinstance(payload.get("data"), dict): raise ValueError("Paystack verification did not succeed")
    return payload["data"]

def mark_order_paid(reference, gateway_data):
    # Validate the server-verified gateway payload before accepting any delivery.
    if not isinstance(gateway_data, dict):
        return False, "invalid_gateway_payload"
    if gateway_data.get("status") != "success":
        return False, "transaction_not_successful"
    try:
        amount = int(gateway_data.get("amount", -1))
    except (ValueError, TypeError):
        amount = -1
    if str(gateway_data.get("currency", "")).upper() != "NGN":
        return False, "currency_mismatch"
    if str(gateway_data.get("reference", "")) != reference:
        return False, "reference_mismatch"

    # BEGIN IMMEDIATE serializes writers and makes duplicate deliveries idempotent.
    with db_conn() as c:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("SELECT * FROM orders WHERE reference=?", (reference,)).fetchone()
        if not row:
            c.rollback()
            return False, "unknown_order"
        if amount != int(row["amount_kobo"]):
            c.rollback()
            return False, "amount_mismatch"
        if row["status"] == "paid":
            c.commit()
            return True, "already_paid"
        if row["status"] != "pending":
            c.rollback()
            return False, "order_not_payable"
        c.execute(
            "UPDATE orders SET status='paid',paid_at=?,gateway_response=? WHERE reference=? AND status='pending'",
            (utcnow(), json.dumps(gateway_data, ensure_ascii=False), reference),
        )
        updated = c.execute("SELECT status FROM orders WHERE reference=?", (reference,)).fetchone()
        c.commit()
        paid = bool(updated and updated["status"] == "paid")
        return paid, "paid" if paid else "state_conflict"

def latest_report():
    with db_conn() as c: row = c.execute("SELECT id,report_json FROM reports WHERE status='ready' ORDER BY created_at DESC LIMIT 1").fetchone()
    if row: return row["id"], json.loads(row["report_json"])
    result = run_pipeline()
    with db_conn() as c: row = c.execute("SELECT id,report_json FROM reports WHERE status='ready' ORDER BY created_at DESC LIMIT 1").fetchone()
    if row: return row["id"], json.loads(row["report_json"])
    return None, None

@app.get("/")
def index():
    return jsonify({"name":APP_NAME,"version":APP_VERSION,"status":"ok","endpoints":{"health":"/health","latest_report_metadata":"/api/reports/latest","public_preview":"/api/reports/preview","initialize_payment":"POST /api/payments/initialize","callback":"/paystack/callback","webhook":"POST /paystack/webhook","paid_report":"/report/<access_token>"}})

@app.get("/health")
def health():
    try:
        with db_conn() as c: c.execute("SELECT 1").fetchone()
        db_status = "ok"
    except Exception: db_status = "error"
    return jsonify({"status":"ok" if db_status == "ok" else "degraded", "database":db_status,"version":APP_VERSION,"discovery_sources":["Grants.gov (US federal opportunities)","World Bank (economic context only; not funding)"],"paystack_configured":bool(PAYSTACK_SECRET),"telegram_configured":bool(TELEGRAM_TOKEN and ADMIN_CHAT_ID),"last_pipeline_status":state_get("last_pipeline_status","not_run"),"last_pipeline_at":state_get("last_pipeline_at"),"last_pipeline_count":state_get("last_pipeline_count"),"last_pipeline_seconds":state_get("last_pipeline_seconds"),"last_pipeline_sources_failed":json.loads(state_get("last_pipeline_sources_failed","{}"))}, 200 if db_status == "ok" else 503)

@app.post("/api/pipeline/run")
def pipeline_run_route():
    if not ADMIN_API_SECRET: return jsonify({"error":"admin_endpoint_not_configured"}), 503
    supplied = request.headers.get("x-admin-secret", "")
    if not hmac.compare_digest(supplied, ADMIN_API_SECRET): return jsonify({"error":"unauthorized"}), 401
    return jsonify(run_pipeline())

@app.get("/api/reports/latest")
def report_metadata():
    with db_conn() as c: row = c.execute("SELECT id,created_at,status,report_json FROM reports WHERE status='ready' ORDER BY created_at DESC LIMIT 1").fetchone()
    if not row: return jsonify({"status":"not_ready","message":"No report has been generated yet."}), 503
    report = json.loads(row["report_json"])
    return jsonify({"report_id":row["id"],"created_at":row["created_at"],"status":row["status"],"version":APP_VERSION,"summary":report.get("summary",{}),"price_naira":REPORT_PRICE_NAIRA,"payment_required_for_full_report":True})

@app.get("/api/reports/preview")
def report_preview():
    """Return a limited public preview without exposing the full paid report."""
    report_id, report = latest_report()
    if not report_id or not report:
        return jsonify({
            "status": "not_ready",
            "message": "No report is available yet."
        }), 503

    grants = report.get("grant_opportunities", [])
    preview = []
    for item in grants[:3]:
        preview.append({
            "title": item.get("title"),
            "source": item.get("source"),
            "url": item.get("url"),
            "status": item.get("status_label"),
            "deadline": item.get("deadline"),
            "country": item.get("country"),
            "eligibility": item.get("eligibility"),
            "eligibility_status": item.get("eligibility_status", "unverified"),
            "applicant_types": item.get("applicant_types", ["unknown"]),
            "geographic_scope": item.get("geographic_scope", "geographic_scope_unverified"),
            "funding_quality_status": item.get("funding_quality_status", "requires_official_verification")
        })

    return jsonify({
        "name": APP_NAME,
        "version": APP_VERSION,
        "status": "preview",
        "report_id": report_id,
        "price_naira": REPORT_PRICE_NAIRA,
        "preview_limit": 3,
        "grant_opportunities": preview,
        "notice": (
            "Preview only. Verify the official notice, deadline and applicant "
            "eligibility before applying. Applicant-type and geographic tags are "
            "heuristic triage only. Grants.gov is a US federal source; Nigerian "
            "applicant eligibility is not guaranteed."
        )
    }), 200


@app.post("/api/payments/initialize")
def initialize_payment():
    if not PAYSTACK_SECRET: return jsonify({"error":"payments_not_configured"}), 503
    if not PUBLIC_BASE_URL or not PUBLIC_BASE_URL.startswith("https://"): return jsonify({"error":"PUBLIC_BASE_URL_must_be_https"}), 503
    body = request.get_json(silent=True) or {}
    email = safe_text(body.get("email"), 254)
    if not valid_email(email): return jsonify({"error":"valid_email_required"}), 400
    report_id, report = latest_report()
    if not report_id: return jsonify({"error":"report_not_available"}), 503
    reference = "GNE_" + uuid.uuid4().hex
    token = secrets.token_urlsafe(32)
    amount_kobo = REPORT_PRICE_NAIRA * 100
    with db_conn() as c:
        c.execute("INSERT INTO orders(reference,email,amount_kobo,status,report_id,access_token_hash,created_at) VALUES(?,?,?,?,?,?,?)", (reference,email,amount_kobo,"pending",report_id,hash_token(token),utcnow()))
    callback = PUBLIC_BASE_URL + "/paystack/callback?" + urlencode({"token":token})
    payload = {"email":email,"amount":amount_kobo,"reference":reference,"callback_url":callback,"metadata":{"report_id":report_id,"engine":APP_NAME}}
    try:
        r = session.post("https://api.paystack.co/transaction/initialize", headers={"Authorization":"Bearer "+PAYSTACK_SECRET,"Content-Type":"application/json"}, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        r.raise_for_status(); response = r.json()
        if not response.get("status") or not response.get("data",{}).get("authorization_url"): raise ValueError("Paystack initialization response invalid")
        with db_conn() as c: c.execute("UPDATE orders SET gateway_response=? WHERE reference=?", (json.dumps({"initialize_status":"success"}),reference))
        return jsonify({"status":"initialized","reference":reference,"authorization_url":response["data"]["authorization_url"],"access_token":token,"report_id":report_id,"amount_naira":REPORT_PRICE_NAIRA})
    except Exception as e:
        log.warning("Payment initialization failed: %s", type(e).__name__)
        with db_conn() as c: c.execute("UPDATE orders SET status='initialization_failed' WHERE reference=? AND status='pending'", (reference,))
        return jsonify({"error":"payment_initialization_failed","reference":reference}), 502

def order_token_valid(reference, token):
    if not token: return False
    with db_conn() as c: row = c.execute("SELECT access_token_hash FROM orders WHERE reference=?", (reference,)).fetchone()
    return bool(row and hmac.compare_digest(row["access_token_hash"], hash_token(token)))

@app.get("/paystack/callback")
def paystack_callback():
    reference = safe_text(request.args.get("reference"), 100)
    token = request.args.get("token", "")
    if not reference or not order_token_valid(reference, token): return jsonify({"error":"invalid_order_access"}), 403
    try:
        data = verify_paystack_transaction(reference)
        ok, reason = mark_order_paid(reference, data)
        if not ok: return jsonify({"error":"payment_not_confirmed","reason":reason}), 402
        return redirect(PUBLIC_BASE_URL + "/report/" + token, code=303)
    except Exception as e:
        log.warning("Callback verification failed: %s", type(e).__name__)
        return jsonify({"error":"payment_verification_failed","message":"Payment could not be verified yet. Retry this callback or contact support with your reference."}), 502

@app.post("/paystack/webhook")
def paystack_webhook():
    if not PAYSTACK_SECRET: return jsonify({"error":"not_configured"}), 503
    signature = request.headers.get("x-paystack-signature", "")
    raw = request.get_data(cache=False)
    expected = hmac.new(PAYSTACK_SECRET.encode(), raw, hashlib.sha512).hexdigest()
    if not signature or not hmac.compare_digest(signature, expected): return jsonify({"error":"invalid_signature"}), 401
    try: event = json.loads(raw.decode("utf-8"))
    except Exception: return jsonify({"error":"invalid_json"}), 400
    if event.get("event") != "charge.success": return jsonify({"status":"ignored"}), 200
    reference = safe_text((event.get("data") or {}).get("reference"), 100)
    if not reference: return jsonify({"error":"missing_reference"}), 400
    try:
        verified = verify_paystack_transaction(reference)
        ok, reason = mark_order_paid(reference, verified)
        if not ok: return jsonify({"status":"not_marked_paid","reason":reason}), 400
        return jsonify({"status":"ok","result":reason}), 200
    except Exception as e:
        log.warning("Webhook processing failed: %s", type(e).__name__)
        return jsonify({"error":"webhook_processing_failed"}), 502

@app.get("/report/<token>")
def paid_report(token):
    if not token or len(token) > 200: return jsonify({"error":"invalid_access_token"}), 403
    digest = hash_token(token)
    with db_conn() as c:
        row = c.execute("SELECT o.status,o.report_id,r.report_json FROM orders o JOIN reports r ON r.id=o.report_id WHERE o.access_token_hash=?", (digest,)).fetchone()
    if not row: return jsonify({"error":"invalid_access_token"}), 403
    if row["status"] != "paid": return jsonify({"error":"payment_required","status":row["status"]}), 402
    return jsonify(json.loads(row["report_json"]))

init_db()
start_pipeline()
wsgi_app = app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
