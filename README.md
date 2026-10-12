# GlobalNaira Engine — funding-quality update

Version: `2.2.0-funding-quality`

## Included
- Flask app served by Gunicorn on port 10000.
- Grants.gov opportunity discovery and World Bank GDP context in separate report sections.
- Conservative status labels: posted but eligibility unverified, forecasted, expired, closed, stale, or unverified.
- Separate applicant eligibility status; keywords never establish eligibility.
- Transparent heuristic scoring reasons; scores are not probabilities of award or payment.
- Additive SQLite migrations for last-seen timestamps, eligibility status, and score explanations. Existing database records and order/payment tables are preserved.
- Source-aware stale handling: records are marked stale only when their own source refresh succeeds and they are absent from that successful response.
- Pipeline reports `success`, `partial_success`, or `error`; a failed source is not treated as an empty successful scan.
- Separate full database totals from the records included in the report sample.
- Paystack initialization, server-side transaction verification, signed webhook validation, atomic/idempotent paid-state updates, token-gated report delivery, and amount/currency/reference checks.
- Public limited preview endpoint (`GET /api/reports/preview`) showing up to three grant records without returning the full paid report.
- Health and admin pipeline endpoints; optional Telegram notifications.

## Deploy
Replace the corresponding files in the existing repository with these package files, commit, and deploy through Render. This archive does not update GitHub or Render automatically. Validate first with Paystack test mode and the deployment health endpoint.

## Environment variables
- `PUBLIC_BASE_URL`: `https://five-star-robot.onrender.com` (must be HTTPS for checkout callbacks).
- `PAYSTACK_SECRET`: secret key from Paystack dashboard. Never share it.
- `REPORT_PRICE_NAIRA`: `25000` to keep the existing price.
- `DATA_DIR`: `/var/data` only if a Render persistent disk is mounted at `/var/data`; otherwise `/tmp/globalnaira` is ephemeral and data can be lost on restart/redeploy.
- `SCAN_INTERVAL_SECONDS`: `3600` for hourly scanning; minimum 300.
- `REQUEST_TIMEOUT_SECONDS`: `15` by default.
- `MAX_ITEMS_PER_SOURCE`: `15` by default; clamped to 5–50.
- `DISCOVERY_QUERY`: search keywords for Grants.gov.
- `DISABLE_BACKGROUND_PIPELINE`: `false` or unset to enable background scanning; `true` to disable it.
- `ADMIN_API_SECRET`: long, unique random secret used in the `x-admin-secret` header for `POST /api/pipeline/run`.
- `TELEGRAM_TOKEN`: optional Telegram bot token.
- `ADMIN_CHAT_ID`: optional destination chat ID.
- `LOG_LEVEL`: `INFO` by default.

No new environment variables are required for this update. Keep existing values unless your Render configuration differs from the instructions above.

## Preview endpoint validation
After deploying, open `https://five-star-robot.onrender.com/api/reports/preview`. It should return HTTP 200 and a JSON preview containing at most three grant records when a report is ready. The endpoint does not prove that opportunities are open or that Nigerian applicants qualify; verify each official notice. If no report is ready, it returns HTTP 503. This package has not been deployed to GitHub or Render automatically.

## Important limitations
- Funding discovery currently uses Grants.gov, a US federal source. Eligibility for Nigerian applicants is not assumed or guaranteed.
- World Bank GDP records are economic context, not grants or procurement opportunities.
- Ranking scores are heuristic relevance rankings, not probabilities of award. Official notices must be checked before applying.
- “Posted” means the source reported a posted status; it does not establish that a deadline is still valid or that an applicant qualifies.
- Source refresh can only mark records stale based on the items returned by that source's successful query; pagination and source query limits can mean records are not returned in a given scan.
- Payment callback/webhook behavior must be tested in Paystack test mode before live mode.
- The report endpoint returns JSON. A user-facing report page, support workflow, rate limiting, and full automated test suite are not included in this package.


## v2.3.0 Funding Quality and Nigerian Eligibility Triage

This release keeps the existing API paths, Paystack flow, paid report access, and preview route. It adds transparent applicant-type and geographic triage labels to opportunity records and the public preview. The engine is intended for both individuals and businesses, and for Nigeria plus international opportunities.

**Important limitation:** these tags are heuristic indicators based on record text and country metadata. They do not verify eligibility. A US-source record remains explicitly unverified for Nigerian applicants unless an authoritative notice has been reviewed. World Bank GDP records remain economic context only and are never funding offers.

### Existing environment variables
No new environment variables are required. Keep existing Render values unchanged. In particular, retain `PAYSTACK_SECRET`, `PUBLIC_BASE_URL`, `REPORT_PRICE_NAIRA`, `ADMIN_API_SECRET` (if pipeline administration is used), and configure `DATA_DIR` to a persistent mounted disk path if SQLite data must survive redeploys/restarts. Never put secret values in source control.

### Smoke tests after deploy
- `GET /` — confirms version and existing endpoint list.
- `GET /health` — checks application/database status.
- `GET /api/reports/latest` — metadata only.
- `GET /api/reports/preview` — up to three preview records including triage labels.
- Test payment initialization only with a controlled test plan; do not make a real charge merely to test deployment.

Current connectors are still Grants.gov and World Bank economic context. This release does **not** claim that new Nigerian grant feeds have been integrated; a later source-integration stage should add and test authoritative Nigerian and international funding feeds without inventing listings.
