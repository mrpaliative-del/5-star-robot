# GlobalNaira Engine — strengthened deployment package

Version: `2.1.0-migration-payment-hardening`

## Included
- Flask app served by Gunicorn on port 10000.
- Grants.gov opportunity discovery and World Bank GDP context kept in separate report sections.
- Heuristic relevance scoring with visible status and eligibility cautions.
- SQLite persistence and deduplication.
- Paystack initialization, server-side transaction verification, signed webhook validation, atomic/idempotent paid-state updates, token-gated report delivery, and amount/currency/reference checks.
- Health and admin pipeline endpoints; optional Telegram notifications.

## Deploy
Replace the corresponding files in the existing repository with these package files, commit, and deploy through Render. This package has not been deployed to your Render service by this archive creation.

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
- `ADMIN_API_SECRET`: long, unique random secret used in `x-admin-secret` header for `POST /api/pipeline/run`.
- `TELEGRAM_TOKEN`: optional Telegram bot token.
- `ADMIN_CHAT_ID`: optional destination chat ID.
- `LOG_LEVEL`: `INFO` by default.

## Important limitations
- Current funding discovery uses Grants.gov, a US federal source. Eligibility for Nigerian applicants is not assumed.
- World Bank GDP records are economic context, not grants or procurement opportunities.
- Ranking scores are heuristics, not probabilities of award. Official notices must be checked before applying.
- Payment webhook/callback behavior must be tested in Paystack test mode before live mode.
- The report endpoint returns JSON. A production user-facing report page, support workflow, rate limiting, and full automated test suite are not included in this package.
- This archive does not update GitHub or Render automatically.
