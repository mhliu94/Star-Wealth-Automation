# Star Wealth API client

An emulator-free Python client for the Star Wealth account configured on this workstation. Android/root/ADB is needed only for the initial profile export. Login, trading-password unlock, quotes, account reads, and order preparation run directly over HTTPS afterward.

**Live authentication passed, including recovery after you signed into the phone again. Order submission and cancellation are implemented but have not been exercised against the live broker. No financial transactions were sent during development. This is not a claim of 100% execution reliability.**

Derived from Star Wealth Android 5.1.2. The [Fangde project](https://github.com/mhliu94/Fangde-Automation) supplied the workflow reference; this is a separate protocol implementation.

## Run on this workstation

Open PowerShell in this folder. The launcher already points to the configured private profile and password material:

```powershell
.\start-local.ps1 status
.\start-local.ps1 auth-check
.\start-local.ps1 market-status
.\start-local.ps1 quote-check AAPL
.\start-local.ps1 orders
```

`auth-check` reads the account and verifies/renews trading access. It does not place an order. If necessary, it logs in again; this can displace the phone's session. Avoid running the phone and script in a continuous login competition.

The private files are in this task's `work/private/` directory, outside this package: `star.session.json`, `star.credentials.json`, and the order journal `orders.sqlite3`. The launcher uses the bundled Python runtime on this PC. Moving only this folder will require the portable setup below.

## Password workflow and recovery

The login and trading passwords you supplied locally have been configured. Raw passwords are not embedded in the package. The stored derived values are password equivalents and must remain private, along with the session file.

To enter the existing passwords again, use the console prompt, which hides input:

```powershell
.\start-local.ps1 setup
.\start-local.ps1 auth-check --force-login
```

Alternatively, load your existing labelled file locally:

```powershell
.\start-local.ps1 setup --from-file 'PATH_TO_LOCAL_CREDENTIALS_FILE'
```

The accepted labels are `account_password` and `trading_password`, separated from their values by `:` or `=`. This uses the existing trading password; it does not set or change the broker password. The console workflow does not depend on the earlier local webpage.

The client treats user login and trading unlock as separate sessions. It logs in before local expiry, or once after the broker reports a missing/invalid/kicked user session. It then refreshes account metadata and unlocks trading with its own password ticket. Trading sessions are renewed before writes and unlocked again when expired or explicitly rejected.

Wrong-password/locked-account responses pause authentication to avoid repeated attempts. Other authentication rejections impose a short cooldown. A new captcha, OTP, or additional login challenge requires the app flow and a new export; these are not bypassed. After a password/salt change, sign in on the phone, export to a new profile path, and rerun setup. The same applies if the selected account changes.

## US order sessions

The supported scope is US equities, whole shares, on the exported standard account route. These rules come from the app's order/session models and resources; live order acceptance remains unverified.

| Selected session | Broker states accepted by this client | Native market | Explicit limit | Validity exposed |
| --- | --- | --- | --- | --- |
| `regular` | `OPEN` | Yes, DAY only | Yes | DAY / GTC for limits |
| `extended` | `PRE_OPEN`, `OPEN`, `AFTER_OPEN` | No | Yes | DAY / GTC |
| `overnight` | `NIGHT`, eligible stocks only | No | Yes | DAY only |
| All sessions | Disabled: account routing not verified | No | Not exposed | Not exposed |

The app documents US market orders for the regular continuous session, 09:30–16:00 Eastern. Use a limit for pre/post-market or overnight trading. The client does not silently convert a market order or switch sessions.

`extended` covers regular plus pre/post-market; it does not include overnight. The API session selector is `timeInForce=2` for extended and `4` for overnight. DAY/GTC is a separate field, `expType=1/2`. Mixing these fields is a source of rejected requests. The broker's current market status is checked instead of calculating weekdays/holidays locally. Closed sessions are rejected locally; this client does not queue orders for the next opening.

Examples below are **dry runs**. Prices are illustrative inputs, not recommendations. Choose the session currently reported by the broker:

```powershell
# Explicit limit during pre/post-market or regular trading:
.\start-local.ps1 order AAPL buy 1 --session extended --price 100.00 --max-notional 100.00

# Explicit limit during overnight trading:
.\start-local.ps1 order AAPL buy 1 --session overnight --price 100.00 --max-notional 100.00

# Optional marketable limit: use the live ask, bounded by your maximum buy price:
.\start-local.ps1 order AAPL buy 1 --session overnight --kind marketable-limit --price-bound 400.00 --max-notional 400.00

# Native market preparation, regular session only:
.\start-local.ps1 order AAPL buy 1 --session regular --kind market --max-notional 400.00
```

A marketable limit uses the current ask for a buy or bid for a sell. `--price-bound` is a maximum buy price or minimum sell price; it blocks a plan outside that bound. The resulting order is still a limit and may remain unfilled as the book changes. Explicit limits preserve the price you choose. Supported price precision is cents at/above $1 and four decimal places below $1; prices are not silently rounded.

Submitting requires the explicit `--live` switch, a unique `--intent-id`, and `--max-notional`. These switches were not used with a live financial operation during development. For native market orders, max-notional is only an estimate based on the current book, not a guaranteed execution-cost cap. Fees are not included in the estimate.

## Are quotes fresh?

Use `quote-check SYMBOL` to see the actual delay flags and timestamp ages on your account. The AAPL overnight samples on October 28 reported both delay flags false, with bid/ask ages of 5.811, 3.543, and 1.393 seconds. This shows an updating overnight book, not a fixed 15-minute delay. It does not establish entitlements for every stock or other sessions.

The response contains separate session prices: `quote` can retain the regular close, `quote.prepost` describes pre/post-market, and `quote.nightQuote` describes overnight trades. A last trade can also be old simply because no trade occurred. **Marketable pricing never uses the regular close or last-trade price.** It uses the timestamped bid/ask book.

Before preparing a native market or marketable-limit order, the client requires:

- An explicit realtime indication and no conflicting delayed flag.
- A book timestamp no more than five seconds old, with at most two seconds of future clock tolerance.
- The expected symbol, matching active broker status, normal tradability, and overnight eligibility when relevant.
- A two-sided, non-crossed book with positive displayed volume on the selected side.

A stale book triggers at most three read-only snapshot attempts, with 0.4 seconds between attempts. It remains blocked if no fresh book arrives; an older quote is never accepted merely to make submission succeed. Explicit limit orders do not need a fresh book because their price comes from you. They still check session and tradability. Keep the computer's clock synchronized.

`quote-check` is diagnostic and reports stale data without retrying. Its exit status alone is not a freshness signal; inspect `realtime_indicated`, `book_timestamp_fresh`, and the identity/status checks. Actual order preparation enforces the checks itself. A fresh timestamp cannot prove that a broker's quote is complete across every execution venue.

## Order outcomes and duplicate prevention

Every live mutation reserves its intent in a durable SQLite journal before sending. Reusing an intent is blocked, including after a restart. Only an explicit authentication rejection permits one recovery/retry; the request and quote/session checks are rebuilt after recovery. A timeout, disconnect, malformed reply, or uncertain acknowledgement is never automatically resent.

Inspect outcomes with:

```powershell
.\start-local.ps1 journal YOUR_INTENT_ID
.\start-local.ps1 orders --date 2026-10-28
.\start-local.ps1 detail BROKER_ORDER_ID
.\start-local.ps1 cancel BROKER_ORDER_ID
```

`cancel` also defaults to preparation only; live cancellation requires `--live --intent-id ...`. An acknowledgement is not a fill or a completed cancellation. Read broker order details to determine the final state. On `OutcomeUnknown` or a journal entry stuck at `pending`, reconcile with broker orders before creating another intent. The journal cannot deduplicate an intentionally different intent ID at the broker. For overnight reconciliation, inspect the relevant calendar and trading dates; `orders` defaults to today's Eastern calendar date and returns up to 100 records. Use `--date`, `--start`, and `--count` to paginate.

## Portable installation and Python use

Python 3.11+ is required. In a virtual environment:

```text
python -m pip install .
python -m starwealth --help
```

To bootstrap a new profile from the rooted, logged-in phone:

```text
python -m starwealth --profile .state/star.session.json export --serial YOUR_ADB_SERIAL --adb PATH_TO_ADB
python -m starwealth --profile .state/star.session.json --credentials .state/star.credentials.json setup
python -m starwealth --profile .state/star.session.json --credentials .state/star.credentials.json auth-check
```

If multiple logged-in users/accounts exist, specify `--uin` and/or `--account-id`. Export refuses to overwrite a profile. You can instead transfer your existing private profile and credentials directly between your own machines, keeping them out of repositories and shared archives. All global path arguments precede the subcommand. Environment equivalents are `STARWEALTH_PROFILE`, `STARWEALTH_CREDENTIALS`, and `STARWEALTH_JOURNAL`.

The CLI serializes access to the profile. Python callers must use `profile_lock` as shown in `examples/read_account.py` and `examples/prepare_order.py`; concurrent calls on the same credentials can rotate session keys unexpectedly.

## Validation and limits

79 automated tests pass. See `VALIDATION.md`, `validation-live.json`, and `validation-quotes.json` for the live checks, measured timestamps, and remaining gaps. `PROTOCOL.md` records the implementation evidence and API mappings.

```text
python -m pip install '.[test]'
python -m pytest tests -q
```

Not implemented: options, fractional shares, HK orders, amendments, IOC/FOK, all-session account routing, strategy scheduling, or automatic completion of new MFA challenges. App/server protocol or entitlement changes may require an update. No code can promise acceptance or fills through outages, account restrictions, competing logins, or changing market conditions.
