# Validation record

Test date: October 28, 2026, America/New_York. Source app: Star Wealth 5.1.2 (5010200), `com.fosunhani.stock`, on the user's rooted Android device. Runtime requests came directly from Python on Windows.

## Live authentication and read checks

| Case | Result | Scope |
| --- | --- | --- |
| Signed/encrypted order-list read | Passed | Direct API, user session |
| Temporary encrypted identity/salt lookup | Passed | Broker ECDSA response signature verified |
| Password login | Passed | New user session and encryption key |
| Trading-password unlock | Passed | Separate session2; observed one-hour lifetime |
| Stale user-session recovery | Passed | Deliberately invalidated local session, then actual broker re-login/read |
| Expired user-session recovery | Passed | Forced local expiry; actual login and read |
| Expired trading-session recovery | Passed | Forced local expiry; actual password unlock |
| Competing phone login | Passed | User signed into phone; broker rejected script with 100034; automatic recovery succeeded |
| Trading-session renewal/key rotation | Passed | Actual Session2Extend with trading-session body encryption |
| Assets and holdings | Passed | Read only |
| US market status / AAPL quote | Passed | Broker status NIGHT |
| Overnight buying-power/lot-size preflight | Passed | Read-only BidAskInfo, not order acceptance |
| Explicit overnight limit preparation | Passed | Dry run: orderType=3, timeInForce=4, expType=1 |
| Overnight marketable-limit preparation | Passed | Dry run using a fresh book and explicit price bound |

The actual competing-login trace, with identifiers and secrets omitted:

```text
orders -> broker error 100034
lookup -> success
login -> success
user_info -> success
orders -> success
unlock -> success
extend_trade -> success
```

The final read-only smoke check is saved in `validation-live.json`. Its verifier explicitly rejected any attempt to call create/cancel.

The portable credential bundle was separately checked after removing all cached sessions. Fresh identity lookup, password login, account metadata refresh, order-list read, trading-password unlock, and trading-session renewal passed. See `validation-portability.json`. This check ran on Windows Python; the target Linux host has not been tested. The bundle itself is private and excluded from the repository.

## Quote freshness

Three AAPL overnight samples at 21:01:03–21:01:06 Eastern had both delay flags false and bid/ask ages of 5.811, 3.543, and 1.393 seconds. The first falls outside the client's five-second threshold; the next two pass. The evidence is in `validation-quotes.json`.

The main quote last-trade field was from the regular session. A separate inspection confirmed `quote.nightQuote` held the active overnight last trade and the book carried current timestamps. The client prices marketable orders from that book, not the regular last price. A later delivered `quote-check AAPL` run correctly reported an aged book as `book_timestamp_fresh: false`, while separating overnight and regular trade timestamps.

These observations establish fresh AAPL overnight data on this account at the sampled times. They do not verify every instrument, regular-hours entitlement, pre/post-market entitlement, consolidated venue coverage, or continuously fresh snapshots. Each order preparation checks the actual response rather than assuming an entitlement remains valid.

## Automated tests

79 passed, covering authenticated encryption, ECDH/signature verification, independent user/trading keys, login recovery codes, repeated-kick bounds, expiry, challenge/incorrect-password handling, credential salt changes, renewal uncertainty, atomic state, cross-process locking, account export selection, session/order-type mappings, quantity/price validation, stale/delayed quotes, a 15-minute-old book despite false delay flags, session-specific quote timestamps, DST ambiguity, bounded quote refresh, durable duplicate prevention, uncertain writes, and the one permitted explicit-authentication retry.

Wrong-password, repeated-kick, malformed-response, and write-outcome failures were simulated in tests. Invalid passwords were not deliberately submitted to the user's live account.

The package also built successfully as a Python wheel with dependency downloads disabled. The source archive passed its ZIP integrity check and a scan for the configured account identifiers, session keys, password material, and source passwords.

## Not live-verified

No live order was created, canceled, amended, or filled. Native market, regular/pre/post-market limit, overnight limit, GTC acceptance, cancellation, and write-side recovery are derived from app code and tested offline, not demonstrated as accepted broker transactions. Live authentication tests alone cannot establish transaction success.

No broker sandbox was configured. All-session mode is disabled because its account routing is unverified. No additional login challenge was completed automatically. These gaps prevent a 100% reliability claim.
