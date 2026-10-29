# Protocol and implementation evidence

This documents the implementation derived from the user's Star Wealth Android 5.1.2 installation. It is not an official public API contract. Account identifiers, passwords, private tokens, APKs, process dumps, and decrypted preferences are excluded from this package.

APK SHA-256: `b594a7a329284180e56e434eb34d1322d43487b06a10355c6b15ffdd5bb5712a`.

## Operations

Requests use an allowlisted host/path registry, verified TLS, and JSON-RPC request IDs. Redirects, arbitrary hosts, and automatic transport retries are not enabled.

| Operation | Host | Path | Body/session mode |
| --- | --- | --- | --- |
| Identity/salt lookup | uapi.fosunhanig.com | /uc/v1/UinQuery | Temporary encrypted exchange |
| Login | uapi.fosunhanig.com | /uc/v1/Login | JSON over TLS; encrypted password ticket |
| User metadata | uapi.fosunhanig.com | /uc/v1/UserDetail | User, GET |
| Trading unlock | uapi.fosunhanig.com | /uc/v1/TradeLogin | User |
| Trading renewal | uapi.fosunhanig.com | /uc/v1/Session2Extend | Trade |
| Orders | tapi.fosunhanig.com | /trade/v5/OrderList | User |
| Assets | tapi.fosunhanig.com | /portfolio/v6/TotalAssets | User |
| Holdings | tapi.fosunhanig.com | /portfolio/v6/Holdings | User |
| Buying-power info | tapi.fosunhanig.com | /trade/v6/BidAskInfo | User |
| Create order | tapi.fosunhanig.com | /trade/v4/OrderCreate | Trade; not live tested |
| Cancel order | tapi.fosunhanig.com | /trade/v4/OrderCancel | Trade; not live tested |
| Order detail | tapi.fosunhanig.com | /trade/v4/OrderDetail | User |
| Market status | mktdata.fosunhanig.com | /market/v2/exchange/status?mkt=us | Quote, GET |
| Quote/book | mktdata.fosunhanig.com | /market/v2/group/quote | Quote |

Unmarked HTTP methods are POST. Order/account parameters include `uin`, `clientId`, `accountId`, and `subAccountId`. Account identity is checked again after login. Optional alternative account routing is not inferred.

## Password tickets and encryption

For the login identity or trading credential identity, respectively:

```text
p1 = PBKDF2-HMAC-SHA256(password UTF-8, salt UTF-8, 10000, 32 bytes)
p2 = PBKDF2-HMAC-SHA256(identity UTF-8 || p1 raw bytes, salt UTF-8, 10000, 32 bytes)
ticket plaintext = identity + " " + Base64(p1) + " " + timestamp_ms + " " + random_key
ticket = AES-256-GCM(p2, plaintext)
```

The random key is 32 ASCII hex characters and is used as 32 bytes for subsequent session encryption. The login identity is `uin`; trading uses `clientInfo.crId` and `clientInfo.salt`. TradeLogin also supplies the selected account's `cltId`, `type=10`, and `mode=0`.

AES-GCM wire representation is Base64(12-byte IV || ciphertext || 16-byte tag). Authenticated API calls use HMAC-SHA256 headers with the **user** random key even when the body is encrypted with the separate **trade** random key. Session2Extend is decrypted with the previous trade key before the newly requested key replaces it.

The HMAC input is the concatenation of method, `https` + host + path, uin, request ID, timestamp, device info, device ID, network, language, product, the empty reserved slot, session header, content type, and source. The exact construction is in `crypto.py` and tested independently from transport.

Temporary lookup exchanges use P-384 ECDH, HKDF-SHA256, and AES-GCM. The returned server nonce/key is authenticated using the pinned public ECDSA verification key and SHA-384 over clientNonce + serverNonce + serverPublicKey. Public server keys are included in `keys.py`; no client private credentials are included.

Evidence: app native crypto implementation and live interoperation. The ticket derivation was also cross-checked against the broker's [login page](https://h5.fotechwealth.com/pages/login.html) and its referenced [common JavaScript bundle](https://cdn.mfosunhani.com/fe/h5-pages/default/js/chunk-common.799011ed.js). The web and app key sets are not assumed interchangeable.

## Order and session evidence

The app's `module/trade/service/OrderType`, `TimeInForce`, `OrderValidityType`, `OrderCreateParams`, `OrderCancelParams`, `OrderCreateResult`, and `OrderCancelInfo` define the request/response fields. US native market is orderType 9; type 7 is not substituted for it. Limit is 3. Direction is 1 buy / 2 sell, marketCode is US, and currency is USD.

`module/trade_entrust/data/TradeEntrustTradeSessionType` maps regular to an absent session field, pre/post to 2, night to 4, and all sessions to 5. `TradeEntrustTradeSessionState` (decompiled class `f`) restricts night/all-session choices to limits. Its all-session eligibility additionally depends on the global feature flag and use of the US option-account route. That route has not been verified here, so flag 5 is not exposed.

App resources explicitly describe the US market-order session as 09:30–16:00 Eastern, night orders as limit-only, and night validity as same-day only. The relevant resource names are `us_market_price_order_explain_trade_time`, `night_session_market_order_only`, and `night_session_validity_today_only`.

Quote evidence is separate from execution evidence: live NIGHT responses contain `quote.nightQuote`, a separately timestamped `orderbook`, and delay flags. `quote.price` can remain the regular close. The implementation checks the book for marketable pricing and does not equate a quote response or buying-power response with order acceptance.

## Recovery and reconciliation

Known user-session errors are 100008, 100031, and 100034; known trading-session errors are 100030 and 100032. The actual competing-login test observed 100034. Password/lock-state codes 110011, 110012, 110013, 110019, and 220001 pause further automatic authentication. Unknown authentication failures receive a bounded cooldown instead of a password loop.

Financial writes have a durable local intent journal. Only explicit authentication rejection permits one rebuilt attempt. Request IDs are not assumed to provide broker idempotency. Network uncertainty leaves the intent unknown/pending for reconciliation; it does not justify another financial write.
