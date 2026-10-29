# Star Wealth listener for KTrader

`start-trading.sh` consumes KTrader JSON commands using the existing Star Wealth
API client. The command contract was checked against
[KTrader](https://github.com/mhliu94/KTrader/tree/2e05281e6ef8c8916690b1bc7196c83570f529b7),
particularly `trading_ui/services/orders.py` and `trading_server/tiger.py`.

## Prepared settings

| Setting | Default |
| --- | --- |
| Kafka bootstrap servers | `45.32.121.19:9092` |
| Input topic | `trading-commands` |
| Account report topic | `account-details` |
| Order report topic | `order-status` |
| Consumer group | `star-24` live; `star-24-dry-run` without `--live` |
| KTrader string/account ID | `STAR-DONG` |
| KTrader numerical ID | `24` |
| Profile | `~/star-configs/star.session.json` |
| Credentials | `~/star-configs/star.credentials.json` |
| Financial intent journal | `~/star-configs/orders.sqlite3` |
| Live command inbox and enable switch | `~/star-configs/ktrader-live.sqlite3` |
| Synthetic FOK recovery state | `~/star-configs/ktrader-live-execution.sqlite3` |
| Report sequences and delivery outbox | `~/star-configs/ktrader-reports.sqlite3` |

The numerical ID is KTrader's ID, never the broker account number. Commands route
by exact `account_id: "STAR-DONG"`; if `account_num_id` is supplied it must be the
integer `24`. Keep the same group and state files across restarts. The profile
lock prevents concurrent listeners or direct API commands using this profile.

Credentials are loaded directly from private files, never copied into this
repository or supplied on the command line. `passwords.txt` is not needed by the
listener. The reusable derived credentials support login and session renewal.

## Install and start

Python 3.11+ with venv support is required. The environment on the prepared host
has already been installed. On a new host:

```bash
cd ~/star-api-scripts
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[kafka]'
chmod 700 ~/star-configs
chmod 600 ~/star-configs/star.session.json ~/star-configs/star.credentials.json
```

Validate local configuration without connecting to Kafka or the broker:

```bash
~/star-api-scripts/start-trading.sh --check-config
```

Start the live listener manually:

```bash
cd ~/star-api-scripts
umask 077
mkdir -p logs
nohup ./start-trading.sh --live > logs/star-trading.log 2>&1 < /dev/null &
echo $! > logs/star-trading.pid
```

It runs in the foreground unless you use `nohup`/`&`. Output is unbuffered, and
the launcher uses `exec`, so the PID is the Python process. Stop it gracefully:

```bash
kill -TERM "$(cat ~/star-api-scripts/logs/star-trading.pid)"
tail -f ~/star-api-scripts/logs/star-trading.log
```

Without `--live`, commands are prepared and logged without broker writes. The
default dry-run group is `star-24-dry-run`; consuming in dry-run mode still
advances that group's Kafka offsets. If overriding the group, keep it separate
from production. Dry runs use a separate persistent
inbox (`ktrader-dry-run.sqlite3`). A new group starts at `latest`; an existing
group resumes committed offsets. No listener is started during setup.
Dry-run mode does not publish account or order reports to production topics.

`--max-notional USD` optionally imposes an order-size cap. `--help` lists the
other overrides, including IDs, Kafka, profile, journal and inbox paths. The
`STARWEALTH_CONFIG_DIR` environment variable changes the private directory;
`STARWEALTH_PYTHON` changes the Python executable.

## Commands and limits

Every command requires a unique `command_id`, exact `account_id`, `type`, and an
ISO `ts` with a timezone. Incoming commands older than 300 seconds, or more than
30 seconds in the future, are rejected. These IDs persist across restarts and
Kafka redelivery; reusing an ID with changed contents is rejected.

| Command | Behavior |
| --- | --- |
| `MARKET_ORDER` | US whole shares; native market only when broker status is `OPEN`. Requires `symbol`, `side` (`BUY`/`SELL`), and exactly one of `qty_shares` or `notional_usd`. |
| `DELAYED_MARKET_ORDER` | Same order rules, with `execute_at` or positive `delay_seconds` from receipt. Persisted before Kafka acknowledgement. |
| `LIMIT_ORDER` | Explicit `limit_price` and whole `qty_shares`. Uses regular session at `OPEN`, extended at `PRE_OPEN`/`AFTER_OPEN`, overnight at `NIGHT`; closed sessions reject. DAY validity. |
| `LIMIT_ORDER_FOK` | Gigamoney-style synthetic FOK: submit a limit, wait two seconds and cancel any unfilled remainder. See below. |
| `CANCEL_OPEN_ORDERS` | Cancel currently eligible US orders on this account, optionally restricted by `symbol`; confirm broker terminal states. |
| `SET_TRADING_ENABLED` | Persists a JSON boolean `trading_enabled`. Initially enabled. Checked when received and again before delayed execution. |

`notional_usd` is converted to whole shares by rounding down against a validated,
fresh book. Native market cost is an estimate; fills and fees can exceed it.
For share-sized market commands without a configured cap, the fresh estimated
notional becomes the per-order cap; a price move before final preparation may
reject the command. Explicit limits use quantity times price, additionally
bounded by `--max-notional` if set. No order type is silently changed to work
outside its supported session.

Example KTrader limit command (set `ts` to the actual current UTC time):

```json
{
  "type": "LIMIT_ORDER",
  "command_id": "unique-command-id",
  "account_id": "STAR-DONG",
  "ts": "2026-10-29T00:00:00Z",
  "symbol": "AAPL",
  "side": "BUY",
  "qty_shares": 1,
  "limit_price": 100.00
}
```

Currency conversion, algorithmic commands, Smart Selling G child orders, IOC,
GTC creation and automatic deadline cancellation remain unsupported. Such
constraints are rejected before placing or bulk-canceling orders. Existing GTC
orders can be reported and canceled.

## Synthetic FOK and bulk cancellation

The behavior follows [GigaMoney Linux's implementation](https://github.com/mhliu94/GigaMoney-Automation-Linux/blob/74a37b7871cbf6ce5b180615aff1d1a9566e99c8/gigamoney/api_trading.py).
`LIMIT_ORDER_FOK`, or `LIMIT_ORDER` with `time_in_force: "FOK"` or
`cancel_unfilled: true`, selects the same workflow:

1. Persist the intent, wait five seconds, and submit an ordinary limit order.
2. Read back the exact acknowledged order and verify its symbol, side, quantity,
   limit price and order type.
3. If still active, wait two seconds and read again. Cancel the remainder only
   when the broker explicitly reports `canCancel: 1`.
4. Poll after delays of 0, 1, 1 and 2 seconds. Completion requires a broker
   terminal status. An acknowledgement alone is never treated as a fill/cancel.

This is not atomic exchange FOK: partial fills can remain after cancellation.
Filled, partially filled then canceled, rejected and expired outcomes are
reported distinctly. Cancellation/fill races report the final broker state.
Submission and cancellation progress are durable. Restarts may complete a first
cancellation proven unsent; a pending or uncertain financial write is never
automatically resent. Unresolved FOKs pause new submissions while periodic
reconciliation continues. Bulk cancellation remains available while trading is
disabled.

Bulk cancellation fully paginates active-order discovery back to 2000, verifies
the selected broker account, and rechecks each exact order before canceling.
When a symbol is provided, it never broadens the operation to other stocks. Each
cancel has a stable journal intent. An unresolved cancel is logged and reported
for reconciliation rather than treated as successful.

## Balance and order reporting

Live mode reports immediately at startup, every 30 seconds after the previous
refresh finishes, and shortly after commands (two-second refresh delay).
`--account-topic`, `--order-topic`, `--report-interval` and
`--report-lookback-days` configure this behavior. Reporting uses the same
serialized client/profile lock as trading.

`account-details` uses key `STAR-DONG` and includes account number `24`, USD cash,
native per-currency cash, US positions, available share quantities, average cost,
the persisted trading-enabled flag and observation timestamp. Cash comes from
`AssetSummary.assetsBreakdown[currency].availableBalance`, verified in the
broker's own asset UI. Converted total assets and margin purchasing power are
never substituted for cash. A separate available-cash-for-trade field is omitted
because its native mapping has not been verified.

`order-status` uses KTrader schema v1 and key `["STAR-DONG","broker-order-id"]`.
Snapshots include original creation time, observed update time, order state,
quantity, cumulative fills, average fill price and command attribution. The
reporter scans recent history plus older open orders and continues tracking old
orders until their terminal outcome is observed. Missing orders are never
assumed canceled. Status and position fields follow the broker's published
[OrderList](https://github.com/fosunwealth/fw-trading/blob/157c52fda8387c2ae658bb2010060df75d269f07/fosun_skills/fosun-trading/doc/OrderList.md)
and [Holdings](https://github.com/fosunwealth/fw-trading/blob/157c52fda8387c2ae658bb2010060df75d269f07/fosun_skills/fosun-trading/doc/Holdings.md)
definitions; incompatible native response shapes fail validation explicitly.

Order sequences and exact pending payloads persist before publication. Only a
Kafka delivery acknowledgement removes a payload from the outbox. Failed sends
retry without executing a trading command again. Each order's sequence is
preserved; current account snapshots are prioritized over a failed order-report
backlog. Account-read or individual order-validation errors do not suppress
independent reports. Logs contain delivery counts and failures without raw
account responses or credentials.

## Restart and reconciliation

The inbox records commands before committing Kafka offsets; each broker write
also uses the existing durable intent journal. Failed/rejected commands are
recorded and consumed; they are not retried automatically. Check logs for
`rejected` and `unknown`. Commands interrupted during execution are marked
`unknown` on restart and never automatically resent. Reconcile broker orders and
the journal before issuing a new intent for an uncertain order.

Delayed orders survive restarts. If a due order is over 300 seconds late (or the
configured `--max-command-age`), it is rejected to avoid trading after long
downtime. Disabling trading prevents new and due orders from executing; those
commands are consumed rather than queued until re-enabled.

For read-only authentication verification while the listener is stopped:

```bash
./start-local.sh auth-check
```

Tests use fake brokers and Kafka consumers:

```bash
.venv/bin/python -m pip install -e '.[kafka,test]'
.venv/bin/python -m pytest tests -q
```

Preparation checks on 2026-10-29: local live-mode configuration, shell syntax,
broker login and trading renewal, native cash normalization, complete empty
holdings/order-history reads, and overnight AAPL limit preparation passed.
Offline tests cover FOK recovery/partial fills/cancel races, bulk cancellation,
order/holding schemas, durable reporting, failed sends and restart behavior.
All 456 tests passed. Kafka metadata confirmed `trading-commands`,
`account-details` and `order-status` exist. Live account-snapshot publication
is awaiting explicit approval; no financial report has been published by setup.
The account currently has no populated orders/holdings, so native nonempty
responses and live financial submission/cancellation remain unexercised. No
financial writes were sent and no trading consumer group was joined. The
operator starts the listener manually.
