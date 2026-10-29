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
| Consumer group | `star-24` live; `star-24-dry-run` without `--live` |
| KTrader string/account ID | `STAR-DONG` |
| KTrader numerical ID | `24` |
| Profile | `~/star-configs/star.session.json` |
| Credentials | `~/star-configs/star.credentials.json` |
| Financial intent journal | `~/star-configs/orders.sqlite3` |
| Live command inbox and enable switch | `~/star-configs/ktrader-live.sqlite3` |

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

This listener explicitly rejects `CANCEL_OPEN_ORDERS`, `LIMIT_ORDER_FOK`, currency
conversion, algorithmic commands, Smart Selling G child orders, IOC/FOK/GTC,
deadlines, and cancel-unfilled semantics. The underlying API has individual
cancellation, but its nonempty order-list/status mapping has not been verified
for safe bulk cancellation. Use `./start-local.sh cancel ORDER_ID` to prepare
individual cancellation while the listener is stopped, then the documented
`--live --intent-id` options to submit it.

Account-details and order-status publishing are not implemented. Verified
read-only account responses expose converted total assets, not native cash
balances, and cannot be used to fabricate KTrader cash/availability snapshots.
The listener therefore does not populate KTrader balance/position/trading-status
displays or Smart Selling G execution feedback. KTrader's ordinary manual order
controls work without account snapshots, provided the account is configured and
is not owned by an algorithm session. Order acknowledgements in logs are not fills.

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

Preparation checks on 2026-10-29: all 185 offline tests passed; local live-mode
configuration and shell syntax passed; Kafka metadata confirmed the broker is
reachable and `trading-commands` exists with one partition. Broker login, trading
unlock/renewal, read-only assets/holdings/order-list calls, and overnight AAPL
limit preparation passed. No financial writes were sent and no consumer group
was joined. Live submission/cancellation remains unverified; the operator starts
the listener manually.
