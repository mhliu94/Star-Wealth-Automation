import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path
import re
import sys
from .client import Client
from .errors import StarWealthError
from .export import export_phone
from .journal import Journal
from .orders import Order, SESSION_FLAGS
from .setup import console_setup, prepare_credentials, serve_setup
from .storage import profile_lock, load_private, save_private


def parser():
    p = argparse.ArgumentParser(description="Star Wealth standalone API (US whole shares)")
    p.add_argument("--profile", default=os.environ.get("STARWEALTH_PROFILE", ".state/star.session.json"))
    p.add_argument("--credentials", default=os.environ.get("STARWEALTH_CREDENTIALS", ".state/star.credentials.json"))
    p.add_argument("--journal", default=os.environ.get("STARWEALTH_JOURNAL", ".state/orders.sqlite3"))
    commands = p.add_subparsers(dest="command", required=True)
    ex = commands.add_parser("export", help="One-time export from rooted Android")
    ex.add_argument("--adb", default="adb")
    ex.add_argument("--serial", required=True)
    ex.add_argument("--uin", type=int)
    ex.add_argument("--account-id")
    setup = commands.add_parser("setup", help="Enter existing passwords locally")
    group = setup.add_mutually_exclusive_group()
    group.add_argument("--web", action="store_true")
    group.add_argument("--from-file", type=Path, help="Local account_password/trading_password text file")
    commands.add_parser("status")
    auth = commands.add_parser("auth-check", help="Verify reads and trading authentication; no trades")
    auth.add_argument("--force-login", action="store_true")
    orders = commands.add_parser("orders")
    orders.add_argument("--date")
    orders.add_argument("--start", type=int, default=0)
    orders.add_argument("--count", type=int, default=100)
    commands.add_parser("assets")
    commands.add_parser("holdings")
    commands.add_parser("market-status")
    q = commands.add_parser("quote")
    q.add_argument("symbol")
    check = commands.add_parser("quote-check", help="Inspect delay flags and active-session quote ages")
    check.add_argument("symbol")
    detail = commands.add_parser("detail")
    detail.add_argument("order_id")
    order = commands.add_parser("order", help="Prepare by default; --live submits")
    order.add_argument("symbol")
    order.add_argument("side", choices=["buy", "sell"])
    order.add_argument("quantity")
    order.add_argument("--kind", choices=["limit", "market", "marketable-limit"], default="limit")
    order.add_argument("--session", choices=SESSION_FLAGS, required=True)
    order.add_argument("--validity", choices=["day", "gtc"], default="day")
    order.add_argument("--price")
    order.add_argument("--price-bound", help="Buy maximum or sell minimum for marketable-limit")
    order.add_argument("--max-notional")
    order.add_argument("--intent-id")
    order.add_argument("--live", action="store_true")
    cancel = commands.add_parser("cancel", help="Prepare by default; --live cancels")
    cancel.add_argument("order_id")
    cancel.add_argument("--intent-id")
    cancel.add_argument("--live", action="store_true")
    journal = commands.add_parser("journal")
    journal.add_argument("intent_id")
    return p


def run(args):
    if args.command == "export":
        if Path(args.profile).exists():
            raise ValueError("Profile already exists; use a new output path for re-export")
        return export_phone(args.adb, args.serial, args.profile, args.uin, args.account_id)
    if args.command == "setup":
        if args.web:
            return {"saved": serve_setup(args.profile, args.credentials)}
        if args.from_file:
            values = {}
            for line in args.from_file.read_text(encoding="utf-8-sig").splitlines():
                if line.strip():
                    parts = re.split(r"[:=]", line, maxsplit=1)
                    if len(parts) != 2:
                        raise ValueError("Credential file requires labelled entries")
                    values[parts[0].strip()] = parts[1].strip()
            with profile_lock(args.profile):
                state = load_private(args.profile)
                creds = prepare_credentials(state, values["account_password"], values["trading_password"])
                save_private(args.credentials, creds)
                state.pop("auth_blocked", None)
                state.pop("auth_retry_after", None)
                save_private(args.profile, state)
        else:
            console_setup(args.profile, args.credentials)
        return {"saved": True}
    with profile_lock(args.profile):
        client = Client.load(args.profile, args.credentials if Path(args.credentials).exists() else None)
        if args.command == "status":
            return {"user_session_fresh": client.auth._fresh(client.state["user"], "session"),
                    "trading_session_fresh": client.auth._fresh(client.state.get("trade"), "session2"),
                    "credentials_available": bool(client.auth.credentials),
                    "authentication_paused": bool(client.state.get("auth_blocked"))}
        if args.command == "auth-check":
            if args.force_login:
                client.auth.login()
            client.orders()
            client.auth.validate_trade()
            return {"account_read": "passed", "trading_unlock_and_renewal": "passed", "orders_submitted": 0}
        if args.command == "orders":
            if args.start < 0 or not 1 <= args.count <= 100:
                raise ValueError("start >= 0 and 1 <= count <= 100 required")
            return client.orders(args.date, args.start, args.count)
        if args.command == "assets":
            return client.read("assets", client.account_params())
        if args.command == "holdings":
            return client.read("holdings", {**client.account_params(), "start": 0, "count": 100, "market": ["us"]})
        if args.command == "quote":
            return client.quote(args.symbol)
        if args.command == "quote-check":
            return client.quote_check(args.symbol)
        if args.command == "market-status":
            return client.market_status()
        if args.command == "detail":
            return client.order_detail(args.order_id)
        if args.command in {"order", "cancel", "journal"}:
            journal = Journal(args.journal)
            try:
                if args.command == "journal":
                    return journal.get(args.intent_id)
                if args.command == "cancel":
                    return client.cancel(args.order_id, journal=journal, intent_id=args.intent_id, live=args.live)
                order = Order(args.symbol, args.side, args.quantity, args.kind, args.session,
                              args.validity, args.price, args.price_bound)
                return client.submit(order, journal=journal, intent_id=args.intent_id,
                                     live=args.live, max_notional=args.max_notional)
            finally:
                journal.close()
    raise ValueError("Unknown command")


def main():
    args = parser().parse_args()
    try:
        print(json.dumps(run(args), ensure_ascii=False, indent=2))
    except (StarWealthError, ValueError, KeyError, OSError) as exc:
        # Never print HTTP payloads, credential material or broker-provided account data on errors.
        message = str(exc) if isinstance(exc, (StarWealthError, ValueError)) else "Check local paths, credentials, and network connectivity"
        print(json.dumps({"error": type(exc).__name__, "message": message}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
