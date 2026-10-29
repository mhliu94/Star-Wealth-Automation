"""Password entry stays on loopback. No passwords in chat, arguments or logs."""
import getpass
import html
from http.server import BaseHTTPRequestHandler, HTTPServer
import secrets
import time
from urllib.parse import parse_qs
from .crypto import password_material
from .storage import save_private, load_private, profile_lock


def prepare_credentials(state, login_password, trade_password):
    if not login_password or not trade_password:
        raise ValueError("Enter both passwords")
    u = state["user"]
    c = u["clientInfo"]
    return {"login": password_material(u["uin"], login_password, u["salt"]),
            "trade": password_material(c["crId"], trade_password, c["salt"])}


def console_setup(profile, credentials):
    with profile_lock(profile):
        state = load_private(profile)
        material = prepare_credentials(state, getpass.getpass("Login password: "),
                                       getpass.getpass("Trading password: "))
        save_private(credentials, material)
        state.pop("auth_blocked", None)
        state.pop("auth_retry_after", None)
        save_private(profile, state)


def serve_setup(profile, credentials, *, port=0, lifetime=900, on_ready=print):
    token = secrets.token_urlsafe(32)
    completed = False
    deadline = time.monotonic() + lifetime

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send_page(self, status, body):
            raw = body.encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Security-Policy", "default-src 'none'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def valid_request(self):
            return self.path == "/" + token and self.headers.get("Host") == address

        def do_GET(self):
            if not self.valid_request():
                self.send_page(404, "Not found"); return
            self.send_page(200, """<!doctype html><meta charset="utf-8"><title>Star Wealth password setup</title>
<h1>Star Wealth password setup</h1><p>This form runs only on your computer. Enter your existing passwords.
It saves password-derived credentials locally; it does not change your broker passwords or submit orders.</p>
<form method="post" autocomplete="off"><p><label>Login password <input type="password" name="login" required></label></p>
<p><label>Trading password <input type="password" name="trade" required></label></p>
<button type="submit">Save credentials locally</button></form>
<p>The next verification step will log in through the API and may sign the phone out.</p>""")

        def do_POST(self):
            nonlocal completed
            origin = self.headers.get("Origin")
            if not self.valid_request() or origin not in (None, "http://" + address):
                self.send_page(403, "Request rejected"); return
            if self.headers.get("Content-Type", "").split(";")[0] != "application/x-www-form-urlencoded":
                self.send_page(415, "Invalid form"); return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 16384:
                    raise ValueError()
                values = parse_qs(self.rfile.read(length).decode(), strict_parsing=True)
                with profile_lock(profile):
                    state = load_private(profile)
                    material = prepare_credentials(state, values["login"][0], values["trade"][0])
                    save_private(credentials, material)
                    state.pop("auth_blocked", None)
                    state.pop("auth_retry_after", None)
                    save_private(profile, state)
                self.send_page(200, "<h1>Credentials saved locally</h1><p>You can close this tab. No order was submitted.</p>")
                completed = True
            except Exception:
                self.send_page(400, "<h1>Unable to save</h1><p>Reopen the setup form and enter both passwords.</p>")

    server = HTTPServer(("127.0.0.1", port), Handler)
    address = f"127.0.0.1:{server.server_port}"
    server.timeout = 1
    try:
        on_ready("http://" + address + "/" + token)
        while not completed and time.monotonic() < deadline:
            server.handle_request()
    finally:
        server.server_close()
    return completed

