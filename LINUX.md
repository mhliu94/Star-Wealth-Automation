# Run on Linux using the private bundle

Requirements: Python 3.11 or later with venv support, Git, and outbound HTTPS access to Star Wealth and the Python package index. No Android phone, emulator, ADB, or password re-entry is needed for the prepared account bundle.

The separately supplied `starwealth-linux-private.tar.gz` contains two files:

- `star.session.json`: minimal account/device configuration, with cached sessions removed so the first run performs a fresh login.
- `star.credentials.json`: reusable login and trading-password derivatives. These can authenticate to the account and must be protected like passwords.

The archive is **private and unencrypted**. Upload it only to your own Linux machine, for example with SCP/SFTP, and keep it out of GitHub. It is deliberately not part of this repository or the source ZIP. Both archived files have mode `0600`.

## Install and import

Upload the private archive to your Linux home directory. In a fresh checkout:

```bash
git clone https://github.com/mhliu94/Star-Wealth-Automation.git
cd Star-Wealth-Automation
python3 -m venv .venv
source .venv/bin/activate
python -m pip install .

umask 077
chmod 600 "$HOME/starwealth-linux-private.tar.gz"
install -d -m 700 .state
tar --extract --gzip --file "$HOME/starwealth-linux-private.tar.gz" \
    --directory .state --keep-old-files --no-same-owner
chmod 600 .state/star.session.json .state/star.credentials.json

python -m starwealth auth-check
python -m starwealth quote-check AAPL
python -m starwealth orders
```

If `python3` is older than 3.11, use your installed `python3.11` or newer to create the virtual environment. If `.state` already contains a profile, preserve it and use a new checkout or separate state directory; the extraction command refuses to overwrite it.

Successful `auth-check` reports `account_read: passed`, `trading_unlock_and_renewal: passed`, and `orders_submitted: 0`. It logs in from the reusable credentials, checks the account, unlocks trading, and renews the trading session. It does not place or cancel an order. First login on Linux can displace the phone/Windows session, so use one active runtime for this account.

The bundle has been tested with a fresh login, account read, trading-password unlock, and renewal using Python on Windows. It was not tested on your Linux host. The client uses ordinary JSON credential files and standard POSIX locking on Linux; there is no Windows credential-encryption dependency.

## Subsequent runs

```bash
cd Star-Wealth-Automation
source .venv/bin/activate
python -m starwealth market-status
python -m starwealth auth-check
```

Keep the `.state` directory writable and persistent. The client saves new sessions there and creates its order-intent journal there when needed. Re-login does not require re-uploading the bundle when an ordinary session expires or is kicked out. A changed password/salt, account change, or new broker login challenge may require updated setup; see the main README.

See `README.md` for order commands, supported sessions, quote freshness checks, and dry-run behavior. All order and cancellation commands default to preparation only. Live execution remains unverified.
