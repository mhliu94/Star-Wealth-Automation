"""Dry run only. Replace the illustrative parameters with your own intent."""
from starwealth.client import Client
from starwealth.orders import Order
from starwealth.storage import profile_lock

with profile_lock(".state/star.session.json"):
    client = Client.load(".state/star.session.json", ".state/star.credentials.json")
    order = Order("AAPL", "buy", "1", session="extended", price="100.00")
    print(client.prepare(order, max_notional="100.00"))
