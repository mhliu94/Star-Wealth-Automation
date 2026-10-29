"""Run after export/setup. Does not place orders."""
from starwealth.client import Client
from starwealth.storage import profile_lock

with profile_lock(".state/star.session.json"):
    client = Client.load(".state/star.session.json", ".state/star.credentials.json")
    print(client.market_status())
    print(client.orders())
