class StarWealthError(Exception):
    pass


class ProtocolError(StarWealthError):
    pass


class BrokerError(StarWealthError):
    def __init__(self, code, message=""):
        self.code = int(code)
        # Broker messages can contain account information. Do not put them in logs.
        self.broker_message = message
        super().__init__(f"Broker error {self.code}")


class AuthenticationRequired(StarWealthError):
    pass


class OutcomeUnknown(StarWealthError):
    """A financial write may have reached the broker. Never automatically resend."""


class DuplicateIntent(StarWealthError):
    pass


USER_SESSION_ERRORS = frozenset({100008, 100031, 100034})
TRADE_SESSION_ERRORS = frozenset({100030, 100032})

