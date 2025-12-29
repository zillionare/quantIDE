class BrokerMiddleware:
    def __init__(self, app, broker):
        self.app = app
        self.broker = broker

    async def __call__(self, scope, receive, send):
        if scope["type"] in ("http", "websocket"):
            scope["broker"] = self.broker
        await self.app(scope, receive, send)
