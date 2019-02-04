from ..event.base import BaseEvent
from ..app.base import BaseApp


__all__ = [
    'BaseCommand',
]


class BaseCommand(BaseEvent):
    async def handle(self, app: BaseApp, corr_id: str):
        await self.execute(app=app, corr_id=corr_id)

    async def execute(self, app: BaseApp, corr_id: str):
        pass


class BaseCommandResult(BaseEvent):
    @property
    def error(self):
        return self.data.get('error')

    @property
    def error_label(self):
        if self.data['error']:
            return self.data['error']['label']
        else:
            return None

    @property
    def error_message(self):
        if self.data['error']:
            return self.data['error'].get('message')
        else:
            return None
