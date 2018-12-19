from ..event.base import BaseEvent
from ..context import Context

__all__ = [
    'BaseCommand',
]


class BaseCommand(BaseEvent):
    async def execute(self, context: Context, corr_id: str):
        pass
