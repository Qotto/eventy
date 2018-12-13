from .base import BaseEvent

__all__ = [
    'EventHandler'
]


class EventHandler:
    async def handle(self, event: BaseEvent, corr_id: str):
        raise NotImplementedError
