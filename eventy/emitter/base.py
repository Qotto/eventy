from ..event.base import BaseEvent

__all__ = [
    'BaseEventEmitter'
]


class BaseEventEmitter:
    def __init__(self, *args, **kw) -> None:
        pass

    async def send(self, event: BaseEvent, destination: str):
        raise NotImplementedError
