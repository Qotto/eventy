from .command import BaseCommand

__all__ = [
    'Service'
]


class Service:

    async def execute(self, command: BaseCommand, corr_id: str):
        raise NotImplementedError
