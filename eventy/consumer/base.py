# coding: utf-8
# Copyright (c) Qotto, 2018-2020

__all__ = [
    'BaseEventConsumer'
]


class BaseEventConsumer:
    async def start(self):
        raise NotImplementedError

    def set_checkpoint_callback(self, checkpoint_callback):
        raise NotImplementedError
