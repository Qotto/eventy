from typing import Dict


class Context:
    def __init__(self):
        self.storage: Dict[str, object] = dict()

    def set(self, object_name: str, object_instance: object):
        self.storage[object_name] = object_instance

    def get(self, object_name: str):
        return self.storage.get(object_name)
