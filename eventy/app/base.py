class BaseApp:
    def set(self, object_name: str, object_instance: object):
        raise NotImplementedError

    def get(self, object_name: str):
        raise NotImplementedError
