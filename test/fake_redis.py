class FakeRedis:
    def __init__(self):
        self.store: dict[str, bytes] = {}

    def get(self, key: str):
        return self.store.get(key)

    def set(self, key: str, value, nx: bool = False):
        if nx and key in self.store:
            return None
        if isinstance(value, str):
            value = value.encode()
        self.store[key] = value
        return True

    def mset(self, values: dict[str, bytes]):
        for key, value in values.items():
            self.set(key, value)
        return True

    def close(self):
        return None
