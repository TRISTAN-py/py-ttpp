class Platform:
    def __init__(self, name: str = ""):
        self.name = name


class PlatformBuilder:
    def __init__(self):
        self.platform = Platform()

    def reset(self) -> Platform:
        self.platform = Platform()
        return self.platform

    def get_object(self) -> Platform:
        return self.platform

    def set_name(self, name: str = ""):
        self.platform.name = name
