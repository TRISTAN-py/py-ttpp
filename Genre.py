class Genre:
    def __init__(self, name: str = ""):
        self.name = name


class GenreBuilder:
    def __init__(self):
        self.genre = Genre()

    def reset(self) -> Genre:
        self.genre = Genre()
        return self.genre

    def get_object(self) -> Genre:
        return self.genre

    def set_name(self, name: str = ""):
        self.genre.name = name
