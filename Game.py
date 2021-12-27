class Game:
    def __init__(self, name: str = "", price: float = .0, platform_ids: set = set(), genre_ids: set = set()):
        self.name: str = name
        self.price: float = price
        self.platform_ids: set = platform_ids
        self.genre_ids: set = genre_ids


class GameBuilder:
    def __init__(self):
        self.game = Game()

    def reset(self) -> Game:
        self.game = Game()
        return self.game

    def get_object(self) -> Game:
        return self.game

    def set_name(self, name: str = ""):
        self.game.name = name

    def set_price(self, price: float = .0):
        self.game.price = price

    def add_platform_ids(self, platform_ids: list):
        for platform_id in platform_ids:
            self.game.platform_ids.add(platform_id)

    def clear_platform_ids(self):
        self.game.platform_ids = set()

    def add_genres(self, genres: list):
        for genre in genres:
            self.game.genre_ids.add(genre)

    def clear_genres(self):
        self.game.genre_ids = set()
