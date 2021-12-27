import sqlite3
from abc import ABC, abstractmethod

import Genre
import Platform
from SubjectObserver import Subject, Observer, DAOUpdateObserver
from DataBaseConnection import DataBaseConnection
import Game
import Memento


class DAO(ABC):
    @abstractmethod
    def get_all(self) -> list:
        pass

    @abstractmethod
    def filter(self, params: list) -> list:
        pass

    @abstractmethod
    def add(self, object_):
        pass

    @abstractmethod
    def remove(self, object_):
        pass

    @abstractmethod
    def update(self, object_old, object_new):
        pass


class DAOFactory(ABC):
    @abstractmethod
    def create_DAO(self):
        pass


class GameDAOFactory(DAOFactory):
    _observer: Observer = None

    def __init__(self, dbcon: DataBaseConnection = None, observer=None):
        self._dbcon = dbcon
        self._observer = observer

    def create_DAO(self) -> DAO:
        dao = GameDAO(self._dbcon)
        if self._observer:
            dao.attach(self._observer)
        return dao


class PlatformDAOFactory(DAOFactory):
    _observer: Observer = None

    def __init__(self, dbcon: DataBaseConnection = None, observer=None):
        self._dbcon = dbcon
        self._observer = observer

    def create_DAO(self) -> DAO:
        dao = PlatformDAO(self._dbcon)
        if self._observer:
            dao.attach(self._observer)
        return dao


class GenreDAOFactory(DAOFactory):
    _observer: Observer = None

    def __init__(self, dbcon: DataBaseConnection = None, observer=None):
        self._dbcon = dbcon
        self._observer = observer

    def create_DAO(self) -> DAO:
        dao = GenreDAO(self._dbcon)
        if self._observer:
            dao.attach(self._observer)
        return dao


class GameDAO(DAO, Subject):
    _last_action: dict = None
    _observers: list = list()
    _dbcon: DataBaseConnection = None

    def __init__(self, dbcon: DataBaseConnection = None):
        self._dbcon = dbcon

    def get_all(self) -> list:
        con = self._dbcon.get_connection()
        statement = """select * from games;"""
        all = list()
        with con:
            for row in con.execute(statement):
                all.append(row)
        return all

    def filter(self, params: list) -> list:
        con = self._dbcon.get_connection()
        if any(params):
            base_statement = """select * from games where """
            param_statements = [f"{param['column']}{param['op']}:{param['column']}" for param in params]
            final_statement = base_statement + " and ".join(param_statements)
            query_params = {param["column"]: param["value"] for param in params}
            filtered = list()
            with con:
                exec = con.execute(final_statement, query_params)
                for row in exec:  # protected from SQL injection
                    filtered.append(row)
            return filtered
        else:
            return self.get_all()

    def add(self, game: Game.Game):
        con = self._dbcon.get_connection()

        bs_game = """insert into games (name, price) values (?, ?)"""
        bs_game_platforms = """insert into game_platforms (game_id, platform_id) values (?, ?)"""
        bs_game_genres = """insert into game_genres (game_id, genre_id) values (?, ?)"""

        avail_platforms = get_all(PlatformDAOFactory(self._dbcon))
        avail_platforms_dct = {platform: id_ for id_, platform in avail_platforms}
        avail_genres = get_all(GenreDAOFactory(self._dbcon))
        avail_genres_dct = {genre: id_ for id_, genre in avail_genres}

        with con:
            cursor = con.cursor()
            cursor.execute(bs_game, (game.name, game.price))
            game_id = cursor.lastrowid

            for platform in list(game.platform_ids):
                try:
                    platform_id = avail_platforms_dct[platform]
                    con.execute(bs_game_platforms, (game_id, platform_id))
                except KeyError:
                    raise sqlite3.IntegrityError()

            for genre in list(game.genre_ids):
                try:
                    genre_id = avail_genres_dct[genre]
                    con.execute(bs_game_genres, (game_id, genre_id))
                except KeyError:
                    raise sqlite3.IntegrityError()

        self._last_action = {
            "action": "add",
            "object": game
        }
        self.notify()

    def remove(self, object_):
        con = self._dbcon.get_connection()
        base_statement = """delete from games where id=:id"""
        delete_cond = [
            {
                "column": "name",
                "value": object_.name,
                "op": "="
            },
            {
                "column": "price",
                "value": object_.price,
                "op": "="
            }
        ]
        to_delete = self.filter(delete_cond)

        with con:
            for td in to_delete:
                con.execute(base_statement, {"id": td[0]})

        self._last_action = {
            "action": "remove",
            "object": object_
        }
        self.notify()

    def update(self, object_old: Game.Game, object_new: Game.Game):
        con = self._dbcon.get_connection()
        base_statement = """update games set name=:name, price=:price where id=:id"""

        update_cond = [
            {
                "column": "name",
                "value": object_old.name,
                "op": "="
            },
            {
                "column": "price",
                "value": object_old.price,
                "op": "="
            }
        ]
        to_update = self.filter(update_cond)

        with con:
            for tu in to_update:
                con.execute(base_statement, {
                    "id": tu[0],
                    "name": object_new.name,
                    "price": object_new.price,
                })

        self._last_action = {
            "action": "update",
            "old": object_old,
            "new": object_new
        }
        self.notify()

    def attach(self, observer: Observer) -> None:
        self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify(self) -> None:
        for observer in self._observers:
            observer.update(self)

    def save(self) -> Memento.Memento:
        return Memento.GameDAOMemento(self._last_action)

    def restore(self, memento: Memento.Memento):
        self._last_action = memento.get_state()
        if self._last_action["action"] == "update":
            self.update(self._last_action["new"], self._last_action["old"])
        else:
            raise NotImplementedError


class PlatformDAO(DAO, Subject):
    _last_action: dict = None
    _observers: list = list()
    _dbcon: DataBaseConnection = None

    def __init__(self, dbcon: DataBaseConnection = None):
        self._dbcon = dbcon

    def get_all(self) -> list:
        con = self._dbcon.get_connection()
        statement = """select * from platforms;"""
        all = list()
        with con:
            for row in con.execute(statement):
                all.append(row)
        return all

    def filter(self, params: list) -> list:
        con = self._dbcon.get_connection()
        if any(params):
            base_statement = """select * from platforms where """
            param_statements = [f"{param['column']}{param['op']}:{param['column']}" for param in params]
            final_statement = base_statement + " and ".join(param_statements)
            query_params = {param["column"]: param["value"] for param in params}
            filtered = list()
            with con:
                exec = con.execute(final_statement, query_params)
                for row in exec:  # protected from SQL injection
                    filtered.append(row)
            return filtered
        else:
            return self.get_all()

    def add(self, platform: Platform.Platform):
        con = self._dbcon.get_connection()
        base_statement = """insert into platforms (name) values (?)"""

        with con:
            con.execute(base_statement, (platform.name,))

        self._last_action = {
            "action": "add",
            "object": platform
        }
        self.notify()

    def remove(self, object_):
        con = self._dbcon.get_connection()
        base_statement = """delete from platforms where id=:id"""
        delete_cond = [
            {
                "column": "name",
                "value": object_.name,
                "op": "="
            }
        ]
        to_delete = self.filter(delete_cond)

        with con:
            for td in to_delete:
                con.execute(base_statement, {"id": td[0]})

        self._last_action = {
            "action": "remove",
            "object": object_
        }
        self.notify()

    def update(self, object_old: Platform.Platform, object_new: Platform.Platform):
        con = self._dbcon.get_connection()
        base_statement = """update platforms set name=:name where id=:id"""
        update_cond = [
            {
                "column": "name",
                "value": object_old.name,
                "op": "="
            }
        ]
        to_update = self.filter(update_cond)

        with con:
            for tu in to_update:
                con.execute(base_statement, {
                    "id": tu[0],
                    "name": object_new.name
                })

        self._last_action = {
            "action": "update",
            "old": object_old,
            "new": object_new
        }
        self.notify()

    def attach(self, observer: Observer) -> None:
        self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify(self) -> None:
        for observer in self._observers:
            observer.update(self)


class GenreDAO(DAO, Subject):
    _last_action: dict = None
    _observers: list = list()
    _dbcon: DataBaseConnection = None

    def __init__(self, dbcon: DataBaseConnection = None):
        self._dbcon = dbcon

    def get_all(self) -> list:
        con = self._dbcon.get_connection()
        statement = """select * from genres;"""
        all = list()
        with con:
            for row in con.execute(statement):
                all.append(row)
        return all

    def filter(self, params: list) -> list:
        con = self._dbcon.get_connection()
        if any(params):
            base_statement = """select * from genres where """
            param_statements = [f"{param['column']}{param['op']}:{param['column']}" for param in params]
            final_statement = base_statement + " and ".join(param_statements)
            query_params = {param["column"]: param["value"] for param in params}
            filtered = list()
            with con:
                exec = con.execute(final_statement, query_params)
                for row in exec:  # protected from SQL injection
                    filtered.append(row)
            return filtered
        else:
            return self.get_all()

    def add(self, genre: Genre.Genre):
        con = self._dbcon.get_connection()
        base_statement = """insert into genres (name) values (?)"""

        with con:
            con.execute(base_statement, (genre.name,))

        self._last_action = {
            "action": "add",
            "object": genre
        }
        self.notify()

    def remove(self, object_):
        con = self._dbcon.get_connection()
        base_statement = """delete from genres where id=:id"""
        delete_cond = [
            {
                "column": "name",
                "value": object_.name,
                "op": "="
            }
        ]
        to_delete = self.filter(delete_cond)

        with con:
            for td in to_delete:
                con.execute(base_statement, {"id": td[0]})

        self._last_action = {
            "action": "remove",
            "object": object_
        }
        self.notify()

    def update(self, object_old: Genre.Genre, object_new: Genre.Genre):
        con = self._dbcon.get_connection()
        base_statement = """update genres set name=:name where id=:id"""
        update_cond = [
            {
                "column": "name",
                "value": object_old.name,
                "op": "="
            }
        ]
        to_update = self.filter(update_cond)

        with con:
            for tu in to_update:
                con.execute(base_statement, {
                    "id": tu[0],
                    "name": object_new.name
                })

            self._last_action = {
                "action": "update",
                "old": object_old,
                "new": object_new
            }
            self.notify()

    def attach(self, observer: Observer) -> None:
        self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify(self) -> None:
        for observer in self._observers:
            observer.update(self)


def get_all(dao_factory: DAOFactory) -> list:
    return dao_factory.create_DAO().get_all()


def filter(dao_factory: DAOFactory, params: list) -> list:
    return dao_factory.create_DAO().filter(params)


def add(dao_factory: DAOFactory, objects: list) -> None:
    for object_ in objects:
        dao_factory.create_DAO().add(object_)


def remove(dao_factory: DAOFactory, objects: list) -> None:
    for object_ in objects:
        dao_factory.create_DAO().remove(object_)


def update(dao_factory: DAOFactory, object_old, object_new) -> None:
    dao_factory.create_DAO().update(object_old, object_new)


if __name__ == "__main__":

    PZ1 = False
    PZ2 = False
    PZ3 = True

    dbcon = DataBaseConnection.get_instance()
    dbcon.open_connection("db.db")
    dbcon.init_tables(drop_tables=True)

    # register observers
    observer = None
    if PZ2:
        observer = DAOUpdateObserver()

    # create factories
    genreDAOFactory = GenreDAOFactory(dbcon, observer)
    gameDAOFactory = GameDAOFactory(dbcon, observer)
    platformDAOFactory = PlatformDAOFactory(dbcon, observer)

    # add data
    genres = [
        Genre.Genre("fps"),
        Genre.Genre("mmo"),
        Genre.Genre("moba")
    ]

    platforms = [
        Platform.Platform("pc"),
        Platform.Platform("ps"),
        Platform.Platform("x")
    ]

    add(genreDAOFactory, genres)
    add(platformDAOFactory, platforms)
    print(get_all(genreDAOFactory))

    gameBuilder = Game.GameBuilder()
    gameBuilder.set_name("csgo")
    gameBuilder.set_price(11.99)
    gameBuilder.add_genres(["fps", "mmo"])
    gameBuilder.add_platform_ids(["pc", "x"])
    add(gameDAOFactory, [gameBuilder.get_object()])

    gameBuilder = Game.GameBuilder()
    gameBuilder.set_name("dota")
    gameBuilder.set_price(0.)
    gameBuilder.add_genres(["moba", "mmo"])
    gameBuilder.add_platform_ids(["pc"])
    add(gameDAOFactory, [gameBuilder.get_object()])

    if PZ1:
        all_games = get_all(gameDAOFactory)
        print(all_games)
        all_platforms = get_all(gameDAOFactory)
        print(all_platforms, "\nFilter:")

        # filter
        params = [
            {
                "column": "price",
                "value": 0,
                "op": ">"
            }
        ]

        f_games = filter(gameDAOFactory, params)
        print(f_games, "\nUpdate:")

        update(gameDAOFactory, gameBuilder.get_object(),
               Game.Game("1.6", 5.99))
        update(platformDAOFactory, platforms[0], Platform.Platform("expensive_platform"))

        all_games = get_all(gameDAOFactory)
        print(all_games)
        all_platforms = get_all(platformDAOFactory)
        print(all_platforms)

        remove(gameDAOFactory, [gameBuilder.get_object()])
        remove(genreDAOFactory, genres)

        all_games = get_all(gameDAOFactory)
        print("\nRemove\n", all_games)
        all_platforms = get_all(platformDAOFactory)
        print(all_platforms)
        all_genres = get_all(genreDAOFactory)
        print(all_genres)

    if PZ3:
        all_games = get_all(gameDAOFactory)
        print(all_games)

        gameDAO = gameDAOFactory.create_DAO()
        result_history = Memento.GameDAOHistory(gameDAO)
        result_history.backup()

        gameDAO.update(gameBuilder.get_object(), Game.Game("1.6", 5.99))
        result_history.backup()

        all_games = get_all(gameDAOFactory)
        print(all_games)

        gameDAO.update(Game.Game("1.6", 5.99), Game.Game("update1", 699999.99))
        result_history.backup()

        all_games = get_all(gameDAOFactory)
        print(all_games)

        gameDAO.update(Game.Game("update1", 699999.99), Game.Game("update2", 799999.99))
        result_history.backup()

        all_games = get_all(gameDAOFactory)
        print(all_games)

        result_history.undo()

        all_games = get_all(gameDAOFactory)
        print(all_games)

        result_history.undo()

        all_games = get_all(gameDAOFactory)
        print(all_games)

        result_history.undo()

        all_games = get_all(gameDAOFactory)
        print(all_games)
