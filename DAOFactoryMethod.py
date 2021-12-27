import sqlite3
from abc import ABC, abstractmethod

import Genre
import Platform
from SubjectObserver import Subject, Observer, DAOUpdateObserver
from DataBaseConnection import DataBaseConnection
import Game


class DAO(ABC):
    @abstractmethod
    def get_all(self, dbcon: DataBaseConnection) -> list:
        pass

    @abstractmethod
    def filter(self, dbcon: DataBaseConnection, params: list) -> list:
        pass

    @abstractmethod
    def add(self, dbcon: DataBaseConnection, object_):
        pass

    @abstractmethod
    def remove(self, dbcon: DataBaseConnection, object_):
        pass

    @abstractmethod
    def update(self, dbcon: DataBaseConnection, object_old, object_new):
        pass


class DAOFactory(ABC):
    @abstractmethod
    def create_DAO(self):
        pass


class GameDAOFactory(DAOFactory):
    def create_DAO(self) -> DAO:
        return GameDAO()


class PlatformDAOFactory(DAOFactory):

    _observer: Observer = None

    def __init__(self, observer=None):
        self._observer = observer

    def create_DAO(self) -> DAO:
        dao = PlatformDAO()
        if self._observer:
            dao.attach(observer)
        return dao


class GenreDAOFactory(DAOFactory):
    def create_DAO(self) -> DAO:
        return GenreDAO()


class GameDAO(DAO):
    def get_all(self, dbcon: DataBaseConnection) -> list:
        con = dbcon.get_connection()
        statement = """select * from games;"""
        all = list()
        with con:
            for row in con.execute(statement):
                all.append(row)
        return all

    def filter(self, dbcon: DataBaseConnection, params: list) -> list:
        con = dbcon.get_connection()
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
            return self.get_all(dbcon)

    def add(self, dbcon: DataBaseConnection, game: Game.Game):
        con = dbcon.get_connection()

        bs_game = """insert into games (name, price) values (?, ?)"""
        bs_game_platforms = """insert into game_platforms (game_id, platform_id) values (?, ?)"""
        bs_game_genres = """insert into game_genres (game_id, genre_id) values (?, ?)"""

        avail_platforms = get_all(dbcon, PlatformDAOFactory())
        avail_platforms_dct = {platform: id_ for id_, platform in avail_platforms}
        avail_genres = get_all(dbcon, GenreDAOFactory())
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

    def remove(self, dbcon: DataBaseConnection, object_):
        con = dbcon.get_connection()
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
        to_delete = self.filter(dbcon, delete_cond)

        with con:
            for td in to_delete:
                con.execute(base_statement, {"id": td[0]})

    def update(self, dbcon: DataBaseConnection, object_old: Game.Game, object_new: Game.Game):
        con = dbcon.get_connection()
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
        to_update = self.filter(dbcon, update_cond)

        with con:
            for tu in to_update:
                con.execute(base_statement, {
                    "id": tu[0],
                    "name": object_new.name,
                    "price": object_new.price,
                })


class PlatformDAO(DAO, Subject):

    _last_action: dict = None
    _observers: list = list()

    def get_all(self, dbcon: DataBaseConnection) -> list:
        con = dbcon.get_connection()
        statement = """select * from platforms;"""
        all = list()
        with con:
            for row in con.execute(statement):
                all.append(row)
        return all

    def filter(self, dbcon: DataBaseConnection, params: list) -> list:
        con = dbcon.get_connection()
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
            return self.get_all(dbcon)

    def add(self, dbcon: DataBaseConnection, platform: Platform.Platform):
        con = dbcon.get_connection()
        base_statement = """insert into platforms (name) values (?)"""

        with con:
            con.execute(base_statement, (platform.name, ))

        self._last_action = {
            "action": "add",
            "object": platform
        }
        self.notify()

    def remove(self, dbcon: DataBaseConnection, object_):
        con = dbcon.get_connection()
        base_statement = """delete from platforms where id=:id"""
        delete_cond = [
            {
                "column": "name",
                "value": object_.name,
                "op": "="
            }
        ]
        to_delete = self.filter(dbcon, delete_cond)

        with con:
            for td in to_delete:
                con.execute(base_statement, {"id": td[0]})

        self._last_action = {
            "action": "remove",
            "object": object_
        }
        self.notify()

    def update(self, dbcon: DataBaseConnection, object_old: Platform.Platform, object_new: Platform.Platform):
        con = dbcon.get_connection()
        base_statement = """update platforms set name=:name where id=:id"""
        update_cond = [
            {
                "column": "name",
                "value": object_old.name,
                "op": "="
            }
        ]
        to_update = self.filter(dbcon, update_cond)

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


class GenreDAO(DAO):
    def get_all(self, dbcon: DataBaseConnection) -> list:
        con = dbcon.get_connection()
        statement = """select * from genres;"""
        all = list()
        with con:
            for row in con.execute(statement):
                all.append(row)
        return all

    def filter(self, dbcon: DataBaseConnection, params: list) -> list:
        con = dbcon.get_connection()
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
            return self.get_all(dbcon)

    def add(self, dbcon: DataBaseConnection, genre: Genre.Genre):
        con = dbcon.get_connection()
        base_statement = """insert into genres (name) values (?)"""

        with con:
            con.execute(base_statement, (genre.name, ))

    def remove(self, dbcon: DataBaseConnection, object_):
        con = dbcon.get_connection()
        base_statement = """delete from genres where id=:id"""
        delete_cond = [
            {
                "column": "name",
                "value": object_.name,
                "op": "="
            }
        ]
        to_delete = self.filter(dbcon, delete_cond)

        with con:
            for td in to_delete:
                con.execute(base_statement, {"id": td[0]})

    def update(self, dbcon: DataBaseConnection, object_old: Genre.Genre, object_new: Genre.Genre):
        con = dbcon.get_connection()
        base_statement = """update genres set name=:name where id=:id"""
        update_cond = [
            {
                "column": "name",
                "value": object_old.name,
                "op": "="
            }
        ]
        to_update = self.filter(dbcon, update_cond)

        with con:
            for tu in to_update:
                con.execute(base_statement, {
                    "id": tu[0],
                    "name": object_new.name
                })


def get_all(dbcon: DataBaseConnection, dao_factory: DAOFactory) -> list:
    return dao_factory.create_DAO().get_all(dbcon)


def filter(dbcon: DataBaseConnection, dao_factory: DAOFactory, params: list) -> list:
    return dao_factory.create_DAO().filter(dbcon, params)


def add(dbcon: DataBaseConnection, dao_factory: DAOFactory, objects: list) -> None:
    for object_ in objects:
        dao_factory.create_DAO().add(dbcon, object_)


def remove(dbcon: DataBaseConnection, dao_factory: DAOFactory, objects: list) -> None:
    for object_ in objects:
        dao_factory.create_DAO().remove(dbcon, object_)


def update(dbcon: DataBaseConnection, dao_factory: DAOFactory, object_old, object_new) -> None:
    dao_factory.create_DAO().update(dbcon, object_old, object_new)


if __name__ == "__main__":
    dbconn = DataBaseConnection.get_instance()
    dbconn.open_connection("db.db")
    dbconn.init_tables()

    # register observers
    observer = DAOUpdateObserver()

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

    add(dbconn, GenreDAOFactory(), genres)
    add(dbconn, PlatformDAOFactory(observer), platforms)

    gameBuilder = Game.GameBuilder()
    gameBuilder.set_name("csgo")
    gameBuilder.set_price(11.99)
    gameBuilder.add_genres(["fps", "mmo"])
    gameBuilder.add_platform_ids(["pc", "x"])
    add(dbconn, GameDAOFactory(), [gameBuilder.get_object()])

    gameBuilder = Game.GameBuilder()
    gameBuilder.set_name("dota")
    gameBuilder.set_price(0.)
    gameBuilder.add_genres(["moba", "mmo"])
    gameBuilder.add_platform_ids(["pc"])
    add(dbconn, GameDAOFactory(), [gameBuilder.get_object()])

    all_games = get_all(dbconn, GameDAOFactory())
    print(all_games)
    all_platforms = get_all(dbconn, PlatformDAOFactory())
    print(all_platforms, "\nFilter:")

    # filter
    params = [
        {
            "column": "price",
            "value": 0,
            "op": ">"
        }
    ]

    f_games = filter(dbconn, GameDAOFactory(), params)
    print(f_games, "\nUpdate:")

    update(dbconn, GameDAOFactory(), gameBuilder.get_object(), Game.Game("1.6", 5.99))
    update(dbconn, PlatformDAOFactory(), platforms[0], Platform.Platform("switch"))

    all_games = get_all(dbconn, GameDAOFactory())
    print(all_games)
    all_platforms = get_all(dbconn, PlatformDAOFactory())
    print(all_platforms)

    remove(dbconn, GameDAOFactory(), [gameBuilder.get_object()])
    remove(dbconn, GenreDAOFactory(), genres)

    all_games = get_all(dbconn, GameDAOFactory())
    print("\nRemove\n", all_games)
    all_platforms = get_all(dbconn, PlatformDAOFactory())
    print(all_platforms)
    all_genres = get_all(dbconn, GenreDAOFactory())
    print(all_genres)
