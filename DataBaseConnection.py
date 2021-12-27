import sqlite3


CREATE_TABLES = ["""
create table if not exists games (
    id integer primary key autoincrement,
    name text not null,
    price real not null
);""",
"""
create table if not exists platforms (
    id integer primary key autoincrement,
    name text not null
);""",
"""
create table if not exists genres (
    id integer primary key autoincrement,
    name text not null
);""",
"""
create table if not exists game_platforms (
    game_id integer not null,
    platform_id integer not null,
    primary key (game_id, platform_id),
    foreign key (game_id) references games(id) on delete cascade,
    foreign key (platform_id) references platforms(id) on delete cascade
);
""",
"""
create table if not exists game_genres (
    game_id integer not null,
    genre_id integer not null,
    primary key (game_id, genre_id),
    foreign key (game_id) references games(id) on delete cascade,
    foreign key (genre_id) references genres(id) on delete cascade
);
"""]


class DataBaseConnection(object):
    __instance = None
    connection = None

    def __init__(self):
        pass

    @classmethod
    def get_connection(cls):
        if not cls.__instance.connection:
            raise ValueError("No connection.")
        return cls.__instance.connection

    @classmethod
    def open_connection(cls, db_file_path: str):
        if cls.__instance.connection:
            # close previous connection
            raise ConnectionError("Close previous connection.")

        # open new connection
        cls.__instance.connection = sqlite3.connect(db_file_path)
        return cls.__instance.connection

    @classmethod
    def close_connection(cls):
        if cls.__instance.connection:
            try:
                cls.__instance.connection.close()
            except Exception:
                pass
            finally:
                cls.__instance.connection = None

    @classmethod
    def get_instance(cls):
        if not cls.__instance:
            cls.__instance = DataBaseConnection()
        return cls.__instance

    @classmethod
    def init_tables(cls):
        con = cls.get_connection()
        with con:
            for statement in CREATE_TABLES:
                con.execute(statement)


if __name__ == "__main__":
    instance = DataBaseConnection.get_instance()
    instance.open_connection("db.db")
    instance.init_tables()
