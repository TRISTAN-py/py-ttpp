from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime

from DataBaseConnection import DataBaseConnection


class Memento(ABC):

    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def get_date(self) -> str:
        pass


class GameDAOMemento(Memento):
    def __init__(self, game=None):
        self._state = game
        self._date: datetime = datetime.now()

    def __repr__(self):
        return self.get_name()

    def get_date(self) -> str:
        return self._date.strftime('%c')

    def get_name(self) -> str:
        return f"{self._state} @ {self.get_date()}"

    def get_state(self):
        return self._state


class GameDAOHistory:
    def __init__(self, gameDAO=None):
        self._mementos = list()
        self._gameDAO = gameDAO

    def backup(self):
        self._mementos.append(self._gameDAO.save())

    def undo(self):
        if not len(self._mementos):
            return

        memento = self._mementos.pop()
        try:
            self._gameDAO.restore(memento)
        except Exception:
            self.undo()

    def show_history(self) -> None:
        for memento in self._mementos:
            print(memento.get_name())
