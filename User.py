import hashlib


class User:
    def __init__(self, login: str = "", password: str = "", role: str = ""):
        self.login: str = login
        self.password: str = self.hash_password(password)
        self.role: str = role

    @classmethod
    def hash_password(cls, password: str) -> str:
        return hashlib.sha3_512(password.encode()).hexdigest()

    def set_password(self, password: str = ""):
        self.password = self.hash_password(password)


class UserBuilder:
    def __init__(self):
        self.user = User()

    def reset(self) -> User():
        self.user = User()
        return self.user

    def get_object(self) -> User:
        return self.user

    def set_login(self, login: str = ""):
        self.user.login = login

    def set_password(self, password: str = ""):
        self.user.set_password(password)
