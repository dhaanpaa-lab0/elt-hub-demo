from os import path, environ
from urllib.parse import quote

from elt_util import chk_path


class EltRunEnvironment:
    def _sysroot(self):
        return path.curdir

    @property
    def pg_db(self):
        return environ.get("PG_DB")

    @property
    def pg_host(self):
        return environ.get("PG_HOST")

    @property
    def pg_port(self):
        return environ.get("PG_PORT")

    @property
    def pg_user(self):
        return environ.get("PG_USER")

    @property
    def pg_password(self):
        return quote(environ.get("PG_PASS"))

    @property
    def pg_url(self):
        return f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}"

    def _int_folder_path(self, *args):
        return chk_path(
            path.join(self._sysroot(), *[arg for arg in args if arg is not None])
        )

    def fldr_inbox(self, file_name=None):
        return self._int_folder_path("in", file_name)

    def fldr_outbox(self, file_name=None):
        return self._int_folder_path("out", file_name)

    def fldr_logs(self, file_name=None):
        return self._int_folder_path("logs", file_name)

    def __init__(self):
        pass
