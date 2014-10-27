from contextlib import contextmanager
import os
import sqlite3

class Store:

    def __init__(self, **data):
        self.__dict__.update(data)

    def update(self, data):
        self.__dict__.update(data)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def get(self, key, default=None):
        return getattr(self, key, default)

ctx = Store()

@contextmanager
def connect(db_path):
    readonly = ctx.readonly
    # delayed import to break import loop with models.py
    from models import init_sql, Keyword, Document
    connection = None
    try:
        db_path_mode = 'file:%s%s' % (db_path, '?mode=ro' if readonly else '')
        connection = sqlite3.connect(db_path_mode, uri=True)
        ctx.cursor = connection.cursor()
        for query in init_sql:
            ctx.cursor.execute(query)
        yield connection

        # Flush cache
        Document.lru.clean(True)
        Keyword.lru.clean(True)

        connection.commit()

    except sqlite3.OperationalError:
        connection and connection.rollback()
        if readonly and not os.path.exists(db_path):
            exit()
    except:
        connection and connection.rollback()

    finally:
        connection and connection.close()

