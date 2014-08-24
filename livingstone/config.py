from contextlib import contextmanager
import sqlite3

from utils import Store

ctx = Store()

@contextmanager
def connect(db_path, readonly=False):
    # delayed import to break import loop with models.py
    from models import init_sql, Keyword, Document
    try:
        db_path = 'file:%s%s' % (db_path, '?mode=ro' if readonly else '')
        connection = sqlite3.connect(db_path, uri=True)
        ctx.cursor = connection.cursor()
        for query in init_sql:
            ctx.cursor.execute(query)
        yield connection

        # Flush cache
        Document.lru.clean(True)
        Keyword.lru.clean(True)

        connection.commit()
    except:
        connection.rollback()
        raise
    finally:
        connection.close()

def set_config():
    pass #TODO
