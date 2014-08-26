from config import ctx
from utils import (LRU, from_bytes, to_bytes, compress, decompress, ranks,
                   to_ascii, get_match_context, limit_offset)
from parser import parse_text_file, parse_pdf_file

# TODO indexes on score
init_sql = [
    'CREATE TABLE IF NOT EXISTS keyword (id INTEGER PRIMARY KEY, word VARCHAR, '
    'score INTEGER, documents BLOB, neighbours BLOB)',

    'CREATE UNIQUE INDEX IF NOT EXISTS keyword_index on keyword (word)',

    'CREATE TABLE  IF NOT EXISTS document '
    '(id INTEGER PRIMARY KEY, uri VARCHAR, score INTEGER, content TEXT)',

    'CREATE UNIQUE INDEX IF NOT EXISTS uri_index on document (uri)',
]

class Keyword:

    def __init__(self, id, word, score=0, documents=None, neighbours=None):
        self.id = id
        self.word = word
        self.score = score
        self.documents = documents
        self.neighbours = neighbours
        self.dirty = False

    @classmethod
    def write(cls, word, keyword):
        if not keyword.dirty:
            return

        documents = to_bytes(keyword.documents)
        neighbours = to_bytes(keyword.neighbours)
        ctx.cursor.execute('UPDATE keyword '
                           'SET score = ?, documents = ?, neighbours = ? '
                           'WHERE id = ?',
                           (keyword.score, documents, neighbours, keyword.id))
    @classmethod
    def read(cls, word):
        ctx.cursor.execute('SELECT id, score, documents, neighbours '
                    'FROM keyword WHERE word = ?', (word,))
        return next(ctx.cursor, None)

    @classmethod
    def create(cls, word):
        ctx.cursor.execute('INSERT INTO keyword (word) VALUES (?)', (word,))
        return ctx.cursor.lastrowid

    lru = LRU(size=10000)

    @classmethod
    def get(cls, word, readonly=False):
        kw = cls.lru.get(word)
        if not kw:
            # Not in lru -> read db
            res = cls.read(word)
            if res is None:
                if readonly:
                    return None
                # Not yet in db -> create row
                documents = neighbours = 0
                id = cls.create(word)
                score = 0
            else:
                id, score, documents, neighbours = res
                documents = 0 if documents is None else from_bytes(documents)
                neighbours = 0 if neighbours is None else from_bytes(neighbours)
                score = 0 if score is None else score
            kw = Keyword(id, word, score, documents, neighbours)
            cls.lru.set(word, kw)
        return kw

    def update(self, doc, neighbours):
        self.dirty = True
        # Update documents array
        self.score += 1
        self.documents |= 1 << doc.id
        self.neighbours |= neighbours

    @classmethod
    def suggest(cls, word):
        limit, offset = limit_offset()
        ctx.cursor.execute(
            "SELECT word from keyword WHERE word like '%s%%' "\
            "ORDER BY word "\
            "limit %s offset %s" % (word, limit, offset))

        for w, in ctx.cursor:
            yield w

    @classmethod
    def neighbours(cls, words):
        kw_array = None
        for word in words:
            word = to_ascii(word)

            kw = Keyword.get(word, readonly=True)
            if not kw:
                return
            if kw_array is None:
                kw_array = kw.neighbours
            else:
                kw_array &= kw.neighbours

        ids = list(ranks(kw_array))
        ctx.cursor.execute(
            'SELECT score, word from keyword WHERE id in (%s) ' \
            'ORDER BY score asc limit 30' % \
            ','.join(str(i) for i in ids))
        yield from ctx.cursor

class Document:

    def __init__(self, id, uri, score=0, content=None):
        self.id = id
        self.uri = uri
        self.score = score
        self.content = content
        self.dirty = False

    @classmethod
    def write(cls, uri, document):
        if not document.dirty:
            return

        content = compress(document.content.encode())
        ctx.cursor.execute('UPDATE document SET content = ?, score = ? '
                           'WHERE id = ?',
                           (content, document.score, document.id))
    @classmethod
    def read(cls, uri):
        ctx.cursor.execute('SELECT id, score, content FROM document '
                           'WHERE uri = ?', (uri,))
        return next(ctx.cursor, None)

    @classmethod
    def create(cls, uri):
        ctx.cursor.execute('INSERT INTO document (uri) VALUES (?)', (uri,))
        return ctx.cursor.lastrowid

    lru = LRU(size=1000)

    @classmethod
    def get(cls, uri):
        doc = cls.lru.get(uri)
        if not doc:
            # Not in lru -> read db
            res = cls.read(uri)
            if res is None:
                # Not yet in db -> create row
                id = cls.create(uri)
                score = 0
                content = ''
            else:
                id, score, content = res
                content = decompress(content).decode()
            doc = Document(id, score, uri)
            cls.lru.set(uri, doc)
        return doc

    @classmethod
    def search(cls, words):
        doc_array = None
        words = [to_ascii(w) for w in  words]
        for word in words:
            kw = Keyword.get(word, readonly=True)
            if not kw:
                continue
            if doc_array is None:
                doc_array = kw.documents
            else:
                doc_array &= kw.documents

        limit, offset = limit_offset()
        ids = list(ranks(doc_array))
        ctx.cursor.execute(
            'SELECT uri, content from document WHERE id in (%s) ' \
            'ORDER BY score desc limit %s offset %s' % (
                ','.join(str(i) for i in ids), limit, offset)
        )
        for uri, content in ctx.cursor:
            match = None
            for line in decompress(content).splitlines():
                line = line.decode()
                idx = to_ascii(line).find(words[0])
                if idx < 0:
                    continue
                match = get_match_context(idx, line)
                break
            yield uri, match

    @classmethod
    def load_file(cls, path):
        if path.endswith('.pdf'):
            content, words = parse_pdf_file(path)
        else:
            content, words = parse_text_file(path)
        if not content:
            return
        doc = Document.get(path)
        doc.dirty = True
        doc.content = content

        # Build neighbours
        neighbours = 0
        for word in words:
            kw = Keyword.get(word)
            neighbours |= 1 << kw.id
        # Update all keywords
        for word in words:
            kw = Keyword.get(word)
            kw.update(doc, neighbours)

# Plug lru discard methods
Keyword.lru.discard = Keyword.write
Document.lru.discard = Document.write
