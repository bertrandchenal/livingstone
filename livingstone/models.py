from config import ctx
from utils import (LRU, from_bytes, to_bytes, compress, decompress, ranks,
                   to_ascii, get_match_context, limit_offset, log)
from parser import load

# TODO indexes on score
init_sql = [
    'CREATE TABLE IF NOT EXISTS keyword (id INTEGER PRIMARY KEY, word VARCHAR, '
    'score INTEGER, documents BLOB, neighbours BLOB)',
    'CREATE UNIQUE INDEX IF NOT EXISTS keyword_index on keyword (word)',

    'CREATE TABLE IF NOT EXISTS document '
    '(id INTEGER PRIMARY KEY, '
    'uri VARCHAR, '
    'score INTEGER, '
    'distance INTEGER, '
    'referer INTEGER, '
    'content TEXT, '
    'FOREIGN KEY(referer) REFERENCES document(id)'
    ')',
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
            'SELECT score, word from keyword ' \
            'WHERE id in (%s) AND score > 1 '\
            'ORDER BY score asc limit 30' % \
            ','.join(str(i) for i in ids))
        yield from ctx.cursor

class Document:

    def __init__(self, id, uri, **kw):
        self.id = id
        self.uri = uri
        self.score = kw.get('score', 0)
        self.content = kw.get('content', None)
        self.referer = kw.get('referer', None)
        self.distance = kw.get('distance', None)
        self.dirty = False

    @classmethod
    def write(cls, uri, document):
        if not document.dirty:
            return
        content = compress(document.content)
        ctx.cursor.execute(
            'UPDATE document SET '
            'content = ?, '
            'score = ?, '
            'referer = ?, '
            'distance = ? '
            'WHERE id = ?',
            (content, document.score, document.referer, document.distance,
             document.id))

    @classmethod
    def read(cls, uri):
        ctx.cursor.execute(
            'SELECT id, score, content, referer, distance '
            'FROM document WHERE uri = ?', (uri,))

        res = next(ctx.cursor, None)
        if res is None:
            return None

        id, score, content, referer, distance = res
        if content is not None:
            content = decompress(content)

        return Document(id, uri, score=score, content=content,
                        referer=referer, distance=distance)

    @classmethod
    def delete(cls, uri):
        ctx.cursor.execute('DELETE FROM document WHERE uri = ?', (uri,))

    @classmethod
    def create(cls, uri):
        ctx.cursor.execute('INSERT INTO document (uri) VALUES (?)', (uri,))
        id = ctx.cursor.lastrowid
        return Document(id, uri)

    lru = LRU(size=1000)

    @classmethod
    def get(cls, uri):
        doc = cls.lru.get(uri)
        if not doc:
            # Not in lru -> read db
            doc = cls.read(uri)
            if doc is None:
                # Not yet in db -> create row
                doc = cls.create(uri)
            cls.lru.set(uri, doc)

        return doc

    @classmethod
    def search(cls, words):
        doc_array = None
        words = [to_ascii(w) for w in  words]
        for word in words:
            kw = Keyword.get(word, readonly=True)
            if not kw:
                return
            if doc_array is None:
                doc_array = kw.documents
            else:
                doc_array &= kw.documents

        limit, offset = limit_offset()
        ids = list(ranks(doc_array))
        ctx.cursor.execute(
            'SELECT uri, content from document ' \
            'WHERE content is not null and id in (%s) ' \
            'ORDER BY score desc limit %s offset %s' % (
                ','.join(str(i) for i in ids), limit, offset)
        )
        for uri, content in ctx.cursor:
            match = None
            for line in decompress(content).splitlines():
                idx = to_ascii(line).find(words[0])
                if idx < 0:
                    continue
                match = get_match_context(idx, line)
                break
            yield uri, match

    @classmethod
    def load_file(cls, path):
        content, words, links = load(path)
        if not content:
            return False

        if ctx.collect_links:
            nb = len(links)
            # Store links
            cls.store_links(links)
            if nb > 1:
                log('%s links collected' % nb, 'green')
            else:
                log('%s link collected' % nb, 'green')
            return True

        doc = Document.get(path)
        if doc.distance is None:
            doc.distance = 0

        doc.dirty = True
        doc.content = content
        log('Document %s loaded (distance: %s, score: %s)' % (
            path, doc.distance, doc.score),
            'green')

        cls.store_links(links)

        # Build neighbours
        neighbours = 0
        for word in words:
            kw = Keyword.get(word)
            neighbours |= 1 << kw.id

        # Update all keywords
        for word in words:
            kw = Keyword.get(word)
            kw.update(doc, neighbours)

        return True

    @classmethod
    def store_links(cls, links, referer=None):
        ref_dist = referer.distance + 1 if referer else 1
        ref_id = referer.id if referer else None

        for link in links:
            new = Document.get(link)
            new.dirty = True
            new.score += 1
            if new.distance is None or new.distance > ref_dist:
                new.distance = ref_dist
                new.newerer = ref_id

    @classmethod
    def crawl(cls):
        ctx.cursor.execute('SELECT uri from document WHERE content is null '
                           'ORDER BY distance asc, score desc, id asc '
                           'LIMIT 10')
        rows = list(ctx.cursor)
        for row in rows:
            uri, = row
            success = cls.load_file(uri)
            if success:
                continue
            cls.delete(uri)

# Plug lru discard methods
Keyword.lru.discard = Keyword.write
Document.lru.discard = Document.write
