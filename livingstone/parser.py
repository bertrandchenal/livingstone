from html.parser import HTMLParser
from urllib import request
from urllib.parse import urljoin, urlsplit, urlunsplit
from unicodedata import normalize
import re

word_re = re.compile('\W+')

def get_words(data):
    for w in word_re.split(data):
        w = normalize('NFKD', w).encode('ascii', 'ignore').lower()
        w.strip()
        if w:
            yield w

class DataHTMLParser(HTMLParser):

    def __init__(self, ignore_tags=[], referrer=None):
        self.ignore_tags = ignore_tags
        self.current_tag = None
        self.words = set()
        self.referrer = referrer
        self.links = set()
        super(DataHTMLParser, self).__init__()

    def handle_starttag(self, tag, attrs):
        self.current_tag = tag
        if tag == 'a':
            for k,v in attrs:
                if k != 'href':
                    continue
                # Complete partial urls with referrer
                url = urljoin(self.referrer, v)
                # remove fragment
                scheme, netloc, path, query, fragment = urlsplit(url)
                url = urlunsplit((scheme, netloc, path, query, ''))
                self.links.add(url)

    def handle_data(self, data):
        data = data.strip()
        if not data:
            return

        if self.current_tag in self.ignore_tags:
            return
        data = self.unescape(data)
        self.words.update(get_words(data))

def parse_url(url):
    f = request.urlopen(url)
    html = f.read()
    parser = DataHTMLParser(['script'], url)
    html = parser.unescape(html.decode())
    parser.feed(html)
    return parser

def parse_file(path):
    with open(path) as f:
        try:
            data = f.read()
        except UnicodeDecodeError:
            print('Unable to load %s as unicode' % path)
            return None, None
        words = get_words(data)
        return data, set(words)
