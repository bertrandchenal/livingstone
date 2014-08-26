from html.parser import HTMLParser
from urllib import request
from urllib.parse import urljoin, urlsplit, urlunsplit
import codecs
import subprocess
import re

from utils import to_ascii
from config import ctx

word_re = re.compile('[\W+]')

def get_words(data):
    for w in word_re.split(data):
        w = to_ascii(w)
        w.strip()
        if 1 < len(w) < 20:
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

def parse_html_url(url):
    f = request.urlopen(url)
    html = f.read()
    parser = DataHTMLParser(['script'], url)
    html = parser.unescape(html.decode())
    parser.feed(html)
    return parser

def parse_text_file(path):
    encoding = ctx.encoding
    with codecs.open(path, encoding=encoding) as f:
        try:
            data = f.read()
        except UnicodeDecodeError:
            print('Unable to load %s as %s' % (path, encoding))
            return None, None
        words = get_words(data)
        return data, set(words)

def parse_pdf_file(path):
    out = subprocess.check_output(['pdftotext', '-enc', 'UTF-8', path, '-'])
    try:
        data = out.decode(ctx.encoding)
    except UnicodeDecodeError:
        print('Unable to load %s as %s' % (path, ctx.encoding))
        return None, None

    words = set(get_words(data))
    return data, words
