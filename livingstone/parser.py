from html.parser import HTMLParser
from urllib import request
from urllib.error import URLError
from urllib.parse import urljoin, urlsplit, urlunsplit
import codecs
import socket
import subprocess
import re

from utils import to_ascii, log
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
        self.content = ''
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

                # generate canonical url
                url = urlunsplit((scheme, netloc, path, query, ''))
                scheme, *_ = urlsplit(url)
                if scheme not in ('http', 'https'):
                    continue
                self.links.add(url)

    def handle_data(self, data):
        data = data.strip()
        if not data:
            return

        if self.current_tag in self.ignore_tags:
            return
        data = self.unescape(data)
        self.content += data
        self.words.update(get_words(data))


def load(uri):
    content_type = None
    charset = None
    scheme, *_ = urlsplit(uri)
    if scheme:
        try:
            # Urllib expect ascii urls
            uri = to_ascii(uri).decode()
            f = request.urlopen(uri, timeout=5)
        except (URLError, socket.error):
            log('Unable to download %s' % uri, color='red')
            return None, None, None

        info = f.info()
        content_type = info.get_content_type()

        if content_type == 'application/pdf':
            log('PDF dowload not yet supported (%s)' % uri, color='brown')
            return None, None, None

        charset = info.get_charset() or 'ISO-8859-1'
        data = f.read().decode(charset)

    else:
        data = read_data(uri)
        if uri.endswith('.html'):
            content_type = 'text/html'
        elif uri.endswith('.pdf'):
            content_type = 'application/pdf'
        else:
            content_type = 'text/plain'

    if not data:
        log('No content in %s' % uri, color='red')
        return None, None, None

    if content_type == 'text/html':
        return parse_html(data, uri)
    elif content_type == 'text/plain':
        return parse_text(data)
    elif content_type == 'application/pdf':
        return parse_pdf_file(uri)

    else:
        log('Content Type "%s" not supported (%s)' % (content_type, uri) ,
            color='brown')

    return None, None, None

def read_data(path):
    encoding = ctx.encoding
    with codecs.open(path, encoding=encoding) as f:
        try:
            data = f.read()
        except UnicodeDecodeError:
            log('Unable to load %s as %s' % (path, encoding), color='red')
            return None
    return data

def parse_html(data, url):
    parser = DataHTMLParser(['script'], url)
    html = parser.unescape(data)
    parser.feed(html)
    return data, parser.words, set(parser.links)

def parse_text(data):
    words = get_words(data)
    return data, set(words), None


def parse_pdf_file(path):
    out = subprocess.check_output(['pdftotext', '-enc', 'UTF-8', path, '-'])
    try:
        data = out.decode(ctx.encoding)
    except UnicodeDecodeError:
        print('Unable to load %s as %s' % (path, ctx.encoding))
        return None, None

    words = set(get_words(data))
    return data, words, None
