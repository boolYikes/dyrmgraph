# TODO: Expand this to use disinfo REST APIs
DISINFO_SOURCES = {
  'sputniknews.com',
  'rt.com',
  'tass.ru',
  'globaltimes.cn',
}


def label(url):
  return any(src in url for src in DISINFO_SOURCES)
