# coding: utf-8
import json
from pypinyin import lazy_pinyin, NORMAL
import jieba


class Librorum(object):
    """ A search engine for phrase autocompletition and searching engine based on Redis
    """
    RESERVED_WORDS = set(('db', 'idx', 'uid', 'term', 'limit'))       # Reserved word by now

    def __init__(self, redis_conn, **kwargs):
        self.config = dict(
            engine_name='',
            structure=dict(),
            cached=False,
            base_score=1,
        )
        self.config.update(kwargs)

        self.database = '%s_db' % self.config['engine_name']
        self.indexbase = '%s_idx' % self.config['engine_name']
        self.redis = redis_conn

        if self.RESERVED_WORDS.intersection(self.config['structure'].keys()):
            raise Exception('structure 中不可存在保留字（%s）！' % str(self.RESERVED_WORDS))

    def search(self, term, **kwargs):
        """ Get the result from database, accepted args are:
        term: word for searching
        limit: how many results you need
        offset: the offset of searching result
        """
        term = ''.join(lazy_pinyin(term))
        result = self.retrieve(term, **kwargs)

        if len(result) is 0:
            return []
        return list(map(lambda s: json.loads(s.decode()),
                        self.redis.hmget(self.database, *result)))

    def retrieve(self, word, limit=0, offset=0, **kwargs):
        """ Get uid list by word, args are the same as self.search """
        word = word.lower()
        words = filter(lambda x: x and True or False, word.split(' '))

        rtv_key = "%s_%s" % (self.indexbase, word)
        rtv_keys = list(map(lambda word: "%s_%s" % (self.indexbase, word), words))
        rtv_keys.extend(self.dbs(kwargs))

        self.redis.zinterstore(rtv_key, rtv_keys)

        return list(map(int, self.redis.zrange(rtv_key, offset, limit-1)))

    def add_item(self, item):
        """ Add new item to database. Item excepted as dict type. Steps:
        1. Save the item to database
        2. Word segementation by blank and Jieba for Chinese
        3. Save the indexes to indexbase
        """
        uid = item['uid']
        term = item.get('term')
        if term is None:
            return
        self.store(item, uid)
        self.index(term, uid)

    def index(self, term, uid, score=1):
        """ Index term and uid with base score
        """
        term = term.lower()

        for k, v in get_indexes(term).items():
            self._index(k, v, uid, score)

    def _index(self, term, weight, uid, score):
        if weight in (0, None):
            pass
        else:
            self.redis.zadd('%s_%s' % (self.indexbase, term),
                            **{str(uid): weight*score})

    def store(self, item, uid=None):
        if not uid:
            uid = item.get(uid)
        assert uid is not None

        for db in self.dbs(item):
            self.redis.sadd(db, uid)

        self.redis.hset(self.database, uid, json.dumps(item))

    def dbs(self, item):
        dbs = []

        for k in self.config['structure']:
            v = item.get(k)
            if v is not None:
                dbs.append('%s_%s=%s' % (self.indexbase, k, item[k]))

        return dbs

    def flush(self):
        """ Clean all the keys used by Librorum """
        prefixs = self.redis.keys('%s*' % self.indexbase)
        for prefix in prefixs:
            self.redis.delete(prefix)
        self.redis.delete(self.database)
        self.redis.delete(self.indexbase)

    def del_item(self, uid):
        pass


def get_indexes(term):
    """ Get all the indexes of `term` """
    cn_words = term.split(' ')
    _ = []
    for word in cn_words:
        _.extend(jieba.cut_for_search(word, HMM=True))
    _ = list(set(_))

    words_count = len(_)

    words_indexes = multi(words_count)(merge_dicts_by_weight(map(split_cn_word, _)))
    return merge_dicts_by_weight([words_indexes, {''.join(cn_words): 1}])


def split_cn_word(cn_word):
    """ Get all the index-weight pairs of a Chinese word """
    pinyin_word = lazy_pinyin(cn_word, NORMAL)
    pinyin = ''.join(pinyin_word)
    py = ''.join(map(lambda x: x[0], pinyin_word))

    word_indexes = split_word(cn_word)
    pinyin_indexes = multi(2)(split_word(pinyin))
    py_indexes = multi(3)(split_word(py))

    return merge_dicts_by_weight([word_indexes, pinyin_indexes, py_indexes])


def split_word(word):
    """ Get all the index-weight pairs of a word
    """
    word_len = len(word)
    _ = {}
    for i in range(1, word_len+1):
        _[word[:i]] = word_len/i

    return _


def merge_dicts_by_weight(dicts):
    """ Merge dicts by the lowest weight """
    _ = {}
    for d in dicts:
        for k in d:
            existing_value = _.get(k)
            if existing_value is None or existing_value > d[k]:
                _[k] = d[k]
    return _


def multi(num):
    def func(item):
        for k in item:
            item[k] *= num
        return item
    return func
