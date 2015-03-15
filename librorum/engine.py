#!/usr/bin/env python
#coding:utf-8
import json
from pypinyin import lazy_pinyin, NORMAL
import jieba


class Librorum(object):
    """doc here"""
    RESERVED_WORDS = set(('db', 'idx', 'uid', 'term', 'limit'))       # 索引保留字，数据结构中禁止使用

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

    def search(self, term, limit=0):
        """根据 retrieve 的结果从数据库中取值"""
        term = ''.join(lazy_pinyin(term))
        result = self.retrieve(term, limit)

        if len(result) is 0:
            return []
        return map(json.loads, self.redis.hmget(self.database, *result))

    def retrieve(self, word, limit=0, **kwargs):
        """根据字符串匹配索引"""
        word = word.lower()
        words = filter(lambda x: x and True or False, word.split(' '))

        rtv_key = "%s_%s" % (self.indexbase, word)
        rtv_keys = map(lambda word: "%s_%s"%(self.indexbase, word), words)
        rtv_keys.extend(self.dbs(kwargs))

        self.redis.zinterstore(rtv_key, rtv_keys)

        return map(int, self.redis.zrange(rtv_key, 0, limit-1))

    def add_item(self, item):
        """
        1. 将item 信息存储在 database 中
        2. 将 term 分词
        3. 将分词以不同的权重存储在 indexbase 中
        """
        uid = item['uid']
        term = item.get('term')
        if term is None:
            return
        self.store(item, uid)
        self.index(term, uid)

    def index(self, term, uid, score=1):
        """索引一个字符串，如果传入 score 参数，则存为 zset，否则存为 set"""
        term = term.lower()

        for k, v in get_indexes(term).iteritems():
            self._index(k, v, uid, score)

    def _index(self, term, weight, uid, score):
        if weight in (0, None):
            pass
        else:
            self.redis.zadd('%s_%s'%(self.indexbase, term),
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
        """暂未实现"""
        prefixs = self.redis.keys('%s*' % self.indexbase)
        for prefix in prefixs:
            self.redis.delete(prefix)
        self.redis.delete(self.database)
        self.redis.delete(self.indexbase)

    def del_item(self, uid):
        pass


def get_indexes(term):
    """ 对给定短语进行分词、拼音化等操作，获取其所有可能索引
    """
    cn_words = term.split(' ')
    _ = []
    for word in cn_words:
        _.extend(jieba.cut_for_search(word, HMM=True))
    _ = list(set(_))

    words_count = len(_)

    words_indexes = multi(words_count)(merge_dicts_by_weight(map(split_cn_word, _)))
    return merge_dicts_by_weight([words_indexes, {''.join(cn_words): 1}])


def split_cn_word(cn_word):
    """ 获取中文词语所有补全形式，进行拼音、分词等操作，但不包括各种索引。
    """
    pinyin_word = lazy_pinyin(cn_word, NORMAL)
    pinyin = ''.join(pinyin_word)
    py = ''.join(map(lambda x: x[0], pinyin_word))

    word_indexes = split_word(cn_word)
    pinyin_indexes = multi(2)(split_word(pinyin))
    py_indexes = multi(3)(split_word(py))

    return merge_dicts_by_weight([word_indexes, pinyin_indexes, py_indexes])


def split_word(word):
    """ 获取英文字词所有补全形式，不包括各种索引，不进行拼音、分词等操作。
    """
    word_len = len(word)
    _ = {}
    for i in range(1, word_len+1):
        _[word[:i]] = word_len/i

    return _


def merge_dicts_by_weight(dicts):
    """ 合并多个权重字典，取最高权重
    """
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
