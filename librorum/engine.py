#!/usr/bin/env python
#coding:utf-8
import json
from pypinyin import lazy_pinyin, NORMAL
import jieba


class Librorum(object):
    """doc here"""
    RESERVED_WORDS = set(('db', 'idx', 'uid', 'term', 'limit'))       # 索引保留字，数据结构中禁止使用

    def __init__(self, redis_conn, engine_name=None, structure=None, cached=False):
        self.cached = cached
        if engine_name is None:
            engine_name = ''
        self.structure = structure
        self.database = '%s_db' % engine_name
        self.indexbase = '%s_idx' % engine_name
        self.redis = redis_conn
        if structure is not None and self.RESERVED_WORDS.intersection(structure.keys()):
            raise Exception('structure 中不可存在保留字（%s）！' % str(self.RESERVED_WORDS))

    def search(self, term, limit=-1):
        """根据 retrieve 的结果从数据库中取值"""
        term = ''.join(lazy_pinyin(term))
        result = self.retrieve(term, limit)

        if result is []:
            return []
        return map(json.loads, self.redis.hmget(self.database, *result)[1:])

    def retrieve(self, word, limit=-1, **kwargs):
        """根据字符串匹配索引"""
        word = word.lower()
        words = filter(lambda x: x and True or False, word.split(' '))

        rtv_key = "%s_%s" % (self.indexbase, word)
        rtv_keys = map(lambda word: "%s_%s"%(self.indexbase, word), words)
        rtv_keys.extend(self.dbs(kwargs))

        self.redis.zinterstore(rtv_key, rtv_keys)

        return map(int, self.redis.zrevrange(rtv_key, 0, limit))

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
        self.index_term(term, uid)

    def index_term(self, term, uid=None):
        """先分词再索引"""
        if uid is None:
            uid = term['uid']
        words = jieba.cut_for_search(term)
        map(lambda s: self.index_cn_word(uid, s), words)

    def index_cn_word(self, uid, cn_word, base_score=0):
        """以中文词为单位，不再分词"""
        pinpin_words = lazy_pinyin(cn_word, NORMAL)
        pinyin = ''.join(pinpin_words)
        py = ''.join(map(lambda x: x[0], pinpin_words))

        self.index_word(uid, cn_word, score=base_score+3)
        self.index_word(uid, pinyin, score=base_score+2)
        self.index_word(uid, py, score=base_score+1)

    def index_word(self, uid, word, **kwargs):
        """索引单词和词语"""
        for i in range(1, len(word)+1):
            self.index(uid, word[:i], **kwargs)

    def index(self, uid, term, score=None):
        """索引一个字符串，如果传入 score 参数，则存为 zset，否则存为 set"""
        term = term.lower()
        if score is None:
            self.redis.sadd('%s_%s'%(self.indexbase, term), uid)
        else:
            self.redis.zadd('%s_%s'%(self.indexbase, term), uid, score)

    def store(self, item, uid=None):
        if not uid:
            uid = item.get(uid)
        assert uid is not None

        for db in self.dbs(item):
            self.redis.sadd(db, uid)

        self.redis.hset(self.database, uid, json.dumps(item))

    def dbs(self, item):
        dbs = []
        if self.structure is not None:
            for k in self.structure:
                v = item.get(k)
                if v is not None:
                    dbs.append('%s_%s=%s' % (self.indexbase, k, item[k]))
        return dbs

    def del_index(self):
        """暂未实现"""
        prefixs = self.redis.smembers(self.indexbase)
        for prefix in prefixs:
            self.redis.delete(prefix)
        self.redis.delete(self.database)
        self.redis.delete(self.indexbase)

    def del_item(self, uid):
        pass
