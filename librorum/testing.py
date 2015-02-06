#!/usr/bin/env python
#coding:utf-8
import unittest
import redis
from engine import Librorum

items = [
    dict(uid=1, term=u'清华大学', t=1),
    dict(uid=2, term=u'北京大学', t=0, n=4),
    dict(uid=3, term=u'QsingHua大学', t=1, n=4),
    dict(uid=4, term=u'Peiking Univ', t=0, n=4),
    dict(uid=5, term=u'北京', t=0, n=4),
    dict(uid=6, term=u'北京大学医学部', t=0, n=4),
    dict(uid=7, term=u'百度', t=0, n=4),
    dict(uid=8, term=u'百度投资', t=0, n=4),
    dict(uid=9, term=u'百度金融机构', t=0, n=4),
]

item, item2, item3, item4, item5, item6, item7, item8, item9 = items

LIMIT = 3


class TestEngine(unittest.TestCase):
    def setUp(self):
        r = redis.Redis()
        structure = dict(t=int, n=int)
        self.structure = structure
        self.lib = Librorum(r, structure=structure)
        for item in items:
            self.lib.add_item(item)

    def tearDown(self):
        pass

    def test_dbs(self):
        assert '_idx_t=1' in self.lib.dbs(items[0])
        assert '_idx_t=0' in self.lib.dbs(items[1])
        assert '_idx_n=4' in self.lib.dbs(items[1])

    def test_retrieve(self):

        kwargs = dict(t=1)
        assert item['uid'] in self.lib.retrieve(u'qinghua', **kwargs)
        assert item['uid'] in self.lib.retrieve(u'qingh', **kwargs)
        assert item['uid'] in self.lib.retrieve(u'qh', **kwargs)
        assert item['uid'] in self.lib.retrieve(u'清华', **kwargs)
        assert item['uid'] in self.lib.retrieve(u'清华大', **kwargs)
        assert item['uid'] in self.lib.retrieve(u'qhdx', **kwargs)

        assert item2['uid'] not in self.lib.retrieve(u'qinghua', **kwargs)
        assert item2['uid'] not in self.lib.retrieve(u'qingh', **kwargs)
        assert item2['uid'] not in self.lib.retrieve(u'qh', **kwargs)
        assert item2['uid'] not in self.lib.retrieve(u'清华', **kwargs)
        assert item2['uid'] not in self.lib.retrieve(u'清华大', **kwargs)
        assert item2['uid'] not in self.lib.retrieve(u'qhdx', **kwargs)

        assert item3['uid'] in self.lib.retrieve(u'Qsinghua', **kwargs)
        assert item3['uid'] in self.lib.retrieve(u'qsing daxue', **kwargs)

        kwargs2 = dict(t=0)
        assert item2['uid'] in self.lib.retrieve(u'bj', **kwargs2)
        assert item2['uid'] in self.lib.retrieve(u'beij', **kwargs2)
        assert item2['uid'] in self.lib.retrieve(u'bjdx', **kwargs2)

        assert item5['uid'] is self.lib.retrieve(u'北京', **kwargs2)[0]

        assert item2['uid'] in self.lib.retrieve(u'北京大', **kwargs2)
        assert item2['uid'] in self.lib.retrieve(u'北京大学', **kwargs2)

        assert item4['uid'] in self.lib.retrieve(u'Peiking', **kwargs2)
        assert item4['uid'] in self.lib.retrieve(u'peiking', **kwargs2)
        assert item4['uid'] in self.lib.retrieve(u'Peiking univ', **kwargs2)

        assert item7['uid'] is self.lib.retrieve(u'baidu')[0]
        assert item7['uid'] is self.lib.retrieve(u'百度')[0]


        b_result = self.lib.retrieve('b', limit=LIMIT)
        assert len(b_result) <= LIMIT

        beijing_result = self.lib.retrieve('beijing', limit=LIMIT)
        assert len(beijing_result) <= LIMIT

    def test_search(self):
        result = self.lib.search(u'beijing')
        for item in result:
            assert item['uid'] in [item2['uid'], item5['uid'], item6['uid']]

        b_result = self.lib.search('b', limit=LIMIT)
        assert len(b_result) <= LIMIT

        beijing_result = self.lib.search('北京', limit=LIMIT)
        print beijing_result
        assert len(beijing_result) <= LIMIT
        b_result = self.lib.retrieve('b', limit=LIMIT)
        assert len(b_result) <= LIMIT

        beijing_result = self.lib.retrieve('beijing', limit=LIMIT)
        assert len(beijing_result) <= LIMIT

    def test_flush(self):
        assert self.lib.redis.exists(self.lib.database) is True
        self.lib.flush()

        assert len(self.lib.retrieve('b')) is 0
        assert len(self.lib.retrieve('q')) is 0
        assert self.lib.redis.exists(self.lib.database) is False


if __name__ == '__main__':
    unittest.main()
