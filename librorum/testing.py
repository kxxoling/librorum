#!/usr/bin/env python
#coding:utf-8
import unittest
import redis
from engine import Librorum


class TestEngine(unittest.TestCase):
    def setUp(self):
        r = redis.Redis()
        structure = dict(t=int, n=int)
        self.structure = structure
        self.lib = Librorum(r, structure=structure)
        self.items = [
            dict(uid=1, term=u'清华大学', t=1),
            dict(uid=33, term=u'北京大学', t=0, n=4),
            dict(uid=23, term=u'QsingHua大学', t=1, n=4),
            dict(uid=13, term=u'Peiking Univ', t=0, n=4),
        ]
        for item in self.items:
            self.lib.add_item(item)

    def tearDown(self):
        pass

    def test_dbs(self):
        assert '_idx_t=1' in self.lib.dbs(self.items[0])
        assert '_idx_t=0' in self.lib.dbs(self.items[1])
        assert '_idx_n=4' in self.lib.dbs(self.items[1])

    def test_retrieve(self):
        item, item2, item3, item4 = self.items

        kwargs = dict(t=1)
        assert item['uid'] in self.lib.retrieve('qinghua', **kwargs)
        assert item['uid'] in self.lib.retrieve('qingh', **kwargs)
        assert item['uid'] in self.lib.retrieve('qh', **kwargs)
        assert item['uid'] in self.lib.retrieve('清华', **kwargs)
        assert item['uid'] in self.lib.retrieve('清华大', **kwargs)
        assert item['uid'] in self.lib.retrieve('qhdx', **kwargs)

        assert item2['uid'] not in self.lib.retrieve('qinghua', **kwargs)
        assert item2['uid'] not in self.lib.retrieve('qingh', **kwargs)
        assert item2['uid'] not in self.lib.retrieve('qh', **kwargs)
        assert item2['uid'] not in self.lib.retrieve('清华', **kwargs)
        assert item2['uid'] not in self.lib.retrieve('清华大', **kwargs)
        assert item2['uid'] not in self.lib.retrieve('qhdx', **kwargs)

        assert item3['uid'] in self.lib.retrieve('Qsinghua', **kwargs)
        assert item3['uid'] in self.lib.retrieve('qsing daxue', **kwargs)

        kwargs2 = dict(t=0)
        assert item2['uid'] in self.lib.retrieve('bj', **kwargs2)
        assert item2['uid'] in self.lib.retrieve('beij', **kwargs2)
        assert item2['uid'] in self.lib.retrieve('bjdx', **kwargs2)
        assert item2['uid'] in self.lib.retrieve('北京', **kwargs2)
        assert item2['uid'] in self.lib.retrieve('北京大', **kwargs2)
        assert item2['uid'] in self.lib.retrieve('北京大学', **kwargs2)

        assert item4['uid'] in self.lib.retrieve('Peiking', **kwargs2)
        assert item4['uid'] in self.lib.retrieve('peiking', **kwargs2)
        assert item4['uid'] in self.lib.retrieve('Peiking univ', **kwargs2)

    def test_search(self):
        result = self.lib.search('beijing')
        assert len(result) == 1
        for item in result:
            assert item['uid'] in [33]


if __name__ == '__main__':
    unittest.main()
