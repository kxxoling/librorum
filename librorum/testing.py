# coding: utf-8
import unittest
import redis
from engine import Librorum, get_indexes, split_cn_word, split_word, merge_dicts_by_weight


items = [
    dict(uid=1, term=u'清华大学', t=1),
    dict(uid=2, term=u'北京大学', t=0, n=4),
    dict(uid=3, term=u'QsingHua大学', t=1, n=4),
    dict(uid=4, term=u'Peiking Univ', t=0, n=4),
    dict(uid=5, term=u'北京', t=0, n=4),
    dict(uid=6, term=u'北京大学医学部', t=0, n=4),
    dict(uid=7, term=u'百度', t=0, n=4),
    dict(uid=8, term=u'百度投资', t=0, n=4),
    dict(uid=9, term=u'成都百度金融机构', t=0, n=4),
    dict(uid=10, term=u'成都百度', t=0, n=4),
    dict(uid=11, term=u'北戴河岸', t=0, n=4),
    dict(uid=12, term=u'北大青鸟', t=0, n=4),
]

item, item2, item3, item4, item5, item6, item7, item8, item9, item10, item11, item12 = items

LIMIT = 3


class TestUtilities(unittest.TestCase):

    def test_merge_dicts_by_weight(self):
        dict1 = dict(k1=2, k3=5, k2=4)
        dict2 = dict(k1=0, k3=5, k2=8)
        merged_dict = merge_dicts_by_weight([dict1, dict2])
        for k, v in merged_dict.items():
            self.assertLessEqual(merged_dict[k], dict1.get(k, 0))
            self.assertLessEqual(merged_dict[k], dict2.get(k, 0))

    def test_split_word(self):
        self.assertIn(u'大学', split_word(u'大学').keys())
        self.assertIn(u'清华大学', split_word(u'清华大学').keys())

    def test_split_cn_word(self):
        term = u'清华大学'
        indexes = split_cn_word(term).keys()
        self.assertIn(term, indexes)
        self.assertIn(u'清华大', indexes)

        baidu_dict = split_cn_word(u'百度')
        self.assertLess(baidu_dict[u'百度'], baidu_dict[u'baidu'])

    def test_get_indexes(self):
        term = items[0]['term']
        indexes = get_indexes(term).keys()
        self.assertIn(u'清华', indexes)
        self.assertIn(u'清华大学', indexes)
        self.assertIn(u'清华大', indexes)
        self.assertIn(u'qinghua', indexes)
        self.assertIn(u'qhd', indexes)
        self.assertIn(u'qh', indexes)
        self.assertIn(u'qhdx', indexes)
        self.assertIn(u'大学', indexes)
        self.assertIn(u'daxue', indexes)

        baidu_indexes = get_indexes(u'百度')
        self.assertLess(baidu_indexes[u'百度'], baidu_indexes[u'baidu'])
        self.assertLess(baidu_indexes[u'baidu'], baidu_indexes[u'bd'])


class TestEngine(unittest.TestCase):
    def setUp(self):
        r = redis.StrictRedis()
        structure = dict(t=int, n=int)
        self.structure = structure
        self.lib = Librorum(r, structure=structure)
        self.lib.flush()
        for item in items:
            self.lib.add_item(item)

    def tearDown(self):
        pass

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
        assert item3['uid'] in self.lib.retrieve(u'qsing', **kwargs)
        assert item3['uid'] in self.lib.retrieve(u'daxue', **kwargs)
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
        assert item8['uid'] is self.lib.retrieve(u'百度')[1] or item10['uid'] is self.lib.retrieve(u'百度')[1]
        assert item9['uid'] is self.lib.retrieve(u'百度')[3]

        # 略有瑕疵，稍后在重构版本中加入取最高权重功能
        assert item11['uid'] in self.lib.retrieve(u'beida')
        assert item12['uid'] in self.lib.retrieve(u'beida')

        b_result = self.lib.retrieve('b', limit=LIMIT)
        assert len(b_result) <= LIMIT

        beijing_result = self.lib.retrieve('beijing', limit=LIMIT)
        assert len(beijing_result) <= LIMIT

    def test_retrieve_limit(self):
        limit = 3
        for term in ['a', 'b', 'q']:
            self.assertGreaterEqual(limit, len(self.lib.retrieve(term, limit)))

    def test_retrieve_offset(self):
        limit = 3
        for term in ['a', 'b', 'q']:
            merged_list = self.lib.retrieve(term, limit, offset=0)
            merged_list.extend(self.lib.retrieve(term, limit=0, offset=limit))
            self.assertSequenceEqual(merged_list, self.lib.retrieve(term, limit=0, offset=0))

    def test_search(self):
        result = self.lib.search(u'beijing')
        for item in result:
            assert item['uid'] in [item2['uid'], item5['uid'], item6['uid']]

        b_result = self.lib.search('b', limit=LIMIT)
        assert len(b_result) <= LIMIT

        beijing_result = self.lib.search('北京', limit=LIMIT)
        assert len(beijing_result) <= LIMIT
        b_result = self.lib.retrieve('b', limit=LIMIT)
        assert len(b_result) <= LIMIT
        assert item7['uid'] is self.lib.search(u'baidu')[0]['uid']

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
