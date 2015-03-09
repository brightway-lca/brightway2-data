# -*- coding: utf-8 -*-
from . import BW2DataTest
from ..search import *
from ..backends.peewee import *


class SearchTest(BW2DataTest):
    def test_add_dataset(self):
        im = IndexManager()
        im.add_dataset({
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        })
        s = Searcher()
        self.assertTrue(s.search(u'lollipop', proxy=False))

    def test_search_dataset(self):
        im = IndexManager()
        im.add_dataset({
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        })
        s = Searcher()
        self.assertEqual(
            s.search(u'lollipop', proxy=False),
            [{'comment': u'', 'product': u'', 'name': u'lollipop',
              'database': u'foo', 'location': u'', 'key': u'foo\u22a1bar',
              'categories': u''}]
        )

    def test_update_dataset(self):
        im = IndexManager()
        ds = {
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        }
        im.add_dataset(ds)
        ds['name'] = u'lemon cake'
        im.update_dataset(ds)
        s = Searcher()
        self.assertEqual(
            s.search(u'lemon', proxy=False),
            [{'comment': u'', 'product': u'', 'name': u'lemon cake',
              'database': u'foo', 'location': u'', 'key': u'foo\u22a1bar',
              'categories': u''}]
        )

    def test_delete_dataset(self):
        im = IndexManager()
        ds = {
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        }
        im.add_dataset(ds)
        s = Searcher()
        self.assertTrue(s.search(u'lollipop', proxy=False))
        im.delete_dataset(ds)
        self.assertFalse(s.search(u'lollipop', proxy=False))

    def test_add_datasets(self):
        im = IndexManager()
        ds = [{
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        }]
        im.add_datasets(ds)
        s = Searcher()
        self.assertTrue(s.search(u'lollipop', proxy=False))

    def test_add_database(self):
        pass

    def test_delete_database(self):
        pass

    def test_search_faceting(self):
        im = IndexManager()
        ds = [{
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop',
            'location': u'CH',
        }, {
            'database': u'foo',
            'code': u'bar',
            'name': u'ice lollipop',
            'location': u'FR',
        }]
        im.add_datasets(ds)
        s = Searcher()
        res = s.search(u'lollipop', proxy=False, facet=u'location')
        self.assertEqual(
            res,
            {
                u'FR': [{'comment': u'', 'product': u'',
                         'name': u'ice lollipop', 'database': u'foo',
                         'location': u'FR', 'key': u'foo\u22a1bar',
                         'categories': u''}],
                u'CH': [{'comment': u'', 'product': u'', 'name': u'lollipop',
                         'database': u'foo', 'location': u'CH',
                         'key': u'foo\u22a1bar', 'categories': u''}]
            }
        )

    def test_return_proxy(self):
        pass

    def test_reset_index(self):
        im = IndexManager()
        ds = {
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        }
        im.add_dataset(ds)
        im.reset()
        s = Searcher()
        self.assertFalse(s.search(u'lollipop', proxy=False))
