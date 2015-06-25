# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import BW2DataTest
from ..search import *
from ..backends.peewee import *


class IndexTest(BW2DataTest):
    def test_add_dataset(self):
        im = IndexManager()
        im.add_dataset({
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        })
        with Searcher() as s:
            self.assertTrue(s.search(u'lollipop', proxy=False))

    def test_search_dataset(self):
        im = IndexManager()
        im.add_dataset({
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        })
        with Searcher() as s:
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
        with Searcher() as s:
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
        with Searcher() as s:
            self.assertTrue(s.search(u'lollipop', proxy=False))
        im.delete_dataset(ds)
        with Searcher() as s:
            self.assertFalse(s.search(u'lollipop', proxy=False))

    def test_add_datasets(self):
        im = IndexManager()
        ds = [{
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        }]
        im.add_datasets(ds)
        with Searcher() as s:
            self.assertTrue(s.search(u'lollipop', proxy=False))

    def test_add_database(self):
        im = IndexManager()
        db = SQLiteBackend(u'foo')
        ds = {(u'foo', u'bar'): {
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        }}
        db.write(ds)
        with Searcher() as s:
            self.assertTrue(s.search(u'lollipop', proxy=False))
        db.make_unsearchable()
        with Searcher() as s:
            self.assertFalse(s.search(u'lollipop', proxy=False))

    def test_add_searchable_database(self):
        im = IndexManager()
        db = SQLiteBackend(u'foo')
        ds = {(u'foo', u'bar'): {
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        }}
        db.write(ds)
        with Searcher() as s:
            self.assertTrue(s.search(u'lollipop', proxy=False))

    def test_modify_database(self):
        im = IndexManager()
        db = SQLiteBackend(u'foo')
        ds = {(u'foo', u'bar'): {
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        }}
        db.write(ds)
        with Searcher() as s:
            self.assertFalse(s.search(u'cream', proxy=False))
        ds2 = {(u'foo', u'bar'): {
            'database': u'foo',
            'code': u'bar',
            'name': u'ice cream'
        }}
        db.write(ds2)
        with Searcher() as s:
            self.assertTrue(s.search(u'cream', proxy=False))

    def test_delete_database(self):
        im = IndexManager()
        db = SQLiteBackend(u'foo')
        ds = {(u'foo', u'bar'): {
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        }}
        db.write(ds)
        with Searcher() as s:
            self.assertTrue(s.search(u'lollipop', proxy=False))
        db.make_unsearchable()
        with Searcher() as s:
            self.assertFalse(s.search(u'lollipop', proxy=False))
        db.make_searchable()
        with Searcher() as s:
            self.assertTrue(s.search(u'lollipop', proxy=False))
        db.delete()
        with Searcher() as s:
            self.assertFalse(s.search(u'lollipop', proxy=False))

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
        with Searcher() as s:
            self.assertFalse(s.search(u'lollipop', proxy=False))


class SearchTest(BW2DataTest):
    def test_basic_search(self):
        im = IndexManager()
        im.add_dataset({
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop'
        })
        with Searcher() as s:
            self.assertTrue(s.search(u'lollipop', proxy=False))

    def test_product_term(self):
        im = IndexManager()
        im.add_dataset({
            'database': u'foo',
            'code': u'bar',
            'reference product': u'lollipop'
        })
        with Searcher() as s:
            self.assertTrue(s.search(u'lollipop', proxy=False))

    def test_comment_term(self):
        im = IndexManager()
        im.add_dataset({
            'database': u'foo',
            'code': u'bar',
            'comment': u'lollipop'
        })
        with Searcher() as s:
            self.assertTrue(s.search(u'lollipop', proxy=False))

    def test_categories_term(self):
        im = IndexManager()
        im.add_dataset({
            'database': u'foo',
            'code': u'bar',
            'categories': (u'lollipop',),
        })
        with Searcher() as s:
            self.assertTrue(s.search(u'lollipop', proxy=False))

    def test_limit(self):
        im = IndexManager()
        im.add_datasets([{
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop {}'.format(x),
        } for x in range(50)])
        with Searcher() as s:
            self.assertEqual(
                len(s.search(u'lollipop', limit=25, proxy=False)),
                25
            )

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
        with Searcher() as s:
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

    def test_single_filter(self):
        im = IndexManager()
        ds = [{
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop',
        }, {
            'database': u'baz',
            'code': u'bar',
            'name': u'lollipop',
        }]
        im.add_datasets(ds)
        with Searcher() as s:
            self.assertEqual(
                s.search(u'lollipop', proxy=False, database=u'foo'),
                [{'comment': u'', 'product': u'', 'name': u'lollipop',
                  'database': u'foo', 'location': u'', 'key': u'foo\u22a1bar',
                  'categories': u''}]
            )

    def test_multifilter(self):
        im = IndexManager()
        ds = [{
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop ice',
            'location': u'BR',
        }, {
            'database': u'foo',
            'code': u'bar',
            'name': u'lollipop',
            'location': u'CH',
        }, {
            'database': u'baz',
            'code': u'bar',
            'name': u'lollipop',
            'location': u'CH',
        }]
        im.add_datasets(ds)
        with Searcher() as s:
            self.assertEqual(
                s.search(u'lollipop', proxy=False, database=u'foo', location=u'CH'),
                [{'comment': u'', 'product': u'', 'name': u'lollipop',
                  'database': u'foo', 'location': u'CH', 'key': u'foo\u22a1bar',
                  'categories': u''}]
            )
