# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import BW2DataTest
from ..search import *
from ..backends.peewee import *


class IndexTest(BW2DataTest):
    def test_add_dataset(self):
        im = IndexManager("foo")
        im.add_dataset({
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop'
        })
        with Searcher("foo") as s:
            self.assertTrue(s.search('lollipop', proxy=False))

    def test_search_dataset(self):
        im = IndexManager("foo")
        im.add_dataset({
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop'
        })
        with Searcher("foo") as s:
            self.assertEqual(
                s.search('lollipop', proxy=False),
                [{'comment': '', 'product': '', 'name': 'lollipop',
                  'database': 'foo', 'location': '', 'code': 'bar',
                  'categories': ''}]
            )

    def test_update_dataset(self):
        im = IndexManager("foo")
        ds = {
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop'
        }
        im.add_dataset(ds)
        ds['name'] = 'lemon cake'
        im.update_dataset(ds)
        with Searcher("foo") as s:
            self.assertEqual(
                s.search('lemon', proxy=False),
                [{'comment': '', 'product': '', 'name': 'lemon cake',
                  'database': 'foo', 'location': '', 'code': 'bar',
                  'categories': ''}]
            )

    def test_delete_dataset(self):
        im = IndexManager("foo")
        ds = {
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop'
        }
        im.add_dataset(ds)
        with Searcher("foo") as s:
            self.assertTrue(s.search('lollipop', proxy=False))
        im.delete_dataset(ds)
        with Searcher("foo") as s:
            self.assertFalse(s.search('lollipop', proxy=False))

    def test_add_datasets(self):
        im = IndexManager("foo")
        ds = [{
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop'
        }]
        im.add_datasets(ds)
        with Searcher("foo") as s:
            self.assertTrue(s.search('lollipop', proxy=False))

    def test_add_database(self):
        db = SQLiteBackend('foo')
        im = IndexManager(db.filename)
        ds = {('foo', 'bar'): {
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop'
        }}
        db.write(ds)
        with Searcher(db.filename) as s:
            self.assertTrue(s.search('lollipop', proxy=False))
        db.make_unsearchable()
        with Searcher(db.filename) as s:
            self.assertFalse(s.search('lollipop', proxy=False))

    def test_add_searchable_database(self):
        db = SQLiteBackend('foo')
        im = IndexManager(db.filename)
        ds = {('foo', 'bar'): {
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop'
        }}
        db.write(ds)
        with Searcher(db.filename) as s:
            self.assertTrue(s.search('lollipop', proxy=False))

    def test_modify_database(self):
        db = SQLiteBackend('foo')
        im = IndexManager(db.filename)
        ds = {('foo', 'bar'): {
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop'
        }}
        db.write(ds)
        with Searcher(db.filename) as s:
            self.assertFalse(s.search('cream', proxy=False))
        ds2 = {('foo', 'bar'): {
            'database': 'foo',
            'code': 'bar',
            'name': 'ice cream'
        }}
        db.write(ds2)
        with Searcher(db.filename) as s:
            self.assertTrue(s.search('cream', proxy=False))

    def test_delete_database(self):
        db = SQLiteBackend('foo')
        im = IndexManager(db.filename)
        ds = {('foo', 'bar'): {
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop'
        }}
        db.write(ds)
        with Searcher(db.filename) as s:
            self.assertTrue(s.search('lollipop', proxy=False))
        db.make_unsearchable()
        with Searcher(db.filename) as s:
            self.assertFalse(s.search('lollipop', proxy=False))
        db.make_searchable()
        with Searcher(db.filename) as s:
            self.assertTrue(s.search('lollipop', proxy=False))
        db.delete()
        with Searcher(db.filename) as s:
            self.assertFalse(s.search('lollipop', proxy=False))

    def test_return_proxy(self):
        pass

    def test_reset_index(self):
        im = IndexManager("foo")
        ds = {
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop'
        }
        im.add_dataset(ds)
        im.create()
        with Searcher("foo") as s:
            self.assertFalse(s.search('lollipop', proxy=False))


class SearchTest(BW2DataTest):
    def test_basic_search(self):
        im = IndexManager("foo")
        im.add_dataset({
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop'
        })
        with Searcher("foo") as s:
            self.assertTrue(s.search('lollipop', proxy=False))

    def test_product_term(self):
        im = IndexManager("foo")
        im.add_dataset({
            'database': 'foo',
            'code': 'bar',
            'reference product': 'lollipop'
        })
        with Searcher("foo") as s:
            self.assertTrue(s.search('lollipop', proxy=False))

    def test_comment_term(self):
        im = IndexManager("foo")
        im.add_dataset({
            'database': 'foo',
            'code': 'bar',
            'comment': 'lollipop'
        })
        with Searcher("foo") as s:
            self.assertTrue(s.search('lollipop', proxy=False))

    def test_categories_term(self):
        im = IndexManager("foo")
        im.add_dataset({
            'database': 'foo',
            'code': 'bar',
            'categories': ('lollipop',),
        })
        with Searcher("foo") as s:
            self.assertTrue(s.search('lollipop', proxy=False))

    def test_limit(self):
        im = IndexManager("foo")
        im.add_datasets([{
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop {}'.format(x),
        } for x in range(50)])
        with Searcher("foo") as s:
            self.assertEqual(
                len(s.search('lollipop', limit=25, proxy=False)),
                25
            )

    def test_search_faceting(self):
        im = IndexManager("foo")
        ds = [{
            'database': 'foo',
            'code': 'bar',
            'name': 'lollipop',
            'location': 'CH',
        }, {
            'database': 'foo',
            'code': 'bar',
            'name': 'ice lollipop',
            'location': 'FR',
        }]
        im.add_datasets(ds)
        with Searcher("foo") as s:
            res = s.search('lollipop', proxy=False, facet='location')
        self.assertEqual(
            res,
            {
                'FR': [{'comment': '', 'product': '',
                         'name': 'ice lollipop', 'database': 'foo',
                         'location': 'FR', 'code': 'bar',
                         'categories': ''}],
                'CH': [{'comment': '', 'product': '', 'name': 'lollipop',
                         'database': 'foo', 'location': 'CH',
                         'code': 'bar', 'categories': ''}]
            }
        )
