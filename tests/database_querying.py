from bw2data import projects
from bw2data.backends import Activity as PWActivity
from bw2data.backends import ActivityDataset
from bw2data.backends import Exchange as PWExchange
from bw2data.backends import ExchangeDataset
from bw2data.backends.utils import convert_backend
from bw2data.database import DatabaseChooser
from bw2data.errors import (
    InvalidExchange,
    MissingIntermediateData,
    UnknownObject,
    UntypedExchange,
    ValidityError,
)
from bw2data.meta import databases, geomapping, methods
from bw2data.tests import BW2DataTest


class DatabaseQuerysetTest(BW2DataTest):
    def extra_setup(self):
        self.db = DatabaseChooser("Order!")
        self.db.write(
            {
                ("Order!", "first"): {
                    "name": "a",
                    "location": "delaware",
                    "reference product": "widget",
                },
                ("Order!", "second"): {
                    "name": "b",
                    "location": "carolina",
                    "reference product": "wiggle",
                },
                ("Order!", "third"): {
                    "name": "c",
                    "location": "baseball",
                    "reference product": "lollipop",
                },
                ("Order!", "fourth"): {
                    "name": "d",
                    "location": "alabama",
                    "reference product": "widget",
                },
            }
        )

    def test_setup_clean(self):
        self.assertEqual(len(databases), 1)
        self.assertTrue("Order!" in databases)
        self.assertEqual(list(methods), [])
        self.assertEqual(len(geomapping), 5)  # GLO
        self.assertTrue("GLO" in geomapping)
        self.assertTrue("carolina" in geomapping)
        self.assertEqual(len(projects), 1)  # Random test project
        self.assertTrue("default" not in projects)

    def test_random_with_global_filters(self):
        self.db.filters = {"product": "lollipop"}
        for _ in range(10):
            self.assertEqual(self.db.random()["name"], "c")

    def test_random_with_local_filters(self):
        for _ in range(10):
            self.assertEqual(self.db.random(filters={"product": "lollipop"})["name"], "c")

    def test_random_with_local_and_global_filters(self):
        db = DatabaseChooser("Newt")
        db.write(
            {
                ("Newt", "first"): {
                    "name": "a",
                    "location": "delaware",
                    "reference product": "widget",
                },
                ("Newt", "second"): {
                    "name": "b",
                    "location": "delaware",
                    "reference product": "wiggle",
                },
                ("Newt", "third"): {
                    "name": "c",
                    "location": "alabama",
                    "reference product": "widget",
                },
                ("Newt", "fourth"): {
                    "name": "d",
                    "location": "alabama",
                    "reference product": "wiggle",
                },
            }
        )
        self.assertTrue(len({db.random()["name"] for _ in range(10)}) > 1)
        db.filters = {"product": "widget"}
        for _ in range(10):
            self.assertEqual(self.db.random(filters={"location": "delaware"})["name"], "a")

    def test_contains_respects_filters(self):
        self.db.filters = {"product": "lollipop"}
        self.assertFalse(("Order!", "fourth") in self.db)

    def test_get_ignores_filters(self):
        self.db.filters = {"product": "giggles"}
        self.assertEqual(self.db.get("fourth")["name"], "d")

    def test_filter(self):
        self.db.filters = {"product": "widget"}
        self.assertEqual(len([x for x in self.db]), 2)

    def test_order_by(self):
        self.db.order_by = "name"
        self.assertEqual([x["name"] for x in self.db], ["a", "b", "c", "d"])

    def test_order_by_bad_field(self):
        with self.assertRaises(AssertionError):
            self.db.order_by = "poopy"

    def test_filter_bad_field(self):
        with self.assertRaises(AssertionError):
            self.db.filters = {"poopy": "yuck"}

    def test_filter_not_dict(self):
        with self.assertRaises(AssertionError):
            self.db.filters = "poopy"

    def test_reset_order_by(self):
        self.db.order_by = "name"
        self.db.order_by = None
        as_lists = [[x["name"] for x in self.db] for _ in range(10)]
        first_elements = {x[0] for x in as_lists}
        self.assertTrue(len(first_elements) > 1)

    def test_reset_filters(self):
        self.db.filters = {"product": "widget"}
        self.assertEqual(len([x for x in self.db]), 2)
        self.db.filters = None
        self.assertEqual(len([x for x in self.db]), 4)

    def test_len_respects_filters(self):
        self.db.filters = {"product": "widget"}
        self.assertEqual(len(self.db), 2)

    def test_convert_same_backend(self):
        database = DatabaseChooser("a database")
        database.write(
            {
                ("a database", "foo"): {
                    "exchanges": [
                        {
                            "input": ("a database", "foo"),
                            "amount": 1,
                            "type": "production",
                        }
                    ],
                    "location": "bar",
                    "name": "baz",
                },
            }
        )
        self.assertFalse(convert_backend("a database", "sqlite"))

    def test_convert_backend(self):
        self.maxDiff = None
        database = DatabaseChooser("a database")
        database.write(
            {
                ("a database", "foo"): {
                    "exchanges": [
                        {
                            "input": ("a database", "foo"),
                            "amount": 1,
                            "type": "production",
                        }
                    ],
                    "location": "bar",
                    "name": "baz",
                },
            }
        )
