# -*- coding: utf-8 -*-
from . import BW2DataTest
from .. import Database, databases
from ..io.import_simapro import SimaProImporter, MissingExchange, detoxify_re
from .fixtures.simapro_reference import background as background_data
import os

SP_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "simapro")


class SimaProImportTest(BW2DataTest):
    def extra_setup(self):
        # SimaPro importer always wants biosphere database
        database = Database("biosphere")
        database.register(
            format="Test data",
            depends=[],
            num_processes=0
        )
        database.write({})

    def filepath(self, name):
        return os.path.join(SP_FIXTURES_DIR, name + '.txt')

    def test_invalid_file(self):
        sp = SimaProImporter(self.filepath("invalid"), depends=[])
        data = sp.load_file()
        with self.assertRaises(AssertionError):
            sp.verify_simapro_file(data)

    def test_overwrite(self):
        database = Database("W00t")
        database.register(format="", depends=[], num_processes=0)
        sp = SimaProImporter(self.filepath("empty"), depends=[], overwrite=True)
        sp.importer()
        self.assertTrue("W00t" in databases)

    def test_no_overwrite(self):
        database = Database("W00t")
        database.register(format="", depends=[], num_processes=0)
        sp = SimaProImporter(self.filepath("empty"), depends=[])
        with self.assertRaises(AssertionError):
            sp.importer()

    def test_import_one_empty_process(self):
        sp = SimaProImporter(self.filepath("empty"), depends=[])
        sp.importer()
        self.assertTrue("W00t" in databases)
        self.assertEqual(len(Database("W00t").load()), 1)

    def test_get_db_name(self):
        sp = SimaProImporter(self.filepath("empty"), depends=[])
        sp.importer()
        self.assertTrue("W00t" in databases)

    def test_set_db_name(self):
        sp = SimaProImporter(self.filepath("empty"), depends=[], name="A different one")
        sp.importer()
        self.assertTrue("A different one" in databases)
        self.assertTrue("W00t" not in databases)

    def test_default_geo(self):
        sp = SimaProImporter(self.filepath("empty"), depends=[], default_geo="Where?")
        sp.importer()
        data = Database("W00t").load().values()[0]
        self.assertEqual("Where?", data['location'])

    def test_no_multioutput(self):
        sp = SimaProImporter(self.filepath("multioutput"), depends=[])
        with self.assertRaises(AssertionError):
            sp.importer()

    def test_detoxify_re(self):
        self.assertFalse(detoxify_re.search("Cheese U"))
        self.assertFalse(detoxify_re.search("Cheese/CH"))
        self.assertTrue(detoxify_re.search("Cheese/CH U"))
        self.assertTrue(detoxify_re.search("Cheese/CH/I U"))
        self.assertTrue(detoxify_re.search("Cheese/CH/I S"))
        self.assertTrue(detoxify_re.search("Cheese/RER U"))
        self.assertTrue(detoxify_re.search("Cheese/CENTREL U"))
        self.assertTrue(detoxify_re.search("Cheese/CENTREL S"))

    def test_simapro_unit_conversion(self):
        sp = SimaProImporter(self.filepath("empty"), depends=[])
        sp.importer()
        data = Database("W00t").load().values()[0]
        self.assertEqual("unit", data['unit'])

    def test_dataset_definition(self):
        sp = SimaProImporter(self.filepath("empty"), depends=[])
        sp.importer()
        data = Database("W00t").load().values()[0]
        self.assertEqual(data, {
            "name": "Fish food",
            "unit": "unit",
            "location": "GLO",
            "categories": ["Agricultural", "Animal production", "Animal foods"],
            "code": u'6524377b64855cc3daf13bd1bcfe0385',
            "exchanges": [{
                'amount': 1.0,
                'loc': 1.0,
                'input': ('W00t', u'6524377b64855cc3daf13bd1bcfe0385'),
                'type': 'production',
                'uncertainty type': 0}],
            "simapro metadata": {
                "Category type": "material",
                "Process identifier": "InsertSomethingCleverHere",
                "Type": "Unit process",
                "Process name": "bikes rule, cars drool",
            }
        })

    def test_production_exchange(self):
        sp = SimaProImporter(self.filepath("empty"), depends=[])
        sp.importer()
        data = Database("W00t").load().values()[0]
        self.assertEqual(data['exchanges'], [{
            'amount': 1.0,
            'loc': 1.0,
            'input': ('W00t', u'6524377b64855cc3daf13bd1bcfe0385'),
            'type': 'production',
            'uncertainty type': 0
        }])

    def test_simapro_metadata(self):
        sp = SimaProImporter(self.filepath("metadata"), depends=[])
        sp.importer()
        data = Database("W00t").load().values()[0]
        self.assertEqual(data['simapro metadata'], {
            "Simple": "yep!",
            "Multiline": ["This too", "works just fine"],
            "But stops": "in time"
        })

    def test_linking(self):
        # Test number of datasets
        # Test internal links
        # Test external links with and without slashes, with and without geo
        database = Database("background")
        database.register(
            format="Test data",
            depends=["background"],
            num_processes=2
        )
        database.write(background_data)
        sp = SimaProImporter(self.filepath("simple"), depends=["background"])
        sp.importer()
        # data = Database("W00t").load()

    def test_missing(self):
        sp = SimaProImporter(self.filepath("missing"), depends=[])
        with self.assertRaises(MissingExchange):
            sp.importer()

    # Test multiple background DBs
