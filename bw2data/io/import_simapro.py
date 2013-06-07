# -*- coding: utf-8 -*
from .. import Database, databases
from ..utils import activity_hash
import csv
import pprint
import re
import warnings


detoxify_pattern = '/[A-Z]{2,10} [SU]$'
detoxify_re = re.compile(detoxify_pattern)


def detoxify(string):
    found = detoxify_re.findall(string)
    assert found, "Can't find geography string"
    geo = found[0][1:-2]
    name = re.sub(detoxify_pattern, '', string)
    return [name, geo]


def is_number(x):
    try:
        float(x)
        return True
    except:
        return False


class SimaProImporter(object):
    def __init__(self, filepath, delimiter="\t", depends=['ecoinvent 2.2'], overwrite=False, name=None, geo="GLO"):
        self.filepath = filepath
        self.delimiter = delimiter
        self.depends = depends
        self.overwrite = overwrite
        self.name = name
        self.geo = geo

    def importer(self):
        raw_data = self.load_file()
        name, data = self.clean_data(raw_data)
        if self.name:
            name = self.name
        data = [self.process_data(obj) for obj in data]
        if not self.overwrite:
            assert name not in databases, "Already imported this project"
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                database = Database(name)
                database.register(
                    format=raw_data[0][0],
                    depends=self.depends,
                    num_processes=len(data)
                )
        else:
            database = Database(name)
        database.write(dict([(obj['code'], obj) for obj in data]))
        database.process()

    def process_data(self, data):
        data['code'] = (self.db_name, activity_hash(data))
        data['location'] = self.geo
        data['exchanges'] = self.link_exchanges(data['exchanges']) + [
            self.production_exchange(data)]
        return data

    def link_exchanges(self, data):
        if not hasattr(self, "background"):
            self.background = {}
            for db in self.depends:
                new_data = Database(db).load()
                self.background.update(**new_data)
        return [self.link_exchange(exc) for exc in data]

    def link_exchange(self, exc):
        found = False
        for key, value in self.background.iteritems():
            if value['name'].lower() == exc[0].lower() and \
                    value['location'] == exc[1]:
                found = True
                break

        if not found:
            raise ValueError

        return {
            'amount': float(exc[2]),
            'input': key,
            'type': 'technosphere',
            'uncertainty type': 0
        }

    def production_exchange(self, data):
        return {
            'amount': data['amount'],
            'input': data['code'],
            'uncertainty type': 0,
            'type': 'production'
        }

    def load_file(self):
        return [x for x in csv.reader(
            open(self.filepath),
            delimiter=self.delimiter
        )]

    def clean_data(self, data):
        # Get project metadata
        assert 'SimaPro' in data[0][0]
        project_name = data[1][1] if data[1][0] == 'Project' else None
        self.db_name = project_name
        process_indices = self.get_process_indices(data)
        process_data = [
            data[process_indices[x]:process_indices[x + 1]]
            for x in range(len(process_indices) - 1)
        ]
        return project_name, [self.get_exchanges(ds) for ds in process_data]

    def get_process_indices(self, data):
        return [x for x in range(2, len(data)) if data[x] and data[x][0] == "Process"] + [len(data) + 1]

    def get_exchanges(self, process_data):
        for x in range(len(process_data)):
            if process_data[x] and process_data[x][0] == 'Products':
                break

        exchanges = [
            line for line in process_data[x + 2:]
            if line and len(line) > 1 and is_number(line[1])
        ]
        exchanges = [detoxify(exc[0]) + exc[1:] for exc in exchanges]
        return {
            'name': process_data[x + 1][0],
            'amount': float(process_data[x + 1][1]),
            'unit': process_data[x + 1][2],
            'allocation': float(process_data[x + 1][3]),
            'categories': (process_data[x + 1][5]).split("\\"),
            'exchanges': exchanges
        }
