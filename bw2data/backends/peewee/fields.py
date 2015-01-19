from peewee import BlobField
import cPickle as pickle


class PickleField(BlobField):
    def db_value(self, value):
        return super(PickleField, self).db_value(
            pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        )

    def python_value(self, value):
        return pickle.loads(str(value))
