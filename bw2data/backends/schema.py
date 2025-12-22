from functools import cache

from peewee import DoesNotExist, TextField, FloatField, IntegerField, Model, Function
from playhouse.signals import pre_save, pre_init

from bw2data.sqlite import CleanJSONField, spread_data_into_fields, add_field_into_data, FastJSONField
from bw2data.errors import UnknownObject
from bw2data.snowflake_ids import SnowflakeIDBaseClass



class ActivityDataset(SnowflakeIDBaseClass):
    data = CleanJSONField(default=dict)  # Set just after
    code = TextField()  # Canonical
    comment = TextField(null=True)
    categories = FastJSONField(null=True)
    synonyms = FastJSONField(null=True)
    database = TextField() # EnumField(DatabaseEnum)
    unit = TextField(null=True) # EnumField(UnitEnum, null=True)
    location = TextField(null=True)  # Reset from `data`
    name = TextField(null=True)  # Reset from `data`
    product = TextField(null=True)  # Reset from `data`
    type = TextField(null=True) # EnumField(TypeEnum, null=True)  # Reset from `data`

    class Meta:
        extra_data_mapping = {"reference product": "product"}


    @property
    def key(self):
        return (self.database, self.code)

ActivityDataset.data.setup_model(ActivityDataset)

class ExchangeDataset(SnowflakeIDBaseClass):
    data = CleanJSONField(default=dict)   # Canonical, except for other C fields
    amount= FloatField(null=True)
    scale = FloatField(null=True)
    shape = FloatField(null=True)
    loc = FloatField(null=True)
    uncertainty_type = IntegerField(null=True)
    minimum = FloatField(null=True)
    maximum = FloatField(null=True)

    unit = TextField(null=True) # EnumField(UnitEnum, null=True)

    name= TextField(null=True)
    input_code = TextField()  # Canonical
    input_database = TextField() # EnumField(DatabaseEnum)
    output_code = TextField()  # Canonical
    output_database = TextField() # EnumField(DatabaseEnum)
    type = TextField(null=True) # EnumField(TypeEnum, null=True)  # Reset from `data`

    class Meta:
        extra_data_mapping = {
            "uncertainty type" : "uncertainty_type",
            "input" : ("input_database", "input_code"),
            "output" : ("output_database", "output_code")}


ExchangeDataset.data.setup_model(ExchangeDataset)


@pre_save(sender=ActivityDataset)
def activity_pre_save(model_class, instance, created):
    spread_data_into_fields(model_class, instance)

@pre_save(sender=ExchangeDataset)
def exchange_pre_save(model_class, instance, created):
    spread_data_into_fields(model_class, instance)

@pre_init(sender=ActivityDataset)
def activity_pre_init(model_class, instance:ActivityDataset):
    add_field_into_data(model_class, instance)


@pre_init(sender=ExchangeDataset)
def activity_pre_init(model_class, instance:ExchangeDataset):
    add_field_into_data(model_class, instance)


@cache
def get_id(key):
    if isinstance(key, int):
        try:
            ActivityDataset.get(ActivityDataset.id == key)
        except DoesNotExist:
            raise UnknownObject
        return key
    else:
        try:
            return ActivityDataset.get(
                ActivityDataset.database == key[0], ActivityDataset.code == key[1]
            ).id
        except DoesNotExist:
            raise UnknownObject


def insert_many(items, modelClass:type[Model], drop_meta_data=False):
    """Generic insert many. Using raw sqlite connection with executemany > super fast"""

    fields = modelClass._meta.sorted_fields

    sql_rows = []
    for item in items:
        spread_data_into_fields(modelClass, item)
        if drop_meta_data :
            item["data"] = {}

        def to_sql(field):
            res = field.db_value(item.get(field.name))

            # For some reason, JSON field returns a peewee json "Function"
            if isinstance(res, Function):
                res = res.arguments[0]
            return res

        # Yield single row
        sql_rows.append(list(to_sql(field) for field in fields))

    # Get raw sqlite connection
    db = modelClass._meta.database
    conn = db.connection()
    cursor = conn.cursor()

    column_names = [f'"{f.column_name}"' for f in fields]
    placeholders = ", ".join(["?"] * len(fields))

    sql = f"""INSERT INTO {modelClass._meta.table_name} ({", ".join(column_names)}) VALUES ({placeholders})"""

    cursor.executemany(sql, sql_rows)


def insert_many_exchanges(exchanges : list[ExchangeDataset], drop_meta_data=False):
    insert_many(exchanges, ExchangeDataset, drop_meta_data=drop_meta_data)

def insert_many_activities(activities : list[ActivityDataset], drop_meta_data = False):
    insert_many(activities, ActivityDataset, drop_meta_data=drop_meta_data)


