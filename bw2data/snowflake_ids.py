import uuid

from peewee import IntegerField
from snowflake import SnowflakeGenerator

from bw2data.signals import SignaledDataset

# Jan 1, 2024
# from datetime import datetime
# (datetime(2024, 1, 1) - datetime.utcfromtimestamp(0)).total_seconds() * 1000.0
EPOCH_START_MS = 1704067200000

# From https://softwaremind.com/blog/the-unique-features-of-snowflake-id-and-its-comparison-to-uuid/
# Snowflake bits:
# Sign bit: 1 bit. It will always be 0. This is reserved for future uses. It can potentially be used
# to distinguish between signed and unsigned numbers.
# Timestamp: 41 bits. Milliseconds since the epoch or custom epoch.
# Datacenter ID: 5 bits, which gives us 2 ^ 5 = 32 datacenters.
# Machine ID: 5 bits, which gives us 2 ^ 5 = 32 machines per datacenter.
# However, `snowflake-id` lumps the two datacenter and machine id values together into an
# `instance` parameter with 2 ^ 10 = 1024 possible values.
# Sequence number: 12 bits. For every ID generated on that machine/process, the sequence number is
# incremented by 1. The number is reset to 0 every millisecond.
snowflake_id_generator = SnowflakeGenerator(instance=uuid.getnode() % 1024, epoch=EPOCH_START_MS)


class SnowflakeIDBaseClass(SignaledDataset):
    id = IntegerField(primary_key=True)

    def save(self, **kwargs):
        if self.id is None:
            # If the primary key column data is already present (even if the object doesn't exist in
            # the database), peewee will make an `UPDATE` query. This will have no effect if there
            # isn't a matching row. Need for force an `INSERT` query instead as we generate the ids
            # ourselves.
            # https://docs.peewee-orm.com/en/latest/peewee/models.html#id4
            self.id = next(snowflake_id_generator)
            kwargs["force_insert"] = True
        super().save(**kwargs)
