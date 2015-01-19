from . import Revision, sqlite3_revisions_db
import dictdiffer
import peewee


class RevisionsInterface(object):
    """Interface to revision functionality"""
    @classmethod
    def _get_description(cls, difference):
        get_elem = lambda x: x if isinstance(x, basestring) else x[0]
        return u", ".join([
            line[0].title() + u" " + get_elem(
                line[1] if line[1] else u",".join([o[0] for o in line[2]])
            ) for line in difference
        ])

    @classmethod
    def _last_key_frame(cls, ds, number=None):
        qs = Revision.select().where(
            Revision.key == ds.dbkey,
            Revision.key_frame == True
        )
        if number is not None:
            qs = qs.where(Revision.number <= number)
        return qs.order_by(Revision.number.desc()).get()

    @classmethod
    def latest(cls, ds):
        return Revision.select().where(Revision.key == ds.dbkey).order_by(Revision.number.desc()).get()

    @classmethod
    def save(cls, ds):
        from .. import Database
        try:
            existing = Database(ds.database).get(ds.key)
        except peewee.DoesNotExist:
            return None
        try:
            revision_number = cls.latest(ds).number + 1
        except peewee.DoesNotExist:
            revision_number = 0
        is_key_frame = not revision_number % 5
        difference = list(dictdiffer.diff(existing.as_dict(), ds.as_dict()))
        description = u"Dataset creation" if not revision_number else \
            cls._get_description(difference)
        if is_key_frame:
            difference = ds.as_dict()
        return Revision.create(
            data=difference,
            key=ds.dbkey,
            number=revision_number,
            key_frame=is_key_frame,
            description=description
        )

    @classmethod
    def revert(cls, ds, number):
        key_frame = cls._last_key_frame(ds, number)
        data = key_frame.data
        revisions = Revision.select().where(
            Revision.key == ds.dbkey,
            Revision.number > key_frame.number,
            Revision.number <= number
        ).order_by(Revision.number.asc())
        for revision in revisions:
            data = dictdiffer.patch(revision.data, data)
        Revision.delete().where(
            Revision.key == ds.dbkey,
            Revision.number >= number
        ).execute()
        return Revision.create(
            data=data,
            key=ds.dbkey,
            number=number,
            key_frame=True,
            description=u"Reverted"
        )

    @classmethod
    def revisions(cls, ds):
        return Revision.select().where(Revision.key == ds.dbkey).order_by(
            Revision.number.desc())

    @classmethod
    def add_database(cls, db):
        data = []
        sqlite3_revisions_db.autocommit = False
        try:
            sqlite3_revisions_db.begin()

            for index, ds in enumerate(db):
                data.append(dict(
                    data=ds.as_dict(),
                    key=ds.dbkey,
                    number=0,
                    key_frame=True,
                    description=u"Dataset creation"
                ))

                if len(data) > 150:
                    Revision.insert_many(data).execute()
                    data = []

            if data:
                Revision.insert_many(data).execute()

            sqlite3_revisions_db.commit()
        except:
            sqlite3_revisions_db.rollback()
            raise
        finally:
            sqlite3_revisions_db.autocommit = True
