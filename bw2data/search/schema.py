from peewee import SQL
from playhouse.sqlite_ext import FTSModel, SearchField, RowIDField


class BwDataFTSModel(FTSModel):
    """
    Upstream FTS3/4 models does not exclude the primary field from
    being passed to the `matchinfo` method.
    """

    @classmethod
    def _search(cls, term, weights, with_score, score_alias, score_fn,
                explicit_ordering):
        if not weights:
            rank = score_fn()
        elif isinstance(weights, dict):
            weight_args = []
            for field in cls._meta.sorted_fields:
                # Attempt to get the specified weight of the field by looking
                # it up using it's field instance followed by name.
                if isinstance(field, SearchField) and not field.unindexed:
                    weight_args.append(
                        weights.get(field, weights.get(field.name, 1.0))
                    )
            rank = score_fn(*weight_args)
        else:
            rank = score_fn(*weights)

        selection = ()
        order_by = rank
        if with_score:
            selection = (cls, rank.alias(score_alias))
        if with_score and not explicit_ordering:
            order_by = SQL(score_alias)

        return (cls
                .select(*selection)
                .where(cls.match(term))
                .order_by(order_by))


class BW2Schema(BwDataFTSModel):
    rowid = RowIDField()
    name = SearchField()
    comment = SearchField()
    product = SearchField()
    categories = SearchField()
    synonyms = SearchField()
    location = SearchField()
    database = SearchField()
    code = SearchField()

    class Meta:
        # Use the porter stemming algorithm to tokenize content.
        options = {'tokenize': 'porter'}
