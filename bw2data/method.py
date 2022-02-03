
# Changes from upstream which need to be included in new layout
# def process_row(self, row):
#     """Given ``(flow, amount, maybe location)``, return a dictionary for array insertion."""
#     try:
#         return {
#             **as_uncertainty_dict(row[1]),
#             "row": get_id(row[0]),
#             "col": (
#                 geomapping[row[2]]
#                 if len(row) >= 3
#                 else geomapping[config.global_location]
#             ),
#         }
#     except UnknownObject:
#         raise UnknownObject("Can't find flow `{}`, specified in CF row `{}` for method `{}`".format({row[0]}, row, self.name))
#     except KeyError:
#         if len(row) >= 3 and row[2] not in geomapping:
#             raise UnknownObject("Can't find location `{}`, specified in CF row `{}` for method `{}`".format({row[2]}, row, self.name))
#         elif config.global_location not in geomapping:
#             raise UnknownObject("Can't find default global location! It's supposed to be `{}`, but this isn't in the `geomapping`".format(config.global_location))

# def process(self, **extra_metadata):
#     try:
#         extra_metadata["global_index"] = geomapping[config.global_location]
#     except KeyError:
#         raise KeyError("Can't find default global location! It's supposed to be `{}`, defined in `config`, but this isn't in the `geomapping`".format(config.global_location))
#     super().process(**extra_metadata)

# Kept only for backwards compatibility
from .utils import abbreviate
from .backends import Method
