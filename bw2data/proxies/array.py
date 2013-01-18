# -*- coding: utf-8 -*
import numpy as np


class DifferentIndexTypesError(StandardError):
    pass


class InconsistentSlicingError(StandardError):
    pass


class ArrayProxy(object):
    """
Provides a dictionary-based interface from database row ids to array indices.

ArrayProxy provides a matrix or array whose indices are translated through a lookup dictionary. Slices are not generally supported.
    """
    def __init__(self, data, row_dict, col_dict=None):
        super(ArrayProxy, self).__init__()
        self.data = data
        self.row_dict = row_dict
        if col_dict:
            self.col_dict = col_dict

    def translate(self, obj, type="row"):
        if hasattr(obj, "id"):
            obj = obj.id
        if isinstance(obj, slice):
            if obj == slice(None):
                return obj
            else:
                # Could support for  sorted dictionary implementations, e.g.
                # http://code.activestate.com/recipes/496761/ or
                # http://www.voidspace.org.uk/downloads/odict.py
                # However, this is not desperately needed...
                raise NotImplementedError("Slices are not supported, "
                    "because their meaning is unclear when translated into"
                    " array indices. If you are confident, you can call "
                    "slices directly on self.data.")
        elif isinstance(obj, (list, tuple)):
            return tuple([self.translate(_obj, type) for _obj in obj])
        else:
            # No longer test for integer keys; subclasses can use strings
            try:
                if type == "row":
                    return self.row_dict[obj]
                elif type == "col":
                    return self.col_dict[obj]
            except KeyError:
                raise KeyError("Provided object %s is not a valid %s key" \
                    % (obj, type))

    def __getitem__(self, *args):
        assert len(args) == 1
        x = self.translate(args[0][0], "row")
        y = self.translate(args[0][1], "col")
        return self.data.__getitem__((x, y),)

    def __setitem__(self, *args):
        assert len(args) == 2
        x = self.translate(args[0][0], "row")
        y = self.translate(args[0][1], "col")
        return self.data.__setitem__((x, y), args[1])

    def __repr__(self):
        return '<%s for %s>' % (self.__class__.__name__, self.data.__repr__(
            )[1:-1])

    def __str__(self):
        return self.data.__str__()

    # Numpy functions
    def any(self, *args, **kwargs):
        return self.data.any(*args, **kwargs)

    def all(self, *args, **kwargs):
        return self.data.all(*args, **kwargs)

    def sum(self, *args, **kwargs):
        return self.data.sum(*args, **kwargs)

    def min(self, *args, **kwargs):
        # This should work for both ndarrays and sparse matrices
        try:
            m = self.data.min(*args, **kwargs)
        except AttributeError:
            m = self.data.data.min(*args, **kwargs)
        while isinstance(m, np.ndarray):
            m = m[0]
        return m

    def max(self, *args, **kwargs):
        try:
            m = self.data.max(*args, **kwargs)
        except AttributeError:
            m = self.data.data.max(*args, **kwargs)
        while isinstance(m, np.ndarray):
            m = m[0]
        return m

    def cumsum(self, *args, **kwargs):
        return self.data.cumsum(*args, **kwargs)

    def mean(self, *args, **kwargs):
        return self.data.mean(*args, **kwargs)

    def row(self, id):
        return self.row_dict[id]

    def col(self, id):
        return self.col_dict[id]

    @property
    def shape(self):
        return self.data.shape

    @property
    def row_dict_rev(self):
        """Build only upon demand"""
        if not hasattr(self, "_row_dict_rev"):
            self._row_dict_rev = dict(zip(self.row_dict.values(),
                self.row_dict.keys()))
        return self._row_dict_rev

    @property
    def col_dict_rev(self):
        if not hasattr(self, "_col_dict_rev"):
            self._col_dict_rev = dict(zip(self.col_dict.values(),
                self.col_dict.keys()))
        return self._col_dict_rev


class OneDimensionalArrayProxy(ArrayProxy):
    """
A special case of ArrayProxy limited to one-dimensional arrays.

Used for supply and demand arrays in LCA calculations.
    """
    def __init__(self, data, row_dict):
        if not len(data.shape) == 1:
            raise AttributeError("Must only be used for one-dimensional array")
        super(OneDimensionalArrayProxy, self).__init__(data, row_dict)

    def __getitem__(self, *args):
        assert len(args) == 1
        x = self.translate(args[0], "row")
        return self.data.__getitem__((x,),)

    def __setitem__(self, *args):
        assert len(args) == 2
        x = self.translate(args[0], "row")
        self.data.__setitem__((x,), args[1])


class ListArrayProxy(object):
    """
An interface to a list of objects that translates lookeups from foo[bar,baz] to foo.indices.index(bar)[baz]. If baz is a slice, returns a generator.

Takes list_, the list of objects, and optionally index_objs, which is an iterable of objects used as indices to list_. index_objs must all be of the same type.
    """

    __slots__ = ["indices", "list", "index_type"]

    def __init__(self, list_, index_objs=None):
        self.list = list_
        if index_objs:
            self.check_indice_types(index_objs)
            if len(index_objs) != len(list_):
                raise ValueError("index_objs must have same length as list_")
            self.indices = list(index_objs)
        else:
            self.indices = None

    def check_indice_types(self, objs):
        l = [type(x) for x in objs]
        if not len(set(l)) == 1:
            raise DifferentIndexTypesError

    def sum(self):
        return sum([sum(obj) for obj in self.list])

    def __unicode__(self):
        if self.indices:
            return "<ListArrayProxy for %s>" % self.indices[0]
        else:
            return "<ListArrayProxy for unknown objects (id %s)>" % id(self)

    def translate_slice(self, sl):
        """Possible translate slice arguments into self.indices terms"""
        if sl.start in self.indices:
            start = self.indices.index(sl.start)
            start_translated = True
        else:
            start = sl.start
            start_translated = False
        if sl.stop in self.indices:
            stop = self.indices.index(sl.stop)
            stop_translated = True
        else:
            stop = sl.stop
            stop_translated = False
        if start_translated != stop_translated and start != None and stop != \
                None:
            raise InconsistentSlicingError(
                "Only one slice element could be found in the indices")
        return slice(start, stop, sl.step)

    def __getitem__(self, args):
        if args == None:
            raise SyntaxError

        if isinstance(args, tuple):
            list_pos = args[0]
        else:
            list_pos = args

        if self.indices and isinstance(list_pos, slice):
            list_pos = self.translate_slice(list_pos)
        elif self.indices and list_pos in self.indices:
            list_pos = self.indices.index(list_pos)

        if not isinstance(args, tuple):
            return self.list[list_pos]
        elif isinstance(list_pos, slice):
            return (obj.__getitem__(*args[1:]) for obj in self.list[list_pos])
        else:
            return self.list[list_pos].__getitem__(*args[1:])

    def __setitem__(self):
        raise NotImplementedError

    def __iter__(self):
        return iter(self.list)

    def __len__(self):
        return len(self.list)

    def __repr__(self):
        return self.__unicode__()
