# -*- coding: utf-8 -*
from . import ArrayProxy


class SparseMatrixProxy(ArrayProxy):
    """
Provides a dictionary-based interface from database row ids to array indices for sparse matrices. Does not assume a certain sparsity structure.
    """
    def __init__(self, data, row_dict, col_dict, *args, **kwargs):
        self.row_dict = row_dict
        self.col_dict = col_dict
        self.data = data
        try:
            self.format = self.data.getformat()
        except AttributeError:
            raise TypeError("Must pass a Scipy sparse matrix")

    def __getitem__(self, *args):
        assert len(args) == 1
        x = self.translate(args[0][0], "row")
        y = self.translate(args[0][1], "col")

        if x == slice(None) or y == slice(None):
            raise NotImplementedError("SparseMatrixProxy doesn't support "
                "slices; use CompressedSparseMatrixProxy for slices")
        return self.data.__getitem__((x, y),)

    def __setitem__(self, *args):
        assert len(args) == 2
        x = self.translate(args[0][0], "row")
        y = self.translate(args[0][1], "col")

        if x == slice(None) or y == slice(None):
            raise NotImplementedError("SparseMatrixProxy doesn't support "
                "assignment by slice")
        self.data.__setitem__((x, y), args[1])

    def get_row_as_dict(self, row):
        raise NotImplementedError("Use CompressedSparseMatrixProxy for "
            "dictionary matrix slices")

    def get_col_as_dict(self, col):
        raise NotImplementedError("Use CompressedSparseMatrixProxy for "
            "dictionary matrix slices")

    def toarray(self):
        return self.data.toarray()

    def todense(self):
        return self.data.todense()

    @property
    def nnz(self):
        # Used to use getnzmax, but that seems to be missing from newest scipy
        return self.data.getnnz()


class CompressedSparseMatrixProxy(SparseMatrixProxy):
    """
Subclass of SparseMatrixProxy for CSR (comma separated row) or CSC (comma separated column) matrices.

This class will create, on demand, the complementary matrix type (e.g. CSC for CSR) for efficient slicing and multiple assignment. If a matrix is changed, a *dirty* flag is set that tells the class to re-create the complementary matrix before accessing its information. All updates to complementary matrices are lazy.
    """

    __slots__ = ['row_dict', 'col_dict', 'format', 'dirty', '_csr', '_csc']

    def __init__(self, data, row_dict, col_dict, *args, **kwargs):
        self.row_dict = row_dict
        self.col_dict = col_dict
        self.dirty = True
        # try:
        self.format = data.getformat()
        if self.format == "csc":
            self._csc = data
        else:
            self._csr = data.tocsr()
            self.format = "csr"

    def __getitem__(self, *args):
        assert len(args) == 1
        x = self.translate(args[0][0], "row")
        y = self.translate(args[0][1], "col")

        if isinstance(x, slice) and isinstance(y, slice):
            raise NotImplementedError(
                "Convert to dense matrix to do slices on multiple dimensions")
        elif isinstance(x, slice):
            if self.dirty and self.format == "csr":
                self._csc = self._csr.tocsc()
            return self._csc.__getitem__((x, y),)
        elif isinstance(y, slice):
            if self.dirty and self.format == "csc":
                self._csr = self._csc.tocsr()
            return self._csr.__getitem__((x, y),)
        else:
            return self.data.__getitem__((x, y),)

    def _get_data(self):
        if self.format == "csr":
            return self._csr
        else:
            return self._csc

    def _set_data(self, data):
        self.dirty = True
        if self.format == "csr":
            self._csr = data
        else:
            self._csc = data

    data = property(_get_data, _set_data)

    def __setitem__(self, *args):
        # Set dirty flag to indicate that matrix has changed, and need to
        # recompute the partner matrix if is accessed
        self.dirty = True
        # TODO: Warn if setting slicing on compressed sparse matrix?
        super(CompressedSparseMatrixProxy, self).__setitem__(*args)

    def get_row_as_dict(self, row):
        obj = self.__getitem__((row, slice(None)),)
        return dict([(self.row_dict_rev[obj.indices[index]], obj.data[index]
            ) for index in xrange(obj.indices.shape[0])])

    def get_col_as_dict(self, col):
        obj = self.__getitem__((slice(None), col),)
        return dict([(self.col_dict_rev[obj.indices[index]], obj.data[index]
            ) for index in xrange(obj.indices.shape[0])])
