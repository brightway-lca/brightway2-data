# -*- coding: utf-8 -*
import numpy as np
import scipy.sparse
import unittest
import warnings
from ..proxies.sparse import SparseMatrixProxy, CompressedSparseMatrixProxy


class SparseMatrixProxyTest(unittest.TestCase):
    def test_sparse_matrix_proxy(self):
        mat = scipy.sparse.lil_matrix((3, 3))
        for x in range(3):
            for y in range(3):
                if x == y:
                    continue
                mat[x, y] = x + 3 * y
        mat = SparseMatrixProxy(mat, {10: 0, 11: 1, 12: 2}, {20: 0, 21: 1,
            22: 2})
        self.assertEqual(mat[10, 20], 0)
        self.assertEqual(mat[11, 22], 7)
        self.assertEqual(mat.nnz, 6)
        mat[11, 21] = 3
        self.assertEqual(mat[11, 21], 3)
        with self.assertRaises(NotImplementedError):
            mat[slice(None), 21]
        with self.assertRaises(NotImplementedError):
            mat[slice(None), 21] = 1

    def test_compressed_sparse_matrix_proxy(self):
        c = scipy.sparse.lil_matrix((3, 3))
        for x in range(3):
            for y in range(3):
                if x == y:
                    continue
                c[x, y] = x + 3 * y
        c = CompressedSparseMatrixProxy(c, {10: 0, 11: 1, 12: 2},
            {20: 0, 21: 1, 22: 2})
        self.assertEqual(c[10, 20], 0)
        self.assertEqual(c[11, 22], 7)
        self.assertEqual(c.nnz, 6)
        self.assertTrue(isinstance(c.data, scipy.sparse.csr.csr_matrix))
        self.assertTrue(np.allclose(
            c[:, 21].todense(),
            np.array(((3,), (0,), (5,)))
            ))
        self.assertTrue(np.allclose(
            c[11, :].todense(),
            np.array((1, 0, 7))
            ))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            c[11, 21] = 3
            self.assertEqual(c[11, 21], 3)
