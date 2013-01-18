import unittest
from ..proxies.array import ArrayProxy, OneDimensionalArrayProxy, \
    ListArrayProxy, DifferentIndexTypesError, InconsistentSlicingError
import numpy as np


class ArrayProxyTest(unittest.TestCase):
    def test_array_proxy(self):
        a = np.zeros((3, 3))
        for x in range(3):
            for y in range(3):
                a[x, y] = x + 3 * y
        a = ArrayProxy(a, {10: 0, 11: 1, 12: 2}, {20: 0, 21: 1, 22: 2})
        self.assertEqual(a[10, 20], 0)
        self.assertTrue(np.allclose(a[11, (20, 21)], np.array((1, 4))))

    def test_array_proxy_object(self):
        class Dummy(object):
            def __init__(self, id):
                self.id = id

        a = np.zeros((3, 3))
        for x in range(3):
            for y in range(3):
                a[x, y] = x + 3 * y
        a = ArrayProxy(a, {10: 0, 11: 1, 12: 2}, {20: 0, 21: 1, 22: 2})
        obj = Dummy(10)
        self.assertEqual(a[obj, 20], 0)

    def test_array_proxy_key_error(self):
        a = np.zeros((3, 3))
        for x in range(3):
            for y in range(3):
                a[x, y] = x + 3 * y
        a = ArrayProxy(a, {10: 0, 11: 1, 12: 2}, {20: 0, 21: 1, 22: 2})
        self.assertRaises(KeyError, a.__getitem__, (20, 10))

        class Vanilla(object):
            pass
        v = Vanilla()
        self.assertRaises(KeyError, a.__getitem__, (v, 20))

    def test_array_proxy_slices(self):
        a = np.zeros((3, 3))
        for x in range(3):
            for y in range(3):
                a[x, y] = x + 3 * y
        a = ArrayProxy(a, {10: 0, 11: 1, 12: 2}, {20: 0, 21: 1, 22: 2})
        self.assertRaises(NotImplementedError, a.__getitem__, (slice(None, 11,
            None), 20))
        # Note: np slices don't preserve shape!
        self.assertTrue(np.allclose(a[:, 21], np.array((3, 4, 5,))))
        self.assertTrue(np.allclose(a[11, :], np.array((1, 4, 7))))

    def test_reverse_dict(self):
        a = np.zeros((3, 3))
        for x in range(3):
            for y in range(3):
                a[x, y] = x + 3 * y
        a = ArrayProxy(a, {10: 0, 11: 1, 12: 2}, {20: 0, 21: 1, 22: 2})
        self.assertFalse(hasattr(a, "_row_dict_rev"))
        self.assertFalse(hasattr(a, "_col_dict_rev"))
        self.assertTrue(a.row_dict_rev)
        self.assertTrue(a.col_dict_rev)
        self.assertEquals(a.row_dict_rev[0], 10)
        self.assertEquals(a.col_dict_rev[0], 20)

    def test_one_dimensional_proxy(self):
        b = np.zeros((3,))
        for x in range(3):
            b[x] = x + 3
        b = OneDimensionalArrayProxy(b, {10: 0, 11: 1, 12: 2})
        self.assertEqual(b[11], 4)
        self.assertTrue(np.allclose(b[:], np.array((3, 4, 5))))
        b[11] = 13
        self.assertEqual(b[11], 13)
        self.assertTrue(np.allclose(b[:], np.array((3, 13, 5))))
        b = np.zeros((3, 3))
        self.assertRaises(AttributeError, OneDimensionalArrayProxy, b, {})
        self.assertRaises(TypeError, OneDimensionalArrayProxy, b, {}, {})


class ListArrayProxyTest(unittest.TestCase):
    def test_list_array_proxy_without_indices(self):
        l = ListArrayProxy((range(10), range(10)))
        self.assertEqual(l[0], range(10))
        self.assertEqual(list(l[:, 1]), [1, 1])
        self.assertEqual(list(l[:, :]), [range(10), range(10)])
        self.assertEqual(list(l[:, 4:6]), [range(10)[4:6], range(10)[4:6]])

    def test_objects_as_indices(self):
        class A(object):
            pass
        m = A()
        n = A()
        l = ListArrayProxy((range(5), range(5, 10)), [m, n])
        self.assertEqual(l[n], range(5, 10))
        self.assertEqual(l.sum(), sum(range(10)))
        self.assertEqual(l[n, :2], [5, 6])

    def test_mismatched_index_length(self):
        self.assertRaises(ValueError, ListArrayProxy, range(5), range(4))

    def test_list_array_proxy_with_indices(self):
        class A(object):
            pass

        class B(object):
            pass
        m = A()
        o = B()
        self.assertRaises(DifferentIndexTypesError, ListArrayProxy, ((), ()),
            [m, o])

    def test_slices_with_indices(self):
        l = ListArrayProxy((range(3), range(5), range(7)),
            (3, 5, 7))
        self.assertEqual(l[:], (range(3), range(5), range(7)))
        self.assertRaises(InconsistentSlicingError, l.__getitem__, slice(3, 4))
        self.assertFalse(l[:3])
        self.assertEqual(l[:5], (range(3),))
        self.assertEqual([x for x in l[::2]], [range(3), range(7)])
        self.assertEqual([x for x in l[5:]], [range(5), range(7)])
