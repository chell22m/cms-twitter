"""
CountMinSketch

Implementation of Count Min Sketch algorithms as described in

G. Cormode and S. Muthukrishnan. An improved data stream summary: The count-min
sketch and its applications.  LATIN 2004, J. Algorithm 55(1): 58-75 (2005).

and further details in:

G. Cormode and S. Muthukrishnan.  Approximating data with the count-min data
structure. IEEE Software, (2012)
http://dimacs.rutgers.edu/%7Egraham/pubs/papers/cmsoft.pdf

Hash function implementation sourced from Rafael Carrascosa's implementation of
Count-Min Sketch:
https://github.com/rafacarrascosa/countminsketch/blob/master/countminsketch.py
"""

import array
import copy
import hashlib
import heapq
import math


class CountMinSketch():
    """
    A basic implementation of the Count Min Sketch data structure.

    This class keeps track of frequencies of objects seen in a stream, so it is
    essentially a fancy counter with constant memory, and constant insertion /
    lookup times for queries.

    Note that the count for items are always slight overestimates of the true
    value.
    """

    def __init__(self, w, d, k=10):
        """
        Initialize a Count-Min Sketch of size w x d.

        :param w: number of columns in sketch - width
        :type w: int

        :param d: number of rows in sketch - depth
        :type d: int

        :param k: number of heavy hitters to keep track of - default = 10
        :type k: int
        """
        if not w or not d:
            raise ValueError('The width and depth of the sketch table must not'
                             ' be zero.')
        self._w = w
        self._d = d
        self._k = k
        self._n = 0  # total count
        self._eps = 2.0 / w  # epsilon = error
        self._delta = 1.0 - 1.0 / math.pow(2, d)  # confidence / accuracy
        self._heap = []  # keeps track of heavy hitters in priority queue heap
        self._top_k = {}  # keeps the top k items for easy lookup
        self._tables = [
            array.array('l', [0 for val in range(w)]) for depth in range(d)]

    def _hash(self, item):
        """
        Retrieve the d hash functions and calculates item's hash for each
        function.

        Source: This function is taken from Rafael Carrascosa's implementation
        of Count-Min Sketch.

        :param item: whose hash needs to be calculated
        :type item: any hashable object

        :return: list of hash ids generated by each d hash functions
        :rtype: List of int
        """
        md5 = hashlib.md5(str(hash(item)).encode('utf-8'))
        for i in range(self._d):
            md5.update(str(i).encode('utf-8'))
            yield int(md5.hexdigest(), 16) % self._w

    def update(self, item, count=1):
        """
        Update the frequency for 'item' by 'count'. This increases the number
        of appearences of 'item'.

        :param item: the item to keep count of
        :type item: any hashable object

        :param count: value to increment the frequency of item by. Holds a
            default value is 1.
        :type count: int

        :raises ValueError: if a negative value for 'count' is given.
        """
        if count < 0:
            raise ValueError(
                'Cannot add negative count value in sketch for item: ' +
                str(item))
        self._n += count
        for table, idx in zip(self._tables, self._hash(item)):
            table[idx] += count

        self._update_heap(item)

    def _update_heap(self, item):
        """
        Update the heap that keeps track of the heavy hitters.

        :param item: the item to keep count of
        :type item: any hashable object
        """
        heap = self._heap
        top_k = self._top_k
        estimate = self.estimate(item)

        # update heap and top_k if item already exists
        if item in top_k:
            top_k[item][0] = estimate
            heapq.heapify(heap)
            return

        pair = [estimate, item]

        # automatically add to heap if number of elements has not reached k
        if len(heap) < self._k:
            top_k[item] = pair
            heapq.heappush(heap, pair)
            return

        if estimate <= heap[0][0]:  # if smaller than the smallest heavy hitter
            return

        # add new item as a heavy hitter and remove smallest
        rmv_estimate, rmv_item = heapq.heappushpop(heap, pair)
        top_k.pop(rmv_item)
        top_k[item] = pair

    def estimate(self, item):
        """
        Return the frequency of item. Note that the estimate will be an
        approximation greater than the true frequency.

        :param item: the item whose frequency must be estimated
        :type item: any hashable object

        :return: the frequncy of item
        :rtype: int
        """
        return min(table[idx] for table, idx in
                   zip(self._tables, self._hash(item)))

    def heavyHitters(self):
        """
        Return the heavy hitters for this sketch.

        :return: heavy hitter elements and their frequencies
        :rtype: dict
        """
        return copy.deepcopy(self._top_k)

    def merge(self, count_min_sketch):
        """
        Merge two Count-Min sketcher together

        :param count_min_sketch: another Count-Min sketch to merge with this
        :type count_min_sketch: `count_min_sketch.CountMinSketch()`
        """
        raise NotImplemented('Method has not yet been implemented')

    def relativeError(self):
        """
        Return the relative error for a query in this sketch

        :return: relative error of the estimate
        :rtype: float
        """
        return self._eps

    def confidence(self):
        """
        Return the confidence for a query in this sketch

        :return: confidence of the estimate
        :rtype: float
        """
        return self._delta

    def totalCount(self):
        """
        Get the total number of items counted

        :return: total number of items counted
        :rtype: int
        """
        return self._n


class CountMinSketchUsingEpsAndDelta(CountMinSketch):

    def __init__(self, eps, delta):
        """
        Initialize a Count-Min Sketch using epsilon and delta parameters.

        2/w = eps ; w = 2/eps
        1/2^depth <= 1-confidence ; depth >= log_{1/2} (1-confidence)

        :param eps: an error at most of epsilon (of the total count) from the
            true frequency of a query.
        :type eps: float

        :param delta: the probability of the error for a query = confidence.
        :type delta: float
        """
        w = int(math.ceil(1 / eps))
        d = int(math.ceil(math.log(1 - delta, base=0.5)))
        super(CountMinSketchUsingEpsAndDelta, self).__init__(w, d)
