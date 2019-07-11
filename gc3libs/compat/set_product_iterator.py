"""
Backport of itertools.product, used in gc3libs.template
"""


class SetProductIterator(object):

    """Iterate over all elements in a cartesian product.

    Argument `factors` is a sequence, all whose items are sequences
    themselves: the returned iterator will return -upon each
    successive invocation- a list `[t_1, t_2, ..., t_n]` where `t_k`
    is an item in the `k`-th sequence.

    Examples::
      >>> list(SetProductIterator([]))
      [[]]
      >>> list(SetProductIterator([1]))
      [[1]]
      >>> list(SetProductIterator([1],[1]))
      [[1, 1]]
      >>> list(SetProductIterator([1,2],[]))
      [[]]
      >>> list(SetProductIterator([1,2],[1]))
      [[1, 1], [2, 1]]
      >>> list(SetProductIterator([1, 2], [1, 2]))
      [[1, 1], [2, 1], [1, 2], [2, 2]]
    """

    def __init__(self, *factors):
        self.__closed = False
        self.__factors = factors
        self.__L = len(factors)
        self.__M = [len(s) - 1 for s in factors]
        self.__m = [0] * self.__L
        self.__i = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.__closed:
            raise StopIteration
        if (0 == self.__L) or (-1 in self.__M):
            # there are no factors, or one of them has no elements
            self.__closed = True
            return []
        else:
            if self.__i < self.__L:
                # will return element corresponding to current multi-index
                result = [s[self.__m[i]]
                          for (i, s) in enumerate(self.__factors)]
                # advance multi-index
                i = 0
                while (i < self.__L):
                    if self.__m[i] == self.__M[i]:
                        self.__m[i] = 0
                        i += 1
                    else:
                        self.__m[i] += 1
                        break
                self.__i = i
                # back to caller
                return result
            else:
                # at end of iteration
                self.__closed = True
                raise StopIteration
