# `inspect.getargspec` is deprecated starting Python 3.0, but
# `getfullargspec` is not available in Python 2. See issue #670.

try:
    # use `getfullargspec` on Python 3
    from inspect import getfullargspec
    def getargspec(fn):
        """
        Return names and default values of a Python function's parameters.
        See https://docs.python.org/3/library/inspect.html
        """
        spec = getfullargspec(fn)
        # drop the keyword-only and annotation info,
        # which is not present in Py 2
        return spec[0:4]

except ImportError:
    # Python 2, we have a non-deprecated `getargspec`
    from inspect import getargspec
