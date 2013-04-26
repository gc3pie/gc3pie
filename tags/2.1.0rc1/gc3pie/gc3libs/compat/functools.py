"""
Provide the `functools.wraps` function even if your Python doesn't
support it.  The rest of `functools` is only available if your Python
has it.
"""

try:
    from functools import *
except ImportError:
    pass

try:
    wraps
except NameError:
    def wraps(original):
        def inner(fn):
            # see functools.WRAPPER_ASSIGNMENTS
            for attribute in ['__module__',
                              '__name__',
                              '__doc__'
                              ]:
                setattr(fn, attribute, getattr(original, attribute))
            # see functools.WRAPPER_UPDATES
            for attribute in ['__dict__',
                              ]:
                if hasattr(fn, attribute):
                    getattr(fn, attribute).update(getattr(original, attribute))
                else:
                    setattr(fn, attribute,
                            getattr(original, attribute).copy())
            return fn
        return inner
