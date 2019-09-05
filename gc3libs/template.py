#! /usr/bin/env python

"""
Support and expansion of programmatic templates.

The module `gc3libs.template` allows creation of textual templates
with a simple object-oriented programming interface: given a string
with a list of substitutions (using the syntax of Python's standard
`substitute` module), a set of replacements can be specified, and the
`gc3libs.template.expansions` function will generate all possible
texts coming from the same template.  Templates can be nested, and
expansions generated recursviely.
"""

# Copyright (C) 2009-2012, 2014, 2019  University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
from builtins import range
from builtins import object
__docformat__ = 'reStructuredText'


import string
import itertools

SetProductIterator = itertools.product


class Template(object):

    """
    A template object is a pair `(obj, keywords)`.  Methods are
    provided to substitute the keyword values into `obj`, and to
    iterate over expansions of the given keywords (optionally
    filtering the allowed combination of keyword values).

    Second optional argument `validator` must be a function that
    accepts a set of keyword arguments, and returns `True` if the
    keyword combination is valid (can be expanded/substituted back
    into the template) or `False` if it should be discarded.
    The default validator passes any combination of keywords/values.
    """

    def __init__(self, template,
                 validator=(lambda kws: True), **extra_args):
        self._template = template
        self._keywords = extra_args
        self._valid = validator

    def substitute(self, **extra_args):
        """
        Return result of interpolating the value of keywords into the
        template.  Keyword arguments `extra_args` can be used to override
        keyword values passed to the constructor.

        If the templated object provides a `substitute` method, then
        return the result of invoking it with the template keywords as
        keyword arguments.  Otherwise, return the result of applying
        Python standard library's `string.Template.safe_substitute()`
        on the string representation of the templated object.

        Raise `ValueError` if the set of keywords/values is not valid
        according to the validator specified in the constructor.
        """
        keywords = self._keywords.copy()
        keywords.update(extra_args)
        if self._valid(keywords):
            try:
                return self._template.substitute(**keywords)
            except AttributeError:
                return string.Template(
                    str(self._template)).safe_substitute(keywords)
        else:
            raise ValueError("Invalid substitution values in template.")

    def __eq__(self, other):
        return (
            self._template == other._template
            and self._keywords == other._keywords
            and self._valid == other._valid
        )

    def __str__(self):
        """Alias for `Template.substitute`."""
        return self.substitute()

    def __repr__(self):
        """
        Return a string representation such that `x == eval(repr(x))`.
        """
        return ''.join(["Template(",
                             ', '.join([repr(self._template)] +
                                      [("%s=%s" % (k, v))
                                       for k, v in list(self._keywords.items())]),
                             ')'])

    def expansions(self, **keywords):
        """
        Iterate over all valid expansions of the templated object
        *and* the template keywords.  Returned items are `Template`
        instances constucted with the expanded template object and a
        valid combination of keyword values.
        """
        keywords_ = self._keywords.copy()
        keywords_.update(keywords)
        for kws in expansions(keywords_):
            for item in expansions(self._template, **kws):
                # propagate expanded keywords upwards;
                # this allows container templates to
                # check validity based on keywords expanded
                # on contained templates as well.
                new_kws = kws.copy()
                for v in list(kws.values()):
                    if isinstance(v, Template):
                        new_kws.update(v._keywords)
                if self._valid(new_kws):
                    yield Template(item, self._valid, **new_kws)


def expansions(obj, **extra_args):
    """
    Iterate over all expansions of a given object, recursively
    expanding all templates found.  How the expansions are actually
    computed, depends on the type of object being passed in the first
    argument `obj`:

    * If `obj` is a `list`, iterate over expansions of items in `obj`.
      (In particular, this flattens out nested lists.)

      Example::

        >>> L = [0, [2, 3]]
        >>> list(expansions(L))
        [0, 2, 3]

    * If `obj` is a dictionary, return dictionary formed by all
      combinations of a key `k` in `obj` with an expansion of the
      corresponding value `obj[k]`.  Expansions are computed by
      recursively calling `expansions(obj[k], **extra_args)`.

      Example::

        >>> D = {'a':1, 'b':[2,3]}
        >>> E = list(expansions(D))
        >>> len(E)
        2
        >>> {'a': 1, 'b': 2} in E
        True
        >>> {'a': 1, 'b': 3} in E
        True

    * If `obj` is a `tuple`, iterate over all tuples formed by the
      expansion of every item in `obj`.  (Each item `t[i]` is expanded
      by calling `expansions(t[i], **extra_args)`.)

      Example::

        >>> T = (1, [2, 3])
        >>> list(expansions(T))
        [(1, 2), (1, 3)]

    * If `obj` is a `Template` class instance, then the returned values
      are the result of applying the template to the expansion of each
      of its keywords.

      Example::

        >>> T1 = Template("a=${n}", n=[0,1])
        >>> E = list(expansions(T1))
        >>> len(E)
        2
        >>> Template('a=${n}', n=0) in E
        True
        >>> Template('a=${n}', n=1) in E
        True

      Note that keywords passed to the `expand` invocation override
      the ones used in template construction::

        >>> T2 = Template("a=${n}")
        >>> E = list(expansions(T2, n=[1,3]))
        >>> Template('a=${n}', n=1) in E
        True
        >>> Template('a=${n}', n=3) in E
        True

    * Any other value is returned unchanged.

      Example:

        >>> V = 42
        >>> list(expansions(V))
        [42]

    """
    if isinstance(obj, dict):
        keys = tuple(obj.keys())  # fix a key order
        for items in SetProductIterator(
                *[list(expansions(obj[keys[i]], **extra_args))
                  for i in range(len(keys))]):
            yield dict((keys[i], items[i]) for i in range(len(keys)))
    elif isinstance(obj, tuple):
        for items in SetProductIterator(
                *[list(expansions(u, **extra_args)) for u in obj]):
            yield tuple(items)
    elif isinstance(obj, list):
        for item in obj:
            for result in expansions(item, **extra_args):
                yield result
    elif isinstance(obj, Template):
        for s in obj.expansions(**extra_args):
            yield s
    else:
        yield obj


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="template",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
