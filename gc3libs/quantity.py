#! /usr/bin/env python
#
"""
Manipulation of quantities with units attached with automated
conversion among compatible units.

For details and the discussion leading up to this,
see: `<https://github.com/uzh/gc3pie/issues/47>`

"""

# Copyright (C) 2011 - 2014, 2019,  University of Zurich. All rights reserved.
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
from __future__ import division
from builtins import object
from past.utils import old_div

# future's own `with_metaclass` does not play well with the `Quantity`
# implementation here, because `Quantity` defines its own `__init__`
# and `__call__` , which `future.utils.with_metaclass` overrides with
# the std ones coming from Python's builtin `type`
from six import add_metaclass

# stdlib imports
import datetime
import operator
import re

try:
    # Python 2
    from types import StringTypes as string_types
except ImportError:
    # Python 3
    string_types = (str,)


# module metadata
__docformat__ = 'reStructuredText'


# utility functions

_QTY_RE = re.compile(
    r'(?P<amount>[+-]?([0-9]+(\.[0-9]+)?|\.[0-9]+)(E[+-]?[0-9]+)?)'
    r'\s*'
    r'(?P<unit>[a-z]+)?',
    re.I | re.X)


def _split_amount_and_unit(val, default_unit=None, allow=None):
    """
    Split `val` into amount and measurement unit.

    The string to be parsed should consist of a number, followed by a
    unit specification. The number and the unit may be separated by 0
    or more spaces.

      >>> _split_amount_and_unit('7 s') == (7.0, 's')
      True
      >>> _split_amount_and_unit('7s') ==  (7.0, 's')
      True
      >>> _split_amount_and_unit('-7.0s') == (-7.0, 's')
      True

    If `val` does not conform to this syntax, a `ValueError` will
    be raised::

      >>> _split_amount_and_unit('foo')
      Traceback (most recent call last):
        ...
      ValueError: Cannot parse quantity 'foo'

    If string `val` only specifies an amount (e.g., ``42``) with no
    unit, then the optional argument `default_unit` provides one::

      >>> _split_amount_and_unit('7', 'min') == (7.0, 'min')
      True

    Note the amount is a floating point number, whereas the unit is
    always a string.  By default, no validity checks are performed
    on the 'unit' part, which can be any word::

      >>> _split_amount_and_unit('7sins') == (7.0, 'sins')
      True

    The optional argument `allow` restricts the resulting quantity to
    be a one of the specified units; if it's not, a `ValueError`
    exception is raised.

      >>> _split_amount_and_unit('42 GB', allow=['s', 'min', 'hour'])
      Traceback (most recent call last):
        ...
      ValueError: Unit 'GB' is not allowed here: only 'hour','min','s' are.

    """
    match = _QTY_RE.search(val)
    if not match:
        raise ValueError("Cannot parse quantity '%s'" % val)
    amount = float(match.group('amount'))
    unit = match.group('unit')
    if unit is None:
        unit = default_unit
    if allow is not None:
        if unit not in allow:
            raise ValueError(
                "Unit '%s' is not allowed here: only %s are." %
                (unit, ",".join([
                    ("'%s'" % a) for a in sorted(allow)])))
    return (amount, unit)


# since the code for `_Quantity` comparison methods is basically the
# same for all methods, we use a decorator-based approach to reduce
# boilerplate code...  Oh, how do I long for LISP macros! :-)
def _make_comparison_function(op, domain):
    """
    Return a function that compares ensures that the two operands
    belong to the same class, and then compares their value (obtained
    by converting to `domain`) with the passed relational operator `op`.

    Completely discards the function being decorated.
    """
    def decorate(fn):
        def cmp_fn(self, other):
            assert self.__class__ is other.__class__, (
                "Cannot compare '%s' with '%s':"
                " Can only compare homogeneous quantities!"
                % (self.__class__.__name__, other.__class__.__name__))
            return op(self.amount(unit=self.base, conv=domain),
                      other.amount(unit=self.base, conv=domain))
        return cmp_fn
    return decorate


class _Quantity(object):

    """
    Represent a dimensioned value.

    A dimensioned value is a pair formed by a magnitude and a
    measurement unit.

    |  >>> qty1 = Memory('1kB')
    |  >>> qty2 = Memory('2 kB')

    This class also implements the basic arithmetic on quantities,
    i.e., multiplication by a scalar value and taking the ratio of two
    homogeneous quantities::

    |  >>> 2 * qty1
    |  2 kB
    |  >>> 2 * qty1 == qty2
    |  True
    |  >>> qty2 / qty1
    |  2

    .. note::

      Representing dimensioned quantities is a multi-faceted problem;
      we make two simplifying assumptions in representing quantities:

      1. *Every quantity is representable as an integral multiple
         of a base quantity* (the base unit).  This holds true for
         representation of RAM memory amounts and time lapses on
         64-bit systems, which is enough for our purposes.

      2. *Homogeneous quantities are instances of the same class.*

      For more background on the 'Quantity' pattern and the issues
      in implementing it, see:

      - http://martinfowler.com/eaaDev/quantity.html
      - http://mail.python.org/pipermail/python-ideas/2010-March/006894.html
      - http://goo.gl/Drta6i

    """

    __slots__ = (
        '_amount',
        '_base',
        '_name',
        '_unit',
    )

    def amount(self, unit=None, conv=(lambda value: value)):
        """
        Return the (numerical) amount of this quantity.

        If the optional argument `unit` is specified, return the
        amount in the given units; otherwise, the amount is expressed
        in the units stored in the `unit`:attr: attribute.

        The optional argument `conv` can be used to convert the
        amounts to a specific numerical domain.  For example,
        `conv=int` returns the amount as an integer multiple of the
        unit amount; in particular, the returned amount will be `0` if
        the quantity is less than one unit amount.
        """
        if unit is None:
            unit = self.unit
        return (old_div(conv(self._amount), conv(unit._amount)))

    @property
    def base(self):
        """
        Return the base unit for all quantities in this class.
        The amount of any quantity is internally stored as a multiple
        of this base unit.

        This is a read-only class-level attribute.
        """
        return self._base

    @property
    def unit(self):
        """
        The unit the amount of this quantity is expressed in.

        This is a read-only attribute: once set in the constructor, it
        cannot be changed.
        """
        return self._unit

    @property
    def name(self):
        """
        The name of this quantity, or `None`.

        Units are named quantities; for any other quantity, the
        attribute `name` evaluates to `None`.

        This is a read-only attribute: once set in the constructor, it
        cannot be changed.
        """
        return self._name

    # instance construction

    _UNITS = {}
    """
    A registry of valid units (by name).

    Units that are listed here are recognized as valid during
    construction of a quantity from a string (see method
    `_new_from_string`:meth:).
    """

    @classmethod
    def register(cls, unit):
        """
        Register a new named quantity, i.e., a unit.
        """
        cls._UNITS[unit.name] = unit

    # since we are subclassing a builtin type,
    # we need to provide `__new__`, not `__init__`
    def __new__(cls, val, unit=None, name=None):
        # dispatch to actual constructor depending on the type of `val`
        if isinstance(val, string_types):
            new = cls._new_from_string(val)
        elif isinstance(val, _Quantity):
            new = cls._new_from_amount_and_unit(val.amount(), val.unit)
        elif unit is cls:
            # special case to bootstrap the base unit
            new = super(_Quantity, cls).__new__(cls)
            new._amount = 1
            new._unit = new
        else:
            # default: quantity is int(val)*unit
            assert unit is not None, (
                "Cannot construct a quantity from amount alone.")
            assert unit.name in cls._UNITS, (
                "Unit '%s' not allowed in '%s' quantity: only %s are."
                % (unit.name, cls.__name__,
                   ','.join([("'%s'" % u) for u in cls._UNITS])))
            new = cls._new_from_amount_and_unit(val, unit)
        new._name = name
        if name is not None:
            cls.register(new)
        return new

    @classmethod
    def _new_from_amount_and_unit(cls, amount, unit):
        new = super(_Quantity, cls).__new__(cls)
        new._amount = amount * unit.amount(cls._base)
        new._unit = unit
        new._name = None
        return new

    @classmethod
    def _new_from_string(cls, val):
        amount, unitname = _split_amount_and_unit(val, allow=cls._UNITS)
        unit = cls._UNITS[unitname]
        return cls._new_from_amount_and_unit(amount, unit)

    def __getnewargs__(self):
        # use the string serialization, as the `(amount, unit, name)`
        # constructor could lead to infinite recursion ("unit" could
        # be the very class itself that we are going to serialize)
        return (("%d %s" % (self.amount(), self.unit.name)), self.name)

    # string representation
    def to_str(self, fmt, unit=None, conv=(lambda value: value)):
        """
        Return a string representation of the quantity.

        Arguments `fmt` and `unit` influence how the string
        representation is formed::

        |  >>> qty = Memory('1024kB')
        |  >>> qty.format('%d [%s]')
        |  '1024 [kB]'
        |  >>> qty.format('%d [%s]', unit=MB)
        |  '1 [MB]'
        |  >>> qty.format('%g%s', unit=GB)
        |  0.001GB

        :param str fmt:
          Format string, with ``%``-style specifiers.

        :param unit:
          Unit quantity; the numeric amount is a multiple of this unit.

        :param conv:
          Passed unchanged to the `amount`:meth: method (which see).
        """
        if unit is None:
            unit = self.unit
        try:
            return (fmt % (self.amount(unit, conv=conv), unit.name))
        except TypeError:  # not all arguments converted
            return (fmt % self.amount(unit, conv=conv)) + unit.name

    def __str__(self):
        """
        Return a human-readable string representation of this quantity.
        """
        return self.to_str("%g%s")

    def __repr__(self):
        """
        Return a string representation that can be read back with
        `eval()`.
        """
        if hasattr(self.unit, 'name'):
            return ('%s(%g, unit=%s)'
                    % (self.__class__.__name__, self.amount(), self.unit.name))
        else:
            return (
                '%s(%g, unit=<%s object at 0x%x>)' %
                (self.__class__.__name__, self.amount(),
                 self.unit.__class__.__name__, id(self.unit)))

    # arithmetic: allow multiplication by a scalar, and division by a quantity
    # (of the same kind)
    @staticmethod
    def _smallest_unit(self, other):
        """
        Return the smallest between `self.unit` and `other.unit`.
        """
        self_unit_amount = self.unit.amount(self.base)
        other_unit_amount = other.unit.amount(self.base)
        if self_unit_amount <= other_unit_amount:
            return self.unit
        else:
            return other.unit

    @classmethod
    def _largest_nonfractional_unit(cls, amount):
        """
        Return largest unit such that `amount` is larger than 1 when
        expressed in that unit.
        """
        units = sorted(cls._UNITS.values())
        prev = units[0].base
        for unit in units:
            if prev.amount(unit.base) <= amount < unit.amount(unit.base):
                return prev
            prev = unit
        return unit

    def __add__(self, other):
        assert isinstance(other, self.__class__), \
            ("Cannot add '%s' to '%s':"
                " can sum only homogeneous quantities."
                % (self.__class__.__name__, other.__class__.__name__))
        unit = self._smallest_unit(self, other)
        return self._new_from_amount_and_unit(
            old_div((self.amount(self.base) + other.amount(self.base)),
            unit.amount(self.base)),
            unit=unit)

    def __sub__(self, other):
        assert isinstance(other, self.__class__), \
            ("Cannot subtract '%s' from '%s':"
                " can only operate on homogeneous quantities."
                % (self.__class__.__name__, other.__class__.__name__))
        unit = self._smallest_unit(self, other)
        return self._new_from_amount_and_unit(
            old_div((self.amount(self.base) - other.amount(self.base)),
            unit.amount(self.base)),
            unit=unit)

    def __mul__(self, coeff):
        if __debug__:
            try:
                float(coeff)
            except (TypeError, ValueError):
                raise TypeError("Cannot multiply '%s' and '%s':"
                                " can only multiply '%s' by a pure number."
                                "" % (coeff.__class__.__name__,
                                      self.__class__.__name__,
                                      self.__class__.__name__))
        return self._new_from_amount_and_unit(coeff * self.amount(), self.unit)
    __rmul__ = __mul__

    def __div__(self, other):
        """
        Return the ratio of two quantities (as a floating-point number),
        or divide a quantity by the specified amount.
        """
        try:
            # the quotient of two (homogeneous) quantities is a ratio (pure
            # number)
            return (old_div(self.amount(self.base, conv=float),
                    other.amount(self.base, conv=float)))
        except AttributeError:
            # we could really return `self * (1.0/other)`, but we want
            # to set the unit to a possibly smaller one (see
            # `_get_best_unit` above) to have a better "human" representation
            try:
                amount = old_div(self.amount(self.base, conv=float), float(other))
            except TypeError:
                raise TypeError(
                    "Cannot divide '%s' by '%s': can only take"
                    " the ratio of '%s' and a pure number or an"
                    " homogeneous quantity." % (self.__class__.__name__,
                                                other.__class__.__name__,
                                                self.__class__.__name__))
            unit = self._largest_nonfractional_unit(amount)
            return self._new_from_amount_and_unit(
                old_div(amount, unit.amount(self.base)), unit)

    # be compatible with `from __future__ import division`
    __truediv__ = __div__

    # FIXME: why doesn't this do the same as `__div__` (+ rounding down?)
    def __floordiv__(self, other):
        """Return the ratio of two quantities (as a whole number)."""
        assert isinstance(other, self.__class__), \
            ("Cannot divide '%s' by '%s':"
             " can only take the ratio of homogeneous quantities."
                % (self.__class__.__name__, other.__class__.__name__))
        # the quotient of two (homogeneous) quantities is a ratio (pure number)
        return (old_div(self.amount(self.base, conv=int),
                other.amount(self.base, conv=int)))

    def __radd__(self, other):
        """
        Allow summing with a null value, even if non-quantity.
        Summing with any other value raises a `TypeError`.

        This is provided only to support built-in reducers like `sum`,
        which start with a zero value and then sum all the arguments
        to it.
        """
        if other == 0 or other == 0.0:
            return self
        else:
            raise TypeError(
                "Unsupported operands for +: %s (type '%s') and %s (type '%s')"
                % (other, type(other), self, type(self)))

    # rich comparison operators, to ensure only homogeneous quantities are
    # compared
    @_make_comparison_function(operator.gt, int)
    def __gt__(self, other):
        pass

    @_make_comparison_function(operator.ge, int)
    def __ge__(self, other):
        pass

    @_make_comparison_function(operator.eq, int)
    def __eq__(self, other):
        pass

    @_make_comparison_function(operator.ne, int)
    def __ne__(self, other):
        pass

    @_make_comparison_function(operator.le, int)
    def __le__(self, other):
        pass

    @_make_comparison_function(operator.lt, int)
    def __lt__(self, other):
        pass


class Quantity(object):

    """
    Metaclass for creating quantity classes.

    This factory creates subclasses of `_Quantity`:class: and
    bootstraps the base unit.

    The name of the base unit is given as argument to the metaclass
    instance::

      >>> @add_metaclass(Quantity('B'))
      ... class Memory1(object):
      ...   pass
      ...
      >>> B = Memory1('1 B')
      >>> print (2*B)
      2B

    Optional keyword arguments create additional units; the argument
    key gives the unit name, and its value gives the ratio of the new
    unit to the base unit.  For example::

      >>> @add_metaclass(Quantity('B', kB=1000, MB=1000*1000))
      ... class Memory2(object):
      ...   pass
      ...
      >>> a_thousand_kB = Memory2('1000kB')
      >>> MB = Memory2('1   MB')
      >>> a_thousand_kB == MB
      True

    Note that the units (base and additional) are also available as
    class attributes for easier referencing in Python code::

      >>> a_thousand_kB == Memory2.MB
      True

    """

    def __init__(self, base_unit_name, **other_units):
        self.base_unit_name = base_unit_name
        self.other_units = other_units

    def __call__(self, clsname, bases, attrs):
        bases = tuple([_Quantity] + list(bases))
        newcls = type(clsname, bases, attrs)
        newcls._UNITS = dict()
        # create base unit
        base = newcls(1, unit=newcls, name=self.base_unit_name)
        newcls._base = base
        # alias the human-readable name to it
        newcls.register(base)
        setattr(newcls, self.base_unit_name, base)
        # create additional units and add them as class attributes
        for name, amount in self.other_units.items():
            unit = newcls(amount, unit=newcls._base, name=name)
            # make new units default to self when printing the amount
            unit._unit = unit
            newcls.register(unit)
            setattr(newcls, name, unit)
        return newcls


@add_metaclass(Quantity(
        # base unit is "bytes"; use the symbol 'B', although this is not the SI
        # usage.
        'B',
        # 10-base units
        kB=1000,
        MB=1000 * 1000,
        GB=1000 * 1000 * 1000,
        TB=1000 * 1000 * 1000 * 1000,
        PB=1000 * 1000 * 1000 * 1000 * 1000,
        # binary base units
        KiB=1024,                # KiBiByte
        MiB=1024 * 1024,           # MiBiByte
        GiB=1024 * 1024 * 1024,      # GiBiByte
        TiB=1024 * 1024 * 1024 * 1024,  # TiBiByte
        PiB=1024 * 1024 * 1024 * 1024 * 1024,  # PiBiByte
    ))
class Memory(object):

    """
    Represent an amount of RAM.

    Construction of a memory quantity can be done by parsing a string
    specification (amount followed by unit)::

        >>> byte = Memory('1 B')
        >>> kilobyte = Memory('1 kB')

    A new quantity can also be defined as a multiple of an existing
    one::

        >>> a_thousand_kB = 1000*kilobyte

    The base-10 units (up to TB, Terabytes) and base-2 (up to TiB,
    TiBiBytes) are available as attributes of the `Memory` class.
    This allows for a third way of constructing quantity objects,
    i.e., by passing the amount and the unit separately to the
    constructor::

        >>> a_megabyte = Memory(1, Memory.MB)
        >>> a_mibibyte = Memory(1, Memory.MiB)

        >>> a_gigabyte = 1*Memory.GB
        >>> a_gibibyte = 1*Memory.GiB

        >>> two_terabytes = 2*Memory.TB
        >>> two_tibibytes = 2*Memory.TiB

    Two memory quantities are equal if they indicate the exact same
    amount in bytes::

        >>> kilobyte == 1000*byte
        True
        >>> a_megabyte == a_mibibyte
        False
        >>> a_megabyte < a_mibibyte
        True
        >>> a_megabyte > a_gigabyte
        False

    Basic arithmetic is possible with memory quantities::

        >>> two_bytes = byte + byte
        >>> two_bytes == 2*byte
        True
        >>> half_gigabyte = a_gigabyte / 2
        >>> a_gigabyte == half_gigabyte * 2
        True
        >>> a_megabyte == a_gigabyte / 1000
        True

    The ratio of two memory quantities is correctly computed as a pure
    (floating-point) number::

        >>> a_gigabyte / a_megabyte
        1000.0

    It is also possible to add memory quantities defined with
    different units; the result is naturally expressed in the smaller
    unit of the two::

        >>> one_gigabyte_and_half = 1*Memory.GB + 500*Memory.MB
        >>> one_gigabyte_and_half
        Memory(1500, unit=MB)

    Note that the two unit class and numeric amount are accessible through
    the `unit` and `amount`:meth: attributes::

        >>> one_gigabyte_and_half.unit
        Memory(1, unit=MB)
        >>> one_gigabyte_and_half.amount()
        1500

    The `amount`:meth: method accepts an optional specification of an
    alternate unit to express the amount into::

        >>> one_gigabyte_and_half.amount(Memory.GB)
        1

    An optional `conv` argument is available to specify a numerical
    domain for conversion, in case the default integer arithmetic
    is not precise enough::

        >>> one_gigabyte_and_half.amount(Memory.GB, conv=float)
        1.5

    The `to_str`:meth: method allows representing a quantity as a
    string, and provides choice of the output format and unit.  The
    format string should contain exactly two ``%``-specifiers: the
    first one is used to format the numerical amount, and the second
    one to format the measurement unit name.

    By default, the unit used originally for defining the quantity is
    used::

        >>> '1 [MB]' == a_megabyte.to_str('%d [%s]')
        True

    This can be overridden by specifying an optional second argument
    `unit`::

        >>> '1000 [kB]' == a_megabyte.to_str('%d [%s]', unit=Memory.kB)
        True

    A third optional argument `conv` can set the numerical type to be
    used for conversion computations::

        >>> '0.001GB' == a_megabyte.to_str('%g%s', unit=Memory.GB, conv=float)
        True

    The default numerical type is `int`, which in particular implies
    that you get a null amount if the requested unit is larger than
    the quantity::

        >>> '0GB' == a_megabyte.to_str('%g%s', unit=Memory.GB, conv=int)
        True

    Conversion to string uses the unit originally used for defining
    the quantity and the ``%g%s`` format::

        >>> str(a_megabyte)
        '1MB'

    """


@add_metaclass(Quantity(
        # base unit is nanoseconds; use the SI symbol 'ns'
        'ns',
        # alternate spellings
        nanosec=1,
        nanosecond=1,
        nanoseconds=1,
        # microsecond(s)
        us=1000,  # approx SI symbol
        microsec=1000,
        microseconds=1000,
        # millisecond(s)
        ms=10 ** 6,
        millisec=10 ** 6,
        milliseconds=10 ** 6,
        # seconds(s)
        s=10 ** 9,
        sec=10 ** 9,
        secs=10 ** 9,
        second=10 ** 9,
        seconds=10 ** 9,
        # minute(s)
        m=60 * 10 ** 9,
        min=60 * 10 ** 9,
        mins=60 * 10 ** 9,
        minute=60 * 10 ** 9,
        minutes=60 * 10 ** 9,
        # hour(s)
        h=60 * 60 * 10 ** 9,
        hr=60 * 60 * 10 ** 9,
        hrs=60 * 60 * 10 ** 9,
        hour=60 * 60 * 10 ** 9,
        hours=60 * 60 * 10 ** 9,
        # day(s)
        d=24 * 60 * 60 * 10 ** 9,
        day=24 * 60 * 60 * 10 ** 9,
        days=24 * 60 * 60 * 10 ** 9,
        # week(s)
        w=7 * 24 * 60 * 60 * 10 ** 9,
        wk=7 * 24 * 60 * 60 * 10 ** 9,
        week=7 * 24 * 60 * 60 * 10 ** 9,
        weeks=7 * 24 * 60 * 60 * 10 ** 9,
    ))
class Duration(object):

    """
    Represent the duration of a time lapse.

    Construction of a duration can be done by parsing a string
    specification; several formats are accepted:

    * A duration is an aggregate of days, hours, minutes and seconds::

        >>> l3 = Duration('1day 4hours 9minutes 16seconds')
        >>> l3.amount(Duration.s) # convert to seconds
        101356

    * Any of the terms can be omitted (in which case it defaults
      to zero)::

        >>> l4 = Duration('1day 4hours 16seconds')
        >>> l4 == l3 - Duration('9 minutes')
        True

    * The unit names can be singular or plural, and any amount of
      space can be added between the time unit name and the
      associated amount::

        >>> l5 = Duration('3 hour 42 minute')
        >>> l6 = Duration('3 hours 42 minutes')
        >>> l7 = Duration('3hours 42minutes')
        >>> l5 == l6 == l7
        True

    * Unit names can also be abbreviated using just the leading
      letter::

        >>> l8 = Duration('3h 42m')
        >>> l9 = Duration('3h42m')
        >>> l8 == l9
        True

    * The abbreviated formats HH:MM:SS and DD:HH:MM:SS are also
      accepted::

        >>> # 1 hour + 1 minute + 1 second
        >>> l1 = Duration('01:01:01')
        >>> l1 == Duration('3661 s')
        True

        >>> # 1 day, 2 hours, 3 minutes, 4 seconds
        >>> l2 = Duration('01:02:03:04')
        >>> l2.amount(Duration.s)
        93784

      However, the formats HH:MM and MM:SS are rejected as ambiguous::

        >>> # is this hours:minutes or minutes:seconds ?
        >>> l0 = Duration('01:02')  # doctest:+ELLIPSIS
        Traceback (most recent call last):
          ...
        ValueError: Duration '01:02' is ambiguous: use '1m 2s' ...

    * Finally, you can specify a duration like any other quantity,
      as an integral amount of a given time unit::

        >>> l1 = Duration('1 day')
        >>> l2 = Duration('86400 s')
        >>> l1 == l2
        True

    A new quantity can also be defined as a multiple of an existing
    one::

        >>> an_hour = Duration('1 hour')
        >>> a_day = 24 * an_hour
        >>> a_day.amount(Duration.h)
        24

    The quantities ``Duration.hours``, ``Duration.minutes`` and
    ``Duration.seconds`` (and their single-letter abbreviations ``h``,
    ``m``, ``s``) are pre-defined with their obvious meaning.

    Also module-level aliases ``hours``, ``minutes`` and ``seconds``
    (and the one-letter forms) are available::

      >>> a_day1 = 24*hours
      >>> a_day2 = 1440*minutes
      >>> a_day3 = 86400*seconds

    This allows for yet another way of constructing duration objects,
    i.e., by passing the amount and the unit separately to the
    constructor::

      >>> a_day4 = Duration(24, hours)

    Two durations are equal if they indicate the exact same
    amount in seconds::

      >>> a_day1 == a_day2
      True
      >>> a_day1.amount(s)
      86400
      >>> a_day2.amount(s)
      86400

      >>> a_day == an_hour
      False
      >>> a_day.amount(minutes)
      1440
      >>> an_hour.amount(minutes)
      60

    Basic arithmetic is possible with durations::

      >>> two_hours = an_hour + an_hour
      >>> two_hours == 2*an_hour
      True
      >>> an_hour == two_hours / 2
      True

      >>> one_hour = two_hours - an_hour
      >>> one_hour.amount(seconds)
      3600

    It is also possible to add duration quantities defined with
    different units; the result is naturally expressed in the smaller
    unit of the two::

        >>> one_hour_and_half = an_hour + 30*minutes
        >>> one_hour_and_half
        Duration(90, unit=m)

    Note that the two unit class and numeric amount are accessible through
    the `unit` and `amount`:meth: attributes::

        >>> one_hour_and_half.unit
        Duration(1, unit=m)
        >>> one_hour_and_half.amount()
        90

    The `amount`:meth: method accepts an optional specification of an
    alternate unit to express the amount into::

        >>> one_hour_and_half.amount(Duration.hours)
        1

    An optional `conv` argument is available to specify a numerical
    domain for conversion, in case the default integer arithmetic
    is not precise enough::

        >>> one_hour_and_half.amount(Duration.hours, conv=float)
        1.5

    The `to_str`:meth: method allows representing a duration as a
    string, and provides choice of the output format and unit.  The
    format string should contain exactly two ``%``-specifiers: the
    first one is used to format the numerical amount, and the second
    one to format the measurement unit name.

    By default, the unit used originally for defining the quantity is
    used::

        >>> '1 [hour]' == an_hour.to_str('%d [%s]')
        True

    This can be overridden by specifying an optional second argument
    `unit`::

        >>> '60 [m]' == an_hour.to_str('%d [%s]', unit=Duration.m)
        True

    A third optional argument `conv` can set the numerical type to be
    used for conversion computations::

        >>> '60.0 [m]' == an_hour.to_str('%.1f [%s]', unit=Duration.m, conv=float)
        True

    The default numerical type is `int`, which in particular implies
    that you get a null amount if the requested unit is larger than
    the quantity::

        >>> '0 [days]' == an_hour.to_str('%d [%s]', unit=Duration.days)
        True

    Conversion to string uses the unit originally used for defining
    the quantity and the ``%g%s`` format::

        >>> str(an_hour)
        '1hour'

    """

    # override ctor to hook `_new_from_timedelta` in
    def __new__(cls, val, unit=None, name=None):
        if isinstance(val, datetime.timedelta):
            return cls._new_from_timedelta(val)
        else:
            return _Quantity.__new__(cls, val, unit, name)

    # override `_new_from_string` to allow more elaborate parsing of time specs
    @classmethod
    def _new_from_string(cls, val):
        """
        Parse string `val` and return a corresponding `Duration` object.

        See the `Duration`:class: documentation for a list of accepted
        spec formats.
        """
        match = _TIMESPEC_RE.match(val)
        if match and match.end() > match.start():
            # build duration in seconds by summing contributions;
            # use last unit as overall unit
            lapse = 0
            last_unit = Duration.s
            for name, unit_lapse, unit in _TIMESPEC_VALS:
                if match.group(name):
                    lapse += int(match.group(name)) * unit_lapse
                    last_unit = unit
            return cls._new_from_amount_and_unit(
                amount=(old_div(lapse, last_unit.amount(Duration.s))),
                unit=last_unit)
        elif ':' in val:
            # since `val` didn't match `_TIMESPEC_RE`, then it must
            # have the form HH:MM or MM:SS, which is ambiguous
            try:
                val1, val2 = val.split(':')
                val1 = int(val1)
                val2 = int(val2)
            except ValueError:
                # `val1` or `val2` cannot be converted to int
                raise ValueError("Cannot parse %r as a time duration." % val)
            raise ValueError("Duration '%s' is ambiguous:"
                             " use '%dm %ds' for %d minutes and %d seconds,"
                             " or '%dh %dm' for %d hours and %d minutes."
                             % (val,
                                val1, val2, val1, val2,
                                val1, val2, val1, val2))
        else:
            # strings like `30 seconds` can be parsed by
            # `_Quantity._new_from_string`; see http://goo.gl/pY44W5
            return super(Duration, cls)._new_from_string(val)

    @classmethod
    def _new_from_timedelta(cls, td):
        """
        Return a duration expressing the same time amount as the Python
        `datetime.timedelta` object `td`.

        Examples::

            >>> from datetime import timedelta
            >>> td1 = timedelta(days=1)
            >>> l1 = Duration(td1)
            >>> l1.amount(Duration.s)
            86400
            >>> l1 == Duration('1 day')
            True

            >>> td2 = timedelta(hours=1, minutes=1, seconds=1)
            >>> l2 = Duration(td2)
            >>> l2.amount(Duration.s)
            3661
        """
        try:
            # Python 2.7 onwards
            return cls._new_from_amount_and_unit(
                int(td.total_seconds()), Duration.s)
        except AttributeError:
            return cls._new_from_amount_and_unit(
                int(td.seconds + td.days * 24 * 3600),
                Duration.s)

    def to_timedelta(duration):
        """
        Convert a duration into a Python `datetime.timedelta` object.

        This is useful to operate on Python's `datetime.time` and
        `datetime.date` objects, which can be added or subtracted to
        `datetime.timedelta`.
        """
        return datetime.timedelta(seconds=duration.amount(Duration.s))

# needed by `Duration._new_from_string`
_TIMESPEC_RE = re.compile(
    r'('
    # 1. allow HH:MM:SS or DD:HH:MM:SS; the form XX:YY is rejected because of
    # ambiguity
    r'((?P<days1>[0-9]+):)?'
    '(?P<hours1>[0-9]+):'
    '(?P<minutes1>[0-9]+):'
    '(?P<secs1>[0-9]+)'
    r'|'
    # 2. allow `4days 3hrs`, `1 hour 4 minutes` and abbreviations `1d 2h 4m`
    r'((?P<days2>[0-9]+) \s* d(ays?))? \s*'
    r'((?P<hours2>[0-9]+) \s* h((ou)?rs?))? \s*'
    r'((?P<minutes2>[0-9]+) \s* m(in(ute)?s?))? \s*'
    r'((?P<secs2>[0-9]+) \s* s(ecs?))?'
    # 3. everything else is forwarded to `_Quantity._new_from_string`
    r')',
    re.X | re.I)
_TIMESPEC_VALS = [
    # regexp group name
    # |          unit time lapse as seconds
    # |          |         corresponding `Duration` unit
    # |          |         |
    ('days1', 24 * 60 * 60, Duration.day),
    ('hours1', 60 * 60, Duration.hour),
    ('minutes1', 60, Duration.minute),
    ('secs1', 1, Duration.s),
    ('days2', 24 * 60 * 60, Duration.day),
    ('hours2', 60 * 60, Duration.hour),
    ('minutes2', 60, Duration.minute),
    ('secs2', 1, Duration.s),
]


# aliases for common units

B = Memory.B
byte = B
bytes = B

kB = Memory.kB
MB = Memory.MB
GB = Memory.GB
TB = Memory.TB

KiB = Memory.KiB
MiB = Memory.MiB
GiB = Memory.GiB
TiB = Memory.TiB

s = Duration.s
secs = s
seconds = s

m = Duration.m
mins = m
minutes = m

h = Duration.h
hrs = h
hours = h

d = Duration.d
days = d


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="quantity",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
