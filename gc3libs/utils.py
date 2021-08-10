#! /usr/bin/env python

"""
Generic Python programming utility functions.

This module collects general utility functions, not specifically
related to GC3Libs.  A good rule of thumb for determining if a
function or class belongs in here is the following: place a function
or class in this module if you could copy its code into the
sources of a different project and it would not stop working.
"""

# Copyright (C) 2009-2019  University of Zurich. All rights reserved.
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
from future import standard_library
standard_library.install_aliases()
from builtins import next
from builtins import range
from builtins import object
__docformat__ = 'reStructuredText'


from codecs import decode
from collections import defaultdict, deque
try:
    # Python 3
    from collections.abc import Mapping, MutableMapping
except ImportError:
    from collections import Mapping, MutableMapping
import contextlib
import functools
from .compat._functools import total_ordering
import locale
import os
import os.path
if bytes == str:
    # Python2's `os.sysconf()` cannot handle sysconf variable names as
    # Unicode strings, so we need to cast them to byte-strings
    def os_sysconf(name):
        return os.sysconf(str(name))
else:
    # Python3
    from os import sysconf as os_sysconf
import random
import re
import shutil
import sys
import tempfile
import time
import io as StringIO


from gc3libs.compat._collections import OrderedDict
from gc3libs.compat._functools import total_ordering
import lockfile

import gc3libs
import gc3libs.exceptions
import gc3libs.debug
from gc3libs.quantity import Duration, Memory


if bytes == str:
    # Python 2
    text_str = unicode
else:
    # Python 3+
    text_str = str


# This fixes an issue with Python 2.4, which does not have
# `shutl.WindowsError`
try:
    WindowsError = shutil.WindowsError
except AttributeError:
    class NeverUsedException(Exception):

        """this exception should never be raised"""
    WindowsError = NeverUsedException


# map suffix to pow multipliers. This is 2.6 compatible.
BYTE_SUFFIX_TO_POW = dict(((l, i)) for i, l in enumerate('kmgtpezy', start=1))
# 2.7 +:  mapped = {l: i for i, l in enumerate('kmgtpezy', start=1)}


def backup(path):
    """
    Rename the filesystem entry at `path` by appending a unique
    numerical suffix; return new name.

    For example,

    1. create a test file:

      >>> import tempfile
      >>> path = tempfile.mkstemp()[1]

    2. then make a backup of it; the backup will end in ``.~1~``:

      >>> path1 = backup(path)
      >>> os.path.exists(path + '.~1~')
      True

    3. re-create the file, and make a second backup: this time the
    file will be renamed with a ``.~2~`` extension:

      >>> open(path, 'w').close()
      >>> path2 = backup(path)
      >>> os.path.exists(path + '.~2~')
      True

    cleaning up tests

      >>> os.remove(path+'.~1~')
      >>> os.remove(path+'.~2~')

    """
    parent_dir = (os.path.dirname(path) or os.getcwd())
    prefix = os.path.basename(path) + '.~'
    p = len(prefix)
    suffix = 1
    for name in [entry for entry in os.listdir(parent_dir)
                 if (entry.startswith(prefix) and entry.endswith('~'))]:
        try:
            n = int(name[p:-1])
            suffix = max(suffix, n + 1)
        except ValueError:
            # ignore non-numeric suffixes
            pass
    new_path = "%s.~%d~" % (path, suffix)
    os.rename(path, new_path)
    return new_path


def basename_sans(path):
    """
    Return base name without the extension.

    This behaves exactly like :func:`os.path.basename` except that the
    last few characters, up to the rightmost dot, are removed as
    well::

      >>> basename_sans('/tmp/foo.txt') == 'foo'
      True

      >>> basename_sans('bar.txt') == 'bar'
      True

    If there is no dot in the file name, no "extension" is chopped
    off::

      >>> basename_sans('baz') == 'baz'
      True

    If there are several dots in the file name, only the last one and
    trailing characters are removed::

      >>> basename_sans('foo.bar.baz') == 'foo.bar'
      True

    Leading directory components are chopped off in any case::

      >>> basename_sans('/tmp/foo.bar.baz') == 'foo.bar'
      True

      >>> basename_sans('/tmp/foo') == 'foo'
      True
    """
    return os.path.splitext(os.path.basename(path))[0]


def cache_for(lapse):
    """
    Cache the result of a (nullary) method invocation for a given
    amount of time. Use as a decorator on object methods whose results
    are to be cached.

    Store the result of the first invocation of the decorated
    method; if another invocation happens before `lapse` seconds
    have passed, return the cached value instead of calling the real
    function again.  If a new call happens after the grace period has
    expired, call the real function and store the result in the cache.

    **Note:** Do not use with methods that take keyword arguments, as
    they will be discarded! In addition, arguments are compared to
    elements in the cache by *identity*, so that invoking the same
    method with equal but distinct object will result in two separate
    copies of the result being computed and stored in the cache.

    Cache results and timestamps are stored into the objects'
    `_cache_value` and `_cache_last_updated` attributes, so the caches
    are destroyed with the object when it goes out of scope.

    The working of the cached method can be demonstrated by the
    following simple code::

        >>> class X(object):
        ...     def __init__(self):
        ...         self.times = 0
        ...     @cache_for(2)
        ...     def foo(self):
        ...             self.times += 1
        ...             return self.times
        >>> x = X()
        >>> x.foo()
        1
        >>> x.foo()
        1
        >>> time.sleep(3)
        >>> x.foo()
        2

    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(obj, *args):
            now = time.time()
            key = (fn, tuple(id(arg) for arg in args))
            try:
                update = ((now - obj._cache_last_updated[key]) > lapse)
            except AttributeError:
                obj._cache_last_updated = defaultdict(lambda: 0)
                obj._cache_value = dict()
                update = True
            if update:
                obj._cache_value[key] = fn(obj, *args)
                obj._cache_last_updated[key] = now
            # gc3libs.log.debug("%s(%s, ...): Using cached value '%s'",
            #                  fn.__name__, obj, obj._cache_value[key])
            return obj._cache_value[key]
        return wrapper
    return decorator


def cat(*args, **extra_args):
    """
    Concatenate the contents of all `args` into `output`.  Both
    `output` and each of the `args` can be a file-like object or a
    string (indicating the path of a file to open).

    If `append` is `True`, then `output` is opened in append-only
    mode; otherwise it is overwritten.
    """
    output = extra_args.get('output', sys.stdout)
    append = extra_args.get('append', True)
    # ensure `output` is a file-like object, opened in write-mode
    try:
        output.write('')
    except:
        output = open(output, ('a' if append else 'w'))
    for arg in args:
        # ensure `arg` is a file-like object, opened in read-mode
        try:
            arg.read(0)
        except:
            arg = open(arg, 'r')
        for line in arg:
            output.write(line)


def check_file_access(path, mode, exception=RuntimeError, isdir=False):
    """
    Test for access to a path; if access is not granted, raise an
    instance of `exception` with an appropriate error message.
    This is a frontend to `os.access`:func:, which see for exact
    semantics and the meaning of `path` and `mode`.

    :param path: Filesystem path to test.
    :param mode: See `os.access`:func:
    :param exception: Class of exception to raise if test fails.
    :param isdir: If `True` then also test that `path` points to a directory.

    If the test succeeds, `True` is returned::

      >>> check_file_access('/bin/sh', os.F_OK)
      True
      >>> check_file_access('/bin/sh', os.R_OK)
      True
      >>> check_file_access('/bin/sh', os.X_OK)
      True
      >>> check_file_access('/tmp', os.X_OK)
      True

    However, if the test fails, then an exception is raised::

      >>> check_file_access('/proc/version', os.W_OK)
      Traceback (most recent call last):
        ...
      RuntimeError: Cannot write to file '/proc/version'.

    If the optional argument `isdir` is `True`, then additionally test
    that `path` points to a directory inode::

      >>> check_file_access('/tmp', os.F_OK, isdir=True)
      True

      >>> check_file_access('/bin/sh', os.F_OK, isdir=True)
      Traceback (most recent call last):
        ...
      RuntimeError: Expected '/bin/sh' to be a directory, but it's not.
    """
    what = ("directory" if isdir else "file")
    if not os.access(path, os.F_OK):
        raise exception("Cannot access %s '%s'." % (what, path))
    if isdir and not os.path.isdir(path):
        raise exception(
            "Expected '%s' to be a directory, but it's not." % path)
    if (mode & os.R_OK) and not os.access(path, os.R_OK):
        raise exception("Cannot read %s '%s'." % (what, path))
    if (mode & os.W_OK) and not os.access(path, os.W_OK):
        raise exception("Cannot write to %s '%s'." % (what, path))
    if (mode & os.X_OK) and not os.access(path, os.X_OK):
        if isdir:
            raise exception("Cannot traverse directory '%s':"
                            " lacks 'x' permission." % path)
        else:
            raise exception("File '%s' lacks execute ('x') permission." % path)
    return True


def deploy_configuration_file(filename, template_filename=None):
    """
    Ensure that configuration file `filename` exists; possibly
    copying it from the specified `template_filename`.

    Return `True` if a file with the specified name exists in the
    configuration directory.  If not, try to copy the template file
    over and then return `False`; in case the copy operations fails,
    a `NoConfigurationFile` exception is raised.

    The `template_filename` is always resolved relative to GC3Libs'
    'package resource' directory (i.e., the ``etc/`` directory in the
    sources.  If `template_filename` is `None`, then it is assumed to
    be the base name of `filename`.
    """
    if template_filename is None:
        template_filename = os.path.basename(filename)
    if not os.path.isabs(filename):
        filename = os.path.abspath(filename)
    if os.path.exists(filename):
        return True
    else:
        try:
            from pkg_resources import (Requirement, resource_filename,
                                       DistributionNotFound)
            import shutil
        except ImportError as err:
            raise gc3libs.exceptions.FatalError(
                "Cannot import required Python modules: %s."
                " Please check GC3Pie installation instructions at"
                " http://gc3pie.googlecode.com/svn/trunk/gc3pie/docs/html/install.html" %  # noqa
                str(err))
        try:
            # copy sample config file
            if not os.path.exists(dirname(filename)):
                os.makedirs(dirname(filename))
            sample_config = resource_filename(
                Requirement.parse("gc3pie"),
                "gc3libs/etc/" +
                template_filename)
            shutil.copyfile(sample_config, filename)
            return False
        except IOError as err:
            gc3libs.log.critical("Failed copying configuration file: %s", err)
            raise gc3libs.exceptions.NoConfigurationFile(
                "No configuration file '%s' was found, and an attempt to"
                " create it failed. Aborting." % filename)
        except DistributionNotFound as ex:
            raise AssertionError(
                "Cannot access resources for Python package: %s."
                " Installation error?" %
                str(ex))


def dirname(pathname):
    """
    Same as `os.path.dirname` but return `.` in case of path names with no
    directory component.
    """
    # FIXME: figure out if this is a desirable outcome.  i.e. do we
    # want dirname to be empty, or do a pwd and find out what the
    # current dir is, or keep the "./".  I suppose this could make a
    # difference to some of the behavior of the scripts, such as
    # copying files around and such.
    return os.path.dirname(pathname) or '.'


class Enum(frozenset):
    """
    A generic enumeration class.  Inspired by: http://goo.gl/1AL5N0
    with some more syntactic sugar added.

    An `Enum` class must be instanciated with a list of strings, that
    make the enumeration "label"::

      >>> Animal = Enum('CAT', 'DOG')

    Each label is available as an instance attribute, evaluating to
    itself::

      >>> Animal.DOG
      'DOG'

      >>> Animal.CAT == 'CAT'
      True

    As a consequence, you can test for presence of an enumeration
    label by string value::

      >>> 'DOG' in Animal
      True

    Finally, enumeration labels can also be iterated upon::

      >>> for a in sorted(Animal): print(a)
      CAT
      DOG
    """
    def __new__(cls, *args):
        return frozenset.__new__(cls, args)

    def __getattr__(self, name):
        if name in self:
            return name
        else:
            raise AttributeError("No '%s' in enumeration '%s'"
                                 % (name, self.__class__.__name__))

    def __setattr__(self, name, value):
        raise SyntaxError("Cannot assign enumeration values.")

    def __delattr__(self, name):
        raise SyntaxError("Cannot delete enumeration values.")


class ExponentialBackoff(object):
    """Generate waiting times with the `exponential backoff`_ algorithm.

    Returned times are in seconds (or fractions thereof); they are
    integral multiples of the basic time slot, which is set with the
    `slot_duration` constructor parameter.

    After `max_retries` have been attempted, any call to this iterator
    will raise a `StopIteration` exception.

    The `ExponentialBackoff` class implements the iterator protocol,
    so you can just retrieve waiting times with the `.next()` method,
    or by looping over it::

      >>> lapses = list(ExponentialBackoff(max_retries=7))
      >>> len(lapses)
      8

    .. _`exponential backoff`: http://goo.gl/PxVICA

    """

    def __init__(self, slot_duration=0.05, max_retries=5):
        self.attempt = -1
        self.slot_duration = slot_duration
        self.max_retries = max_retries

    def __iter__(self):
        return self

    def __next__(self):
        """Return next waiting time."""
        self.attempt += 1
        if self.attempt > self.max_retries:
            raise StopIteration
        return self.slot_duration * random.randint(0, 2 ** self.attempt - 1)

    def wait(self):
        """Wait for another while."""
        time.sleep(next(self))


def first(seq):
    """
    Return the first element of sequence or iterator `seq`.
    Raise `TypeError` if the argument does not implement
    either of the two interfaces.

    Examples::

      >>> s = [0, 1, 2]
      >>> first(s)
      0

      >>> s = {'a':1, 'b':2, 'c':3}
      >>> first(sorted(s.keys())) == 'a'
      True
    """
    try:  # try iterator interface
        return next(seq)
    except TypeError:
        pass
    try:  # seq is no iterator, try indexed lookup
        return seq[0]
    except IndexError:
        pass
    raise TypeError(
        "Argument to `first()` method needs to be iterator or sequence.")


def fgrep(literal, filename):
    """
    Iterate over all lines in a file that contain the `literal` string.
    """
    with open(filename, 'r') as stream:
        for line in stream:
            if literal in line:
                yield line


def from_template(template, **extra_args):
    """
    Return the contents of `template`, substituting all occurrences
    of Python formatting directives '%(key)s' with the corresponding values
    taken from dictionary `extra_args`.

    If `template` is an object providing a `read()` method, that is
    used to gather the template contents; else, if a file named
    `template` exists, the template contents are read from it;
    otherwise, `template` is treated like a string providing the
    template contents itself.
    """
    if hasattr(template, 'read') and callable(template.read):
        template_contents = template.read()
    elif os.path.exists(template):
        template_file = file(template, 'r')
        template_contents = template_file.read()
        template_file.close()
    else:
        # treat `template` as a string
        template_contents = template
    # substitute `extra_args` into `t` and return it
    return (template_contents % extra_args)


def from_encoded_bytes(chars, encodings):
    """
    Convert *chars* to Python unicode string, trying different encodings.

    Try converting byte string *chars* to a Python text string (type
    ``unicode`` on Py2, type ``str`` on Py3), trying each of the
    encodings specified until one of them succeeds.

    If none of the encodings work, raise ``UnicodeDecodeError``.

    :param bytes chars:
      Byte string to convert to text
    :param list encodings:
      List of encodings to try, in sequence. (e.g., ``['utf-8', 'latin-1']``
    :raises UnicodeDecodeError:
      When none of the encodings can successfully convert the given byte string.
    """
    for encoding in encodings:
        try:
            return decode(chars, encoding)
        except UnicodeDecodeError:
            pass
    raise UnicodeDecodeError(
        "Cannot decode byte string {0!r}; tried encodings {1!r}."
        .format(chars, encodings))


def from_filesystem_bytes(chars):
    """
    Convert *chars* to Python unicode string, trying different encodings.

    This function should be used to make a Python text string (type
    ``unicode`` on Python 2, type ``str`` on Python 3) out of a byte
    string of characters that result from a filesystem lookup
    operation.  Conversion to a text string is attempted using the
    following encodings, in order:

    #. the encoding determined by the current locale
       (as determined by Python's ``locale.getlocale()``);
    #. the "user's preferred encoding", as determined
       by Python's ``locale.getpreferredencoding()``;
    #. UTF-8 encoding;
    #. direct map of byte values 0x0 through 0xff to the corresponding
       Unicode code points.

    The latter conversion will *not* be a valid text conversion (i.e.,
    it will not preserve any text representation of the string),
    unless the ISO-8859-1 (aka "latin-1") encoding is used.

    However, the issue we need to solve here is the mismatch of
    Python's use of text strings to represent path names with UNIX' C
    library use of byte strings to represent the same.  Since the UNIX
    kernel has no notion of character encoding in path names (as
    opposed to Windows, which uses UNICODE since Win95), there is
    actually no guarantee that a given file name can be decoded.  This
    is particularly evident on multi-user Linux/UNIX systems where you
    may be using e.g. the ``en_US.UTF-8`` locale but your Russian
    colleague may be using KOI8-R...

    :param bytes chars:
      Byte string to convert to text
    :raises UnicodeDecodeError:
      When none of the encodings can successfully convert the given byte string.
    """
    encodings = ['utf-8', 'latin-1']
    preferred = locale.getpreferredencoding()
    if preferred:
        encodings.insert(0, preferred)
    default = locale.getlocale()[1]
    if default:
        encodings.insert(0, default)
    return from_encoded_bytes(chars, encodings)


def from_terminal_bytes(chars):
    """
    Convert *chars* to Python unicode string, using current locale encoding.

    This function should be used to make a Python text string (type
    ``unicode`` on Python 2, type ``str`` on Python 3) out of a byte
    string of characters that were inputed by users in a terminal
    application (e.g., returned by ``input()`` or typed as
    command-line arguments).

    If the current locale encoding cannot be determined, then the byte
    string is assumed to be an ASCII-only string.

    :param bytes chars:
      Byte string to convert to text
    :raises UnicodeDecodeError:
      When none of the encodings can successfully convert the given byte string.
    """
    return decode(chars, locale.getlocale()[1] or 'ascii')



def get_available_physical_memory():
    """
    Return size of available memory (as a `gc3libs.quantity.Memory` object).
    The figure only refers to RAM, i.e., *physical* memory as opposed
    to *virtual* memory (swap).

    Should work on any POSIX system that supports the
    ``_SC_AVPHYS_PAGES`` variable in the ``sysconf()`` C library call.

    :raise NotImplementedError:
      If syscalls to determine amount of available physical memory
      are not implemented on this system.
    """
    try:
        pagesize = os_sysconf('SC_PAGE_SIZE')
        avail_pages = os_sysconf('SC_AVPHYS_PAGES')
        return Memory(avail_pages * pagesize, unit=Memory.B)
    except ValueError:
        raise NotImplementedError(
            "Cannot determine amount of available physical memory.")


def get_linux_memcg_limit():
    """
    Return memory limit in this process' Linux memory cgroup.
    Return value is a `gc3libs.quantity.Memory` object,
    or ``None`` if no limit can be detected.

    As the Linux "memory cgroup" mechanism implements different
    limits, and not all of them might have been set/enforced, we we
    read possible limits in supposedly ascending order ("soft" limits
    *should* be lower than "hard" limits) and return first one that
    exists.

    See also: `<https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt>`_
    """
    memcg_path = None
    try:
        for line in fgrep(':memory:', '/proc/self/cgroup'):
            _, _, memcg_path = line.rstrip().split(':')
            break
    except EnvironmentError:
        # older kernel, no file `/proc/self/cgroup`
        pass
    if memcg_path is None:
        # no memory cgroup?
        return None
    # XXX: hard-coded cgroupfs mountpoint path
    memcg_dir = os.path.join('/sys/fs/cgroup/memory', memcg_path.lstrip('/'))
    limit = None
    # read possible limits in ascending order ("soft" limits *should*
    # be lower than "hard" limits) and return first one that exists
    for limit_file in [
            'memory.soft_limit_in_bytes',
            'memory.limit_in_bytes',
            'memory.memsw.limit_in_bytes',
    ]:
        path = os.path.join(memcg_dir, limit_file)
        try:
            with open(path, 'r') as limit_file:
                limit = int(limit_file.read())
                break
        except (EnvironmentError, ValueError):
            pass
    if limit:
        return Memory(limit, unit=Memory.B)
    else:
        return None


def get_max_real_memory():
    """
    Return maximum size of available *real* memory.
    Return value is a `gc3libs.quantity.Memory` object,
    or ``None`` if no limit can be detected.

    Various sources are polled for a limit, and the minimum is returned:

    - *available physical memory*, as reported by sysconf(3);
    - current resource limits (also known as ``ulimit`` in shell programming)
      ``RLIMIT_DATA`` and ``RLIMIT_AS``;
    - current Linux memory cgroup limits.
    """
    limits = []
    # Python's `posix` module does not expose an interface to the libc
    # `getrlimit()` call, so we resort to parsing `/proc/self/limits`
    # if available
    try:
        ulimit, _ = parse_linux_proc_limits()
        rlimit_as = ulimit['max_address_space']
        if rlimit_as:
            limits.append(rlimit_as)
        rlimit_data = ulimit['max_data_size']
        if rlimit_data:
            limits.append(rlimit_data)
    except (IOError, ValueError):
        pass
    # try using sysconf()
    try:
        avail_ram = get_available_physical_memory()
        limits.append(avail_ram)
    except NotImplementedError:
        pass
    # try Linux memory cgroup
    memcg_limit = get_linux_memcg_limit()
    if memcg_limit:
        limits.append(memcg_limit)
    # whichever is lower
    if limits:
        return min(limits)
    else:
        return None


def get_num_processors():
    """
    Return number of online processor cores.
    """
    # try different strategies and use first one that succeeeds
    try:
        return os.cpu_count()  # Py3 only
    except AttributeError:
        pass
    try:
        import multiprocessing
        return multiprocessing.cpu_count()
    except ImportError:  # no multiprocessing?
        pass
    except NotImplementedError:
        # multiprocessing cannot determine CPU count
        pass
    return None


def get_scheduler_and_lock_factory(lib):
    """
    Return factories for creating a period task scheduler and locks.

    The scheduler will be a scheduler class from the APScheduler_
    framework (which see for the API), and the lock factory is an
    appropriate locking object for synchronizing independently running
    tasks. Example::

        sched_factory, lock_factory = _get_scheduler_and_lock_factory('threading')
        sched = sched_factory()
        sched.add_job(task1, 'interval', seconds=5)
        sched.add_job(task2, 'interval', seconds=30)

        shared_data_lock = lock_factory()

        def task1():
          # ...
          with shared_data_lock:
            # modify shared data

    Argument `lib` is one of: ``threading``, ``gevent``, ``tornado``,
    ``asyncio`` (Python 3.5+ "async" system), ``twisted``, ``qt``;
    each of them selects a scheduler and lock objects compatible with
    the named framework for concurrent processing.

    .. _APScheduler: https://apscheduler.readthedocs.org/en/latest/userguide.html
    """
    if lib == 'threading':
        from apscheduler.schedulers.background import BackgroundScheduler
        from threading import Lock
        return (BackgroundScheduler, Lock)
    elif lib == 'gevent':
        from apscheduler.schedulers.gevent import GeventScheduler
        from gevent.lock import Semaphore
        return (GeventScheduler, Semaphore)
    elif lib in ['asyncio', 'tornado', 'twisted', 'qt']:
        raise NotImplemented(
            "Support for {lib} is not yet available!"
            .format(lib=lib))
    else:
        raise ValueError(
            "Library '{lib}' is unknown to `{mod}._get_scheduler_and_lock_factory()`"
            .format(lib=lib, mod=__name__))


def getattr_nested(obj, name):
    """
    Like Python's `getattr`, but perform a recursive lookup if `name` contains
    any dots.
    """
    dots = name.count('.')
    if dots == 0:
        return getattr(obj, name)
    else:
        first, rest = name.split('.', 1)
        return getattr_nested(getattr(obj, first), rest)


def grep(pattern, filename):
    """
    Iterate over all lines in a file that match the `pattern` regular
    expression.
    """
    rx = re.compile(pattern)
    with open(filename, 'r') as file:
        for line in file:
            if rx.search(line):
                yield line


def ifelse(test, if_true, if_false):
    """
    Return `if_true` is argument `test` evaluates to `True`,
    return `if_false` otherwise.

    This is just a workaround for Python 2.4 lack of the
    conditional assignment operator::

      >>> a = 1
      >>> b = ifelse(a, "yes", "no"); print(b)
      yes
      >>> b = ifelse(not a, 'yay', 'nope'); print(b)
      nope

    """
    if test:
        return if_true
    else:
        return if_false


# original source: https://gist.github.com/jtriley/7270594
def insert_char_every_n_chars(string, char='\n', every=64):
    return char.join(
        string[i:i + every] for i in range(0, len(string), every))


def irange(start, stop, step=1):
    """
    Iterate over all values greater or equal than `start` and less than `stop`.
    (Or the reverse, if `step < 0`.)

    Example::

      >>> list(irange(1, 5))
      [1, 2, 3, 4]
      >>> list(irange(0, 8, 3))
      [0, 3, 6]
      >>> list(irange(8, 0, -2))
      [8, 6, 4, 2]

    Unlike the built-in `range` function, `irange` also accepts
    floating-point values::

      >>> list(irange(0.0, 1.0, 0.5))
      [0.0, 0.5]

    Also unlike the built-in `range`, *both* `start` and `stop` have
    to be specified::

      >>> try:
      ...   irange(42)
      ... except TypeError:
      ...   print("missing required argument!")
      missing required argument!

    Of course, a null `step` is not allowed::

      >>> try:
      ...   list(irange(1, 2, 0))
      ... except AssertionError as err:
      ...   assert 'Null step in irange.' in str(err)
    """
    assert float(step) != 0.0, "Null step in irange."
    value = start
    if step > 0:
        while value < stop:
            yield value
            value += step
    else:  # step < 0
        while value > stop:
            yield value
            value += step


def lock(path, timeout, create=True):
    """
    Lock the file at `path`.  Raise a `LockTimeout` error if the lock
    cannot be acquired within `timeout` seconds.

    Return a `lock` object that should be passed unchanged to the
    `gc3libs.utils.unlock` function.

    If no `path` points to a non-existent location, an empty file is
    created before attempting to lock (unless `create` is `False`).
    An attempt is made to remove the file in case an error happens.

    See also: `gc3libs.utils.unlock`:func:
    """
    # ``FileLock`` requires that the to-be-locked file exists; if it
    # does not, we create an empty one (and avoid overwriting any
    # content, in case another process is also writing to it).  There
    # is thus no race condition here, as we attempt to lock the file
    # anyway, and this will stop concurrent processes.
    if not os.path.exists(path) and create:
        open(path, "a").close()
        created = True
    else:
        created = False
    try:
        lck = lockfile.FileLock(path, threaded=False)
        lck.acquire(timeout=timeout)
    except Exception:
        if created:
            try:
                os.remove(path)
            except:
                pass
        raise
    return lck


class History(object):
    """
    A list of messages with timestamps and (optional) tags.

    The `append` method should be used to add a message to the `History`::

      >>> L = History()
      >>> L.append('first message')
      >>> L.append('second one')

    The `last` method returns the text of the last message appended, with its
    timestamp::

      >>> L.last().startswith('second one at')
      True

    Iterating over a `History` instance returns message texts in the
    temporal order they were added to the list, with their timestamp::

      >>> for msg in L: print(msg) # doctest: +ELLIPSIS
      first message ...

    """

    def __init__(self):
        self._messages = []

    def append(self, message, *tags):
        """
        Append a message to this `History`.

        The message is timestamped with the time at the moment of the
        call.

        The optional `tags` argument is a sequence of strings. Tags
        are recorded together with the message and may be used to
        filter log messages given a set of labels. *(This feature is
        not yet implemented.)*

        """
        self._messages.append((message, time.time(), tags))

    def last(self):
        """
        Return text of last message appended.
        If log is empty, return empty string.
        """
        if len(self._messages) == 0:
            return ''
        else:
            return self.format_message(self._messages[-1])

    def format_message(self, message):
        """Return a formatted message, appending to the message its timestamp
        in human readable format.

        """
        return "%s at %s" % (
            message[0], time.asctime(time.localtime(message[1])))

    # shortcut for append
    def __call__(self, message, *tags):
        """Shortcut for `History.append` (which see)."""
        self.append(message, *tags)

    def __iter__(self):
        """Iterate over messages in the temporal order they were added."""
        return iter([self.format_message(record) for record in self._messages])

    def __str__(self):
        """Return all messages texts in a single string, separated by newline
        characters."""
        return '- ' + '\n- '.join([self.format_message(record)
                               for record in self._messages]) + '\n'


def lookup(obj, name):
    """
    Return attribute or item with the given name in collection `obj`.

    :raise LookupError:
      If `obj` has no attribute nor item with the given name.

    This is meant for cases when different versions of an API may
    either return a dictionary (hence, key/``__getitem__``-based
    lookup) or an object/namespace (hence, ``.``/``getattr``-style
    lookup) and you want to handle them in a uniform way.

    The following examples demo it::

      >>> D = {'a':1, 'b':2}
      >>> lookup(D, 'a')
      1
      >>> lookup(D, 'c')
      Traceback (most recent call last):
        ...
      LookupError: Object ... has no attribute nor key named `c`

      >>> class X(object):
      ...   a = 1
      ...   b = 2
      >>> x = X()
      >>> lookup(x, 'a')
      1
      >>> lookup(x, 'c')
      Traceback (most recent call last):
        ...
      LookupError: Object ... has no attribute nor key named `c`
    """
    try:
        return getattr(obj, name)
    except AttributeError:
        pass
    try:
        return obj[name]
    except (KeyError, TypeError):
        pass
    raise LookupError(
        'Object {0} has no attribute nor key named `{1}`'
        .format(obj, name))


def mkdir(path, mode=0o777):
    """
    Like `os.makedirs`, but does not throw an exception if PATH
    already exists.
    """
    if not os.path.exists(path):
        os.makedirs(path, mode)


def mkdir_with_backup(path, mode=0o777):
    """
    Like `os.makedirs`, but if `path` already exists and is not empty,
    rename the existing one to a backup name (see the `backup` function).

    Unlike `os.makedirs`, no exception is thrown if the directory
    already exists and is empty, but the target directory permissions
    are not altered to reflect `mode`.
    """
    if os.path.isdir(path):
        if len(os.listdir(path)) > 0:
            # directory already exists and is non-empty; backup it and
            # make a new one
            backup(path)
            os.makedirs(path, mode)
        else:
            # keep existing empty directory
            pass
    else:
        os.makedirs(path, mode)


def movefile(src, dst, overwrite=False, changed_only=True, link=False):
    """
    Move a file from `src` to `dst`; return `True` if the move was
    actually made.

    The `overwrite` and `changed_only` optional arguments
    have the same effect as in `copyfile`:func: (which see).

    If `dst` is a directory, a file with the same basename as `src` is
    created (or overwritten) in the directory specified.

    Return ``True`` or ``False``, depending on whether the source file
    was actually moved to the destination.

    See also: `copyfile`:func:
    """
    if os.path.isdir(dst):
        dst = os.path.join(dst, os.path.basename(src))
    if os.path.exists(dst) and not overwrite:
        return False
    if samefile(src, dst):
        return False
    if not os.path.exists(dst):
        dstdir = dirname(dst)
        if not os.path.exists(dstdir):
            os.makedirs(dstdir)
    else:
        # `dst` exists, check for changes
        if changed_only:
            sstat = os.stat(src)
            dstat = os.stat(dst)
            if (sstat.st_size ==
                    dstat.st_size and sstat.st_mtime <= dstat.st_mtime):
                # same size and destination more recent, do not move
                return False
    try:
        shutil.move(src, dst)
    except WindowsError:
        pass
    return True


def movetree(src, dst, overwrite=False, changed_only=True):
    """
    Recursively move an entire directory tree rooted at `src`.

    The `overwrite` and `changed_only` optional arguments
    have the same effect as in `copytree`:func: (which see).

    See also: `copytree`:func:.
    """
    assert os.path.isdir(src), \
        ("Source path `%s` does not name an existing directory" % src)
    errors = []
    if not os.path.exists(dst):
        os.makedirs(dst)
    for name in os.listdir(src):
        srcname = os.path.join(src, name)
        dstname = os.path.join(dst, name)
        try:
            if os.path.isdir(srcname):
                errors.extend(
                    movetree(srcname, dstname, overwrite, changed_only))
            else:
                movefile(srcname, dstname)
        except (IOError, os.error) as why:
            errors.append((srcname, dstname, why))
    return errors


def move_recursively(src, dst, overwrite=False, changed_only=True):
    """
    Move `src` to `dst`, descending it recursively if necessary.

    The `overwrite` and `changed_only` optional arguments
    have the same effect as in `copytree`:func: (which see).
    """
    if os.path.isdir(src):
        movetree(src, dst, overwrite, changed_only)
    else:
        movefile(src, dst, overwrite, changed_only)


def occurs(pattern, filename, match=grep):
    """
    Return ``True`` if a line in `filename` matches `pattern`.

    The `match` argument selects how exactly `pattern` is searched for
    in the contents of `filename`:

    * when `match=grep` (default), then `pattern` is a regular
      expression that is searched for (unanchored) in every line;

    * when `match=fgrep`, then `pattern` is a string that is searched
      for literally in every line;

    * more in general, the `match` function should return an iterator
      over matches of `pattern` within the contents of `filename`: if
      at least one match is found, `occurs` will return ``True``.

    :param str pattern:  Pattern to search for
    :param str filename: Path name of the file to search into
    :param match: Function returning iterator over matches
    """
    try:
        # look for the first match -- if one is found, we're done
        next(match(pattern, filename))
        return True
    except StopIteration:
        return False


def parse_linux_proc_limits(data=None):
    """
    Return dictionary mapping limit name to corresponding value.
    In case the actual limit is 'unlimited', value is set to ``None``.
    """
    if data is None:
        with open('/proc/self/limits', 'r') as src:
            data = src.read()
    lines = data.split('\n')
    assert lines
    # Linux' `/proc/self/limits` shows data in 4 columns, but: (1)
    # value in the first column may contain spaces, and (2) the value
    # in the last column may be missing.  However, values in the same
    # column (incl. header) are left-aligned on the same screen
    # position (for human reading) so we need only note the starting
    # position for each data column.
    header = lines[0]
    name_col = header.index('Limit')
    soft_limit_col = header.index('Soft Limit')
    hard_limit_col = header.index('Hard Limit')
    unit_col = header.index('Units')
    # parse data line-by-line
    soft_limits = {}
    hard_limits = {}
    for line in lines[1:]:
        if line == '':
            continue
        name = line[name_col:soft_limit_col].rstrip()
        soft_limit = line[soft_limit_col:hard_limit_col].rstrip()
        if unit_col < len(line):
            hard_limit = line[hard_limit_col:unit_col].rstrip()
            unit = line[unit_col:].rstrip()
        else:
            # no value for `Unit`
            hard_limit = line[hard_limit_col:].rstrip()
            unit = None
        name = name.lower().replace(' ', '_')
        soft_limits[name] = _parse_ulimit(soft_limit, unit)
        hard_limits[name] = _parse_ulimit(hard_limit, unit)
    return soft_limits, hard_limits


def _parse_ulimit(limit, unit):
    """
    Helper function for `parse_linux_proc_limits`.
    """
    if limit == 'unlimited':
        return None
    else:
        limit = int(limit)
    # translate to `gc3libs.quantity` unit
    if unit == 'bytes':
        cls = Memory
        unit = Memory.B
    elif unit == 'seconds':
        cls = Duration
        unit = Duration.s
    elif unit == 'us':
        cls = Duration
        unit = Duration.us
    else:
        # "files", "signals", "processes", etc.
        cls = (lambda v, u: v)
        unit = None
    return cls(limit, unit)


def parse_range(spec):
    """
    Return minimum, maximum, and stepping value for a range.

    Argument `spec` must be a string of the form `LOW:HIGH:STEP`,
    where LOW, HIGH and STEP are (integer or floating-point) numbers.
    Example::

      >>> parse_range('1:10:2')
      (1, 10, 2)

      >>> parse_range('1.0:3.5:0.5')
      (1.0, 3.5, 0.5)

    Note that, as soon as *any* one of LOW, HIGH, STEP is not an
    integer, *all* of them are parsed as Python floats::

      >>> parse_range('1:3:0.5')
      (1.0, 3.0, 0.5)

      >>> parse_range('1.0:3:1')
      (1.0, 3.0, 1.0)

      >>> parse_range('1:3.0:1')
      (1.0, 3.0, 1.0)

    The final part `:STEP` can be omitted if the step is `1`::

      >>> parse_range('2:5')
      (2, 5, 1)

      >>> parse_range('1.0:3.0')
      (1.0, 3.0, 1.0)

    Finally, note that `parse_range` does not perform any kind of
    check on the validity of the resulting range; so it is possible to
    parse a string into an empty range or range specification with
    stepping 0::

      >>> parse_range('1:-5:10')
      (1, -5, 10)

      >>> parse_range('1:2:0')
      (1, 2, 0)

    As a special case to simplify user interfaces, a single number is
    accepted as a *degenerate* range: it will be parsed as a range
    whose content is just the given number::

      >>> parse_range('42')
      (42, 43, 1)
    """
    colons = spec.count(':')
    if colons == 2:
        low, high, step = spec.split(':')
    elif colons == 1:
        low, high = spec.split(':')
        step = '1'  # parsed to int or float later on
    elif colons == 0:
        low = spec
        if '.' in low:
            high = str(float(low) + 1)
        else:
            high = str(int(low) + 1)
        step = '1'  # parsed to int or float later on
    else:
        raise ValueError(
            "Argument `spec` must have the form 'LOW:HIGH:STEP',"
            " where LOW, HIGH and STEP are (integer or"
            " floating-point) numbers.")
    # are low, high, step to floats or ints?
    if ('.' in low) or ('.' in high) or ('.' in step):
        low = float(low)
        high = float(high)
        step = float(step)
    else:
        low = int(low)
        high = int(high)
        step = int(step)
    return low, high, step


def prettyprint(
        D,
        indent=0,
        width=0,
        maxdepth=None,
        step=4,
        only_keys=None,
        output=sys.stdout,
        _key_prefix='',
        _exclude=None):
    """
    Print dictionary instance `D` in a YAML-like format.
    Each output line consists of:

      * `indent` spaces,
      * the key name,
      * a colon character ``:``,
      * the associated value.

    If the total line length exceeds `width`, the value is printed
    on the next line, indented by further `step` spaces; a value of 0 for
    `width` disables this line wrapping.

    Optional argument `only_keys` can be a callable that must return
    `True` when called with keys that should be printed, or a list of
    key names to print.

    Dictionary instances appearing as values are processed recursively
    (up to `maxdepth` nesting).  Each nested instance is printed
    indented `step` spaces from the enclosing dictionary.
    """
    # be sure we do not try to recursively dump `D`
    if _exclude is None:
        _exclude = set()
    _exclude.add(id(D))
    for k, v in sorted(D.items()):
        leading_spaces = indent * ' '
        full_name = "%s%s" % (_key_prefix, k)
        if only_keys is not None:
            try:
                # is `only_keys` a filter function?
                if not only_keys(str(full_name)):
                    continue
            except TypeError:
                # no, then it must be a list of key names, check for
                # keys having the same number of dots as in the prefix
                level = _key_prefix.count('.')
                found = False
                for name in only_keys:
                    # take only the initial segment, up to a "level" dots
                    dots = min(name.count('.'), level) + 1
                    prefix = '.'.join(name.split('.')[:dots])
                    if str(full_name) == prefix:
                        found = True
                        break
                if not found:
                    continue
        # ignore excluded items
        if id(v) in _exclude:
            continue
        # To make a 'key' valid in YAML it must not start with one of the following chars
        sk = str(k)
        sk = sk if sk[0] not in  u'\0 \t\r\n\x85\u2028\u2029-?:,[]{}#&*!|>\'\"%@`' else  "'%s'" % sk
        first = ''.join([leading_spaces, sk, ': '])
        if isinstance(
                v, (dict, Mapping, OrderedDict)):
            if maxdepth is None or maxdepth > 0:
                if maxdepth is None:
                    depth = None
                else:
                    depth = maxdepth - 1
                sstream = StringIO.StringIO()
                prettyprint(v, indent + step, width, depth, step,
                            only_keys, sstream, full_name + '.', _exclude)
                second = sstream.getvalue()
                sstream.close()
            elif maxdepth == 0:
                second = "..."
        elif isinstance(v, (list, tuple)):
            second = ', '.join([str(item) for item in v])
        else:
            second = str(v)
        # wrap overlong lines, and always wrap if the second part is multi-line
        if (width > 0 and len(first) + len(second)
                > width) or ('\n' in second):
            first += '\n'
        # indent a multi-line block by indent+step spaces
        if '\n' in second:
            lines = second.splitlines()
            # keep indentation relative to first line
            dedent = 0
            line0 = lines[0].expandtabs(step)
            while line0[dedent].isspace():
                dedent += 1
            # rebuild `second`, indenting each line by (indent+step) spaces
            second = ''
            for line in lines:
                second = ''.join([
                    second,
                    ' ' * (indent + step),
                    line.rstrip().expandtabs(step)[dedent:],
                    '\n'
                ])
        # there can be multiple trailing '\n's, which we remove here
        second = second.rstrip()
        # finally print line(s)
        output.write('%s%s\n' % (first, second))


def progressive_number(qty=None, id_filename=None):
    """
    Return a positive integer, whose value is guaranteed to
    be monotonically increasing across different invocations
    of this function, and also across separate instances of the
    calling program.

    This is accomplished by using a system-wide file which holds the
    "next available" ID.  The location of this file can be set using
    the ``GC3PIE_ID_FILE`` environment variable, or programmatically
    using the `id_filename` argument.  By default, the "next ID" file
    is located at ``~/.gc3/next_id.txt``:file:

    Example::

      >>> # create "next ID" file in a temporary location
      >>> import tempfile, os
      >>> (fd, tmp) = tempfile.mkstemp()

      >>> n = progressive_number(id_filename=tmp)
      >>> m = progressive_number(id_filename=tmp)
      >>> m > n
      True

    If you specify a positive integer as argument, then a list of
    monotonically increasing numbers is returned.  For example::

      >>> ls = progressive_number(5, id_filename=tmp)
      >>> len(ls)
      5

    (clean up test environment)
      >>> os.remove(tmp)

    In other words, `progressive_number(N)` is equivalent to::

      nums = [ progressive_number() for n in range(N) ]

    only more efficient, because it has to obtain and release the lock
    only once.

    After every invocation of this function, the last returned number
    is stored into the file passed as argument `id_filename`.  If the
    file does not exist, an attempt to create it is made before
    allocating an id; the method can raise an `IOError` or `OSError`
    if `id_filename` cannot be opened for writing.

    *Note:* as file-level locking is used to serialize access to the
    counter file, this function may block (default timeout: 30
    seconds) while trying to acquire the lock, or raise a
    `LockTimeout` exception if this fails.

    :raise: LockTimeout, IOError, OSError

    :returns: A positive integer number, monotonically increasing with
            every call.  A list of such numbers if argument `qty` is a
            positive integer.
    """
    assert qty is None or qty > 0, \
        "Argument `qty` must be a positive integer"
    if id_filename is None:
        id_filename = os.environ.get('GC3PIE_ID_FILE',
                                     os.path.expanduser('~/.gc3/next_id.txt'))
    # ensure directory exists, otherwise the error message is obscure;
    # see Issue 486 for details
    id_dirname = dirname(id_filename)
    if not os.path.exists(id_dirname):
        os.makedirs(id_dirname)
    # XXX: can raise 'LockTimeout'
    lck = lock(id_filename, timeout=30, create=True)
    id_file = open(id_filename, 'r+')
    id = int(id_file.read(8) or "0", 16)
    id_file.seek(0)
    if qty is None:
        id_file.write(
            "%08x -- DO NOT REMOVE OR ALTER THIS FILE: it is used internally"
            " by the gc3libs\n" % (id + 1))
    else:
        id_file.write(
            "%08x -- DO NOT REMOVE OR ALTER THIS FILE: it is used internally"
            " by the gc3libs\n" % (id + qty))
    id_file.close()
    unlock(lck)
    if qty is None:
        return id + 1
    else:
        return [(id + n) for n in range(1, qty + 1)]


def read_contents(path):
    """
    Return the whole contents of the file at `path` as a single string.

    Example::

      >>> read_contents('/dev/null')
      ''

      >>> import tempfile
      >>> (fd, tmpfile) = tempfile.mkstemp()
      >>> w = open(tmpfile, 'w')
      >>> w.write('hey') and None  # make doctest compatible with Py2 and Py3
      >>> w.close()
      >>> read_contents(tmpfile)
      'hey'

    (If you run this test, remember to do cleanup afterwards)
    ::

      >>> os.remove(tmpfile)

    """
    with open(path, 'r') as stream:
        return stream.read()


def remove(path):
    """
    Remove a file, but raise no exception if it does not exist.
    """
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def safe_repr(obj):
    """
    Return a string describing Python object `obj`.

    Avoids calling any Python magic methods, so should be safe to use
    as a 'last resort' in implementation of `__str__` and `__repr__`.
    """
    return ("%s@%x" % (obj.__class__.__name__, id(obj)))


def same_docstring_as(referenced_fn):
    """
    Function decorator: sets the docstring of the following function
    to the one of `referenced_fn`.

    Intended usage is for setting docstrings on methods redefined in
    derived classes, so that they inherit the docstring from the
    corresponding abstract method in the base class.
    """
    def decorate(f):
        f.__doc__ = referenced_fn.__doc__
        return f
    return decorate


def samefile(path1, path2):
    """
    Like `os.path.samefile` but return `False` if either one of the
    paths does not exist.
    """
    try:
        return os.path.samefile(path1, path2)
    except OSError as err:
        if err.errno == 2:  # ENOENT
            return False
        raise


def sh_quote_safe(arg):
    """
    Escape a string for safely passing as argument to a shell command.

    Return a single-quoted string that expands to the exact literal
    contents of `text` when used as an argument to a shell command.
    Examples (note that backslashes are doubled because of Python's
    string read syntax)::

      >>> print(sh_quote_safe("arg"))
      'arg'
      >>> print(sh_quote_safe("'arg'"))
      ''\\''arg'\\'''

    """
    return ("'" + str(arg).replace("'", r"'\''") + "'")


def sh_quote_safe_cmdline(args):
    """
    Single-quote a list of strings for passing to the shell as a
    command.  Return string comprised of the quoted arguments,
    concatenated and separated by spaces.

    Examples::

      >>> print(sh_quote_safe_cmdline(['sh', '-c', 'echo c(1,2,3)']))
      'sh' '-c' 'echo c(1,2,3)'
    """
    return ' '.join((sh_quote_safe(arg) for arg in args))


_DQUOTE_RE = re.compile(r'(\\*)"')
"""Regular expression for escaping double quotes in strings."""


def sh_quote_unsafe(arg):
    """
    Double-quote a string for passing as argument to a shell command.

    Return a double-quoted string that expands to the contents of
    `text` but still allows variable expansion and ``\\``-escapes
    processing by the UNIX shell.  Examples (note that backslashes are
    doubled because of Python's string read syntax)::

      >>> print(sh_quote_unsafe("arg"))
      "arg"
      >>> print(sh_quote_unsafe('"arg"'))
      "\\"arg\\""
      >>> print(sh_quote_unsafe(r'"\\"arg\\""'))
      "\\"\\\\\\"arg\\\\\\"\\""

    """
    return ('"' + _DQUOTE_RE.sub(r'\1\1\"', str(arg)) + '"' )


def sh_quote_unsafe_cmdline(args):
    """
    Double-quote a list of strings for passing to the shell as a
    command.  Return string comprised of the quoted arguments,
    concatenated and separated by spaces.

    Examples::

      >>> print(sh_quote_unsafe_cmdline(['sh', '-c', 'echo $HOME']))
      "sh" "-c" "echo $HOME"
    """
    return ' '.join((sh_quote_unsafe(arg) for arg in args))


@total_ordering
class PlusInfinity(object):

    """
    An object that is greater-than any other object.

        >>> x = PlusInfinity()

        >>> x > 1
        True
        >>> 1 < x
        True
        >>> 1245632479102509834570124871023487235987634518745 < x
        True

        >>> x > sys.maxsize
        True
        >>> x < sys.maxsize
        False
        >>> sys.maxsize < x
        True

    `PlusInfinity` objects are actually larger than *any* given Python
    object::

        >>> x > 'azz'
        True
        >>> x > object()
        True

    Relational operators try to return the correct value when
    comparing `PlusInfinity` to other instances of `PlusInfinity`::

        >>> y = PlusInfinity()
        >>> x < y
        False
        >>> x <= y
        True
        >>> x == y
        True
        >>> x >= y
        True
        >>> x > y
        False

    Finally, addition and subtraction of a finite number from
    `PlusInfinity` always results in `PlusInfinity`::

        >>> y = x - 1
        >>> x == y
        True
        >>> y = x + 1
        >>> x == y
        True

    Note that this used to be a singleton with special handling of 'is'.
    This was removed because it was apparently unused, and a lot of extra code.

    """
    def __gt__(self, other):
        return type(self) != type(other)

    def __eq__(self, other):
        return type(self) == type(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __add__(self, other):
        return self

    def __sub__(self, other):
        if other == self:
            raise RuntimeError(
                "Undefined result subtracting +infinity from +infinity.")
        return self


# In Python 2.7 still, `DictMixin` is an old-style class; thus, we need
# to make `Struct` inherit from `object` otherwise we loose properties
# when setting/pickling/unpickling and *very importantly* the ability to
# use `@property` ...
class Struct(MutableMapping):
    """
    A `dict`-like object, whose keys can be accessed with the usual
    '[...]' lookup syntax, or with the '.' get attribute syntax.

    Examples::

      >>> a = Struct()
      >>> a['x'] = 1
      >>> a.x
      1
      >>> a.y = 2
      >>> a['y']
      2

    Values can also be initially set by specifying them as keyword
    arguments to the constructor::

      >>> a = Struct(z=3)
      >>> a['z']
      3
      >>> a.z
      3

    Like `dict` instances, `Struct`s have a `copy` method to get a
    shallow copy of the instance:

      >>> b = a.copy()
      >>> b.z
      3

    """

    def __init__(self, initializer=None, **extra_args):
        if initializer is not None:
            try:
                # initializer is `dict`-like?
                for name, value in initializer.items():
                    self[name] = value
            except AttributeError:
                # initializer is a sequence of (name,value) pairs?
                for name, value in initializer:
                    self[name] = value
        for name, value in extra_args.items():
            self[name] = value

    def copy(self):
        """Return a (shallow) copy of this `Struct` instance."""
        return Struct(self)

    # the `DictMixin` class defines all std `dict` methods, provided
    # that `__getitem__`, `__setitem__` and `keys` are defined.
    def __delitem__(self, name):
        del self.__dict__[name]

    def __getitem__(self, name):
        return self.__dict__[name]

    def __setitem__(self, name, val):
        self.__dict__[name] = val

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def keys(self):
        return list(self.__dict__.keys())


def string_to_boolean(word):
    """
    Convert `word` to a Python boolean value and return it.
    The strings `true`, `yes`, `on`, `1` (with any
    capitalization and any amount of leading and trailing
    spaces) are recognized as meaning Python `True`::

      >>> string_to_boolean('yes')
      True
      >>> string_to_boolean('Yes')
      True
      >>> string_to_boolean('YES')
      True
      >>> string_to_boolean(' 1 ')
      True
      >>> string_to_boolean('True')
      True
      >>> string_to_boolean('on')
      True

    Any other word is considered as boolean `False`::

      >>> string_to_boolean('no')
      False
      >>> string_to_boolean('No')
      False
      >>> string_to_boolean('Nay!')
      False
      >>> string_to_boolean('woo-hoo')
      False

    This includes also the empty string and whitespace-only::

      >>> string_to_boolean('')
      False
      >>> string_to_boolean('  ')
      False

    """
    return word.strip().lower() in ['true', 'yes', 'on', '1']


@contextlib.contextmanager
def tempdir(**kwargs):
    """
    A context manager for creating and then deleting a temporary directory.

    All arguments are passed unchanged to the `tempfile.mkdtemp`
    standand library function.

    (Original source and credits: http://stackoverflow.com/a/10965572/459543)
    """
    tmpdir = tempfile.mkdtemp(**kwargs)
    try:
        yield tmpdir
    finally:
        if os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir, ignore_errors=True)


def to_bytes(s):
    """
    Convert string `s` to an integer number of bytes.  Suffixes like
    'KB', 'MB', 'GB' (up to 'YB'), with or without the trailing 'B',
    are allowed and properly accounted for.  Case is ignored in
    suffixes.

    Examples::

      >>> to_bytes('12')
      12
      >>> to_bytes('12B')
      12
      >>> to_bytes('12KB')
      12000
      >>> to_bytes('1G')
      1000000000

    Binary units 'KiB', 'MiB' etc. are also accepted:

      >>> to_bytes('1KiB')
      1024
      >>> to_bytes('1MiB')
      1048576

    """
    # get the multiplier
    k = 1024 if 'i' in s.lower() else 1000
    # strip bad stuff away now
    s = s.lower().strip(' ib')
    # if it is just a number, return as int
    if s.isdigit():
        return int(s)
    num, unit = s[:-1], s[-1]
    return int(num) * (k ** BYTE_SUFFIX_TO_POW[unit])


def to_str(arg, origin='ascii'):
    """
    Convert *arg* to a Python text string.

    If *arg* is already a text string (i.e., a ``unicode`` object in
    Python 2, and a ``str`` object in Python 3), then return it
    unchanged.  As an exception, if *arg* is ``None``, return
    ``None`` (unchanged).

    Second argument *origin* determines the handling of *arg* when
    *arg* is a byte-string:

    - if *origin* is ``'filesystem'`` then *arg* is converted to a
      text string using `from_filesystem_bytes`:func: (which see);

    - if *origin* is ``'terminal'`` then conversion of *arg* is
      attempted using `from_terminal_bytes`:func: (which see);

    - otherwise, *origin* is interpreted as an encoding name, and byte
      string *arg* is decoded using that encoding's rules.

    If *arg* is neither a text string nor a byte string, then
    conversion to string is attempted using Python's built-in
    ``str()`` function.
    """
    if arg is None:
        return None
    elif isinstance(arg, text_str):
        return arg  # unmodified
    elif isinstance(arg, bytes):
        if origin == 'filesystem':
            return from_filesystem_bytes(arg)
        elif origin == 'terminal':
            return from_terminal_bytes(arg)
        else:
            # interpret `origin` as encoding
            return decode(arg, origin)
    else:
        return text_str(arg)


def touch(path):
    """
    Ensure a regular file exists at `path`.

    If the file already exists, its access and modification time are
    updated.

    (This is a very limited and stripped down version of the ``touch``
    POSIX utility.)
    """
    open(path, 'a').close()


def unlock(lock):
    """
    Release a previously-acquired lock.

    Argument `lock` should be the return value of a previous
    `gc3libs.utils.lock` call.

    See also: `gc3libs.utils.lock`:func:
    """
    lock.release()


def update_parameter_in_file(path, var_in, new_val, regex_in):
    '''
    Updates a parameter value in a parameter file using predefined regular
    expressions in `_loop_regexps`.

    :param path:    Full path to the parameter file.
    :param var_in:  The variable to modify.
    :param new_val: The updated parameter value.
    :param regex:   Name of the regular expression that describes the format
                    of the parameter file.
    '''
    _loop_regexps = {
        'bar-separated': (r'([a-z]+[\s\|]+)'
                          r'(\w+)'  # variable name
                          r'(\s*[\|]+\s*)'  # bars and spaces
                          r'([\w\s\.,;\[\]\-]+)'  # value
                          r'(\s*)'),
        'space-separated': (r'(\s*)'
                            r'(\w+)'  # variable name
                            r'(\s+)'  # spaces (filler)
                            r'([\w\s\.,;\[\]\-]+)'  # values
                            r'(\s*)'),  # spaces (filler)
    }
    isfound = False
    if regex_in in _loop_regexps.keys():
        regex_in = _loop_regexps[regex_in]
    para_file_in = open(path, 'r')
    para_file_out = open(path + '.tmp', 'w')
    for line in para_file_in:
        if not line.rstrip():
            continue
        (a, var, b, old_val, c) = re.match(regex_in, line.rstrip()).groups()
        gc3libs.log.debug(
            "Read variable '%s' with value '%s' ...", var, old_val)
        if var == var_in:
            isfound = True
            upd_val = new_val
        else:
            upd_val = old_val
        para_file_out.write(a + var + b + upd_val + c + '\n')
    para_file_out.close()
    para_file_in.close()
    # move new modified content over the old
    os.rename(path + '.tmp', path)
    if not isfound:
        gc3libs.log.critical(
            'update_parameter_in_file could not find parameter'
            ' in specified file')


def write_contents(path, data):
    """
    Overwrite the contents of the file at `path` with the given data.
    If the file does not exist, it is created.

    Example::

      >>> import tempfile
      >>> (fd, tmpfile) = tempfile.mkstemp()
      >>> write_contents(tmpfile, 'big data here') and None # discard return value on Py3
      >>> read_contents(tmpfile)
      'big data here'

    (If you run this test, remember to clean up afterwards)
    ::

      >>> os.remove(tmpfile)

    """
    mode = 'wb' if isinstance(data, bytes) else 'w'
    with open(path, mode) as stream:
        return stream.write(data)


class YieldAtNext(object):

    """
    Provide an alternate protocol for generators.

    Wrap a Python generator object, and buffer the return values from
    `send` and `throw` calls, returning `None` instead. Return the
    yielded value --or raise the `StopIteration` exception-- upon the
    subsequent call to the `next` method.
    """

    __slots__ = ['_generator', '_saved', '_stop_iteration']

    def __init__(self, generator):
        self._generator = generator
        self._saved = deque()
        self._stop_iteration = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._stop_iteration:
            raise StopIteration
        elif self._saved:
            return self._saved.popleft()
        else:
            return next(self._generator)

    def send(self, value):
        try:
            self._saved.append(self._generator.send(value))
        except StopIteration:
            self._stop_iteration = True

    def throw(self, *excinfo):
        try:
            self._saved.append(self._generator.throw(*excinfo))
        except StopIteration:
            self._stop_iteration = True


##
# Main
##

if __name__ == '__main__':
    import doctest
    doctest.testmod(name='utils',
                    optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS)
