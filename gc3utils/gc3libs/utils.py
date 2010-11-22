#! /usr/bin/env python
"""
Generic Python programming utility functions.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
__docformat__ = 'reStructuredText'
__version__ = '$Revision$'


import gc3libs
import os
import os.path
import re
import shelve
import sys
import time
import UserDict

# import for send_mail
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders


from Exceptions import *
from arclib import *
from lockfile import FileLock
    

# ================================================================
#
#                     Generic functions
#
# ================================================================


class defaultdict(dict):
    """
    A backport of `defaultdict` to Python 2.4
    See http://docs.python.org/library/collections.html
    """
    def __new__(cls, default_factory=None):
        return dict.__new__(cls)
    def __init__(self, default_factory):
        self.default_factory = default_factory
    def __missing__(self, key):
        try:
            return self.default_factory()
        except:
            raise KeyError("Key '%s' not in dictionary" % key)
    def __getitem__(self, key):
        if not dict.__contains__(self, key):
            dict.__setitem__(self, key, self.__missing__(key))
        return dict.__getitem__(self, key)


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
      >>> first(sorted(s.keys()))
      'a'
    """
    try: # try iterator interface
        return seq.next()
    except AttributeError:
        pass
    try: # seq is no iterator, try indexed lookup
        return seq[0]
    except IndexError:
        pass
    raise TypeError("Argument to `first()` method needs to be iterator or sequence.")


# In Python 2.7 still, `DictMixin` is an old-style class; thus, we need
# to make `Struct` inherit from `object` otherwise we loose properties
# when setting/pickling/unpickling
class Struct(object, UserDict.DictMixin):
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
    """
    def __init__(self, initializer=None, **kw):
        if initializer is not None:
            try:
                # initializer is `dict`-like?
                for name, value in initializer.items():
                    self[name] = value
            except AttributeError: 
                # initializer is a sequence of (name,value) pairs?
                for name, value in initializer:
                    self[name] = value
        for name, value in kw.items():
            self[name] = value

    # the `DictMixin` class defines all std `dict` methods, provided
    # that `__getitem__`, `__setitem__` and `keys` are defined.
    def __setitem__(self, name, val):
        self.__dict__[name] = val
    def __getitem__(self, name):
        return self.__dict__[name]
    def keys(self):
        return self.__dict__.keys()


def progressive_number():
    """
    Return a positive integer, whose value is guaranteed to
    be monotonically increasing across different invocations
    of this function, and also across separate instances of the
    calling program.

    Example::

      >>> n = progressive_number()
      >>> m = progressive_number()
      >>> m > n
      True

    After every invocation of this function, the returned number
    is stored into the file ``~/.gc3/next_id.txt``.

    *Note:* as file-level locking is used to serialize access to the
    counter file, this function may block (default timeout: 30
    seconds) while trying to acquire the lock, or raise an exception
    if this fails.
    """
    # FIXME: should use global config value for directory
    id_filename = os.path.expanduser("~/.gc3/next_id.txt")
    # ``FileLock`` requires that the to-be-locked file exists; if it
    # does not, we create an empty one (and avoid overwriting any
    # content, in case another process is also writing to it).  There
    # is thus no race condition here, as we attempt to lock the file
    # anyway, and this will stop concurrent processes.
    if not os.path.exists(id_filename):
        open(id_filename, "a").close()
    lock = FileLock(id_filename, threaded=False) 
    lock.acquire(timeout=30) # XXX: can raise 'LockTimeout'
    id_file = open(id_filename, 'r+')
    id = int(id_file.read(8) or "0", 16)
    id +=1 
    id_file.seek(0)
    id_file.write("%08x -- DO NOT REMOVE OR ALTER THIS FILE: it is used internally by the gc3libs\n" % id)
    id_file.close()
    lock.release()
    return id


def defproperty(fn):
    """
    Decorator to define properties with a simplified syntax in Python 2.4.
    See http://code.activestate.com/recipes/410698-property-decorator-for-python-24/#c6
    for details and examples.
    """
    p = { 'doc':fn.__doc__ }
    p.update(fn())
    return property(**p)


def dirname(pathname):
    """
    Same as `os.path.dirname` but return `.` in case of path names with no directory component.
    """
    dirname = os.path.dirname(pathname)
    if not dirname:
        dirname = '.'
    # FIXME: figure out if this is a desirable outcome.  i.e. do we want dirname to be empty, or do a pwd and find out what the current dir is, or keep the "./".  I suppose this could make a difference to some of the behavior of the scripts, such as copying files around and such.
    return dirname


class Enum(frozenset):
    """
    A generic enumeration class.  Inspired by:
    http://stackoverflow.com/questions/36932/whats-the-best-way-to-implement-an-enum-in-python/2182437#2182437
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

      >>> for a in Animal: print a
      DOG
      CAT
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


def deploy_configuration_file(filename, template_filename=None):
    """
    Ensure that configuration file `filename` exists; possibly
    copying it from the specified `template_filename`.

    Return `True` if a file with the specified name exists in the 
    configuration directory.  If not, try to copy the template file
    over and then return `False`; in case the copy operations fails, 
    a `NoConfigurationFile` exception is raised.

    If parameter `filename` is not an absolute path, it is interpreted
    as relative to `gc3libs.Default.RCDIR`; if `template_filename` is
    `None`, then it is assumed to be the same as `filename`.
    """
    if template_filename is None:
        template_filename = os.path.basename(filename)
    if not os.path.isabs(filename):
        filename = os.path.join(Default.RCDIR, filename)
    if os.path.exists(filename):
        return True
    else:
        try:
            # copy sample config file 
            if not os.path.exists(dirname(filename)):
                os.makedirs(dirname(filename))
            from pkg_resources import Requirement, resource_filename
            sample_config = resource_filename(Requirement.parse("gc3libs"), 
                                              "gc3libs/etc/" + template_filename)
            import shutil
            shutil.copyfile(sample_config, filename)
            return False
        except IOError, x:
            gc3libs.log.critical("CRITICAL ERROR: Failed copying configuration file: %s" % x)
            raise NoConfigurationFile("No configuration file '%s' was found, and an attempt to create it failed. Aborting." % filename)
        except ImportError:
            raise NoConfigurationFile("No configuration file '%s' was found. Aborting." % filename)


def from_template(template, **kw):
    """
    Return the contents of `template`, substituting all occurrences
    of Python formatting directives '%(key)s' with the corresponding values 
    taken from dictionary `kw`.

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
    # substitute `kw` into `t` and return it
    return (template_contents % kw)


class Log(object):
    """
    A list of messages with timestamps and (optional) tags.

    The `append` method should be used to add a message to the `Log`::

      >>> L = Log()
      >>> L.append('first message')
      >>> L.append('second one')

    Iterating over a `Log` instance returns message texts in the
    temporal order they were added to the list::

      >>> for msg in L: print msg
      first message
      second one

    """
    def __init__(self):
        self._messages = [ ]

    def append(self, message, *tags):
        self._messages.append((message, time.time(), tags))

    def __iter__(self):
        return iter([record[0] for record in self._messages])

    def __str__(self):
        return str.join('\n', [record[0] for record in self._messages])


def mkdir_with_backup(path):
    """
    Like `os.makedirs`, but if `path` already exists, rename the
    existing one appending a `.NUMBER` suffix.
    """
    if os.path.isdir(path):
        # directory exists; find a suitable extension and rename
        parent_dir = os.path.dirname(path)
        prefix = os.path.basename(path) + '.'
        p = len(prefix)
        suffix = 1
        for name in [ x for x in os.listdir(parent_dir) if x.startswith(prefix) ]:
            try:
                n = int(name[p:])
                suffix = max(suffix, n+1)
            except ValueError:
                # ignore non-numeric suffixes
                pass
        os.rename(path, "%s.%d" % (path, suffix))
    os.makedirs(path)


def safe_repr(obj):
    """
    Return a string describing Python object `obj`.  Avoids calling
    any Python magic methods, so should be safe to use as a "last
    resort" in implementation of `__str__` and `__repr__` magic.
    """
    return ("<`%s` instance @ %x>" 
            % (obj.__class__.__name__, id(obj)))


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


# see http://stackoverflow.com/questions/31875/is-there-a-simple-elegant-way-to-define-singletons-in-python/1810391#1810391
class Singleton(object):
    """
    Derived classes of `Singleton` can have only one instance in the
    running Python interpreter.

       >>> x = Singleton()
       >>> y = Singleton()
       >>> x is y
       True

    """
    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'):
            cls._instance = super(Singleton, cls).__new__(cls, *args, **kw)
        return cls._instance


class PlusInfinity(Singleton):
    """
    An object that is greater-than any other object.

        >>> x = PlusInfinity()

        >>> x > 1
        True
        >>> 1 < x
        True
        >>> 1245632479102509834570124871023487235987634518745 < x
        True

        >>> x > sys.maxint
        True
        >>> x < sys.maxint
        False
        >>> sys.maxint < x
        True

    `PlusInfinity` objects are actually larger than *any* given Python
    object::

        >>> x > 'azz'
        True
        >>> x > object()
        True

    Note that `PlusInfinity` is a singleton, therefore you always get
    the same instance when calling the class constructor::

        >>> x = PlusInfinity()
        >>> y = PlusInfinity()
        >>> x is y
        True

    Relational operators try to return the correct value when
    comparing `PlusInfinity` to itself::

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

    """
    def __gt__(self, other):
        if self is other:
            return False
        else:
            return True
    def __ge__(self, other):
        return True
    def __lt__(self, other):
        return False
    def __le__(self, other):
        if self is other:
            return True
        else:
            return False
    def __eq__(self, other):
        if self is other:
            return True
        else:
            return False
    def __ne__(self, other):
        return not self.__eq__(other)



def string_to_boolean(word):
    """
    Convert `word` to a Python boolean value and return it.
    The strings `true`, `yes`, `on`, `1` (with any
    capitalization and any amount of leading and trailing
    spaces) are recognized as meaning Python `True`.
    Any other word is considered as boolean `False`.
    """
    if word.strip().lower() in [ 'true', 'yes', 'on', '1' ]:
        return True
    else:
        return False


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
    last = -1
    unit = s[last].lower()
    if unit.isdigit():
        # `s` is a integral number
        return int(s)
    if unit == 'b':
        # ignore the the 'b' or 'B' suffix
        last -= 1
        unit = s[last].lower()
    if unit == 'i':
        k = 1024
        last -= 1
        unit = s[last].lower()
    else:
        k = 1000
    # convert the substring of `s` that does not include the suffix
    if unit.isdigit():
        return int(s[0:(last+1)])
    if unit == 'k':
        return int(float(s[0:last])*k)
    if unit == 'm':
        return int(float(s[0:last])*k*k)
    if unit == 'g':
        return int(float(s[0:last])*k*k*k)
    if unit == 't':
        return int(float(s[0:last])*k*k*k*k)
    if unit == 'p':
        return int(float(s[0:last])*k*k*k*k*k)
    if unit == 'e':
        return int(float(s[0:last])*k*k*k*k*k*k)
    if unit == 'z':
        return int(float(s[0:last])*k*k*k*k*k*k*k)
    if unit == 'y':
        return int(float(s[0:last])*k*k*k*k*k*k*k*k)

 
def send_mail(send_from, send_to, subject, text, files=[], server="localhost"):

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    
    msg.attach(MIMEText(text))
    
    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 
                        'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)
        
    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()



if __name__ == '__main__':
    import doctest
    doctest.testmod()
