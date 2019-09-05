#! /usr/bin/env python

"""
Utility classes and methods for dealing with URLs.
"""

# Copyright (C) 2011-2019  University of Zurich. All rights reserved.
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
from builtins import str


from collections import namedtuple
import os
import urllib.parse

from .persistence.serialization import Persistable
from .utils import to_str

__docformat__ = 'reStructuredText'


_UrlFields = namedtuple('_UrlFields', [
    # watch out that this is *exactly* the order used in
    # `_UrlFields.__new__()` calls below!
    'scheme', 'netloc', 'path',
    'hostname', 'port', 'query',
    'username', 'password', 'fragment'
])


class Url(_UrlFields):

    """
    Represent a URL as a named-tuple object.  This is an immutable
    object that cannot be changed after creation.

    The following read-only attributes are defined on objects of class `Url`.

    =========   =====   ===================================  ==================
    Attribute   Index   Value                                if not present
    =========   =====   ===================================  ==================
    scheme      0       URL scheme specifier                 empty string
    netloc      1       Network location part                empty string
    path        2       Hierarchical path                    empty string
    query       3       Query component                      empty string
    hostname    4       Host name (lower case)               None
    port        5       Port number as integer (if present)  None
    username    6       User name                            None
    password    7       Password                             None
    fragment    8       URL fragment (part after ``#``)      empty string
    =========   =====   ===================================  ==================

    There are two ways of constructing `Url` objects:

    * By passing a string `urlstring`::

        >>> u = Url('http://www.example.org/data')

        >>> u.scheme == 'http'
        True
        >>> u.netloc == 'www.example.org'
        True
        >>> u.path == '/data'
        True

      The default URL scheme is ``file``::

        >>> u = Url('/tmp/foo')
        >>> u.scheme == 'file'
        True
        >>> u.path == '/tmp/foo'
        True

      However, if a ``#`` character is present in the path name, it
      will be taken as separating the path from the "fragment"::

        >>> u = Url('/tmp/foo#1')
        >>> u.path == '/tmp/foo'
        True
        >>> u.fragment == '1'
        True

      Please note that extra leading slashes '/' are interpreted as
      the begining of a network location:

        >>> u = Url('//foo/bar')
        >>> u.path == '/bar'
        True
        >>> u.netloc == 'foo'
        True
        >>> Url('///foo/bar').path == '/foo/bar'
        True

      (Check RFC 3986 http://tools.ietf.org/html/rfc3986)

      If `force_abs` is `True` (default), then the `path`
      attribute is made absolute, by calling `os.path.abspath` if
      necessary::

        >>> u = Url('foo/bar', force_abs=True)
        >>> os.path.isabs(u.path)
        True

      Otherwise, if `force_abs` is `False`, then the `path`
      attribute stores the passed string unchanged::

        >>> u = Url('foo', force_abs=False)
        >>> os.path.isabs(u.path)
        False
        >>> u.path == 'foo'
        True

      Other keyword arguments can specify defaults for missing parts
      of the URL::

        >>> u = Url('/tmp/foo', scheme='file', netloc='localhost')
        >>> u.scheme == 'file'
        True
        >>> u.netloc == 'localhost'
        True
        >>> u.path == '/tmp/foo'
        True

      Query attributes are also supported::

        >>> u = Url('http://www.example.org?foo=bar')
        >>> u.query == 'foo=bar'
        True

      and so are fragments::

        >>> u = Url('postgresql://user@db.example.org#table=data')
        >>> u.fragment == 'table=data'
        True

    * By passing keyword arguments only, to construct an `Url` object
      with exactly those values for the named fields::

        >>> u = Url(scheme='http', netloc='www.example.org', path='/data')

      In this form, the `force_abs` parameter is ignored.

    See also: http://goo.gl/9WcRvR
    """
    __slots__ = ()

    def __new__(cls, urlstring=None, force_abs=True,
                scheme='file', netloc='', path='',
                hostname=None, port=None, query='',
                username=None, password=None, fragment=''):
        """
        Create a new `Url` object.  See the `Url`:class: documentation
        for invocation syntax.
        """
        if urlstring is not None:
            if isinstance(urlstring, Url):
                # copy constructor
                return _UrlFields.__new__(cls,
                    urlstring.scheme, urlstring.netloc, urlstring.path,
                    urlstring.hostname, urlstring.port, urlstring.query,
                    urlstring.username, urlstring.password, urlstring.fragment
                )
            else:
                # XXX: `future` provides a backport of Py3's
                # `urlsplit()` function; however, the implementation
                # requires that all arguments have the same string
                # type of the 1st one (i.e., `urlstring` here).  This
                # is a source of problems as default value for
                # `scheme` is a load-time constant (and thus either
                # unicode or byte string depending on Python version
                # and on whether `unicode_literals` is in effect), but
                # `urlstring`'s type depends on how it was produced
                # (user input, pickled file, result of some library
                # call, etc). So we convert `urlstring` to `future`'s
                # `newstr` type to match the value of `scheme` set at
                # load-time.
                urlstring = to_str(urlstring, 'filesystem')

                # parse `urlstring` and use kwd arguments as default values
                try:
                    urldata = urllib.parse.urlsplit(
                        urlstring,
                        scheme=to_str(scheme, 'filesystem'),
                        allow_fragments=True)
                    if urldata.path is not None:
                        path = urldata.path
                    if urldata.scheme == 'file' \
                       and not os.path.isabs(path) \
                       and force_abs:
                        path = os.path.abspath(path)
                    # Python 2.6 parses fragments only for http(s),
                    # for any other scheme, the fragment is returned as
                    # part of the path...
                    if '#' in path:
                        path_, fragment_ = path.split('#')
                        urldata = urllib.parse.SplitResult(
                            urldata.scheme, urldata.netloc,
                            path_, urldata.query, fragment_)
                    return _UrlFields.__new__(cls,
                        urldata.scheme or to_str(scheme, 'filesystem'),
                        urldata.netloc or to_str(netloc, 'filesystem'),
                        path,
                        urldata.hostname or to_str(hostname, 'filesystem'),
                        urldata.port or to_str(port, 'filesystem'),
                        urldata.query or to_str(query, 'filesystem'),
                        urldata.username or to_str(username, 'filesystem'),
                        urldata.password or to_str(password, 'filesystem'),
                        urldata.fragment or to_str(fragment, 'filesystem'),
                    )
                except (ValueError, TypeError, AttributeError) as err:
                    raise ValueError(
                        "Cannot parse string '%s' as a URL: %s: %s"
                        % (urlstring, err.__class__.__name__, err))
        else:
            # no `urlstring`, use kwd arguments
            return _UrlFields.__new__(
                cls,
                to_str(scheme, 'filesystem'),
                to_str(netloc, 'filesystem'),
                to_str(path, 'filesystem'),
                to_str(hostname, 'filesystem'),
                to_str(port, 'filesystem'),
                to_str(query, 'filesystem'),
                to_str(username, 'filesystem'),
                to_str(password, 'filesystem'),
                to_str(fragment, 'filesystem')
            )

    def __getnewargs__(self):
        """Support pickling/unpickling `Url` class objects."""
        return (None, False,  # urlstring, force_abs
                self.scheme, self.netloc, self.path, self.hostname, self.port,
                self.query, self.username, self.password)

    # In Py3, for some reason `Url` does not inherit `_UrlFields`'
    # definition of `__hash__()`, thus leading to a "Unhashable type:
    # Url" errors.  So let's be explicit and state that a `Url` is
    # nothing but a `_UrlField` with some convenience methods added.
    def __hash__(self):
        return _UrlFields.__hash__(self)

    def __repr__(self):
        """
        Return a printed representation of this object, such that
        `eval(repr(x)) == x`.
        """
        return (
            "Url(scheme=%r, netloc=%r, path=%r, hostname=%r,"
            " port=%r, query=%r, username=%r, password=%r, fragment=%r)"
            %
            (self.scheme,
             self.netloc,
             self.path,
             self.hostname,
             self.port,
             self.query,
             self.username,
             self.password,
             self.fragment))

    def __str__(self):
        """
        Return a URL string corresponding to the URL object `urldata`.

            >>> u = Url('gsiftp://gridftp.example.org:2811/data')
            >>> str(u)
            'gsiftp://gridftp.example.org:2811/data'
            >>> u = Url('swift://swift.example.org:8080/v1?querystring')
            >>> str(u)
            'swift://swift.example.org:8080/v1?querystring'

        If the URL was constructed by parsing a URL string, a
        different string can result, although equivalent according to
        the URL definition::

            >>> u1 = Url('/tmp/foo')
            >>> str(u1)
            'file:///tmp/foo'

        """
        url = self.path
        # XXX: assumes all our URLs are of netloc-type!
        if url and not url.startswith('/'):
            url = '/' + url
        url = '//' + (self.netloc or '') + url
        if self.scheme:
            url = self.scheme + ':' + url
        if self.query:
            url += '?%s' % self.query
        if self.fragment:
            url += '#%s' % self.fragment
        return url

    def __eq__(self, other):
        """
        Return `True` if `self` and `other` are equal `Url` objects,
        or if their string representations match.

        Examples:

          >>> u = Url('/tmp/foo')
          >>> u == Url('/tmp/foo')
          True
          >>> u == Url('file:///tmp/foo')
          True
          >>> u == Url('http://example.org/')
          False

          >>> u == str(u)
          True
          >>> u == '/tmp/foo'
          True
          >>> u == 'file:///tmp/foo'
          True
          >>> u == 'http://example.org'
          False

          >>> u == 42
          False

          >>> u == Url('file:///tmp/foo?bar')
          False

        """
        try:
            # The `tuple.__eq__` call can only be used if both `self`
            # and `other` are `tuple` subclasses; we know that `self`
            # is, but we need to check `other`.
            return ((isinstance(other, tuple) and tuple.__eq__(self, other))
                    or str(self) == str(other)
                    or tuple.__eq__(self, Url(other)))
        except ValueError:
            # `other` is not a URL and cannot be made into one
            return False

    def __ne__(self, other):
        """
        The opposite of `__eq__`.
        """
        return not self.__eq__(other)

    def adjoin(self, relpath):
        """
        Return a new `Url`, constructed by appending `relpath` to the
        path section of this URL.

        Example::

            >>> u0 = Url('http://www.example.org')
            >>> u1 = u0.adjoin('data')
            >>> str(u1)
            'http://www.example.org/data'

            >>> u2 = u1.adjoin('moredata')
            >>> str(u2)
            'http://www.example.org/data/moredata'

        Even if `relpath` starts with `/`, it is still appended to the
        path in the base URL::

            >>> u3 = u2.adjoin('/evenmore')
            >>> str(u3)
            'http://www.example.org/data/moredata/evenmore'

        Optional query attribute is left untouched::

            >>> u4 = Url('http://www.example.org?bar')
            >>> u5 = u4.adjoin('foo')
            >>> str(u5)
            'http://www.example.org/foo?bar'

        """
        if relpath.startswith('/'):
            relpath = relpath[1:]
        return Url(scheme=self.scheme, netloc=self.netloc,
                   path=os.path.join((self.path or '/'), relpath),
                   hostname=self.hostname, port=self.port,
                   username=self.username, password=self.password,
                   query=self.query, fragment=self.fragment)


class _UrlDict(dict):
    """
    Base class for `UrlKeyDict`:class: and `UrlValueDict`:class:
    """
    __slots__ = (
        '_force_abs',
    )

    def __init__(self, iter_or_dict=None, force_abs=False, **extra_kv):
        self._force_abs = force_abs
        if iter_or_dict is not None:
            try:
                # if `iter_or_dict` is a dict-like object, then it has
                # `iteritems()`
                for k, v in iter_or_dict.items():
                    self[k] = v
            except AttributeError:
                # then assume `iter_or_dict` is an iterator over (key, value)
                # pairs
                for k, v in iter_or_dict:
                    self[k] = v
        if extra_kv:
            for k, v in extra_kv.items():
                self[k] = v

    def __repr__(self):
        return ('{0}({1}, force_abs={2})'
                .format(self.__class__.__name__, dict(self), self._force_abs))



class UrlKeyDict(_UrlDict):

    """
    A dictionary class enforcing that all keys are URLs.

    Strings and/or objects returned by `urlparse` can
    be used as keys.  Setting a string key automatically
    translates it to a URL:

       >>> d = UrlKeyDict()
       >>> d['/tmp/foo'] = 1
       >>> for k in d.keys(): print (type(k), k.path) # doctest:+ELLIPSIS
       <class '....Url'> /tmp/foo

    Retrieving the value associated with a key works with both the
    string or the url value of the key:

        >>> d['/tmp/foo']
        1
        >>> d[Url('/tmp/foo')]
        1

    Key lookup can use both the string or the `Url` value as well:

        >>> '/tmp/foo' in d
        True
        >>> Url('/tmp/foo') in d
        True
        >>> 'file:///tmp/foo' in d
        True
        >>> 'http://example.org' in d
        False

    Class `UrlKeyDict` supports initialization by copying items from
    another `dict` instance or from an iterable of (key, value)
    pairs::

        >>> d1 = UrlKeyDict({ '/tmp/foo':'foo', '/tmp/bar':'bar' })
        >>> d2 = UrlKeyDict([ ('/tmp/foo', 'foo'), ('/tmp/bar', 'bar') ])
        >>> d1 == d2
        True

    An empty `UrlKeyDict` instance is returned by the constructor
    when called with no parameters::

        >>> d0 = UrlKeyDict()
        >>> len(d0)
        0

    If `force_abs` is `True`, then all paths are converted to
    absolute ones in the dictionary keys.

        >>> d = UrlKeyDict(force_abs=True)
        >>> d['foo'] = 1
        >>> for k in d.keys(): print(os.path.isabs(k.path))
        True

        >>> d = UrlKeyDict(force_abs=False)
        >>> d['foo'] = 2
        >>> for k in d.keys(): print(os.path.isabs(k.path))
        False

    """

    __slots__ = ()

    def __setitem__(self, key, value):
        if not isinstance(key, Url):
            key = Url(key, self._force_abs)
        super(UrlKeyDict, self).__setitem__(key, value)

    # these two methods are necessary to have key-lookup work with
    # strings as well

    def __getitem__(self, key):
        if not isinstance(key, Url):
            key = Url(key, self._force_abs)
        return super(UrlKeyDict, self).__getitem__(key)

    def __contains__(self, key):
        if not isinstance(key, Url):
            key = Url(key, self._force_abs)
        return super(UrlKeyDict, self).__contains__(key)


class UrlValueDict(_UrlDict):

    """
    A dictionary class enforcing that all values are URLs.

    Strings and/or objects returned by `urlparse` can
    be used as values.  Setting a string value automatically
    translates it to a URL:

       >>> d = UrlValueDict()
       >>> d[1] = '/tmp/foo'
       >>> d[2] = Url('file:///tmp/bar')
       >>> for v in d.values(): print (type(v), v.path) # doctest:+ELLIPSIS
       <class '....Url'> /tmp/foo
       <class '....Url'> /tmp/bar

    Retrieving the value associated with a key always returns the
    URL-type value, regardless of how it was set::

        >>> d[1] == Url(scheme='file', netloc='', path='/tmp/foo', \
              hostname=None, port=None, query='', \
              username=None, password=None, fragment='')
        True

    Class `UrlValueDict` supports initialization by any of the
    methods that work with a plain `dict` instance::

        >>> d1 = UrlValueDict({ 'foo':'/tmp/foo', 'bar':'/tmp/bar' })
        >>> d2 = UrlValueDict([ ('foo', '/tmp/foo'), ('bar', '/tmp/bar') ])
        >>> d3 = UrlValueDict(foo='/tmp/foo', bar='/tmp/bar')

        >>> d1 == d2
        True
        >>> d2 == d3
        True

    In particular, an empty `UrlDict` instance is returned by the
    constructor when called with no parameters::

        >>> d0 = UrlValueDict()
        >>> len(d0)
        0

    If `force_abs` is `True`, then all paths are converted to
    absolute ones in the dictionary values.

        >>> d = UrlValueDict(force_abs=True)
        >>> d[1] = 'foo'
        >>> for v in d.values(): print(os.path.isabs(v.path))
        True

        >>> d = UrlValueDict(force_abs=False)
        >>> d[2] = 'foo'
        >>> for v in d.values(): print(os.path.isabs(v.path))
        False

    """

    __slots__ = ()

    def __setitem__(self, key, value):
        if not isinstance(value, Url):
            value = Url(value, self._force_abs)
        super(UrlValueDict, self).__setitem__(key, value)


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="url",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
