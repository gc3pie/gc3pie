#! /usr/bin/env python
#
"""
Utility classes and methods for dealing with URLs.
"""
# Copyright (C) 2011-2018  University of Zurich. All rights reserved.
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
#
__docformat__ = 'reStructuredText'


import os
import urlparse


# XXX: rewrite using `collections.namedtuple` when we no longer
# support 2.4 and 2.5
class Url(tuple):

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

        >>> u.scheme
        'http'
        >>> u.netloc
        'www.example.org'
        >>> u.path
        '/data'

      The default URL scheme is ``file``::

        >>> u = Url('/tmp/foo')
        >>> u.scheme
        'file'
        >>> u.path
        '/tmp/foo'

      However, if a ``#`` character is present in the path name, it
      will be taken as separating the path from the "fragment"::

        >>> u = Url('/tmp/foo#1')
        >>> u.path
        '/tmp/foo'
        >>> u.fragment
        '1'

      Please note that extra leading slashes '/' are interpreted as
      the begining of a network location:

        >>> u = Url('//foo/bar')
        >>> u.path
        '/bar'
        >>> u.netloc
        'foo'
        >>> Url('///foo/bar').path
        '/foo/bar'

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
        >>> u.path
        'foo'

      Other keyword arguments can specify defaults for missing parts
      of the URL::

        >>> u = Url('/tmp/foo', scheme='file', netloc='localhost')
        >>> u.scheme
        'file'
        >>> u.netloc
        'localhost'
        >>> u.path
        '/tmp/foo'

      Query attributes are also supported::

        >>> u = Url('http://www.example.org?foo=bar')
        >>> u.query
        'foo=bar'

      and so are fragments::

        >>> u = Url('postgresql://user@db.example.org#table=data')
        >>> u.fragment
        'table=data'

    * By passing keyword arguments only, to construct an `Url` object
      with exactly those values for the named fields::

        >>> u = Url(scheme='http', netloc='www.example.org', path='/data')

      In this form, the `force_abs` parameter is ignored.

    See also: http://goo.gl/9WcRvR
    """
    __slots__ = ()

    _fields = ['scheme', 'netloc', 'path',
               'hostname', 'port', 'query',
               'username', 'password', 'fragment']

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
                return tuple.__new__(cls, (
                    urlstring.scheme, urlstring.netloc, urlstring.path,
                    urlstring.hostname, urlstring.port, urlstring.query,
                    urlstring.username, urlstring.password, urlstring.fragment
                ))
            else:
                # parse `urlstring` and use kwd arguments as default values
                try:
                    urldata = urlparse.urlsplit(
                        urlstring, scheme=scheme, allow_fragments=True)
                    # Python 2.6 parses fragments only for http(s),
                    # for any other scheme, the fragment is returned as
                    # part of the path...
                    if '#' in urldata.path:
                        path_, fragment_ = urldata.path.split('#')
                        urldata = urlparse.SplitResult(
                            urldata.scheme, urldata.netloc,
                            path_, urldata.query, fragment_)
                    if urldata.scheme == 'file' and not os.path.isabs(
                            urldata.path) and force_abs:
                        urldata = urlparse.urlsplit(
                            'file://' + os.path.abspath(urldata.path))
                    return tuple.__new__(cls, (
                        urldata.scheme or scheme,
                        urldata.netloc or netloc,
                        urldata.path or path,
                        urldata.hostname or hostname,
                        urldata.port or port,
                        urldata.query or query,
                        urldata.username or username,
                        urldata.password or password,
                        urldata.fragment or fragment,
                        ))
                except (ValueError, TypeError, AttributeError) as err:
                    raise ValueError(
                        "Cannot parse string '%s' as a URL: %s: %s"
                        % (urlstring, err.__class__.__name__, err))
        else:
            # no `urlstring`, use kwd arguments
            return tuple.__new__(cls, (
                scheme, netloc, path,
                hostname, port, query,
                username, password, fragment
            ))

    def __getattr__(self, name):
        try:
            return self[self._fields.index(name)]
        except ValueError:
            raise AttributeError("'%s' object has no attribute '%s'"
                                 % (self.__class__.__name__, name))

    def __getnewargs__(self):
        """Support pickling/unpickling `Url` class objects."""
        return (None, False,  # urlstring, force_abs
                self.scheme, self.netloc, self.path, self.hostname, self.port,
                self.query, self.username, self.password)

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


class UrlKeyDict(dict):

    """
    A dictionary class enforcing that all keys are URLs.

    Strings and/or objects returned by `urlparse` can
    be used as keys.  Setting a string key automatically
    translates it to a URL:

       >>> d = UrlKeyDict()
       >>> d['/tmp/foo'] = 1
       >>> for k in d.keys(): print (type(k), k.path) # doctest:+ELLIPSIS
       (<class '....Url'>, '/tmp/foo')

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

    Differently from `dict`, initialization from keyword arguments
    alone is *not* supported:

        >>> d3 = UrlKeyDict(foo='foo') # doctest: +ELLIPSIS
        Traceback (most recent call last):
            ...
        TypeError: __init__() got an unexpected keyword argument 'foo'


    An empty `UrlKeyDict` instance is returned by the constructor
    when called with no parameters::

        >>> d0 = UrlKeyDict()
        >>> len(d0)
        0

    If `force_abs` is `True`, then all paths are converted to
    absolute ones in the dictionary keys.

        >>> d = UrlKeyDict(force_abs=True)
        >>> d['foo'] = 1
        >>> for k in d.keys(): print os.path.isabs(k.path)
        True

        >>> d = UrlKeyDict(force_abs=False)
        >>> d['foo'] = 2
        >>> for k in d.keys(): print os.path.isabs(k.path)
        False

    """

    def __init__(self, iter_or_dict=None, force_abs=False):
        self._force_abs = force_abs
        if iter_or_dict is not None:
            try:
                # if `iter_or_dict` is a dict-like object, then it has
                # `iteritems()`
                for k, v in iter_or_dict.iteritems():
                    self[k] = v
            except AttributeError:
                # then assume `iter_or_dict` is an iterator over (key, value)
                # pairs
                for k, v in iter_or_dict:
                    self[k] = v

    def __contains__(self, key):
        # this is necessary to have key-lookup work with strings as well
        return (dict.__contains__(self, key)
                or key in self.keys())

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError as ex:
            # map `key` to a URL and try with that
            try:
                return dict.__getitem__(self, Url(key, self._force_abs))
            except:
                raise ex

    def __setitem__(self, key, value):
        try:
            dict.__setitem__(self, Url(key, self._force_abs), value)
        except:
            dict.__setitem__(self, key, value)


class UrlValueDict(dict):

    """
    A dictionary class enforcing that all values are URLs.

    Strings and/or objects returned by `urlparse` can
    be used as values.  Setting a string value automatically
    translates it to a URL:

       >>> d = UrlValueDict()
       >>> d[1] = '/tmp/foo'
       >>> d[2] = Url('file:///tmp/bar')
       >>> for v in d.values(): print (type(v), v.path) # doctest:+ELLIPSIS
       (<class '....Url'>, '/tmp/foo')
       (<class '....Url'>, '/tmp/bar')

    Retrieving the value associated with a key always returns the
    URL-type value, regardless of how it was set::

        >>> repr(d[1]) == "Url(scheme='file', netloc='', path='/tmp/foo', " \
        "hostname=None, port=None, query='', username=None, password=None, fragment='')"
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
        >>> for v in d.values(): print os.path.isabs(v.path)
        True

        >>> d = UrlValueDict(force_abs=False)
        >>> d[2] = 'foo'
        >>> for v in d.values(): print os.path.isabs(v.path)
        False

    """

    def __init__(self, iter_or_dict=None, force_abs=False, **extra_args):
        self._force_abs = force_abs
        if iter_or_dict is not None:
            try:
                # if `iter_or_dict` is a dict-like object, then it has
                # `iteritems()`
                for k, v in iter_or_dict.iteritems():
                    self[k] = v
            except AttributeError:
                # then assume `iter_or_dict` is an iterator over (key, value)
                # pairs
                for k, v in iter_or_dict:
                    self[k] = v
        for k, v in extra_args.iteritems():
            self[k] = v

    def __setitem__(self, key, value):
        try:
            dict.__setitem__(self, key, Url(value, self._force_abs))
        except:
            dict.__setitem__(self, key, value)


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="url",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
