#! /usr/bin/env python

"""
This module implements "pollers". A "Poller" is an object that
monitors a given URL and returns `events` whenever a new object is
created inside that URL.
"""

# Copyright (C) 2017-2019,  University of Zurich. All rights reserved.
# Copyright (C) 2020,  ETH Zurich. All rights reserved.
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
from builtins import object
from abc import ABCMeta, abstractmethod
from collections import defaultdict
import os
import stat
from warnings import warn

# GC3Pie imports
import gc3libs
from gc3libs.url import Url
from future.utils import with_metaclass


# Some pollers depend on the presence of specific Python modules;
# delay ImportError until actually used but raise a warning.
class _Unavailable(object):
    def __init__(self, missing):
        self.__missing = missing
        warn("Trying to initialize `{module}` which is not available."
             " A placeholder object will be used instead, but it will raise"
             " `ImportError` later if there is any actual attempt at using it."
             .format(module=self.__missing),
             ImportWarning)
    def __getattr__(self, name):
        raise ImportError(
            "Trying to actually use module `{module}`"
            " which could not be imported. Aborting."
            .format(module=self.__missing))

try:
    import inotify_simple as inotify
except:
    inotify = _Unavailable('inotify_simple')

try:
    import swiftclient
except ImportError:
    swiftclient = _Unavailable('swiftclient')


##
#
# Registration support
#
##

_AVAILABLE_POLLERS = defaultdict(list)

def make_poller(url, **extra):
    """
    Factory method that returns the registered poller for the specified
    :py:mod:`gc3libs.url.Url`.
    """

    url = Url(url)
    try:
        registered = _AVAILABLE_POLLERS[url.scheme]
    except KeyError:
        raise ValueError(
            "No poller associated with scheme `{0}`"
            .format(url.scheme))
    for cls in registered:
        try:
            poller = cls(url, **extra)
            gc3libs.log.debug("Using class %s to poll URL %s", cls, url)
            return poller
        except Exception as err:
            gc3libs.log.debug(
                "Could not create poller for scheme `%s` with class %s: %s",
                url.scheme, cls, err)
    raise ValueError(
        "No registered class could be instanciated to poll URL {0}"
        .format(url))


def register_poller(scheme):
    def register_class(cls):
        _AVAILABLE_POLLERS[scheme].append(cls)
        return cls
    return register_class


class Poller(with_metaclass(ABCMeta, object)):
    """
    Abstract class for an URL Poller.

    A :py:class:`Poller` is a class that tracks new events on a
    specific :py:class:`Url`. When calling the :py:meth:`get_events()`
    it will return a list of tuples (Url, mask) containing the events
    occurred for each one of the underlying URLs.
    """

    __slots__ = ['url']

    def __init__(self, url, **kw):
        self.url = Url(url)

    @abstractmethod
    def get_new_events(self):
        """
        Iterate over events that happened since last call to this method.

        Returns a list of tuples *(subject, event)*.

        A *subject* is a unique identifier for a watched "thing": the
        exact form and type depends on the actual concrete class;
        pollers that watch the filesystem or HTTP-accessible resources
        will use a URL (:class:`gc3libs.url.Url`) as a *subject*
        instance, but e.g. pollers that watch a database table might
        use a row ID instead.

        The associated *event* is one or more of the following
        strings:

        - ``created``: the subject has been created since the last
          call to ``get_new_events()``;

        - ``modified``: the subject has changed since the last call to
          ``get_new_events()``;

        - ``deleted``: the subject has been deleted since the last
          call to ``get_new_events()``;

        Depending on the concrete poller class, some events might
        never occur, or cannot be detected.  Most notably, only
        filesystem-watching pollers might be able to generate
        meaningful ``modified`` events.
        """
        raise NotImplementedError(
            "Abstract method `Poller.get_events()` called "
            " - this should have been defined in a derived class.")


##
#
# Filesystem polling
#
##

@register_poller('file')
class INotifyPoller(Poller):
    """
    Use Linux' inofity to track new events on the specified filesystem location.

    :params recurse:
      When ``True``, automatically track events in any
      (already-existing or newly-created) subdirectory.

    This poller is used by default when the `inotify_simple` Python
    package is available and the URL has a `file` schema.

    .. warning::

        On Linux, the maximum number of inotify descriptors that
        a user can open is limited by the kernel parameters:

        * ``fs.inotify.max_user_instances``
        * ``fs.inotify.max_user_watches``
        * ``fs.inotify.max_queued_events``

        See also the `inotify(7) manpage <http://linux.die.net/man/7/inotify>`_
    """

    __slots__ = [
        '_ifd',
        '_recurse',
        '_wd',
    ]

    def __init__(self, url, recurse=False, **kw):
        # normalize filesystem paths by removing trailing slashes;
        # this is necessary for the check at the beginning of
        # `self.watch()` to work correctly when path == self.url.path
        try:
            path = os.path.abspath(url.path)
        except AttributeError:
            path = os.path.abspath(url)
        super(INotifyPoller, self).__init__(path, **kw)

        self._recurse = recurse
        self._ifd = inotify.INotify()
        self._wd = {}

        # Ensure inbox directory exists
        if not os.path.exists(self.url.path):
            gc3libs.log.info(
                "Inbox directory `%s` does not exist,"
                " creating it ...", self.url.path)
            os.makedirs(self.url.path)

        self.watch(self.url.path)
        if self._recurse:
            for dirpath, dirnames, filenames in os.walk(self.url.path):
                self.watch(dirpath)
                for name in filenames:
                    path = os.path.join(self.url.path, dirpath, name)
                    self.watch(path)

    @property
    def recurse(self):
        """
        Whether the poller is watching the entire directory tree pointed
        to by ``self.url``, or only the directory at its top level.
        """
        return self._recurse

    def watch(self, path):
        path = os.path.abspath(path)
        # FIXME: this is not enough, as we might be adding a watch to
        # a file located in a subdirectory of `self.url` without *recurse*
        assert path.startswith(self.url.path), (
            "Request to watch path `%s`"
            " which does not lie in directory tree rooted at `%s`",
            path, self.url.path)
        wd = self._ifd.add_watch(path,
                                 # only watch for inotify(7) events that
                                 # translate to something we can report
                                 (0
                                  |inotify.flags.CLOSE_WRITE
                                  |inotify.flags.CREATE
                                  |inotify.flags.DELETE
                                  |inotify.flags.EXCL_UNLINK
                                  |inotify.flags.MODIFY
                                  |inotify.flags.OPEN
                                 ))
        self._wd[wd] = path
        gc3libs.log.debug(
            "Added watch for path `%s` as watch descriptor %d ", path, wd)

    def get_new_events(self):
        new_events = []
        ievents = self._ifd.read(0)
        cumulative = defaultdict(int)
        for ievent in ievents:
            try:
                path = os.path.join(self._wd[ievent.wd], ievent.name)
            except KeyError:
                raise AssertionError(
                    "Received event {0} for unknown watch descriptor {1}"
                    .format(ievent, ievent.wd))
            # if `name` is empty, it's the same directory
            cumulative[path] |= ievent.mask
            accumulated = cumulative[path]
            # we want to dispatch a single `created` or `modified`
            # event, so check once the file is closed what past events
            # have been recorded
            if (ievent.mask & inotify.flags.CLOSE_WRITE):
                if (accumulated & inotify.flags.CREATE):
                    new_events.append(
                        self.__make_event(path, 'created'))
                elif (accumulated & inotify.flags.MODIFY):
                    new_events.append(
                        self.__make_event(path, 'modified'))
            if (ievent.mask & inotify.flags.DELETE):
                new_events.append(
                    self.__make_event(path, 'deleted'))
            if (self._recurse
                and ievent.mask & inotify.flags.ISDIR
                and ievent.mask & inotify.flags.CREATE):
                # A new directory has been created. Add a watch
                # for it too and for all its subdirectories. Also,
                # we need to trigger new events for any file
                # created in it.
                for (rootdir, dirnames, filenames) in os.walk(path):
                    for dirname in dirnames:
                        self.watch(os.path.join(rootdir, dirname))
                    for filename in filenames:
                        # report creation event
                        new_events.append(
                            (Url(os.path.join(rootdir, filename)),
                             'created'))
        return new_events

    def __make_event(self, relpath, event):
        if relpath:
            url = Url(os.path.join(self.url.path, relpath))
        else:
            url = self.url
        return (url, event)


@register_poller('file')
class FilePoller(Poller):
    """
    Track events on the filesystem using Python's standard `os` module.

    :params recurse:
      When ``True``, automatically track events in any
      (already-existing or newly-created) subdirectory.

    .. warning::

      In order to issue `'modified'` events, this class relies on
      checking an inode's `st_mtime` field, which only provides
      1-second resolution.  Modification events that happen too close
      will not be told apart as distinct; in particular, modifying a
      file less than 1s after creating it will not be detected.

    This implementation is only used to track Url with `file` schema
    whenever the :py:mod:`inotify_simple` module is not available.
    """

    __slots__ = [
        '_recurse',
        '_watched'
    ]

    def __init__(self, url, recurse=False, **kw):
        super(FilePoller, self).__init__(url, **kw)

        root = self.url.path
        if not os.path.exists(root):
            gc3libs.log.warning(
                "Inbox root directory `%s` does not exist,"
                " creating it ...", root)
            os.makedirs(root)
        self._recurse = recurse
        self._watched = {}

        self.watch(root)

    @property
    def recurse(self):
        """
        Whether the poller is watching the entire directory tree pointed
        to by ``self.url``, or only the directory at its top level.
        """
        return self._recurse

    def watch(self, path):
        info = os.stat(path)
        self._watched[path] = info.st_mtime
        if stat.S_ISDIR(info.st_mode) and self._recurse:
            for entry in os.listdir(path):
                if entry in ['.', '..']:
                    continue
                child_path = os.path.join(path, entry)
                self.watch(child_path)


    def get_new_events(self):
        new_events = []
        # take a copy of the list of watched files, since
        # `self._get_events_*` may modify the `self._watched` dict
        for path in list(self._watched.keys()):
            if os.path.isdir(path):
                new_events += self._get_events_dir(path)
            else:
                new_events += self._get_events_file(path)
        return new_events


    def _get_events_file(self, path):
        if not os.path.exists(path):
            self._watched.pop(path)
            return [(Url(path), 'deleted')]

        event = None
        mtime = os.stat(path).st_mtime
        if mtime > self._watched[path]:
            event = 'modified'
        self._watched[path] = mtime

        if event:
            return [(Url(path), event)]
        else:
            return []

    def _get_events_dir(self, dirpath):
        new_events = []

        # check for new files
        contents = set(
            os.path.join(dirpath, entry)
            for entry in os.listdir(dirpath)
            if entry not in ['.', '..']
        )
        for path in contents:
            if path not in self._watched:
                new_events.append((Url(path), 'created'))
                # if path is a directory, add it to watch list only if
                # poller was created with `recurse=True`
                if (not os.path.isdir(path)) or self._recurse:
                    self.watch(path)

        return new_events


class SwiftPoller(Poller):
    """
    Periodically check a SWIFT bucket and trigger
    events when new objects are created.

    Right now, a valid URL can be one of the following form:

    - If the keystone endpoint is reachable via HTTP, either one of:

      * swift://<user>+<tenant>:<password>@<keystone-url>?container
      * swt://<user>+<tenant>:<password>@<keystone-url>?container

    - If the keystone endpoint is reachable via HTTPS, either one of:

      * swifts://<user>+<tenant>:<password>@<keystone-url>?container
      * swts://<user>+<tenant>:<password>@<keystone-url>?container

    We assume that keystone auth version 2 is used.
    """
    def __init__(self, url, **kw):
        super(SwiftPoller, self).__init__(url, **kw)

        try:
            self.username, self.project_name = self.url.username.split('+')
        except ValueError:
            raise gc3libs.exceptions.InvalidValue(
                "Missing project/tenant name in SWIFT URL '{0}'"
                .format(self.url))
        self.password = self.url.password
        self.container = self.url.query

        if not self.container:
            raise gc3libs.exceptions.InvalidValue(
                "Missing bucket name in SWIFT URL '{0}'"
                .format(self.url))
        # FIXME: also check for hostname and password?

        if url.scheme in ['swifts', 'swts']:
            auth_url = 'https://%s' % self.url.hostname
        else:
            auth_url = 'http://%s' % self.url.hostname
        if self.url.port:
            auth_url += ":%d" % self.url.port
        if self.url.path:
            auth_url += self.url.path
        self.auth_url = auth_url

        self.conn = swiftclient.Connection(
            authurl=self.auth_url,
            user=self.username,
            key=self.password,
            os_options = {
                "auth_url": self.auth_url,
                "project_name": self.project_name,
                "username": self.username,
                "password": self.password},
            auth_version='2')

        # List containers
        _, containers = self.conn.get_account()
        gc3libs.log.debug(
            "Successfully connected to SWIFT storage '%s':"
            " %d containers found", self.conn.url, len(containers))
        if self.container not in [a.get('name') for a in containers]:
            gc3libs.log.warning(
                "Container %s not found at SWIFT URL '%s'",
                self.container, self.url)

        _, objects = self.conn.get_container(self.container)
        self._known_objects = {}
        for obj in objects:
            url = Url(str(self.url) + '&name=%s' % obj['name'])
            self._known_objects[url] = obj

    def get_new_events(self):
        # List objects in container
        _, objects = self.conn.get_container(self.container)
        newevents = []

        objurls = []
        for obj in objects:
            url = Url('{baseurl}&name={objname}'
                      .format(baseurl=self.url,
                              objname=obj['name']))
            objurls.append(url)
            if url not in self._known_objects:
                self._known_objects[url] = obj
                newevents.append((url, 'created'))
        for url in list(self._known_objects):
            if url not in objurls:
                newevents.append((url, 'deleted'))
                self._known_objects.pop(url)
        return newevents


# register for multiple schemes
register_poller('swift')(SwiftPoller)
register_poller('swifts')(SwiftPoller)
register_poller('swt')(SwiftPoller)
register_poller('swts')(SwiftPoller)


# main: run tests
if "__main__" == __name__:
    import doctest
    doctest.testmod(name="poller",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
