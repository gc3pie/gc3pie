#! /usr/bin/env python
#
"""
This module implements "pollers". A "Poller" is an object that
monitors a given URL and returns `events` whenever a new object is
created inside that URL.
"""
# Copyright (C) 2017-2018, University of Zurich. All rights reserved.
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

# stdlib imports
from abc import ABCMeta, abstractmethod
import os
import stat

# Conditional imports. Some pollers depend on the presence of specific
# Python modules.
try:
    import inotifyx
except:
    inotifyx = None

try:
    import swiftclient
except ImportError:
    swiftclient = None

# GC3Pie imports
import gc3libs
from gc3libs.url import Url


##
#
# Event representation
#
##

# These are directly taken from `inotifyx`
events = {
    'IN_ACCESS':        1,
    'IN_ALL_EVENTS':    4095,
    'IN_ATTRIB':        4,
    'IN_CLOSE':         24,
    'IN_CLOSE_NOWRITE': 16,
    'IN_CLOSE_WRITE':   8,
    'IN_CREATE':        256,
    'IN_DELETE':        512,
    'IN_DELETE_SELF':   1024,
    'IN_DONT_FOLLOW':   33554432,
    'IN_IGNORED':       32768,
    'IN_ISDIR':         1073741824,
    'IN_MASK_ADD':      536870912,
    'IN_MODIFY':        2,
    'IN_MOVE':          192,
    'IN_MOVED_FROM':    64,
    'IN_MOVED_TO':      128,
    'IN_MOVE_SELF':     2048,
    'IN_ONESHOT':       2147483648,
    'IN_ONLYDIR':       16777216,
    'IN_OPEN':          32,
    'IN_Q_OVERFLOW':    16384,
    'IN_UNMOUNT':       8192
}

def get_mask_description(mask):
    """
    Return an ASCII string describing the mask field in terms of
    bitwise-or'd IN_* constants, or 0.  The result is valid Python code
    that could be eval'd to get the value of the mask field.  In other
    words, for a given event:
    """
    parts = []
    for name, value in events.items():
        if (mask & value):
            parts.append(name)
    if parts:
        return '|'.join(parts)
    else:
        return '0'


##
#
# Registration support
#
##

_available_pollers = {}

def register_poller(scheme, cls, condition=True,
                    fail_msg="missing requisite Python module"):
    if condition:
        gc3libs.log.debug(
            "Registering poller `%s` for url scheme `%s`",
            cls.__name__, scheme)
        _available_pollers[scheme] = cls
    else:
        gc3libs.log.warning(
            "Not registering class `%s`"
            " as handler for URL schema `%s`: %s",
            cls.__name__, fail_msg)

def make_poller(url, mask=events['IN_ALL_EVENTS'], **kw):
    """
    Factory method that returns the registered poller for the specified
    :py:mod:`gc3libs.url.Url`.
    """

    url = Url(url)
    try:
        cls = _available_pollers[url.scheme]
    except KeyError:
        raise ValueError(
            "No poller associated with scheme `{0}`"
            .format(url.scheme))
    return cls(url, mask, **kw)


class Poller(object):
    """
    Abstract class for an URL Poller.

    A :py:class:`Poller` is a class that tracks new events on a
    specific :py:class:`Url`. When calling the :py:meth:`get_events()`
    it will return a list of tuples (Url, mask) containing the events
    occurred for each one of the underlying URLs.
    """

    __metaclass__ = ABCMeta

    __slots__ = ['url', 'mask']

    def __init__(self, url, mask, **kw):
        self.url = Url(url)
        self.mask = mask

    @abstractmethod
    def get_events(self):
        """
        Returns a list of tuple (url, mask).

        Depending on the implementation, some events will make no
        sense as they are not clearly defined, or it's not possible to
        listen for those events.
        """
        raise NotImplementedError(
            "Abstract method `Poller.get_events()` called "
            " - this should have been defined in a derived class.")


##
#
# Filesystem polling
#
##

class INotifyPoller(Poller):
    """
    Use `inotifyx` to track new events on the specified filesystem location.

    :params recurse:
      When ``True``, automatically track events in any
      (already-existing or newly-created) subdirectory.

    This poller is used by default when the `inotifyx` Python package
    is available and the URL has a `file` schema.

    .. warning::

        On Linux, the maximum number of inotify descriptors that
        a user can open is limited by the kernel parameters:

        * ``fs.inotify.max_user_instances``
        * ``fs.inotify.max_user_watches``
        * ``fs.inotify.max_queued_events``

        See also the `inotify(7) manpage <http://linux.die.net/man/7/inotify>`_
    """

    def __init__(self, url, mask=events['IN_ALL_EVENTS'],
                 recurse=False, **kw):
        super(INotifyPoller, self).__init__(url, mask, **kw)

        self._recurse = recurse
        self._ifd = inotifyx.init()
        self._watched = set()

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

    def watch(self, path):
        if path not in self._watched:
            gc3libs.log.debug("Adding watch for path %s" % path)
            inotifyx.add_watch(self._ifd, path, self.mask)
            self._watched.add(path)

    def get_events(self):
        new_events = []
        for path in self._watched:
            ievents = inotifyx.get_events(self._ifd, 0)
            for event in ievents:
                # if `name` is empty, it's the same directory
                abspath = os.path.join(path, event.name)
                url = Url(abspath) if event.name else self.url
                new_events.append((url, event.mask))
                if (self._recurse
                    and event.mask & inotifyx.IN_ISDIR
                    and event.mask & inotifyx.IN_CREATE):
                    # A new directory has been created. Add a watch
                    # for it too and for all its subdirectories. Also,
                    # we need to trigger new events for any file
                    # created in it.
                    for (rootdir, dirnames, filenames) in os.walk(abspath):
                        for dirname in dirnames:
                            self.watch(os.path.join(rootdir, dirname))
                        for filename in filenames:
                            # Trigger a fake event
                            new_events.append(
                                (Url(os.path.join(rootdir, filename)),
                                 events['IN_CLOSE_WRITE']|events['IN_CREATE']))
        return new_events

register_poller('file', INotifyPoller, inotifyx,
                "missing required Python module `inotifyx`."
                " Use `pip install inotifyx` to install it.")


class FilePoller(Poller):
    """
    Track events on the filesystem using Python's standard `os` module.

    :params recurse:
      When ``True``, automatically track events in any
      (already-existing or newly-created) subdirectory.

    In contrast with the `INotifyPoller`:class:, only file and
    directory creation events are reported.

    This implementation is only used to track Url with `file` schema
    whenever :py:mod:`inotifyx` module is not available.
    """

    __slots__ = ['recurse', '_watched']

    def __init__(self, url, mask=events['IN_ALL_EVENTS'],
                 recurse=False, **kw):
        super(FilePoller, self).__init__(url, mask, **kw)

        root = self.url.path
        if not os.path.exists(root):
            gc3libs.log.warning(
                "Inbox root directory `%s` does not exist,"
                " creating it ...", root)
            os.makedirs(root)
        self.recurse = recurse
        self._watched = {}

        self.watch(root)


    def watch(self, path):
        info = os.stat(path)
        self._watched[path] = info
        if stat.S_ISDIR(info.st_mode) and self.recurse:
            for entry in os.listdir(path):
                if entry in ['.', '..']:
                    continue
                child_path = os.path.join(path, entry)
                self.watch(child_path)


    def get_events(self):
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
            return [(Url(path), events['IN_DELETE'])]

        event = 0
        info = os.stat(path)
        if info.st_mtime > self._watched[path].st_mtime:
            event |= events['IN_MODIFY']
        if info.st_ctime > self._watched[path].st_ctime:
            event |= events['IN_ATTRIB']
        self._watched[path] = info

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
                # we cannot really know when a user is done writing
                # so `IN_CLOSE_WRITE` is technically incorrect here
                event = events['IN_CLOSE_WRITE'] | events['IN_CREATE']
                if os.path.isdir(path):
                    event |= events['IN_ISDIR']
                new_events.append((Url(path), event))
                self.watch(path)

        return new_events

register_poller('file', FilePoller, not inotifyx,
                "using inotify-based poller instead.")


class SwiftPoller(Poller):
    """
    Periodically check a SWIFT bucket and trigger
    events when new objects are created.

    Right now, a valid url can be one of the following form:

    - If the keystone endpoint is reachable via HTTP, either one of:

      * swift://<user>+<tenant>:<password>@<keystone-url>?container
      * swt://<user>+<tenant>:<password>@<keystone-url>?container

    - If the keystone endpoint is reachable via HTTPS, either one of:

      * swifts://<user>+<tenant>:<password>@<keystone-url>?container
      * swts://<user>+<tenant>:<password>@<keystone-url>?container

    We assume that keystone auth version 2 is used.
    """
    def __init__(self, url, mask, **kw):
        super(SwiftPoller, self).__init__(url, mask, **kw)

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
        # also check for hostname and password?

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
        accstat, containers = self.conn.get_account()
        gc3libs.log.debug(
            "Successfully connected to SWIFT storage '%s':"
            " %d containers found", self.conn.url, len(containers))
        if self.container not in [a.get('name') for a in containers]:
            gc3libs.log.warning(
                "Container %s not found at SWIFT URL '%s'",
                self.container, self.url)

        constat, objects = self.conn.get_container(self.container)

        self._known_objects = {}
        for obj in objects:
            url = Url(str(self.url) + '&name=%s' % obj['name'])
            self._known_objects[url] = obj

    def get_events(self):
        # List objects in container
        constat, objects = self.conn.get_container(self.container)
        newevents = []

        objurls = []
        for obj in objects:
            url = Url(str(self.url) + '&name=' + obj['name'])
            objurls.append(url)
            if url not in self._known_objects:
                self._known_objects[url] = obj
                # Here it's correct not to put IN_CREATE because on
                # swift you will see an object only when it has been
                # completely uploaded.
                newevents.append((url, events['IN_CLOSE_WRITE']))
        for url in list(self._known_objects):
            if url not in objurls:
                newevents.append((url, events['IN_DELETE']))
                self._known_objects.pop(url)
        return newevents

register_poller('swift', SwiftPoller, swiftclient)
register_poller('swt', SwiftPoller, swiftclient)
register_poller('swifts', SwiftPoller, swiftclient)
register_poller('swts', SwiftPoller, swiftclient)


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="poller",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
