#! /usr/bin/env python
#
"""This module contains various `pollers`. A Poller is an object that
keeps track of a generic URL and returns `events` whenever a new
object is created inside that URL.

"""
# Copyright (C) 2012-2013, GC3, University of Zurich. All rights reserved.
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

from .url import Url
import os
import logging
log = logging.getLogger('gc3.gc3libs')

# Conditional imports. Some pollers depend on the presence of specific
# Python modules.
try:
    import inotifyx
except:
    log.warning(
        "Module inotifyx not found. INotifyPoller class will not be available")
    inotifyx = None

try:
    import swiftclient
except ImportError:
    log.warning(
        "Module swiftclient not found. SwiftPoller will bot be available.")
    swiftclient = None


# These are directly took from inotifyx
events = {'IN_ACCESS': 1,
 'IN_ALL_EVENTS': 4095,
 'IN_ATTRIB': 4,
 'IN_CLOSE': 24,
 'IN_CLOSE_NOWRITE': 16,
 'IN_CLOSE_WRITE': 8,
 'IN_CREATE': 256,
 'IN_DELETE': 512,
 'IN_DELETE_SELF': 1024,
 'IN_DONT_FOLLOW': 33554432,
 'IN_IGNORED': 32768,
 'IN_ISDIR': 1073741824,
 'IN_MASK_ADD': 536870912,
 'IN_MODIFY': 2,
 'IN_MOVE': 192,
 'IN_MOVED_FROM': 64,
 'IN_MOVED_TO': 128,
 'IN_MOVE_SELF': 2048,
 'IN_ONESHOT': 2147483648,
 'IN_ONLYDIR': 16777216,
 'IN_OPEN': 32,
 'IN_Q_OVERFLOW': 16384,
 'IN_UNMOUNT': 8192}

def get_mask_description(mask):
    """
    Return an ASCII string describing the mask field in terms of
    bitwise-or'd IN_* constants, or 0.  The result is valid Python code
    that could be eval'd to get the value of the mask field.  In other
    words, for a given event:
    """
    parts = []
    for name, value in events.items():
        if mask&value:
            parts.append(name)
    if parts:
        return str.join("|", parts)
    else:
        return '0'


available_pollers = {}


class Poller(object):
    """Abstract class for an Url Poller.

    A :py:class:`Poller` is a class that tracks new events on a
    specific :py:class:`Url`. When calling the :py:meth:`get_events()`
    it will return a list of tuples (Url, mask) containing the events
    occurred for each one of the underlying url.

    """
    def __init__(self, url, mask, **kw):
        self.url = Url(url)
        self.mask = mask

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


def register_poller(scheme, cls):
    # We might want to add some check here...
    log.debug("Registering poller '%s' for url scheme '%s'",
              cls.__name__, scheme)

    available_pollers[scheme] = cls


class INotifyPoller(Poller):
    """Poller implementation that uses inotifyx to track new events on the
    specified Url.

    :params recurse: When set to `True`, automatically track also
    events in any already existing or newly created subfolder.

    This poller is used by default when the system supports INotify
    and the Url has a `file` schema

    WARNING: on Linux the maximum number of inotify descriptors that
    an user can open is limited by the kernel parameters:

    * fs.inotify.max_user_instances
    * fs.inotify.max_user_watches
    * fs.inotify.max_queued_events

    also cfr. inotify(7) manpage http://linux.die.net/man/7/inotify
    """

    def __init__(self, url, mask, recurse=False, **kw):
        Poller.__init__(self, url, mask, **kw)

        self._recurse = recurse
        self._ifds = {}
        # Ensure inbox directory exists
        if not os.path.exists(self.url.path):
            log.warning("Inbox directory `%s` does not exist,"
                        " creating it.", self.url.path)
            os.makedirs(self.url.path)

        ifd = inotifyx.init()
        inotifyx.add_watch(ifd, self.url.path, self.mask)
        log.debug("Adding watch for path %s" % self.url.path)
        self._ifds[self.url.path] = ifd

        if self._recurse:
            for dirpath, dirnames, filename in os.walk(self.url.path):
                for dirname in dirnames:
                    abspath = os.path.join(self.url.path,
                                           dirpath,
                                           dirname)
                    log.debug("Adding watch for path %s" % abspath)
                    self._add_watch(abspath)

    def _add_watch(self, path):
        if path not in self._ifds:
            log.debug("Adding watch for path %s" % path)
            ifd = inotifyx.init()
            inotifyx.add_watch(ifd, path, self.mask)
            self._ifds[path] = ifd

    def get_events(self):
        newevents = []
        for path, ifd in self._ifds.items():
            ievents = inotifyx.get_events(ifd, 0)
            for event in ievents:
                # if `name` is empty, it's the same directory
                abspath = os.path.join(path, event.name)
                url = Url(abspath) if event.name else self.url
                newevents.append((url, event.mask))
                if self._recurse and \
                   event.mask & inotifyx.IN_ISDIR and \
                   event.mask & inotifyx.IN_CREATE:
                    # New directory has been created. We need to add a
                    # watch for this directory too and for all its
                    # subdirectories. Also, we need to trigger new
                    # events for any other file created in it

                    for (rootdir, dirnames, filenames) in os.walk(abspath):
                        for dirname in dirnames:
                            self._add_watch(os.path.join(rootdir, dirname))
                        for filename in filenames:
                            # Trigger a fake event
                            newevents.append((Url(os.path.join(rootdir, filename)),
                                              events['IN_CLOSE_WRITE']|events['IN_ALL_EVENTS']))
        return newevents

if inotifyx:
    register_poller('file', INotifyPoller)


class FilePoller(Poller):
    """Poller implementation that uses regular `os` module to track for
    new events on a filesystem.

    This implementation is used to track Url with `file` schema
    whenever :py:mod:`inotifyx` module is not available.

    """
    def __init__(self, url, mask, **kw):
        Poller.__init__(self, url, mask, **kw)
        self._path = self.url.path
        if not os.path.exists(self.url.path):
            log.warning("Inbox directory `%s` does not exist,"
                        " creating it.", self.url.path)
            os.makedirs(self.url.path)
        self._known_files = {}
        for path in os.listdir(self._path):
            abspath = os.path.join(self._path, path)
            stat = os.stat(abspath)
            self._known_files[path] = stat

    def get_events(self):
        dircontents = [os.path.join(self._path, path) for path in os.listdir(self._path)]
        # Check if new files have been created or old ones updated
        newevents = []
        for path in dircontents:
            stat = os.stat(path)

            # We can only check if:
            #
            if path not in self._known_files:
                # We can only get
                event = events['IN_CLOSE_WRITE']|events['IN_CREATE']
                if os.path.isdir(path):
                    event|=events['IN_ISDIR']
                self._known_files[path] = stat
                newevents.append((Url(path), event))
            elif stat.st_mtime > self._known_files[path].st_mtime:
                # File was updated?
                raise

        # check if some file was deleted
        for path in list(self._known_files):
            if path not in dircontents:
                newevents.append((Url(path), events['IN_DELETE']))
                self._known_files.pop(path)
        return newevents

if not inotifyx:
    register_poller('file', FilePoller)


class SwiftPoller(Poller):
    """Poller that periodically checks a swift bucket and trigger new
    events when new objects are created.

    Right now, a valid url can be one of the following form:

    If the keystone endpoint is reachable via http, either one of:

    swift://<user>+<tenant>:<password>@<keystone-url>?container
    swt://<user>+<tenant>:<password>@<keystone-url>?container

    if the keystone endpoint is reachable via https, either one of:

    swifts://<user>+<tenant>:<password>@<keystone-url>?container
    swts://<user>+<tenant>:<password>@<keystone-url>?container

    and we assume auth version 2 is used (keystone)

    """
    def __init__(self, url, mask, **kw):
        Poller.__init__(self, url, mask, **kw)

        try:
            self.username, self.project_name = self.url.username.split('+')
        except ValueError:
            raise gc3libs.exceptions.InvalidValue(
                "Missing tenant name in swift url '%s'", self.url)
        self.password = self.url.password
        self.container = self.url.query

        if not self.container:
            raise gc3libs.exceptions.InvalidValue(
                "Missing bucket name in swift url '%s'", self.url)
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
        log.debug("Successfully connected to storage '%s'. %d containers found",
                  self.conn.url, len(containers))
        if self.container not in [a.get('name') for a in containers]:
            log.warning("Container %s not found for swift url '%s'",
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
            url = Url(str(self.url) + '&name=%s' % obj['name'])
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


if swiftclient:
    register_poller('swift', SwiftPoller)


def get_poller(url, mask=events['IN_ALL_EVENTS'], **kw):
    """
    Constructor method that returns the right poller for the specified
    :py:mod:`gc3libs.url.Url`.

    """

    url = Url(url)
    try:
        pollercls = available_pollers[url.scheme]
    except KeyError:
        if url.scheme not in available_pollers:
            raise ValueError(
                "No poller found for scheme `%s`", url.scheme)
    return pollercls(url, mask, **kw)


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="poller",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
