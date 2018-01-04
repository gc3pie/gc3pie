#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012-2018 University of Zurich. All rights reserved.
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


# stdlib imports
import csv
import itertools
import os
import sys
import shutil

# GC3Pie imports
import gc3libs
import gc3libs.exceptions
import gc3libs.persistence
import gc3libs.persistence.store
import gc3libs.utils
from gc3libs.workflow import TaskCollection


class Session(list):

    """
    A 'session' is a persistent collection of tasks.

    Tasks added to the session are persistently recorded using an
    instance of `gc3libs.persistence.Store`.  Stores can be shared
    among different sessions: each session knows wich jobs it 'owns'.

    A session is associated to a directory, which holds all the data
    releated to that session. Specifically, two files are always
    created in the session directory andused internally by this class:

    * `index.txt`: contains a list of all job IDs
      associated with this session;
    * `store.url`:  its contents are the URL of the store to create
      (as would be passed to the `gc3libs.persistence.make_store` factory).

    To only argument needed to instantiate a session is the `path` of
    the directory; the directory name will be used as the identifier
    of the session itself.  For example, the following code creates a
    temporary directory and the two files mentioned above inside it::

        >>> import tempfile; tmpdir = tempfile.mktemp(dir='.')
        >>> session = Session(tmpdir)
        >>> sorted(os.listdir(tmpdir))
        ['created', 'session_ids.txt', 'store.url']

    When a `Session` object is created with a `path` argument pointing
    to an existing valid session, the index of jobs is automatically
    loaded into memory, and the store pointed to by the ``store.url``
    file in the session directory will be used, *disregarding the
    contents of the `store_url` argument.*

    In other words, the `store_url` argument is only used when
    *creating a new session*.  If no `store_url` argument is passed
    (i.e., it has its default value), a `Session` object will
    instantiate and use a `FileSystemStore`:class: store, keeping data
    in the ``jobs`` subdirectory of the session directory.

    Methods `add` and `remove` are provided to manage the collection;
    the `len()` operator returns the number of tasks in the session;
    iteration over a session returns the tasks one by one::

        >>> task1 = gc3libs.Task()
        >>> id1 = session.add(task1)
        >>> task2 = gc3libs.Task()
        >>> id2 = session.add(task2)
        >>> len(session)
        2
        >>> for t in session:
        ...     print(type(t))
        <class 'gc3libs.Task'>
        <class 'gc3libs.Task'>
        >>> session.remove(id1)
        >>> len(session)
        1

    When passed the `flush=False` optional argument, methods `add` and
    `remove` do not update the session metadata: i.e., the tasks are
    added or removed from the store and the in-memory task list, but
    the updated task list is not saved back to disk.  This is useful
    when making many changes in a row; call `Session.flush` to persist
    the full set of changes.

    The `Store`:class: object is anyway accessible in the
    `store`:attr: attribute of each `Session` instance::

        >>> type(session.store)
        <class 'gc3libs.persistence.filesystem.FilesystemStore'>

    However, `Session` defines methods `save` and `load` as a
    convenient proxy to the corresponding `Store` methods::

        >>> obj = gc3libs.persistence.Persistable()
        >>> oid = session.save(obj)
        >>> obj2 = session.load(oid)
        >>> obj.persistent_id == obj2.persistent_id
        True

    The whole session data can be removed by using method `destroy`::

        >>> session.destroy()
        >>> os.path.exists(session.path)
        False

    """

    INDEX_FILENAME = 'session_ids.txt'
    STORE_URL_FILENAME = "store.url"
    TIMESTAMP_FILES = {'start': 'created',
                       'end': 'finished'}

    DEFAULT_JOBS_DIR = 'jobs'

    def __init__(self, path, create=True, store_or_url=None, **extra_args):
        """
        First argument `path` is the path to the session directory.

        The `create` argument is used to control the behavior in case the
        session directory does not exists already:

        * If `create` is ``True`` (default) and the session directory
          does not exists then a new session will be created.

        * If `create` is ``False`` and the session directory does not
          exists an error will be raised.

        Optional argument `store_or_url` is *either* an existing valid
        `gc3libs.persistence.store.Store` instance, *or* the URL of
        the store, as would be passed to function
        `gc3libs.persistence.store.make_store`:func:; any additional
        keyword arguments are passed to `make_store` unchanged.

        .. note::

          The optional `store_or_url` argument and following keyword arguments
          are used if and only if a new session is being *created*; they are
          ignored when loading existing sessions!

        By default `gc3libs.persistence.filesystem.FileSystemStore`:class:
        (which see) is used for providing a new session with a store.
        """
        self.path = os.path.abspath(path)
        self.name = os.path.basename(self.path)
        self.tasks = dict()
        # Session not yet created
        self.created = -1
        self.finished = -1
        self.cmdline = extra_args.get('cmdline', None)

        # load or make session
        if os.path.isdir(self.path):
            # Session already exists?
            try:
                self._load_session(**extra_args)
            except IOError as err:
                gc3libs.log.debug("Cannot load session '%s': %s", path, err)
                if err.errno == os.errno.ENOENT:  # "No such file or directory"
                    if create:
                        gc3libs.log.debug(
                            "Assuming session is incomplete or corrupted,"
                            " creating it again.")
                        self._create_session(store_or_url, **extra_args)
                    else:
                        raise gc3libs.exceptions.InvalidArgument(
                            "Directory '%s' does not contain a valid session" %
                            self.path)
                else:
                    raise
        else:
            if create:
                self._create_session(store_or_url, **extra_args)
            else:
                raise gc3libs.exceptions.InvalidArgument(
                    "Session '%s' not found" % self.path)

    def _create_session(self, store_or_url, **extra_args):
        if isinstance(store_or_url, gc3libs.persistence.store.Store):
            self.store = store_or_url
        else:
            if store_or_url is None:
                store_or_url = os.path.join(self.path, self.DEFAULT_JOBS_DIR)
            # Ensure session directory exists before `make_store` is
            # called, or else SQLite raises an "OperationalError:
            # unable to open database file None None"
            gc3libs.utils.mkdir(self.path)
            self.store = gc3libs.persistence.make_store(store_or_url, **extra_args)
        self.store_url = self.store.url
        self._save_store_url_file()
        self._save_index_file()

        # Save the current command line on ``created`` file.
        fd = open(os.path.join(
            self.path,
            self.TIMESTAMP_FILES['start']), 'w')
        fd.write(str.join(' ', sys.argv) + '\n')
        fd.close()

        self.set_start_timestamp()

    def _load_session(self, **extra_args):
        """
        Load an existing session from disk.

        Keyword arguments are passed to the `make_store` factory
        method unchanged.

        Any error that occurs while loading jobs from disk is ignored.
        """
        try:
            store_fname = os.path.join(self.path, self.STORE_URL_FILENAME)
            self.store_url = gc3libs.utils.read_contents(store_fname).strip()
            gc3libs.log.debug("Loading session from URL %s ...", self.store_url)
        except IOError:
            gc3libs.log.info(
                "Unable to load session: file %s is missing.", store_fname)
            raise
        if 'store' in extra_args:
            self.store = extra_args['store']
            self.store_url = self.store.url
        else:
            self.store = gc3libs.persistence.make_store(
                self.store_url, **extra_args)

        idx_filename = os.path.join(self.path, self.INDEX_FILENAME)
        with open(idx_filename) as idx_file:
            ids = idx_file.read().split()

        try:
            start_file = os.path.join(
                self.path, self.TIMESTAMP_FILES['start'])
            self.created = os.stat(start_file).st_mtime
        except OSError:
            gc3libs.log.warning(
                "Unable to recover starting time from existing session:"
                " file %s is missing." % (start_file))

        for task_id in ids:
            try:
                self.tasks[task_id] = self.store.load(task_id)
            except Exception as err:
                if gc3libs.error_ignored(
                        # context:
                        # - module
                        'session',
                        # - class
                        'Session',
                        # - method
                        'load',
                        # - actual error class
                        err.__class__.__name__,
                        # - additional keywords
                        'persistence',
                ):
                    gc3libs.log.warning(
                        "Ignoring error from loading '%s': %s", task_id, err)
                else:
                    # propagate exception back to caller
                    raise

    def destroy(self):
        """
        Remove the session directory and all the tasks it contains
        from the store which are associated to this session.

        .. note::

          This will remove the associated task storage *if and only if*
          the storage is contained in the session directory!

        """
        for task_id in self.tasks:
            self._recursive_remove_from_store(task_id)
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    # collection management

    def add(self, task, flush=True):
        """
        Add a `Task` to the current session, save it to the associated
        persistent storage, and return the assigned `persistent_id`::

            >>> # create new, empty session
            >>> import tempfile; tmpdir = tempfile.mktemp(dir='.')
            >>> session = Session(tmpdir)
            >>> len(session)
            0

            >>> # add a task to it
            >>> task = gc3libs.Task()
            >>> tid1 = session.add(task)
            >>> len(session)
            1

        Duplicates are silently ignored: the same object can be added
        many times to the session, but gets the same ID each time::

            >>> # add a different task
            >>> tid2 = session.add(task)
            >>> len(session)
            1
            >>> tid1 == tid2
            True

            >>> # do cleanup
            >>> session.destroy()
            >>> os.path.exists(session.path)
            False

        """
        newid = self.store.save(task)
        self.tasks[newid] = task
        if flush:
            self.flush()
        return newid

    def forget(self, task_id, flush=True):
        """
        Remove task identified by `task_id` from the current session
        *but not* from the associated storage.
        """
        if task_id not in self.tasks:
            raise gc3libs.exceptions.InvalidArgument(
                "Task '%s' not found in session" % task_id)
        self.tasks.pop(task_id)
        if flush:
            self.flush()

    def _recursive_remove_from_store(self, task_id):
        """
        Remove a task from the store and, if the object has a `tasks`
        attribute containing a list of other tasks, remove them from the store
        """
        queue = [task_id]
        while queue:
            toremove = queue.pop()
            obj = self.store.load(toremove)
            try:
                for child in obj.tasks:
                    queue.append(child.persistent_id)
            except AttributeError:
                pass
            try:
                self.store.remove(toremove)
            except Exception as ex:
                gc3libs.log.warning(
                    "Error removing task id `%s` from the store:"
                    " %s" % ex)

    def remove(self, task_id, flush=True):
        """
        Remove task identified by `task_id` from the current session
        *and* from the associated storage.
        """
        self._recursive_remove_from_store(task_id)
        self.forget(task_id, flush)

    def __len__(self):
        return len(self.tasks)

    def __iter__(self):
        return self.tasks.itervalues()

    def iter_workflow(self):
        task_collections = filter(lambda x: isinstance(x, TaskCollection),
                                  self.tasks.values())
        proper_tasks = set(self.tasks.values()).difference(task_collections)
        return itertools.chain(
            *([proper_tasks]
              + map(lambda x: x.iter_workflow(), task_collections)))

    def list_ids(self):
        """
        Return set of all task IDs belonging to this session.
        """
        return self.tasks.keys()

    def list_names(self):
        """
        Return set of names of tasks belonging to this session.
        """
        return set(task.jobname for task in self.tasks.values())

    # persistence management

    def flush(self):
        """
        Update session metadata.

        Should be used after a save/remove operations, to ensure that
        the session state and metadata is correctly persisted.
        """
        # create directory if it does not exists
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        # Update store.url and job_ids.db files
        self._save_store_url_file()
        self._save_index_file()

    def load(self, obj_id):
        """
        Load an object from persistent storage and return it.

        This is just a convenience proxy for calling method `load` on
        the `Store` instance associated with this session.
        """
        return self.store.load(obj_id)

    def save(self, obj):
        """
        Save an object to the persistent storage and return
        `persistent_id` of the saved object.

        This is just a convenience proxy for calling method `save` on
        the `Store` instance associated with this session.

        The object is *not* added to the session, nor is session
        meta-data updated::

            # create an empty session
            >>> import tempfile; tmpdir = tempfile.mktemp(dir='.')
            >>> session = Session(tmpdir)
            >>> 0 == len(session)
            True

            # use `save` on an object
            >>> obj = gc3libs.persistence.Persistable()
            >>> oid = session.save(obj)

            # session is still empty
            >>> 0 == len(session)
            True

            # do cleanup
            >>> session.destroy()
            >>> os.path.exists(session.path)
            False
        """
        return self.store.save(obj)

    def save_all(self, flush=True):
        """
        Save all modified tasks to persistent storage.
        """
        for task in self.tasks.itervalues():
            if task.changed:
                self.save(task)
        if flush:
            self.flush()

    def _save_index_file(self):
        """
        Save job IDs to the default session index.
        """
        idx_filename = os.path.join(self.path, self.INDEX_FILENAME)
        try:
            idx_fd = open(idx_filename, 'w')
            for task_id in self.tasks:
                idx_fd.write(str(task_id))
                idx_fd.write('\n')
            idx_fd.close()
        except:
            idx_fd.close()
            raise

    def _save_store_url_file(self):
        """
        Save the storage URL to a session file.

        If the destination file does not exists, it will
        be created; if it exists, it will be overwritten.
        """
        store_url_filename = os.path.join(self.path, self.STORE_URL_FILENAME)
        gc3libs.utils.write_contents(store_url_filename, str(self.store_url))

    def _touch_file(self, filename, time=None):
        """
        Touch a file which is inside the session directory, updating
        modification and access time to `time` and creating the file
        if it does not exists.

        If `time` is None, the current time will be used.

        It returns the value used for `time`
        """
        filename = os.path.join(self.path, filename)
        open(filename, 'a').close()
        if time is None:
            os.utime(filename, None)
        else:
            os.utime(filename, (time, time))
        return os.stat(filename).st_mtime

    def set_start_timestamp(self, time=None):
        """
        Create a file named `created` in the session directory. It's
        creation/modification time will be used to know when the
        session has sarted.
        """
        self.created = self._touch_file(self.TIMESTAMP_FILES['start'], time)

    def set_end_timestamp(self, time=None):
        """
        Create a file named `finished` in the session directory. It's
        creation/modification time will be used to know when the
        session has finished.

        Please note that `Session` does not know when a session is
        finished, so this method should be called by a
        `SessionBasedScript`:class: class.
        """
        self.finished = self._touch_file(self.TIMESTAMP_FILES['end'], time)


# main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="filesystem",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
