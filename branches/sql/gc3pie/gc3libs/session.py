#! /usr/bin/env python
#
"""
"""
# Copyright (C) 2012, GC3, University of Zurich. All rights reserved.
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


# stdlib imports
import os
import shutil
import cPickle as pickle

# GC3Pie imports
import gc3libs
import gc3libs.persistence
import gc3libs.utils


class Session(object):
    """
    A 'session' is a set of jobs which are stored in a given
    persistent store. Each session know wich jobs belong to it, so you
    can share the same store among multiple independent sessions.

    A session is associated to a directory, which holds all the data
    releated to that session. Specifically, two files are always
    created in the session directory andused internally by this class:

    * `job_ids.db`: contains a list (in Python `pickle` format) of all job IDs associated with this session;
    * `store.url`:  its contents are the URL of the store.

    To only argument needed to instantiate a session is the `path` of
    the directory; the directory name which will be used as the
    identifier of the session itself.  For example, the following code
    creates a directory named `tmpfname` and inside it the two files
    mentioned before::

        >>> import tempfile
        >>> tmpfname = tempfile.mktemp(dir='.')
        >>> session = Session(tmpfname)
        >>> sorted(os.listdir(tmpfname))
        ['job_ids.pickle', 'store.url']

    To load, save or replace objects in the store you should use the
    methods defined in `Session`, which work like the equivalent
    methods of the `Store`:class: class::

        >>> obj = gc3libs.persistence.Persistable()
        >>> id_ = session.save(obj)
        >>> obj2 = session.load(id_)
        >>> obj.persistent_id == obj2.persistent_id
        True

    The `Store`:class: object is accessible in the `store`:attr:
    attribute of each `Session` instance.

    When a `Session` object is created with a `path` argument pointing
    to an existing valid session, the index of jobs is automatically
    loaded into memory.

    If no `store_url` argument is passed, a `Session` object will
    instantiate and use a `FileSystemStore`:class: store, keeping data
    in the ``jobs`` subdirectory of the session directory.

    """

    JOBIDS_DB = 'job_ids.pickle'
    STORE_URL_FILENAME = "store.url"

    def __init__(self, path, store_url=None, output_dir=None, **kw):
        """
        First argument `path` is the path to the session directory.

        The `store_url` argument is the URL of the store, as would be
        passed to function
        `gc3libs.persistence.store.make_store`:func:; any additional
        keyword argument are passed to `maek_store` unchanged.

        .. warning::

          If the session directory already exists and contains a valid
          ``store.url`` file, the `store_url` argument (and any
          keyword arguments) will be *ignored.*

        By default the
        `gc3libs.persistence.filesystem.FileSystemStore`:class: (which
        see) is used for providing the session with a store.

        The `output_dir` argument is the directory in which the store
        will save the output of the jobs. **FIXME:** not yet implemented
        """
        self.path = os.path.abspath(path)
        self.job_ids = []
        if os.path.isdir(self.path):
            # Session already exists?
            try:
                self._load_session()
            except IOError, err:
                gc3libs.log.debug("Cannot load session '%s': %s", path, str(err))
                if err.errno == 2: # "No such file or directory"
                    gc3libs.log.debug("Assuming session is incomplete or corrupted, creating it again.")
                    self._create_session(path, store_url, **kw)
                else:
                    raise
        else:
            self._create_session(path, store_url, **kw)

    def _create_session(self, path, store_url, **kw):
        self.path = path
        # Must create its directory before `make_store` is called,
        # or SQLite raises an "OperationalError: unable to open
        # database file None None"
        gc3libs.utils.mkdir(path)
        if not store_url:
            store_url = os.path.join(self.path, 'jobs')
        self.store_url = store_url
        self.store = gc3libs.persistence.make_store(store_url, **kw)
        #self.output_dir = output_dir
        self._update_store_url_file()
        self._update_job_ids_file()

    def _load_session(self, **kw):
        """
        Load an existing session from disk.

        Keyword arguments are passed to the `make_store` factory
        method unchanged.
        """
        try:
            store_fname = os.path.join(self.path, self.STORE_URL_FILENAME)
            self.store_url = gc3libs.utils.read_contents(store_fname)
        except IOError:
            gc3libs.log.error(
                "Unable to load session: file %s is missing." % (store_fname))
            raise
        self.store = gc3libs.persistence.make_store(self.store_url, **kw)
        jobid_filename = os.path.join(self.path, self.JOBIDS_DB)
        if os.path.isfile(jobid_filename):
            fd_job_ids = open(jobid_filename, 'r')
            try:
                job_ids = pickle.load(fd_job_ids)
                if job_ids:
                    self.job_ids = job_ids
            finally:
                fd_job_ids.close()

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
        self._update_store_url_file()
        self._update_job_ids_file()

        jobids_filename = os.path.join(self.path, self.JOBIDS_DB)
        gc3libs.utils.write_contents(jobids_filename, pickle.dumps(self.job_ids, pickle.HIGHEST_PROTOCOL))

    def load(self, jobid):
        """
        Load the object identified by `persistent_id` from the
        persistent store and return it.
        """
        if jobid not in self.job_ids:
            raise gc3libs.exceptions.LoadError(
                "Unable to find any object with ID '%s'" % jobid)

        return self.store.load(jobid)

    def save(self, obj):
        """
        Save an object to the persistent storage and add it to the
        list of jobs in the current session.  Return the
        `persistent_id` of the saved object.
        """
        newid = self.store.save(obj)
        if newid not in self.job_ids:
            self.job_ids.append(newid)
        # Save the list of current jobs to disk, to avoid inconsistency
        self.flush()
        return newid

    def remove(self, jobid):
        """
        Remove job identified by `jobid` from the current session
        *and* from the storage.
        """
        if jobid not in self.job_ids:
            raise InvalidArgument(
                "Job id %s not found in current session" % jobid)
        self.store.remove(jobid)
        self.job_ids.remove(jobid)
        # Save the list of current jobs to disk, to avoid inconsistency
        self.flush()

    def list(self):
        """
        Return a list of all Job IDs belonging to this session.
        """
        return self.job_ids

    def load_all(self):
        """
        Load all jobs belonging to the session from the persistent
        storage and returns them as a list.
        """
        return [ self.load(jobid) for jobid in self.job_ids ]

    def remove_session(self):
        """
        Remove the session directory and remove also all the jobs from
        the store which are associated to this session.
        """
        for jobid in self.job_ids:
            self.store.remove(jobid)
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def _update_store_url_file(self):
        """
        Update the store url file. If the file does not exists it will
        be created. If it exists it will be overwritten.
        """
        store_url_filename = os.path.join(self.path, self.STORE_URL_FILENAME)
        gc3libs.utils.write_contents(store_url_filename, self.store_url)

    def _update_job_ids_file(self):
        """
        Update the job ids files, in order to avoid inconsistencies.
        """
        jobids_file = os.path.join(self.path, self.JOBIDS_DB)
        gc3libs.utils.write_contents(jobids_file, pickle.dumps(self.job_ids, pickle.HIGHEST_PROTOCOL))
