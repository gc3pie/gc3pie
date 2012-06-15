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

import os
import shutil

import gc3libs
import gc3libs.persistence

import cPickle as Pickle


class Session(object):
    """
    A Session is a set of jobs which are stored in a specific
    persistency store. Each session know wich jobs belong to it, so
    you can share the same persistency store among multiple
    indepentent sessions.

    A session is associated to a directory, which will holds
    everything which is releated to that session. Specifically, two
    files are used internally by the class and these are:

    `job_ids.db`: contains a list of all job ids associated with this
    session

    `store.url`: contains a string which is the url of the store. By
    default a `Session` will instantiate a `FileSystemStore`:class:
    store.

    To only argument needed to instantiate a session is the `path` of
    the directory, which will be the identifier of the session itself.

    >>> import tempfile
    >>> tmpfname = tempfile.mktemp(dir='.')
    >>> session = Session(tmpfname)

    This will create a directory named `tmpfname` and inside it the
    two files mentioned before.

    >>> sorted(os.listdir(tmpfname))
    ['job_ids.db', 'store.url']

    To load, save or replace objects from the store you should use the
    methods defined in `Session`, which work like the equivalent
    methods of the `Store`:class: class.

    >>> obj = gc3libs.persistence.Persistable()
    >>> id_ = session.save(obj)
    >>> obj2 = session.load(id_)
    >>> obj.persistent_id == obj2.persistent_id
    True

    but you can always access directly the `store`:attr: attribute:

    >>> session.store.replace(id_, obj2)

    Loading a previously created session is done using the
    `load_session()` method:

    >>> session2 = Session(tmpfname)
    >>> session2.load_session()

    while saving a session is done with

    >>> session.save_session()

    which will save also all the objects of the session to the
    persistency store.
    """

    STORE_URL_FILENAME = "store.url"

    def __init__(self, path, store_url=None, output_dir=None):
        """
        The only mandatory argument is `path`, which is the path to
        the session directory. It will usually be just a name and
        thus will be considered as a relative path.

        The `store_url` argument is the url of the store. By default
        the persistency store used is FileSystemStore which will
        points to the `jobs` subdirectory of the session
        directory. Please note, however, that if the session already
        exists and contains a valid ``store.url`` file, the store_url
        argument will be *ignored*.

        the `output_dir` argument is the directory in which the store
        will save the output of the jobs. FIXME: not yet implemented
        """
        self.path = os.path.abspath(path)
        self.job_ids = []
        if os.path.isdir(self.path):
            self.load_session()
        else:
            if not store_url:
                store_url = os.path.join(self.path, 'jobs')
            self.store_url = store_url
            self.output_dir = output_dir
            os.mkdir(self.path)
            self.store = gc3libs.persistence.make_store(self.store_url)
            self.__update_store_url_file()
            self.__update_job_ids_file()

    def load_session(self):
        """
        Load session from disk.

        This method will work also for new sessions, even if the
        associated directory does not exist yet, but only if the
        `store_url` option was given, otherwise `Session` will be
        unable to create a `Store`:class:
        """
        try:
            store_fname = os.path.join(self.path, Session.STORE_URL_FILENAME)
            fd = open(store_fname)
            self.store_url = fd.readline()
            fd.close()
        except IOError:
            if hasattr(self, 'store_url'):
                gc3libs.log.debug(
                    "Loading session: missing store url file `%s`."
                    "Continuing with string %s" % (
                        store_fname, self.store_url))
            else:
                raise gc3libs.exceptions.InvalidUsage(
                    "Unable to load session. File %s is missing." % (
                        store_fname))

        self.store = gc3libs.persistence.make_store(self.store_url)
        jobid_file = os.path.join(self.path, 'job_ids.db')
        if os.path.isfile(jobid_file):
            fd_job_ids = open(jobid_file, 'r')
            job_ids = Pickle.load(fd_job_ids)
            if job_ids:
                self.job_ids = job_ids
            fd_job_ids.close()

    def save_session(self):
        """
        Save current session to disk.
        """
        # create directory if it does not exists
        if not os.path.exists(self.path):
            os.mkdir(self.path)

        # Update store.url and job_ids.db files
        self.__update_store_url_file()
        self.__update_job_ids_file()

    def load(self, jobid):
        """
        Load the object identified by `jobid` from the persistency
        store and return it.
        """
        return self.store.load(jobid)

    def save(self, obj):
        """
        Save an object to the persistency store and add it to the
        list of jobs in the current session.
        """
        newid = self.store.save(obj)
        if newid not in self.job_ids:
            self.job_ids.append(newid)
        # Save the list of current jobs to disk, to avoid inconsistency
        self.save_session()
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
        self.save_session()

    def list(self):
        """
        Return a list of all Job IDs belonging to this session.
        """
        return self.job_ids

    def load_all(self):
        """Load all jobs belonging to the session from the persistency
        store and returns them as a list.
        """
        jobs = []
        for jobid in self.job_ids:
            jobs.append(self.load(jobid))
        return jobs

    def remove_session(self):
        """
        Remove the session directory and remove also all the jobs from
        the store which are associated to this session.
        """
        for jobid in self.job_ids:
            self.store.remove(jobid)
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def __update_store_url_file(self):
        """
        Update the store url file. If the file does not exists it will
        be created. If it exists it will be overwritten.
        """
        fd = open(os.path.join(
            self.path,
            Session.STORE_URL_FILENAME), 'w')
        fd.write(self.store_url)
        fd.close()

    def __update_job_ids_file(self):
        """
        Update the job ids files, in order to avoid inconsistencies.
        """
        fd_job_ids = open(os.path.join(self.path, 'job_ids.db'), 'w')
        Pickle.dump(self.job_ids, fd_job_ids)
        fd_job_ids.close()
