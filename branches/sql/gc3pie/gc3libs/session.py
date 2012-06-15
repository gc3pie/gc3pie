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

import gc3libs.persistence

import cPickle as Pickle


class Session(object):
    """
    A session is made of a list of persistent jobs and a directory in
    which their output is stored when retrived.

    A `Session` will allow you to recover the persistency store.

    A `Session` will always create a directory named after its name in
    which everything related to that session will be stored.

    A Session's directory will always contain a file named
    ``store.url`` containing the URL to the persistence used for the
    store.

    By default, the persistency used is a FilesystemStore pointing to
    the ``SESSIONDIR/jobs`` directory.

    A session will know which jobs belong to the session.

    When dealing with persistency, you should use the `load()` and
    `save()` methods of the session instead of getting the store and
    using it, since otherwise the session will never know which jobs
    belong to this session.

    When calling Session.save() the job will be added to the jobs of
    this session _and_ saved to the store.
    """

    STORE_URL_FILENAME = "store.url"

    def __init__(self, path, store_url=None, output_dir=None):
        """
        The `path` argument is the path to the session directory. It
        will usually be just a name and thus will be considered as a
        relative path.

        The `store_url` argument is the url of the store.

        the `output_dir` argument is the directory in which the store
        will save the output of the jobs.

        It will cleaned by the `remove_session()`:meth: method only if
        it's inside the session.
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

    def load_session(self):
        """
        Load the session from the disk.
        """
        fd = open(os.path.join(self.path, Session.STORE_URL_FILENAME))
        self.store_url = fd.readline()
        fd.close()

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
        Save the current session to disk.

        * check that the store url file is correct, if not, overwrite
          it!

        * save jobs to the store
        """
        # create directory if it does not exists
        if not os.path.exists(self.path):
            os.mkdir(self.path)

        self.store = gc3libs.persistence.make_store(self.store_url,
                                idfactory=gc3libs.persistence.JobIdFactory())

        fd_job_ids = open(os.path.join(self.path, 'job_ids.db'), 'w')
        Pickle.dump(self.job_ids, fd_job_ids)
        fd_job_ids.close()

    def load(self, persistent_id):
        """Load an object from the persistency store
        """
        return self.store.load(persistent_id)

    def save(self, obj):
        """
        Save an object to the persistency store and add it to the
        list of jobs in the current session
        """
        newid = self.store.save(obj)
        if newid not in self.job_ids:
            self.job_ids.append(newid)
        return newid

    def list(self):
        """
        Return the list of all Job IDs belonging to this session
        """
        return self.job_ids

    def load_all(self):
        """Load all jobs belonging to the session from the persistency
        store and returns them as a list
        """
        jobs = []
        for jobid in self.job_ids:
            jobs.append(self.load(jobid))
        return jobs

    def remove_session(self):
        """
        Remove all data related to that session.

        Remove also the jobs from the store.
        """
        for jobid in self.job_ids:
            self.store.remove(jobid)
        shutil.rmtree(self.path)

    def __update_store_url_file(self):
        """
        Write the store url file. If the file does not exists it will
        be created. If it exists it will be overwritten.
        """
        fd = open(os.path.join(
            self.path,
            Session.STORE_URL_FILENAME), 'w')
        fd.write(self.store_url)
        fd.close()
