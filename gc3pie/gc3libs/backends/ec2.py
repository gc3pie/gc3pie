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


# GC3Pie imports
import gc3libs
import gc3libs.exceptions
from gc3libs import log, Run
from gc3libs.utils import same_docstring_as, samefile, copy_recursively, Struct
from gc3libs.backends import LRMS

class EC2Instance(object):
    """
    Represents an instance running on a remote host.
    """
    pass
    

class EC2Lrms(LRMS):
    """
    EC2 resource.
    """
    def __init__(self, name,
                 # these parameters are inherited from the `LRMS` class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime, 
                 # these are specific of the EC2Lrms class
                 pool_size_max, pool_size_min,
                 auth=None, **extra_args):
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth, **extra_args)

        
        self.pool_size_max = pool_size_max
        if self.pool_size_max is not None:
            self.pool_size_max = int(self.pool_size_max)
        self.pool_size_min = int(pool_size_min)
        # self.children is a dictionary of 'jobid' => 'resource'
        self.clouds = {}
        # We need to get resources from the configuration object,
        # which we don't have.
        self.remote_resource = None
        
    @same_docstring_as(LRMS.cancel_job)
    def cancel_job(self, app):
        cloud_resource = self._clouds[app.job_id]
        cloud_resource.cancel_job(app.job_id)

    @same_docstring_as(LRMS.free)
    def free(self, app):
        """
        Free up any remote resources used for the execution of `app`.
        In particular, this should delete any remote directories and
        files.

        Call this method when `app.execution.state` is anything other
        than `TERMINATED` results in undefined behavior and will
        likely be the cause of errors later on.  Be cautious.
        """
        cloud_resource = self._clouds[app.job_id]
        # XXX: this should probably done only when no other VMs are
        # using this resource.
        cloud_resource.free()
        vm = self._vms[app.job_id]
        if not vm:
            self._vms[app.job_id] = self._get_vm(app.job_id)
        vm.terminate()
        
        raise NotImplementedError("Abstract method `LRMS.free()` called - this should have been defined in a derived class.")

    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        raise NotImplementedError("Abstract method `LRMS.get_resource_status()` called - this should have been defined in a derived class.")

    @same_docstring_as(LRMS.get_results)
    def get_results(self, job, download_dir, overwrite=False):
        raise NotImplementedError("Abstract method `LRMS.get_results()` called - this should have been defined in a derived class.")

    @same_docstring_as(LRMS.update_job_state)
    def update_job_state(self, app):
        raise NotImplementedError("Abstract method `LRMS.update_state()` called - this should have been defined in a derived class.")

    @same_docstring_as(LRMS.submit_job)
    def submit_job(self, application, job):
        # Create an instance
        vm = connection.run_instances(images['Ubuntu 12.04 server amd64'].id, key_name=keypair) # instance_type='m1.large', user_data=''
        

        cloud_resource = self._create_resource()
        self._clouds[app.job_id]

        cloud_resource = self._clouds[app.job_id]
        cloud_resource.free()
        vm = self._vms[app.job_id]
        if not vm:
            self._vms[app.job_id] = self._get_vm(app.job_id)
        vm.terminate()

    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        raise NotImplementedError("Abstract method `LRMS.peek()` called - this should have been defined in a derived class.")

    @same_docstring_as(LRMS.validate_data)
    def validate_data(self, data_file_list=None):
        raise NotImplementedError("Abstract method 'LRMS.validate_data()' called - this should have been defined in a derived class.")

    @same_docstring_as(LRMS.close)
    def close(self):
        raise NotImplementedError("Abstract method 'LRMS.close()' called - this should have been defined in a derived class.")
        

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="ec2",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
