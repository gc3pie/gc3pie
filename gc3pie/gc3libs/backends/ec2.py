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
import paramiko
import time

# EC2 APIs
import boto
import boto.ec2.regioninfo

# GC3Pie imports
import gc3libs
import gc3libs.exceptions
import gc3libs.url
from gc3libs import Run
from gc3libs.utils import same_docstring_as
from gc3libs.backends import LRMS

class InstanceNotFound(Exception):
    pass

available_subresource_types = [gc3libs.Default.SHELLCMD_LRMS]

class EC2Lrms(LRMS):
    """
    EC2 resource.
    """
    def __init__(self, name,
                 # these parameters are inherited from the `LRMS` class
                 architecture, max_cores, max_cores_per_job,
                 max_memory_per_core, max_walltime, 
                 # these are specific of the EC2Lrms class
                 pool_size_max, pool_size_min, ec2_region,
                 keypair_name, public_key, image_id,
                 auth=None, **extra_args):
        LRMS.__init__(
            self, name,
            architecture, max_cores, max_cores_per_job,
            max_memory_per_core, max_walltime, auth, **extra_args)

        self.subresource_type = self.type.split('+', 1)[1]
        if self.subresource_type not in available_subresource_types:
            raise Exception("Invalid resource type: %s" % self.type)

        self.region = ec2_region

        self.pool_size_max = int(pool_size_max)
        if self.pool_size_max > 0:
            self.pool_size_max = int(self.pool_size_max)
        self.pool_size_min = int(pool_size_min)
        # Mapping of job.ec2_instance_id => LRMS
        self.resources = {}

        auth = self._auth_fn()
        self.ec2_access_key = auth.ec2_access_key
        self.ec2_secret_key = auth.ec2_secret_key
        self.ec2_url = gc3libs.url.Url(self.ec2_url)

        self.keypair_name = keypair_name
        self.public_key = public_key

        self.image_id = image_id

        region = boto.ec2.regioninfo.RegionInfo(name=ec2_region, endpoint=self.ec2_url.hostname)

        self._conn = boto.connect_ec2(aws_access_key_id=self.ec2_access_key,
                                      aws_secret_access_key=self.ec2_secret_key, 
                                      is_secure=False, 
                                      port=self.ec2_url.port,
                                      host=self.ec2_url.hostname,
                                      path=self.ec2_url.path, 
                                      region=region)

        # Check if the desired keypair is present
        keypairs = dict((k.name, k) for k in self._conn.get_all_key_pairs())
        if self.keypair_name not in keypairs:
            gc3libs.log.info("Keypair `%s` not found: creating it using public key `%s`" \
                                 % (self.keypair_name, self.public_key))
            # Create keypair if it does not exist and give an error if it 
            # exists but have different fingerprint
            self._setup_keypair()
        else:
            keyfile = os.path.expanduser(self.public_key)
            try:
                pkey = paramiko.DSSKey.from_private_key_file(keyfile[:-4])
            except:
                pkey = paramiko.RSAKey.from_private_key_file(keyfile[:-4])

            # Check key fingerprint
            localkey_fingerprint = ':'.join(i.encode('hex') for i in pkey.get_fingerprint())
            if localkey_fingerprint != keypairs[self.keypair_name].fingerprint:
                gc3libs.log.error("Keypair `%s` is present but has different fingerprint: "
                                  "%s != %s. Aborting!" % (self.keypair_name,
                                                           localkey_fingerprint,
                                                           keypairs[self.keypair_name].fingerprint))
                raise Exception("Keypair `%s` is present but has different fingerprint: "
                                "%s != %s. Aborting!" % (self.keypair_name,
                                                         localkey_fingerprint,
                                                         keypairs[self.keypair_name].fingerprint))

        # Optional parameters must be removed from `extra_args`
        # dictionary
        self.subresource_args = extra_args

    def _get_vm(self, vm_id):
        """
        Return the instance with id `vm_id`, if any. 
        """
        reservation = self._conn.get_all_instances(instance_ids=[vm_id])
        instances = dict((i.id, i) for i in reservation.instances)
        if vm_id not in instances:
            raise InstanceNotFound("Instance with id %s has not found in EC2 cloud %s" % (vm_id, self.auth.ec2_auth_url))
        return instances[vm_id]

    def _make_resource(self, remote_ip):
        """
        Create a resource associated to the instance with `remote_ip`
        ip using configuration file parameters.
        """
        args = self.subresource_args.copy()
        args['frontend'] = remote_ip
        cfg = gc3libs.config.Configuration()
        return cfg._make_resource(args)

    def _get_remote_resource(self, vm_id):
        """
        Return the resource associated to the virtual machine with `vm_id`.
        """
        if vm_id not in self.resources:
            vm = self._get_vm(vm_id)
            self.resources[vm_id] = self._make_resource(vm.public_dns_name)
        return self.resources[vm_id]

    @LRMS.authenticated    
    def _setup_keypair(self):
        """
        Create a new keypair using values found in the configuration file
        """
        fd = open(os.path.expanduser(self.public_key))
        try:
            key_material = fd.read()
            imported_key = self._conn.import_key_pair(self.keypair_name, key_material)

            gc3libs.log.info("Successfully imported key `%s` with fingerprint `%s`"
                             " as keypir `%s`" % (imported_key.name,
                                                  imported_key.fingerprint,
                                                  self.keypair_name))
        except:
            fd.close()

    @same_docstring_as(LRMS.cancel_job)
    def cancel_job(self, app):
        resource = self._get_remote_resource(app.ec2_instance_id)
        return resource.cancel_job(app.job_id)

    @same_docstring_as(LRMS.free)
    def free(self, app):
        """
        Free up any remote resources used for the execution of `app`.
        In particular, this should terminate any remote instance used by the job.

        Call this method when `app.execution.state` is anything other
        than `TERMINATED` results in undefined behavior and will
        likely be the cause of errors later on.  Be cautious.
        """
        vm = self._get_vm(app.ec2_instance_id)
        vm.terminate()
        resource = self.resources[app.ec2_instance_id]
        # XXX: this should probably done only when no other VMs are
        # using this resource.
        resource.free()

    @same_docstring_as(LRMS.get_resource_status)
    def get_resource_status(self):
        self.updated = False
        for resource in self.resources.values():
            resource.get_resource_status()
        return self

    @same_docstring_as(LRMS.get_results)
    def get_results(self, job, download_dir, overwrite=False):
        resource = self._get_remote_resource(job.ec2_instance_id)
        return resource.get_results(job, download_dir, overwrite=False)

    @same_docstring_as(LRMS.update_job_state)
    def update_job_state(self, app):
        if app.ec2_instance_id not in self.resources:
            self.resources[app.ec2_instance_id] = self._make_resource(app.ec2_instance_ip)
        
        return self.resources[app.ec2_instance_id].update_job_state(app)
        

    def _create_instance(self):
        """
        Create an instance.
        """
        args={'key_name'  : self.keypair_name,
              'min_count' : 1,
              'max_count' : 1}
        if 'instance_type' in self:
            args['instance_type'] = self.instance_type
        if 'user_data' in self:
            args['user_data'] = self.user_data

        # FIXME: we should add check/creation of proper security
        # groups

        reservation = connection.run_instances(self.image_id, **args)
        vm = reservation.instances[0]
        
        # wait until the instance is ready
        while vm.update() == 'pending':
            time.sleep(3)
        return vm

    @same_docstring_as(LRMS.submit_job)
    def submit_job(self, job):
        """
        Create an instance, create a resource related to that instance and then submit the job on that instance
        """
        # Create an instance
        # max_count and min_count are set in order to be sure that only one instance will be run
        vm = self._create_instance()

        job.ec2_instance_id = vm.id

        self.resources[vm.id] = self._make_resource(vm.public_dns_name)
        return self.resources[vm.id].submit_job(job)

    @same_docstring_as(LRMS.peek)
    def peek(self, app, remote_filename, local_file, offset=0, size=None):
        raise NotImplementedError("Abstract method `LRMS.peek()` called - this should have been defined in a derived class.")

    @same_docstring_as(LRMS.validate_data)
    def validate_data(self, data_file_list=None):
        raise NotImplementedError("Abstract method 'LRMS.validate_data()' called - this should have been defined in a derived class.")

    @same_docstring_as(LRMS.close)
    def close(self):
        for resource in self.resources.values():
            resource.close()
        

## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="ec2",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
