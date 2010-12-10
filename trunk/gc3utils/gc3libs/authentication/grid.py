#! /usr/bin/env python
#
"""
Authentication support with Grid proxy certificates.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
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

import sys
import shlex
import getpass
import os
import subprocess

# NG's default packages install arclib into /opt/nordugrid/lib/pythonX.Y/site-packages;
# add this anyway in case users did not set their PYTHONPATH correctly
import sys
sys.path.append('/opt/nordugrid/lib/python%d.%d/site-packages' 
                % sys.version_info[:2])
import arclib

import gc3libs
from gc3libs.authentication import Auth
from gc3libs.Exceptions import ConfigurationError, RecoverableAuthError, UnrecoverableAuthError

class GridAuth(object):

    def __init__(self, **auth):

        # test validity
        assert auth['type'] == 'voms-proxy' or auth['type'] == 'grid-proxy',\
            "Configuration error. Unknown type: %s. Valid types: [voms-proxy, grid-proxy]" \
            % auth.type
        assert auth['usercert'] == 'manual' or auth['usercert'] == 'slcs',  \
            "Configuration error. Unknown usercert: %s. Valid types: [voms-proxy, grid-proxy]" \
            % auth.usercert

        self.user_cert_valid = False
        self.proxy_valid = False
        self.__dict__.update(auth)


    def check(self):
        gc3libs.log.debug('Checking auth: grid')

        self.user_cert_valid = _user_certificate_is_valid()
        self.proxy_valid = _voms_proxy_is_valid()
 
        return ( self.user_cert_valid and self.proxy_valid )
    
    def enable(self):
        # Obtain username. Depends on type + usercert combination.
        if self.usercert == 'slcs':
            # Check if aai_username is already set. If not, ask interactively
            try:
                self.aai_username
            except AttributeError:
                self.aai_username = raw_input('Insert AAI username: ')

            # Check if idp is already set.  If not, ask interactively
            try:
                self.idp
            except AttributeError:
                self.idp = raw_input('Insert AAI identity provider (for UZH use uzh.ch): ')

        # Check information for grid/voms proxy
        if self.type == 'voms-proxy':
            # Check if vo is already set.  If not, ask interactively
            try:
                self.vo
            except AttributeError:
                self.vo = raw_input('Insert VO name: ' )

            # UserName set, go interactive asking password
            if self.usercert == 'slcs':
                message = 'Insert AAI password for user '+self.aai_username+': '
            else:
                if self.type == 'voms-proxy':
                    message = 'Insert voms proxy password: '
                else:
                    message = 'Insert grid proxy password: '

            input_passwd = getpass.getpass(message)
            
            # Start renewing credential
            new_cert = False

            # User certificate
            if not self.user_cert_valid:
                if self.usercert == 'manual':
                    raise UnrecoverableAuthError('User credential expired')

                _cmd = shlex.split("slcs-init --idp %s -u %s -p %s -k %s" 
                                   % (self.idp, self.aai_username, input_passwd, input_passwd))
                gc3libs.log.debug("Executing slcs-init --idp %s -u %s" 
                                  % (self.idp, self.aai_username))

                try:
                    p = subprocess.Popen(_cmd,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    (stdout, stderr) = p.communicate()
                    if p.returncode != 0:
                        # assume transient error (i.e wrong password or so)
                        gc3libs.log.error("RecoverableAuthError: %s" % stdout)
                        raise RecoverableAuthError(stdout) 
                except (OSError) as x:
                    gc3libs.log.error("UnrecoverableAuthError: %s" % str(x))
                    raise UnrecoverableAuthError(str(x))

                new_cert = True
                gc3libs.log.info('Create new SLCS certificate [ ok ].')


            # renew proxy if cert has changed or proxy expired
            if new_cert or not self.proxy_valid:
                if self.type == 'voms-proxy':
                    # Try renew voms credential; another interactive command
                    gc3libs.log.debug("No valid proxy found; trying to get "
                                      " a new one by 'voms-proxy-init' ...")
                    _cmd = shlex.split("voms-proxy-init -valid 24:00 -voms "
                                       "%s -q -pwstdin" % self.vo)

                elif self.type == 'grid-proxy':
                    # Try renew grid credential; another interactive command
                    gc3libs.log.debug("No valid proxy found; trying to get "
                                      "a new one by 'grid-proxy-init' ...")
                    _cmd = shlex.split("grid-proxy-init -valid 24:00 -q -pwstdin")

                try:

                    _echo_cmd = shlex.split("echo %s" % input_passwd)
                    p1 = subprocess.Popen(_echo_cmd, stdout=subprocess.PIPE)
                    p2 = subprocess.Popen(_cmd,
                                          stdin=p1.stdout,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT)
                    (stdout, stderr) = p2.communicate()

                    # dispose content of password
                    input_passwd = None

                    if p2.returncode != 0:
                        gc3libs.log.error("voms-proxy-init returned exitcode %d. Message: %s."
                                          % (p2.returncode, stdout))

                except ValueError as x:
                    # is this more a programming error ?
                    gc3libs.log.error("TO_BE_CONFIRMED: UnrecoverableAuthError: %s" % str(x))
                    raise RecoverableAuthError(str(x))
                except OSError as x:
                    gc3libs.log.error("UnrecoverableAuthError: %s" % str(x))
                    raise UnrecoverableAuthError(str(x))
                
            if not self.check():
                raise RecoverableAuthError("Temporary failure in auth 'grid' enabling. "
                                           "grid/voms proxy status: [%s]. "
                                           "user certificate status: [%s]" 
                                           % (self.proxy_valid, self.user_cert_valid))
            
            return True

def _voms_proxy_is_valid():
    try:
        c = arclib.Certificate(arclib.PROXY)
        return not c.IsExpired()
    except:
        return False


def _user_certificate_is_valid():
    try:
        c = arclib.Certificate(arclib.USERCERT)
        return not c.IsExpired()
    except:
        return False


Auth.register('grid-proxy', GridAuth)
Auth.register('voms-proxy', GridAuth)



## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="arc",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
