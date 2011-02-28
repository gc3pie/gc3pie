#! /usr/bin/env python
#
"""
Authentication support with Grid proxy certificates.
"""
# Copyright (C) 2009-2011 GC3, University of Zurich. All rights reserved.
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
__version__ = '1.0rc2 (SVN $Revision$)'

import sys
import shlex
import getpass
import os
import subprocess
import errno
import time


import gc3libs
from gc3libs.authentication import Auth
import gc3libs.exceptions
import gc3libs.utils

class GridAuth(object):

    def __init__(self, **auth):

        try:
            # test validity
            assert auth['type'] == 'voms-proxy' or auth['type'] == 'grid-proxy',\
                "Configuration error: Unknown type: %s. Valid types: [voms-proxy, grid-proxy]" \
                % auth.type
            assert auth['cert_renewal_method'] == 'manual' or auth['cert_renewal_method'] == 'slcs',  \
                "Configuration error: Unknown cert_renewal_method: %s. Valid types: [voms-proxy, grid-proxy]" \
                % auth.cert_renewal_method
            
            self.user_cert_valid = False
            self.proxy_valid = False
            self.__dict__.update(auth)
            self.validity_timestamp = 0 # Default init value for auth enalbed validity. 
        except AssertionError, x:
            raise gc3libs.exceptions.ConfigurationError('Erroneous configuration parameter: %s' % str(x))


    def check(self):
        gc3libs.log.debug('Checking auth: grid')

        if (self.validity_timestamp - int(time.time())) > gc3libs.Default.PROXY_VALIDITY_THRESHOLD:
            gc3libs.log.debug('Grid credentials still within validity period. Will not actually check.')
            return True

        self.user_cert_valid = _user_certificate_is_valid()
        self.proxy_valid = _voms_proxy_is_valid()
        self.validity_timestamp = _get_proxy_expires_time()

        return ( self.user_cert_valid and self.proxy_valid )

    
    def enable(self):
        # Obtain username. Depends on type + cert_renewal_method combination.
        if self.cert_renewal_method == 'slcs':
            # Check if aai_username is already set. If not, ask interactively
            try:
                self.aai_username
            except AttributeError:
                self.aai_username = raw_input('Insert AAI username: ')

            # Check if idp is already set.  If not, ask interactively
            try:
                self.idp
            except AttributeError:
                self.idp = raw_input('Insert AAI identity provider (use command `slcs-info` to list them): ')

        # Check information for grid/voms proxy
        if self.type == 'voms-proxy':
            # Check if vo is already set.  If not, ask interactively
            try:
                self.vo
            except AttributeError:
                self.vo = raw_input('Insert VO name: ' )

            # UserName set, go interactive asking password
            if self.cert_renewal_method == 'slcs':
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
                if self.cert_renewal_method == 'manual':
                    raise gc3libs.exceptions.UnrecoverableAuthError("User certificate expired, please renew it.")

                _cmd = shlex.split("slcs-init --idp %s -u %s -p %s -k %s" 
                                   % (self.idp, self.aai_username, input_passwd, input_passwd))
                gc3libs.log.debug("Executing slcs-init --idp %s -u %s" 
                                  % (self.idp, self.aai_username))

                try:
                    p = subprocess.Popen(_cmd,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    (stdout, stderr) = p.communicate()
                    if p.returncode != 0:
                        # assume transient error (i.e wrong password or so)
                        raise gc3libs.exceptions.RecoverableAuthError("Error running '%s': %s."
                                                   " Assuming temporary failure, will retry later." 
                                                   % (_cmd, stdout)) 
                except OSError, x:
                    if x.errno == errno.ENOENT or x.errno == errno.EPERM \
                            or x.errno == errno.EACCES:
                        raise gc3libs.exceptions.UnrecoverableAuthError("Failed running '%s': %s."
                                                     " Please verify that the command 'slcs-init' is"
                                                     " available on your $PATH and that it actually works."
                                                     % (_cmd, str(x)))
                    else:
                        raise gc3libs.exceptions.UnrecoverableAuthError("Failed running '%s': %s."
                                                     % (_cmd, str(x)))
                except Exception, ex:
                    # Intercept any other Error that subprocess may raise
                    gc3libs.log.debug("Unexpected error in GridAuth: %s: %s" 
                                      % (ex.__class__.__name__, ex.message))
                    raise gc3libs.exceptions.UnrecoverableAuthError("Error renewing SLCS certificate: %s" 
                                                 % str(ex))

                new_cert = True
                gc3libs.log.info('Created new SLCS certificate.')


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
                        gc3libs.log.warning("Command 'voms-proxy-init' exited with code %d: %s."
                                            % (p2.returncode, stdout))

                except ValueError, x:
                    # FIXME: is this more a programming error ?
                    raise gc3libs.exceptions.RecoverableAuthError(str(x))
                except OSError, x:
                    if x.errno == errno.ENOENT or x.errno == errno.EPERM or x.errno == errno.EACCES:
                        if self.type == 'grid-proxy':
                            cmd = 'grid-proxy-init'
                        elif self.type == 'voms-proxy':
                            cmd = 'voms-proxy-init'
                        else:
                            # should not happen!
                            raise AssertionError("Unknown auth type '%s'" % self.type)
                        raise gc3libs.exceptions.UnrecoverableAuthError("Failed running '%s': %s."
                                                     " Please verify that this command is available in"
                                                     " your $PATH and that it works."
                                                     % (cmd, str(x)))
                    else:
                        raise gc3libs.exceptions.UnrecoverableAuthError("Unrecoverable error in enabling"
                                                     " Grid authentication: %s" % str(x))
                except Exception, ex:
                    # Intercept any other Error that subprocess may raise 
                    gc3libs.log.debug("Unhandled error in GridAuth: %s: %s" 
                                      % (ex.__class__.__name__, ex.message))
                    raise gc3libs.exceptions.UnrecoverableAuthError(str(ex.message))
                
            if not self.check():
                raise gc3libs.exceptions.RecoverableAuthError("Temporary failure in enabling Grid authentication."
                                           " Grid/VOMS proxy status: %s."
                                           " user certificate status: %s" 
                                           % (gc3libs.utils.ifelse(self.proxy_valid,
                                                                   "valid", "invalid"),
                                              gc3libs.utils.ifelse(self.user_cert_valid,
                                                                   "valid", "invalid")))
            
            self.validity_timestamp = _get_proxy_expires_time()
            return True


# ARC's default packages install arclib into /opt/nordugrid/lib/pythonX.Y/site-packages; 
# add this anyway in case users did not set their PYTHONPATH correctly 
sys.path.append('/opt/nordugrid/lib/python%d.%d/site-packages'
                % sys.version_info[:2])

def _get_proxy_expires_time():
    import arclib

    try:
        c = arclib.Certificate(arclib.PROXY)
        return c.Expires().GetTime()
    except:
        return 0

def _voms_proxy_is_valid():
    import arclib

    try:
        c = arclib.Certificate(arclib.PROXY)
        return not c.IsExpired()
    except:
        return False


def _user_certificate_is_valid():
    import arclib

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
