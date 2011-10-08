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
__version__ = 'development version (SVN $Revision$)'

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

            # read `remember_password` setting; default to 'False'
            if auth.has_key('remember_password'):
                auth['remember_password'] = gc3libs.utils.string_to_boolean(auth['remember_password'])
            else:
                auth['remember_password'] = False
            
            self.user_cert_valid = False
            self.proxy_valid = False
            self._expiration_time = 0 # initially set expiration time way back in the past
            self._passwd = None
            self.__dict__.update(auth)

        except AssertionError, x:
            raise gc3libs.exceptions.ConfigurationError('Erroneous configuration parameter: %s' % str(x))


    def check(self):
        gc3libs.log.debug('Checking auth: GridAuth')

        remaining = int(self._expiration_time - time.time())
        if remaining > gc3libs.Default.PROXY_VALIDITY_THRESHOLD:
            gc3libs.log.debug(
                "Grid credentials assumed valid for another %d seconds,"
                " will not actually check.", remaining)
            return True

        self.user_cert_valid = (0 != _user_certificate_expiration_time())
        self._expiration_time = _proxy_expiration_time()
        self.proxy_valid = (0 != self._expiration_time)

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

            if self._passwd is None:
                self._passwd = getpass.getpass(message)
            
            # Start renewing credential
            new_cert = False

            # User certificate
            if not self.user_cert_valid:
                if self.cert_renewal_method == 'manual':
                    raise gc3libs.exceptions.UnrecoverableAuthError("User certificate expired, please renew it.")

                _cmd = shlex.split("slcs-init --idp %s -u %s -p %s -k %s" 
                                   % (self.idp, self.aai_username,
                                      self._passwd, self._passwd))
                gc3libs.log.debug("Executing slcs-init --idp %s -u %s"
                                  " -p ****** -k ******"
                                  % (self.idp, self.aai_username))

                try:
                    p = subprocess.Popen(_cmd,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    (stdout, stderr) = p.communicate()

                    if p.returncode != 0:
                        # Assume transient error (i.e wrong password or so).
                        raise gc3libs.exceptions.RecoverableAuthError(
                            "Error running slcs-init: %s."
                            " Assuming temporary failure, will retry later." 
                            % stdout) 
                # Note: to avoid printing the user's password in plaintext, we do not print the whole command in the error.
                except OSError, x:
                    if x.errno == errno.ENOENT or x.errno == errno.EPERM \
                            or x.errno == errno.EACCES:
                        raise gc3libs.exceptions.UnrecoverableAuthError(
                            "Failed running slcs-init: %s."
                            " Please verify that it is available on your $PATH and that it actually works."
                            % str(x))
                    else:
                        raise gc3libs.exceptions.UnrecoverableAuthError(
                            "Failed running slcs-init: %s." % str(x))
                except Exception, ex:
                    # Intercept any other Error that subprocess may raise
                    gc3libs.log.debug("Unexpected error in GridAuth: %s: %s" 
                                      % (ex.__class__.__name__, str(ex)))
                    raise gc3libs.exceptions.UnrecoverableAuthError(
                        "Error renewing SLCS certificate: %s" % str(ex))

                new_cert = True
                gc3libs.log.info('Created new SLCS certificate.')


            # renew proxy if cert has changed or proxy expired
            if new_cert or not self.proxy_valid:
                if self.type == 'voms-proxy':
                    # Try renew voms credential; another interactive command
                    gc3libs.log.debug("No valid proxy found; trying to get "
                                      " a new one by 'voms-proxy-init' ...")
                    _cmd = shlex.split("voms-proxy-init -valid 24:00 -rfc"
                                       " -q -pwstdin -voms %s" % self.vo)

                elif self.type == 'grid-proxy':
                    # Try renew grid credential; another interactive command
                    gc3libs.log.debug("No valid proxy found; trying to get "
                                      "a new one by 'grid-proxy-init' ...")
                    _cmd = shlex.split("grid-proxy-init -valid 24:00 -q -pwstdin")

                try:
                    p1 = subprocess.Popen(_cmd,
                                          stdin=subprocess.PIPE,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT)
                    (stdout, stderr) = p1.communicate("%s\n" % self._passwd)

                    # dispose content of password
                    if not self.remember_password:
                        self._passwd = None

                    if p1.returncode != 0:
                        gc3libs.log.warning("Command 'voms-proxy-init' exited with code %d: %s."
                                            % (p1.returncode, stdout))

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
                                      % (ex.__class__.__name__, str(ex)))
                    raise gc3libs.exceptions.UnrecoverableAuthError(str(ex))
                
            if not self.check():
                raise gc3libs.exceptions.RecoverableAuthError("Temporary failure in enabling Grid authentication."
                                           " Grid/VOMS proxy status: %s."
                                           " user certificate status: %s" 
                                           % (gc3libs.utils.ifelse(self.proxy_valid,
                                                                   "valid", "invalid"),
                                              gc3libs.utils.ifelse(self.user_cert_valid,
                                                                   "valid", "invalid")))
            
            self._expiration_time = _proxy_expiration_time()
            return True


# ARC's default packages install arclib into /opt/nordugrid/lib/pythonX.Y/site-packages; 
# add this anyway in case users did not set their PYTHONPATH correctly 
sys.path.append('/opt/nordugrid/lib/python%d.%d/site-packages'
                % sys.version_info[:2])
# this is where arc0 libraries are installed from release 11.05
sys.path.append('/usr/lib/pymodules/python%d.%d/'
                % sys.version_info[:2])

def _proxy_expiration_time():
    """
    Return the proxy expiration time (as UNIX epoch), or 0 if the
    proxy is already expired or not available at all.
    """
    import arclib
    try:
        pxy = arclib.Certificate(arclib.PROXY)
        expires = pxy.Expires().GetTime()
        if expires < time.time():
            gc3libs.log.info("VOMS proxy '%s' is expired.", pxy.GetCertFilename())
            return 0
        else:
            gc3libs.log.info("VOMS proxy '%s' valid until %s.", pxy.GetCertFilename(),
                             time.strftime("%a, %d %b %Y %H:%M:%S (local time)",
                                           time.localtime(expires)))
            return expires
    except arclib.CertificateError, ex:
        gc3libs.log.debug("Checking Grid proxy expiration time: %s", str(ex))
        return 0


def _user_certificate_expiration_time():
    import arclib
    try:
        crt = arclib.Certificate(arclib.USERCERT)
        expires = crt.Expires().GetTime()
        if expires < time.time():
            gc3libs.log.info("Grid certificate '%s' is expired.", crt.GetCertFilename())
            return 0
        else:
            gc3libs.log.info("Grid certificate '%s' valid until %s.", crt.GetCertFilename(),
                             time.strftime("%a, %d %b %Y %H:%M:%S (local time)",
                                           time.localtime(expires)))
            return expires
    except arclib.CertificateError, ex:
        gc3libs.log.debug("Checking Grid certificate expiration time: %s", str(ex))
        return 0


Auth.register('grid-proxy', GridAuth)
Auth.register('voms-proxy', GridAuth)



## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="arc",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
