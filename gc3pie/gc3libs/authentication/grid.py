#! /usr/bin/env python
#
"""
Authentication support with Grid proxy certificates.
"""
# Copyright (C) 2009-2012 GC3, University of Zurich. All rights reserved.
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

import errno
import getpass
import os
import random
import string
import subprocess
import sys
import time


import gc3libs
from gc3libs.authentication import Auth
import gc3libs.exceptions
import gc3libs.utils
from gc3libs import Default

# ARC's default packages install arclib into /opt/nordugrid/lib/pythonX.Y/site-packages; 
# add this anyway in case users did not set their PYTHONPATH correctly 
sys.path.append('/opt/nordugrid/lib/python%d.%d/site-packages'
                % sys.version_info[:2])
# this is where arc0 libraries are installed from release 11.05
sys.path.append('/usr/lib/pymodules/python%d.%d/'
                % sys.version_info[:2])


## detect ARC version
arc_flavour = None
try:
    import arclib
    arc_flavour = Default.ARC0_LRMS
except ImportError:
    gc3libs.log.warning("Failed importing ARC0 libraries")
try:
    import arc
    arc_flavour = Default.ARC1_LRMS
except ImportError:
    gc3libs.log.warning("Failed importing ARC1 libraries")


## random password generator
_random_password_letters = string.ascii_letters + string.digits

def random_password(length=24):
  return str.join('', [random.choice(_random_password_letters) for _ in xrange(length)])


class GridAuth(object):

    def __init__(self, **auth):

        try:
            # test validity
            assert auth['type'] in ['voms-proxy', 'grid-proxy' ], (
                "Configuration error: Unknown type: %s."
                " Valid types are 'voms-proxy' or 'grid-proxy'"
                % auth['type'])
            assert auth['cert_renewal_method'] in ['manual', 'slcs'], (
                "Configuration error: Unknown cert_renewal_method: %s."
                " Valid types are 'voms-proxy' and 'grid-proxy'"
                % auth['cert_renewal_method'])

            # read `remember_password` setting; default to 'False'
            if 'remember_password' in auth:
                auth['remember_password'] = gc3libs.utils.string_to_boolean(auth['remember_password'])
            else:
                auth['remember_password'] = False

            # read `private_cert_copy` setting; default to 'False'
            if 'private_credentials_copy' in auth:
                auth['private_credentials_copy'] = gc3libs.utils.string_to_boolean(auth['private_credentials_copy'])
            else:
                auth['private_credentials_copy'] = False
            if auth['private_credentials_copy']:
                assert auth['cert_renewal_method'] == 'slcs', (
                    "Configuration error: 'private_credentials_copy'"
                    " can only be used with the 'slcs' certificate renewal.")
                if 'private_copy_directory' not in auth:
                    # reset `private_credentials_copy` as it's useless
                    # w/out the copy directory
                    gc3libs.log.warning(
                        "auth/%s: 'private_credentials_copy' is set,"
                        " but no value for 'private_directory_copy' was passed:"
                        " the setting is ineffective"
                        " and no private copy will be kept.", auth['name'])
                    auth['private_credentials_copy'] = False
                elif not os.path.exists(auth['private_copy_directory']):
                    raise gc3libs.exceptions.ConfigurationError(
                        "Incorrect setting '%s'"
                        " for 'private_copy_directory' in auth/%s:"
                        " directory does not exist."
                        % (auth['private_copy_directory'], auth['name']))
                elif not os.path.isdir(auth['private_copy_directory']):
                    raise gc3libs.exceptions.ConfigurationError(
                        "Incorrect setting '%s'"
                        " for 'private_copy_directory' in auth/%s:"
                        " path does not point to a directory."
                        % (auth['private_copy_directory'], auth['name']))

            self.user_cert_valid = False
            self.proxy_valid = False
            self._expiration_time = 0 # initially set expiration time way back in the past
            self._passwd = None
            self._keypass = None
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

        # WARNING: the following code might be counter-intuitive, but
        # it's what we need! When `private_credentials_copy` is in
        # effect, we have to force the *certificate* renewal in order
        # to store it in the private directory as a side-effect.  When
        # `remember_password` is in effect, we choose force the
        # *proxy* renewal in order to store the SWITCHaai password as
        # a side-effect. (We could renew the cert to the same purpose,
        # and that would guarantee 10 days of operations, but
        # `slcs-init` is still slower than `voms-proxy-init`...)

        if self.private_credentials_copy and self._keypass is None:
            # force cert renewal
            self.user_cert_valid = False
        else:
            self.user_cert_valid = (0 != self.get_end_time("usercert"))

        self._expiration_time = self.get_end_time("proxy")
        if self.remember_password and self._passwd is None:
            # force proxy renewal to store password
            self.proxy_valid = False
        else:
            self.proxy_valid = (0 != self._expiration_time)

        return (self.user_cert_valid and self.proxy_valid)

    
    def enable(self):
        # User certificate
        new_cert = False
        if not self.user_cert_valid:
            # Obtain username? Depends on type + cert_renewal_method combination.
            if self.cert_renewal_method == 'slcs':
                # Check if aai_username is already set. If not, ask interactively
                try:
                    self.aai_username
                except AttributeError:
                    self.aai_username = raw_input('Insert SWITCHaai username: ')

                # Check if idp is already set.  If not, ask interactively
                try:
                    self.idp
                except AttributeError:
                    self.idp = raw_input('Insert SWITCHaai Identity Provider (use the command `slcs-info` to list them): ')

            if self._passwd is not None:
                shib_passwd = self._passwd
            else:
                # ask passwd interactively
                shib_passwd = getpass.getpass('Insert SWITCHaai password for user %s:' % self.aai_username)

            if self.private_credentials_copy:
                key_passwd = random_password()
            else:
                key_passwd = shib_passwd

            new_cert = self.renew_cert(shib_passwd, key_passwd)
            # save passwds for later use
            if new_cert and self.remember_password:
                self._passwd = shib_passwd
            if new_cert and self.private_credentials_copy:
                self._keypass = key_passwd

        # renew proxy if cert has changed or proxy expired
        if new_cert or not self.proxy_valid:
            # have to renew proxy, check that we have all needed info and passwd
            if self.type == 'voms-proxy':
                # Check if vo is already set.  If not, ask interactively
                try:
                    self.vo
                except AttributeError:
                    self.vo = raw_input('Insert VO name: ' )

            if self._keypass is not None:
                # `private_credentials_copy` in effect, use the stored random passwd
                keypass = self._keypass
            elif self._passwd is not None:
                # `remember_password` in effect, use the stored SWITCHaai passwd
                keypass = self._passwd
            else:
                # ask interactively
                if self.type == 'voms-proxy':
                    message = 'Insert voms proxy password: '
                elif self.type == 'grid-proxy':
                    message = 'Insert grid proxy password: '
                keypass = getpass.getpass(message)

            self.renew_proxy(keypass)

        # check that all is OK
        if not self.check():
            raise gc3libs.exceptions.RecoverableAuthError(
                "Temporary failure in enabling Grid authentication."
                " Grid/VOMS proxy is %s."
                " User certificate is %s." 
                % (gc3libs.utils.ifelse(self.proxy_valid,
                                        "valid", "invalid"),
                   gc3libs.utils.ifelse(self.user_cert_valid,
                                        "valid", "invalid")))
        return True


    def renew_cert(self, shib_passwd, key_passwd):
        if self.cert_renewal_method == 'manual':
            raise gc3libs.exceptions.UnrecoverableAuthError(
                "User certificate expired and renewal set to 'manual', please renew it.")

        try:
            cmd = [
                'slcs-init',
                '--idp',      self.idp,
                '--user',     self.aai_username,
                ]
            if self.private_credentials_copy:
                cmd.extend(['--storedir', self.private_copy_directory])
            p = subprocess.Popen(cmd,
                                 stdin= subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
            (stdout, stderr) = p.communicate('%s\n%s\n' %
                                             (shib_passwd, key_passwd))
            if p.returncode != 0:
                # Assume transient error (i.e wrong password or so).
                raise gc3libs.exceptions.RecoverableAuthError(
                    "Error running '%s': %s."
                    " Assuming temporary failure, will retry later." 
                    % (str.join(' ', cmd), stdout))
        except OSError, x:
            if x.errno in [errno.ENOENT, errno.EPERM, errno.EACCES]:
                raise gc3libs.exceptions.UnrecoverableAuthError(
                    "Failed running '%s': %s."
                    " Please verify that it is available on your $PATH and that it actually works."
                    % (str.join(' ', cmd), str(x)))
            else:
                # other error; presume it's wrong password or a
                # network glitch.... so retry later.
                raise gc3libs.exceptions.RecoverableAuthError(
                    "Failed running '%s': %s." % (str.join(' ', cmd), str(x)))
        except Exception, ex:
            # Intercept any other Error that subprocess may raise
            gc3libs.log.debug("Unexpected error in GridAuth: %s: %s" 
                              % (ex.__class__.__name__, str(ex)))
            raise gc3libs.exceptions.UnrecoverableAuthError(
                "Error renewing SLCS certificate: %s" % str(ex))

        gc3libs.log.info('Created new SLCS certificate.')
        if self.private_credentials_copy:
            os.environ['X509_USER_CERT'] = os.path.join(self.private_copy_directory, 'usercert.pem')
            os.environ['X509_USER_KEY'] = os.path.join(self.private_copy_directory, 'userkey.pem')
        return True


    def renew_proxy(self, passwd, _arc_flavour=None):
        global arc_flavour # module-level param
        if _arc_flavour is None:
            _arc_flavour = arc_flavour

        if _arc_flavour == Default.ARC1_LRMS:
            gc3libs.log.debug(
                "Proxy support in ARC1 libraries is not yet functional. Falling back to ARC0 method.")
            return self.renew_proxy(passwd, Default.ARC0_LRMS)

        if self.type == 'voms-proxy':
            assert _arc_flavour == Default.ARC0_LRMS
            # first make sure existing proxy is properly removed
            # run voms-proxy-destroy. This guarantees that the renewal
            # takes the recorded self.password into account
            gc3libs.log.debug("Executing voms-proxy-destroy ...")
            try:
                p = subprocess.Popen('voms-proxy-destroy', stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                (stdout, stderr) = p.communicate()
            except Exception, x:
                gc3libs.log.error("Got the following error from 'voms-proxy-destroy',"
                                  " but I'm ignoring it:"
                                  " %s: %s" % (x.__class__,x.message))

            # Try renew voms credential; another interactive command
            gc3libs.log.debug("Trying to get a new proxy by 'voms-proxy-init' ...")
            cmd = ['voms-proxy-init', '-valid', '24:00', '-rfc', '-q', '-pwstdin']
            if self.vo is not None:
                cmd.extend(['-voms', self.vo ])

        elif self.type == 'grid-proxy':
            assert _arc_flavour == Default.ARC0_LRMS
            gc3libs.log.debug("Trying to get a new proxy by 'voms-proxy-init' ...")
            cmd = ['grid-proxy-init', '-valid', '24:00', '-q', '-pwstdin']

        if self.private_credentials_copy:
            # XXX: does `grid-proxy-init` support `-out`? do we care?
            cmd.extend(['-out', os.path.join(self.private_copy_directory, 'proxy.pem')])

        try:
            p1 = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            (stdout, stderr) = p1.communicate("%s\n" % passwd)

            if p1.returncode != 0:
                # Cannot use voms-proxy-init return code as validation of the command.
                # just report the warning
                gc3libs.log.warning("Command '%s' exited with code %d: %s."
                                    % (str.join(' ', cmd), p1.returncode, stdout))

            if self.private_credentials_copy:
                os.environ['X509_USER_PROXY'] = os.path.join(self.private_copy_directory, 'proxy.pem')

        except ValueError, x:
            # FIXME: is this more a programming error ?
            raise gc3libs.exceptions.RecoverableAuthError(str(x))

        except OSError, x:
            if x.errno in [errno.ENOENT, errno.EPERM, errno.EACCES]:
                raise gc3libs.exceptions.UnrecoverableAuthError(
                    "Failed running '%s': %s."
                    " Please verify that this command is available in"
                    " your $PATH and that it works."
                    % (str.join(' ', cmd), str(x)))
            else:
                raise gc3libs.exceptions.UnrecoverableAuthError(
                    "Unrecoverable error in enabling Grid authentication: %s" % str(x))

        except Exception, ex:
            # Intercept any other Error that subprocess may raise 
            gc3libs.log.debug("Unhandled error in GridAuth: %s: %s" 
                              % (ex.__class__.__name__, str(ex)))
            raise gc3libs.exceptions.UnrecoverableAuthError(str(ex))

        return True

    @staticmethod
    def get_end_time(cert_type):
        global arc_flavour # module-level constant

        if arc_flavour == Default.ARC0_LRMS:
            # use ARC libraries
            try:
                if cert_type == "proxy":
                    cert = arclib.Certificate(arclib.PROXY)
                elif cert_type == "usercert":
                    cert = arclib.Certificate(arclib.USERCERT)
                else:
                    raise UnrecoverableAuthError("Unsupported cert type '%s'" % cert_type)
                expires = cert.Expires().GetTime()
            except arclib.CertificateError:
                # ARClib cannot read/access cert, consider it expired
                # to force renewal
                return 0

        elif arc_flavour == Default.ARC1_LRMS:
            # use ARC1 libraries
            try:
                userconfig = arc.UserConfig()
                if cert_type == "proxy":
                    cert = arc.Credential(userconfig.ProxyPath(), "", "", "")
                elif cert_type == "usercert":
                    cert = arc.Credential(userconfig.CertificatePath(), "", "", "")
                else:
                    raise UnrecoverableAuthError("Unsupported cert type '%s'" % cert_type)
                expires = cert.GetEndTime().GetTime()
            except arclib.CredentialError:
                # ARClib cannot read/access cert, consider it expired
                # to force renewal
                return 0

        else:
            # XXX: should this be `AssertionError` instead? (it's a programming bug...)
            raise UnrecoverableAuthError("Wrong ARC flavour specified '%s'" % str(arc))

        if expires < time.time():
            gc3libs.log.info("%s is expired." % cert_type)
            return 0
        else:
            gc3libs.log.info("%s valid until %s.", cert_type,
                             time.strftime("%a, %d %b %Y %H:%M:%S (local time)",
                                           time.localtime(expires)))
            return expires


Auth.register('grid-proxy', GridAuth)
Auth.register('voms-proxy', GridAuth)


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="arc",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
