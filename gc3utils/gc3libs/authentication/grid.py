#! /usr/bin/env python
#
"""
Authentication support with Grid proxy certificates.
"""
# Copyright (C) 2009-2010 GC3, University of Zurich. All rights reserved.
#
# Includes parts adapted from the ``bzr`` code, which is
# copyright (C) 2005, 2006, 2007, 2008, 2009 Canonical Ltd
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


import arclib
from gc3libs.authentication import Auth
import gc3libs
import getpass
import os
import subprocess
import sys


class GridAuth(object):

    def __init__(self, **authorization):
        self.user_cert_valid = False
        self.proxy_valid = False
        self.__dict__.update(authorization)

    def is_valid(self):
        try:
            # Try required values
            self.type
            self.usercert
            
            return True
        except:
            return False

    def check(self):
        gc3libs.log.debug('Checking authentication: GRID')
        try:
            self.user_cert_valid = _user_certificate_is_valid()
            self.proxy_valid = _voms_proxy_is_valid()
        except Exception, x:
            gc3libs.log.error('Error checking GRID authentication: %s', str(x), exc_info=True)

        return ( self.user_cert_valid and self.proxy_valid )
     
    def enable(self):
        try:

            # Need to renew user certificate
            # if slcs-enabled, try slcs-init
            # otherwise trow an Exception
            if self.usercert == 'slcs':
                # Check if aai_username is already set. If not, ask interactively
                try:
                    self.aai_username
                except AttributeError:
                    # Go interactive
                    self.aai_username = raw_input('Insert AAI/Switch username for user '+getpass.getuser()+': ')
                    
                # Check if idp is already set.  If not, ask interactively
                try:
                    self.idp
                except AttributeError:
                    self.idp = raw_input('Insert AAI/Switch idp for user '+getpass.getuser()+': ')            

            # Check information for grid/voms proxy
            if self.type == 'voms-proxy':
                # Check if vo is already set.  If not, ask interactively
                try:
                    self.vo
                except AttributeError:
                    self.vo = raw_input('Insert VO name for user '+getpass.getuser()+': ')

            # UserName set, go interactive asking password
            if self.usercert == 'slcs':
                message = 'Insert AAI/Switch password for user  '+self.aai_username+' :'
            else:
                if self.type == 'voms-proxy':
                    message = 'Insert voms proxy password :'
                else:
                    # Assume grid-proxy
                    message = 'Insert grid proxy password :'

            input_passwd = getpass.getpass(message)
            
            # Start renewing credential
            new_cert = False

            # User certificate
            if not self.user_cert_valid:
                if not self.usercert == 'slcs':
                    raise Exception('User credential expired')

                gc3libs.log.debug('No valid certificate found; trying to get new one by slcs-init ...')
                returncode = subprocess.call(["slcs-init", "--idp", self.idp, "-u", self.aai_username,
                                              "-p", input_passwd, "-k", input_passwd],
                                             close_fds=True)

                if returncode != 0:
                    raise SLCSException("Failed while running 'slcs-init'")
                new_cert = True
                gc3libs.log.info('Create new SLCS certificate [ ok ].')

            # renew proxy if cert has changed or proxy expired
            if new_cert or not self.proxy_valid:
                if self.type == 'voms-proxy':
                    # Try renew voms credential; another interactive command
                    gc3libs.log.debug("No valid proxy found; trying to get a new one by 'voms-proxy-init' ...")
                    proxy_enable_command_list = ['voms-proxy-init',
                                                 '-valid', '24:00',
                                                 '-voms', self.vo,
                                                 '-q','-pwstdin']
                elif self.type == 'grid-proxy':
                    # Try renew voms credential; another interactive command
                    gc3libs.log.debug("No valid proxy found; trying to get a new one by 'grid-proxy-init' ...")
                    proxy_enable_command_list = ['grid-proxy-init',
                                                 '-valid', '24:00',
                                                 '-q','-pwstdin']
                else:
                    # No valid proxy methods recognized
                    raise Exception('No valid proxy methods found')

                devnull = open('/dev/null', 'w')
                p1 = subprocess.Popen(['echo',input_passwd], stdout=subprocess.PIPE)
                p2 = subprocess.Popen(proxy_enable_command_list,
                                      stdin=p1.stdout,
                                      stdout=subprocess.PIPE,
                                      stderr=devnull)
                p2.communicate()
                devnull.close()
                input_passwd = None # dispose content of password

            return self.check()

        except Exception, x:
            raise AuthenticationException('Failed renewing GRID credential: %s: %s'
                                          % (x.__class__.__name__, str(x)))


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
