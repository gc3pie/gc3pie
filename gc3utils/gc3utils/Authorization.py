import sys
import os
import subprocess
import getpass

import gc3utils
import Default
from Exceptions import *

import arclib

class Auth(object):
    def __init__(self,auto_enable=True):
        self.auto_enable=True
        self.__auths = { }

    def get(self,auth_type):
        if not self.__auths.has_key(auth_type):
            if auth_type is Default.SMSCG_AUTHENTICATION:
                a = ArcAuth()
            elif auth_type is Default.SSH_AUTHENTICATION:
                a = SshAuth()
            elif auth_type is Default.NONE_AUTHENTICATION:
                a = NoneAuth()
            else:
                raise AuthenticationException("Auth.get() called with invalid `auth_type` (%s)", auth_type)

            if not a.check():
                if self.auto_enable:
                    try:
                        a.enable()
                    except Exception, x:
                        gc3utils.log.debug("Got exception while enabling auth '%s',"
                                           " will remember for next invocations:"
                                           " %s: %s" % (auth_type, x.__class__.__name__, str(x)))
                        a = x
                else:
                    a = AuthenticationException("No valid credentials of type '%s'"
                                                " and `auto_enable` not set." % auth_type)
            self.__auths[auth_type] = a

        a = self.__auths[auth_type]
        if isinstance(a, Exception):
            raise a
        return a


class ArcAuth(object):
    def check(self):
        gc3utils.log.debug('Checking authentication: GRID')
        try:
            if _user_certificate_is_valid() and _voms_proxy_is_valid(): 
                return True
        except Exception, x:
            gc3utils.log.error('Error checking GRID authentication: %s', str(x), exc_info=True)
        return False
     
    def enable(self):
        try:
            # Get AAI username
            gc3utils.log.debug("Reading AAI username from file '%s'", Default.AAI_CREDENTIAL_REPO)
            _aaiUserName = None
            try: 
                _fileHandle = open(Default.AAI_CREDENTIAL_REPO, 'r')
                _aaiUserName = _fileHandle.read()
                _fileHandle.close()
                _aaiUserName = _aaiUserName.strip()
                gc3utils.log.debug("Read aaiUserName: '%s'", _aaiUserName)
            except IOError, x:
                gc3utils.log.error("Cannot read AAI credential file '%s': %s",
                                   Default.AAI_CREDENTIAL_REPO, str(x), exc_info=True) 
                # do not raise, will ask for username interactively

            VOMSPROXYINIT = ['voms-proxy-init','-valid','24:00','-voms','smscg','-q','-pwstdin']
            SLCSINFO = "openssl x509 -noout -checkend 3600 -in ~/.globus/usercert.pem"

            if _aaiUserName is None:
                # Go interactive
                _aaiUserName = raw_input('Insert AAI/Switch username for user '+getpass.getuser()+': ')
            # UserName set, go interactive asking password
            input_passwd = getpass.getpass('Insert AAI/Switch password for user '+_aaiUserName+' : ')

            gc3utils.log.debug('Checking slcs status')
            new_cert = False
            if ( _user_certificate_is_valid() != True ):
                # Failed because slcs credential expired
                # trying renew slcs
                # this should be an interctiave command
                gc3utils.log.debug('No valid certificate found; trying to get new one by slcs-init ...')
                # FIXME: hard-coded UZH stuff!
                returncode = subprocess.call(["slcs-init", "--idp", "uzh.ch", "-u", _aaiUserName, 
                                              "-p", input_passwd, "-k", input_passwd],
                                             close_fds=True)
                if returncode != 0:
                    raise SLCSException("Got error trying to run 'slcs-init'")
                new_cert = True
                gc3utils.log.info('Successfully gotten new SLCS certificate.')

            # if cert has changed, we need a new proxy as well
            if new_cert or not _voms_proxy_is_valid():
                # Try renew voms credential; another interactive command
                gc3utils.log.debug("No valid proxy found; trying to get a new one by 'voms-proxy-init' ...")
                p1 = subprocess.Popen(['echo',input_passwd], stdout=subprocess.PIPE)
                p2 = subprocess.Popen(VOMSPROXYINIT, stdin=p1.stdout, stdout=subprocess.PIPE)
                p2.communicate()
                input_passwd = None # dispose content of passord variable
                if p2.returncode != 0:
                    # Failed renewing voms credential
                    # FATAL ERROR
                    raise VOMSException("Could not get new proxy by running 'voms-proxy-init':"
                                        " got return code %s" % p2.returncode)
                gc3utils.log.info("Successfully gotten new VOMS proxy.")

            return True

        except Exception, x:
            raise AuthenticationException('Failed renewing GRID credential: %s: %s'
                                          % (x.__class__.__name__, str(x)))


class SshAuth(object):
    def check(self):
        return True
    def enable(self):
        return True


class NoneAuth(object):
    def check(self):
        return True

    def enable(self):
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
