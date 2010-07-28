import sys
import os
import subprocess
import getpass

from InformationContainer import *
import gc3utils
import Default
from Exceptions import *

import arclib

class Auth(object):
    types = {}
    # new proposal
    def __init__(self, authorizations, auto_enable):
        self.auto_enable = auto_enable
        self.__auths = { }
        self._auth_dict = authorizations
        self._auth_type = { }
        for auth_name, auth_params in self._auth_dict.items():
            self._auth_type[auth_name] = Auth.types[auth_params['type']]

    def get(self, auth_name):
        if not self.__auths.has_key(auth_name):
            try:
                a =  self._auth_type[auth_name](** self._auth_dict[auth_name])

                if not a.check():
                    if self.auto_enable:
                        try:
                            a.enable()
                        except Exception, x:
                            gc3utils.log.debug("Got exception while enabling auth '%s',"
                                               " will remember for next invocations:"
                                               " %s: %s" % (auth_name, x.__class__.__name__, str(x)))
                            a = x
                    else:
                        a = AuthenticationException("No valid credentials of type '%s'"
                                                    " and `auto_enable` not set." % auth_name)
            except KeyError:
                a = ConfigurationError("Unknown auth '%s' - check configration file" % auth_name)
            except Exception, x:
                a = AuthenticationException("Got error while creating auth '%s': %s: %s"
                                            % (auth_name, x.__class__.__name__, str(x)))

            self.__auths[auth_name] = a

        a = self.__auths[auth_name]
        if isinstance(a, Exception):
            raise a
        return a

    @staticmethod
    def register(auth_type, ctor):
        Auth.types[auth_type] = ctor

class ArcAuth(object):
    def __init__(self, **authorization):
        self.__dict__.update(authorization)

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
            try:
                self.aai_username
            except AttributeError:
                # Go interactive
                self.aai_username = raw_input('Insert AAI/Switch username for user '+getpass.getuser()+': ')

            try:
                self.idp
            except AttributeError:
                self.idp = raw_input('Insert AAI/Switch idp for user '+getpass.getuser()+': ')

            try:
                self.vo
            except AttributeError:
                self.vo = raw_input('Insert VO name for user '+getpass.getuser()+': ')

            # UserName set, go interactive asking password
            input_passwd = getpass.getpass('Insert AAI/Switch password for user '+self.aai_username+' : ')

            gc3utils.log.debug('Checking slcs status')
            new_cert = False
            if ( _user_certificate_is_valid() != True ):
                # Failed because slcs credential expired
                # trying renew slcs
                # this should be an interctiave command
                gc3utils.log.debug('No valid certificate found; trying to get new one by slcs-init ...')
                returncode = subprocess.call(["slcs-init", "--idp", self.idp, "-u", self.aai_username,
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
                devnull = open('/dev/null', 'w')
                p1 = subprocess.Popen(['echo',input_passwd], stdout=subprocess.PIPE)
                p2 = subprocess.Popen(['voms-proxy-init',
                                       '-valid', '24:00',
                                       '-voms', self.vo, 
                                       '-q','-pwstdin'], 
                                      stdin=p1.stdout, 
                                      stdout=subprocess.PIPE,
                                      stderr=devnull)
                p2.communicate()
                devnull.close()
                input_passwd = None # dispose content of password
                # variable `voms-proxy-init` may exit with status code
                # 1 even if the proxy was successfully created (e.g.,
                # "Error: verify failed. Cannot verify AC signature!")
                # Therefore, the only realiable way to detect if we
                # have a valid proxy seems to be a direct call to
                # `voms-proxy-info`...
                returncode = subprocess.call(['voms-proxy-info', 
                                              '--exists', '--valid', '23:30', # XXX: matches 24:00 request above
                                              # FIXME: hard-coded value!
                                              '--acexists', self.vo],
                                             close_fds=True)
                if returncode != 0:
                    # Failed renewing voms credential
                    # FATAL ERROR
                    raise VOMSException("Could not get a valid proxy by running 'voms-proxy-init':"
                                        " got return code %s" % p2.returncode)
                gc3utils.log.info("Successfully gotten new VOMS proxy.")

            return True

        except Exception, x:
            raise AuthenticationException('Failed renewing GRID credential: %s: %s'
                                          % (x.__class__.__name__, str(x)))


class SshAuth(object):
    def __init__(self, **authorization):
        self.__dict__.update(authorization)

    def check(self):
        return True
    def enable(self):
        return True


class NoneAuth(object):
    def __init__(self, **authorization):
        self.__dict__.update(authorization)

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

Auth.register('ssh', SshAuth)
Auth.register('voms', ArcAuth)
Auth.register('none', NoneAuth)
