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
                if not a.is_valid():
                    raise Exception('Missing required configuration parameters')

                if not a.check():
                    if self.auto_enable:
                        try:
                            a.enable()
                        except Exception, x:
                            gc3utils.log.debug("Got exception while enabling auth '%s',"
                                               " will remember for next invocations:"
                                               " %s: %s" % (auth_name, x.__class__.__name__, x))
                            a = x
                    else:
                        a = AuthenticationException("No valid credentials of type '%s'"
                                                    " and `auto_enable` not set." % auth_name)
            except KeyError:
                a = ConfigurationError("Unknown auth '%s' - check configuration file" % auth_name)
            except Exception, x:
                a = AuthenticationException("Got error while creating auth '%s': %s: %s"
                                            % (auth_name, x.__class__.__name__, x))

            self.__auths[auth_name] = a

        a = self.__auths[auth_name]
        if isinstance(a, Exception):
            raise a
        return a

    @staticmethod
    def register(auth_type, ctor):
        Auth.types[auth_type] = ctor


class ArcAuth(object):
    user_cert_valid = False
    proxy_valid = False

    def __init__(self, **authorization):
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
        gc3utils.log.debug('Checking authentication: GRID')
        try:
            self.user_cert_valid = _user_certificate_is_valid()
            self.proxy_valid = _voms_proxy_is_valid()
        except Exception, x:
            gc3utils.log.error('Error checking GRID authentication: %s', str(x), exc_info=True)

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
            
            # End collecting information


            # Start renewing credential

            new_cert = False

            # User certificate
            if not self.user_cert_valid:
                if not self.usercert == 'slcs':
                    raise Exception('User credential expired')

                gc3utils.log.debug('No valid certificate found; trying to get new one by slcs-init ...')
                returncode = subprocess.call(["slcs-init", "--idp", self.idp, "-u", self.aai_username,
                                              "-p", input_passwd, "-k", input_passwd],
                                             close_fds=True)

                if returncode != 0:
                    raise SLCSException("Failed while running 'slcs-init'")
                new_cert = True
                gc3utils.log.info('Create new SLCS certificate [ ok ].')


            # renew proxy if cert has changed or proxy expired
            if new_cert or not self.proxy_valid:
                if self.type == 'voms-proxy':
                    # Try renew voms credential; another interactive command
                    gc3utils.log.debug("No valid proxy found; trying to get a new one by 'voms-proxy-init' ...")
                    proxy_enable_command_list = ['voms-proxy-init',
                                                 '-valid', '24:00',
                                                 '-voms', self.vo,
                                                 '-q','-pwstdin']
                elif self.type == 'grid-proxy':
                    # Try renew voms credential; another interactive command
                    gc3utils.log.debug("No valid proxy found; trying to get a new one by 'grid-proxy-init' ...")
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


class SshAuth(object):
    def __init__(self, **authorization):
        self.__dict__.update(authorization)

    def is_valid(self):
        try:
            self.type
            self.username
            return True
        except:
            return False

    def check(self):
        return True
    def enable(self):
        return True


class NoneAuth(object):
    def __init__(self, **authorization):
        self.__dict__.update(authorization)

    def is_valid(self):
        return True
    
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
Auth.register('voms-proxy', ArcAuth)
Auth.register('grid-proxy', ArcAuth)
Auth.register('none', NoneAuth)
