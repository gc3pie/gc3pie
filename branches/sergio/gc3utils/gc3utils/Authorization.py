import Default
import logging
import utils
import sys
import os

class Auth(object):
    def __init__(self,auto_enable=True):
        self.auto_enable=True
        self.__auths = { }
        self.log = logging.getLogger('gc3utils')

    def get(self,auth_type):
        if not self.__auths.has_key(auth_type):
            if auth_type == Default.ARC_LRMS:
                a = ArcAuth()
            elif auth_type==Default.SSH_LRMS:
                a = SshAuth()
            elif auth_type=='none':
                a = NoneAuth()
            else:
                raise Exception("Invalid auth_type in Auth()")

            if not a.check():
                if self.auto_enable:
                    try:
                        a.enable()
                    except Exception, x:
                        a = x
                else:
                    a = AuthenticationException()
            self.__auths[auth_type] = a

        a = self.__auths[auth_type]
        if isinstance(a, Exception):
            raise a
        return a

class ArcAuth(object):
    def __init__(self):
        self.log = logging.getLogger('gc3utils')
                
    def check(self):
        self.log.debug('Checking authentication: GRID')
        if ( (not utils.check_grid_authentication()) | (not utils.check_user_certificate()) ):
            self.log.error('Check authentication failed')
            return False
        return True

    def enable(self):
        try:
            # Get AAI username
            _aaiUserName = None
                
            Default.AAI_CREDENTIAL_REPO = os.path.expandvars(Default.AAI_CREDENTIAL_REPO)
            self.log.debug('checking AAI credential file [ %s ]',Default.AAI_CREDENTIAL_REPO)
            if ( os.path.exists(Default.AAI_CREDENTIAL_REPO) & os.path.isfile(Default.AAI_CREDENTIAL_REPO) ):
                _fileHandle = open(Default.AAI_CREDENTIAL_REPO,'r')
                _aaiUserName = _fileHandle.read()
                _fileHandle.close()
                _aaiUserName = _aaiUserName.rstrip("\n")
                self.log.debug('_aaiUserName: %s',_aaiUserName)

            # Renew credential
            utils.renew_grid_credential(_aaiUserName)


#            self.AAI_CREDENTIAL_REPO = os.path.expandvars(self.AAI_CREDENTIAL_REPO)
#            self.log.debug('checking AAI credential file [ %s ]',self.AAI_CREDENTIAL_REPO)
#            if ( os.path.exists(self.AAI_CREDENTIAL_REPO) & os.path.isfile(self.AAI_CREDENTIAL_REPO) ):
#                self.log.debug('Opening AAI credential file in %s',self.AAI_CREDENTIAL_REPO)
#                _fileHandle = open(self.AAI_CREDENTIAL_REPO,'r')
#                _aaiUserName = _fileHandle.read()
#                _aaiUserName = _aaiUserName.rstrip("\n")
#                self.log.debug('_aaiUserName: %s',_aaiUserName)
#            
#            # Renew credential 
#            utils.renew_grid_credential(_aaiUserName)
        except:
            self.log.critical('Failed renewing grid credential [%s]',sys.exc_info()[1])
            return False
        return True



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

    
    
    
        
