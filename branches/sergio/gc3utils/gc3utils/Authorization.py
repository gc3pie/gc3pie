class AuthenticationFailed(Exception):
    pass

class Auth(object):
    def __init__(auto_enable=True):
        self.auto_enable=True
        self.__auths = { }
    def get(self,auth_type):
        if not self.__auths.has_key(auth_type):
            if auth_type=='arc':
                a = ArcAuth()
            elif auth_type=='ssh':
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
                    a = AuthenticationFailed()
            self.__auths[auth_type] = a

        a = self.__auths[auth_type]
        if isinstance(a, Exception):
            raise a
        return a


class ArcAuth(object):
    def check(self):
        logging.debug('Checking authentication: GRID')
        if ( (not utils.check_grid_authentication()) | (not utils.check_user_certificate()) ):
            logging.error('Check authentication failed')
            return False
        return True

    def enable(self):
        try:
            # Get AAI username
            _aaiUserName = None
                
            self.AAI_CREDENTIAL_REPO = os.path.expandvars(self.AAI_CREDENTIAL_REPO)
            logging.debug('checking AAI credential file [ %s ]',self.AAI_CREDENTIAL_REPO)
            if ( os.path.exists(self.AAI_CREDENTIAL_REPO) & os.path.isfile(self.AAI_CREDENTIAL_REPO) ):
                logging.debug('Opening AAI credential file in %s',self.AAI_CREDENTIAL_REPO)
                _fileHandle = open(self.AAI_CREDENTIAL_REPO,'r')
                _aaiUserName = _fileHandle.read()
                _aaiUserName = _aaiUserName.rstrip("\n")
                logging.debug('_aaiUserName: %s',_aaiUserName)
            
            # Renew credential 
            utils.renew_grid_credential(_aaiUserName)
        except:
            logging.critical('Failed renewing grid credential [%s]',sys.exc_info()[1])
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

    
    
    
        
