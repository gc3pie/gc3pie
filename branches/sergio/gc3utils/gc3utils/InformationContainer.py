class InformationContainer():

    def __init__(self, **resource_info):
        self.__dict__.update(resource_info)

    def __repr__(self):
        args = ['%s=%s' % (k, repr(v)) for (k,v) in vars(self).items()]
        return 'InformationContainer%s)' % ', '.join(args)
    
    def update(self, **resource_info):
        self.__dict__.update(resource_info)
        
    def isValid(self): abstract
