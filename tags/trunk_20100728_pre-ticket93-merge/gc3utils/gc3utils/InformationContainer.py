import Exceptions

class InformationContainer(dict):

    def __init__(self, initializer=None, **keywd):
        if initializer is None:
            dict.__init__(self,**keywd)
        else:
            dict.__init__(self,initializer,**keywd)
        if not self.is_valid():
            raise Exceptions.InvalidInformationContainerError('Object `%s` of class `%s` failed validity check.' % (self, self.__class__.__name__))

    def __setattr__(self, key, value):
        self[key] = value

    def __getattr__(self, key):
        return self[key]

    def is_valid(self):
        raise NotImplementedError("Abstract method `is_valid()` called - this should have been defined in a derived class.")
