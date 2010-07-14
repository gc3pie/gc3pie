from Exceptions import *
from utils import Struct

class InformationContainer(Struct):

    def __init__(self, initializer=None, **keywd):
        Struct.__init__(self, initializer, **keywd)
        if not self.is_valid():
            raise InvalidInformationContainerError('Object `%s` of class `%s` failed validity check.' % (self, self.__class__.__name__))

    def is_valid(self):
        raise NotImplementedError("Abstract method `is_valid()` called - this should have been defined in a derived class.")
