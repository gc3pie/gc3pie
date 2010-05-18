class InformationContainer(dict):

    def __setattr__(self, key, value):
        self[key] = value

    def __getattr__(self, key):
        return self[key]

