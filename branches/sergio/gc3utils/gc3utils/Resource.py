import types
# -----------------------------------------------------
# Resource lrms
#

class Resource():
    def __init__(self, **entries): self.__dict__.update(entries)

    def __repr__(self):
        args = ['%s=%s' % (k, repr(v)) for (k,v) in vars(self).items()]
        return 'Resource(%s)' % ', '.join(args)

    def update(x, **entries):
        if type(x) == types.DictType: x.update(entries)
        else: x.__dict__.update(entries)
        return x
