import sys

from gc3libs.persistence import *
fs = FilesystemStore('/tmp')


class MyList(list):
    """
    Add a `__dict__` to `list`, so that creating a `persistent_id`
    entry on an instance works.
    """
    pass

ls = fs.load(sys.argv[1])
#print ls

try:
    if ls[0].my_note != 'This is j1':
        raise Error
    if ls[1].my_note != 'This is j2':
        raise Error
    if ls[2].my_other_note != 'Yet another note.':
        raise Error
except:
    print 'FAIL'
    sys.exit(1)

# if we get here, all is fine and well
print 'OK'
sys.exit(0)
