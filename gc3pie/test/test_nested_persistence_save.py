from gc3libs.persistence import *
fs = FilesystemStore('/tmp')

from gc3libs.application.gamess import GamessApplication

j1 = GamessApplication('fake_input_file.inp', output_dir='/tmp')
j2 = GamessApplication('fake_input_file.inp', output_dir='/tmp')
j3 = GamessApplication('fake_input_file.inp', output_dir='/tmp')

j1.my_note = 'This is j1'
j2.my_note = 'This is j2'
j3.my_other_note = 'Yet another note.'

class MyList(list):
    """
    Add a `__dict__` to `list`, so that creating a `persistent_id`
    entry on an instance works.
    """
    pass

ls = MyList([j1, j2, j3])

fs.save(ls)
# print ID for the test driver to pick up
print ls.persistent_id


