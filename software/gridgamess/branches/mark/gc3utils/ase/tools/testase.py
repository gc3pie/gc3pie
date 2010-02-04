#!/usr/bin/env python
from sys import argv
display = (len(argv) != 2 or argv[1] != '--no-display')
from ase.test import test
test(2, display=display)
