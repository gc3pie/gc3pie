#!/usr/bin/env python3

from __future__ import absolute_import
import sys, time
print(sys.version)

for ix in range(5):
  print('hello', ix)
  time.sleep(3)