#http://stackoverflow.com/questions/2082850/real-time-subprocess-popen-via-stdout-and-pipe
import os, sys
print(os.getcwd())
from subprocess import Popen, PIPE
p = Popen(['./writer.py'], stdout = PIPE)
for line in p.stdout:
    print(line)
    sys.stdout.flush()
##print(p.stdout.read())

p.wait()
#output = p.communicate()[0]
#print(output)
