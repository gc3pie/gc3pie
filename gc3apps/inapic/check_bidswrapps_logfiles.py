import argparse
import os
from glob import glob



parser = argparse.ArgumentParser(description='Checks bidsapps logfiles')
parser.add_argument('logfiles_dir', help='The directory with the bidsapps logfile folders.')
args = parser.parse_args()

os.chdir(args.logfiles_dir)

files = glob("*/*.log")
good = []
bad = []

for f in files:
    with open(f) as fi:
        status = fi.readline()

    jobname = f.split(os.sep)[0]

    if status.startswith("[ok]"):
        good.append(jobname)
    else:
        bad.append(jobname)

print("*"*30)
print("ok jobs", good)
print("*"*30)
print("bad jobs", bad)
print("*"*30)
print("*"*30)


print("%s ok jobs"%len(good))
print("%s bad jobs"%len(bad))