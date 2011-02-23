#! /bin/sh
#
# Test save/load of a data structure (list) containing items that
# should be persisted separately (jobs).  Doing the test correctly
# might require two separate instances of the Python interpreter (one
# for saving and one for loading), which this shell script tries to do
# properly. Rewrite everything in Python ,one day.
#

test_script_dir="$(dirname "$0")"

id=$(python "${test_script_dir}"/test_nested_persistence_save.py)
python "${test_script_dir}"/test_nested_persistence_load.py "$id"