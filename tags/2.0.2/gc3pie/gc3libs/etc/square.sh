#!/bin/bash

INPUTFILE=$1

for v in `cat $INPUTFILE`; do echo "Integer: " $v " Square: " `expr $v \* $v`; done

