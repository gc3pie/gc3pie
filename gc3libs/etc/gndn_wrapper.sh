#!/bin/bash

echo "[`date`] Start"

cd $1
sh < command.txt

echo "[`date`] Stop"
