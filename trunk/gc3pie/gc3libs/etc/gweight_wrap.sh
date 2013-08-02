#!/bin/bash

echo "[`date`] Start"

usage()
{
cat << EOF
usage: $0 options

This script runs an Rscript needing the following arguments

OPTIONS:
   -h      Show this message
   -m      Master driver script
   -w      weight function
   -d      reference data
EOF
}

if [[ $# -le 0 ]]
then
    usage
    exit 1
fi

# Set defauls values
MASTER="$HOME/bin/run.R"
WEIGHT="$HOME/bin/f_get_weight.r"
DATA="$HOME/data/two_mode_network.rda"
EDGES=$1
shift

while getopts ":m:w:d:" opt; do
  case $opt in
    m)
      MASTER=$OPTARG
      ;;
    w)
      WEIGHT=$OPTARG
      ;;
    d)
      DATA=$OPTARG
      ;;
    \?)
      usage
      exit 1
      ;;
  esac
done

echo -e "Arguments:\nMASTER:\t${MASTER}\nWEIGHT:\t${WEIGHT}\nEDGES:\t${EDGES}\nDATA:\t${DATA}\n"

if [[ -z $MASTER ]] || [[ -z $WEIGHT ]] || [[ -z $EDGES ]] || [[ -z $DATA ]]
then
    echo "Failed while verifying arguments"
    usage
    exit 1
fi

echo -n "Checking R installation... "
RES=`command Rscript --version 2>&1`
if [ $? -ne 0 ]; then
   echo "[failed]"
   echo $RES
   exit 1
else
   echo "[ok]"
fi


echo "Running: Rscript --vanilla $MASTER $WEIGHT $EDGES $DATA"

# Rscript --vanilla ./bin/run.R ./bin/f_get_weight.r input.csv ./data/two_mode_network.rda
Rscript --vanilla $MASTER $WEIGHT $EDGES $DATA

RET=$?

echo "[`date`] Finished with code $RET"

exit $RET
