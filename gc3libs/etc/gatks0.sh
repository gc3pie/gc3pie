#!/bin/bash
#
# gatks0.sh -- wrapper script for executing GATK stage 0
#
# Authors: Sergio Maffioletti <sergio.maffioletti@uzh.ch>
#          Riccardo Murri <riccardo.murri@uzh.ch>
#
# Copyright (c) 2015,2016 S3IT, University of Zurich, http://www.s3it.uzh.ch/
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
# Note: take haplotype.caller and run the java part
# 
# Input:
# * .bam and .bai
# NEED: 
# * goat.genome (need to update): single file
#   * .fa .fa.fai .dict 
#
# * make goat.genome an option to be passed to ggatk for stage0
# * GATK could also be an option passed to ggatk (it's just a .jar file)
# SNAPSHOT
#  * GATK current version
#  * goat.genome current version

me=$(basename "$0")

## defaults
gatk="/apps/GenomeAnalysisTK.jar"
goat="/apps/goat.genome/goat_scaffoldFG_V1.1.normalised.22.07.fa"
memory="2GB"
# no settings for this script

## Exit status codes (mostly following <sysexits.h>)

# successful exit
EX_OK=0
EX_OSFILE=72

## helper functions

function die () {
    rc="$1"
    shift
    (echo -n "$me: ERROR: ";
        if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
    exit $rc
}


## usage info

usage () {
    cat <<__EOF__
Usage:
  $me [options] [INPUT BAM] [INPUT BAI] [SAMPLENAME] [ARGS]

Run GATK stage 0, passing ARGS (if any) as command-line arguments.

Options:
  -v            Enable verbose logging
  -h            Print this help text
  -g            Use specific GATK .jar file
  -f            Use specific goat.genome file
  -m            Amount of memory to be allocated (in GB)

__EOF__
}

warn () {
  (echo -n "$me: WARNING: ";
      if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
}

have_command () {
  type "$1" >/dev/null 2>/dev/null
}

require_command () {
  if ! have_command "$1"; then
    die $EX_UNAVAILABLE "Could not find required command '$1' in system PATH. Aborting."
  fi
}

is_absolute_path () {
    expr match "$1" '/' >/dev/null 2>/dev/null
}


## parse command-line

short_opts='dhvg:f:m:'
long_opts='debug,help,verbose,gatk,goat,memory'

getopt -T > /dev/null
rc=$?
if [ "$rc" -eq 4 ]; then
    # GNU getopt
    args=$(getopt --name "$me" --shell sh -l "$long_opts" -o "$short_opts" -- "$@")
    if [ $? -ne 0 ]; then
        die 1 "Type '$me --help' to get usage information."
    fi
    # use 'eval' to remove getopt quoting
    eval set -- $args
else
    # old-style getopt, use compatibility syntax
    args=$(getopt "$short_opts" "$@")
    if [ $? -ne 0 ]; then
        die 1 "Type '$me --help' to get usage information."
    fi
    set -- $args
fi

while [ $# -gt 0 ]; do
    case "$1" in
	--gatk|-g)     shift; gatk=$1 ;;
	--goat|-f)     shift; goat=$1 ;;
	--memory|-m)   shift; memory=$1 ;;	
        --verbose|-v)  verbose='--verbose' ;;
        --debug|-d)    verbose='--verbose'; set -x ;;
        --help|-h)     usage; exit 0 ;;
        --)            shift; break ;;
    esac
    shift
done

## sanity checks

# the BAM,BAI and SAMPLENAME arguments have to be present
if [ $# -lt 3 ]; then
    die 1 "Missing required arguments BAM and BAI files and SAMPLENAME. Type '$me --help' to get usage help."
fi

bam=$1
shift
bai=$1
shift
samplename=$1

## main
echo "=== ${me}: Starting at `date '+%Y-%m-%d %H:%M:%S'`"

require_command java

command="java -Xmx${memory}g -d64 -jar ${gatk}\
     -T HaplotypeCaller\
     --emitRefConfidence GVCF\
     -minPruning 3 -stand_call_conf 30 \
     -stand_emit_conf 10 \
     -R ${goat} -I ${bam} -o ${samplename}.g.vcf"

# run script
echo "=== ${me}: Running: ${command}"
# S3IT_DBG
# command="dd if=/dev/urandom of=${samplename}.g.vcf bs=1M count=1; cp ${samplename}.g.vcf ${samplename}.g.vcf.idx"
eval $command
rc=$?
echo "=== ${me}: Script ended with exit code $rc"

## Checking output
echo -n "Checking output file ${samplename}.g.vcf... "
if [ -e ${samplename}.g.vcf ]; then
    echo "[ok]"
    rc=$EX_OK
else
    echo "[failed]"
    rc=$EX_OSFILE
fi

## All done.
echo "=== ${me}: Script done at `date '+%Y-%m-%d %H:%M:%S'`."

exit $rc
