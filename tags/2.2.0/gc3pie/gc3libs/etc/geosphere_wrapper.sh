#! /bin/bash
#
# geoshpere_wrap.sh -- base wrapper script for executing Grok and HGS
# 
# Authors: Sergio Maffioletti <sergio.maffioletti@gc3.uzh.ch>
#
# Copyright (c) 2013-2014 GC3, University of Zurich, http://www.gc3.uzh.ch/ 
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
#-----------------------------------------------------------------------
PROG=$(basename "$0")

# Find local commands
TAR=`which tar`
UNZIP=`which unzip`    
S3CMD=`which s3cmd`
S3CFG="./etc/s3cfg"
GROK="/home/gc3-user/bin/grokgrok"
HGS="/home/gc3-user/bin/hgshgs"
TODAY=`date +%Y-%m-%d`
DEBUG=0
FAILED=0
CUR_DIR="$PWD"
OUTPUT_ARCHIVE=""

function log {
    if [ $DEBUG -ne 0 ]; then
	echo "DEBUG: $1"
    fi
}

function verify_s3 {
    echo "Dumping S3cmd configuration"
    $S3CMD -c $S3CFG --dump-config 1>/dev/null 2>&1
    return $?
}

function open_tar_archive {
    ARCHIVE=$1
    # Check whether tar command is available

    if [ -z "$TAR" ]; then
	# No tar command available
	echo "Cannot open .tgz archive. TAR command not found"
	return 1
    fi
    # Open archive
    log "$TAR xfz $ARCHIVE"
    $TAR xfz $ARCHIVE
    return $?
}

function open_zip_archive {
    ARCHIVE=$1
    # Check whether tar command is available

    if [ -z "$UNZIP" ]; then
	# No unzip command available
	echo "Cannot open .zip archive. UNZIP command not found"
	return 1
    fi
    # Open archive
    log "$UNZIP -qq -u $ARCHIVE"
    $UNZIP -qq -u $ARCHIVE
    return $?
}

function get_local_archive_mimetype {
    INPUT=$1
    if [ ! -e $INPUT ]; then
	echo "Input archive not found"
	return 1
    fi

    # Determine mime/type
    # Supported data type:
    #   .zip archive (use unzip to open)
    #   .tgz archive (use tar xfz to open)
    # Unsupported formats:
    #   .tar, .zip.gz
    log "file --mime-type -b $INPUT"
    MIME=`file --mime-type -b $INPUT`
    return $?
}

function get_s3_archive_mimetype {
    S3_URL=$1

    # Verify S3cmd configuration
    if [ -z "$S3CMD" ]; then
	echo "S3cmd not found"
	return 1
    fi

    # Inspect archive and try to guess
    # MIME type
    log "$S3CMD -c $S3CFG info ${S3_URL} | grep -i mime | awk -F: '{print $2}'"
    MIME=`$S3CMD -c $S3CFG info ${S3_URL} | grep -i mime | awk -F: '{print $2}'`
    return $?
}

function get_s3_archive {
    S3_URL=$1
    LOCAT_DES=$2

    log "$S3CMD -c $S3CFG get ${S3_URL} ${LOCAL_DEST}"
    $S3CMD -c $S3CFG get ${S3_URL} ${LOCAL_DEST}
    return $?
}

function get_s3_bucket {
    S3_URL=$1
    LOCAT_DES=$2

    log "$S3CMD -c $S3CFG --check-md5 sync ${S3_URL} ${LOCAL_DEST}"
    $S3CMD -c $S3CFG sync ${S3_URL} ${LOCAL_DEST}
    return $?
}


function put_s3_archive {
    FILE_TO_UPLOAD=$1
    S3_URL=$2

    log "$S3CMD -c $S3CFG put ${FILE_TO_UPLOAD} ${S3_URL}"
    $S3CMD -c $S3CFG put ${FILE_TO_UPLOAD} ${S3_URL}
    return $?
}


## configuration defaults

## Parse command line options.
USAGE=`cat <<EOF
Usage: 
      $PROG [options] <input archive> <working dir> <output container>

Options:
      -g <grok binary file>    path to 'grok' binary. Default in PATH
      -h <hgs binary file>     path to 'hgs' binary. Default in PATH
      -d                       enable debug
EOF`

while getopts "g:h:o:d" opt; do
    case $opt in
	d )
	    DEBUG=1
	    ;;
	# o )
	#     OUTPUT_ARCHIVE="$OPTARG"
	#     ;;
	g )
	    GROK="$OPTARG"
	    if [ ! -x $GROK ]; then
		echo "CRITICAL: grok file not executable"
		exit 1
	    fi
	    ;;
	h )
	    HGS="$OPTARG"
	    if [ ! -x $HGS ]; then
		echo "CRITICAL: hgs file not executable"
		exit 1
	    fi
	    ;;
	\? )
	    usage
	    exit 1
	    ;;
    esac
done

# Remove the switches we parsed above.
# shift `expr $OPTIND - 1`
shift $(($OPTIND -1))

# We want 2 non-option argument:
# 1. Input archive in s3:// format
# 2. Working dir of the HGS model
# 3. Output container in s3:// format
# Remove this block if you don't need it.
if [ $# -ne 3 ]; then
    echo "$USAGE"
    exit 1
fi

INPUT_ARCHIVE=$1
WORKING_DIR=$2
OUTPUT_URL=$3

OUTPUT_ARCHIVE=`basename ${OUTPUT_URL}`

echo "[`date +%Y-%m-%d" "%H:%M:%S`] Start"
echo -e "\n"

# Determine input type
# Allowed input types:
# application/x-tar
# inode/directory
# 
# This will also instruct the script whether to use 
# a shared filesystem or not
log "Checking input archive... "

case ${INPUT_ARCHIVE} in
    "s3://"* )
	# S3 URL
	log "S3 archive type"
	log "getting mimetype... "
	verify_s3
	if [ $? -ne 0 ]; then
	    echo "CRITICAL: Cannot use s3cmd command."
	    echo "Please check s3cfg configuration."
	    exit 1
	fi
	get_s3_archive_mimetype $INPUT_ARCHIVE
	if [ $? -ne 0 ]; then
	    echo "CRITICAL: Failed getting S3 archive mimetype"
	    exit 1
	fi
	log "Downloading archive from S3... "
	get_s3_archive ${INPUT_ARCHIVE}
	if [ $? -ne 0 ]; then
	    echo "CRITICAL: Failed getting S3 archive"
	    exit 1
	fi
	log "S3 archive download completed."
	;;
    "./"* )
	# Local archive
	log "Local archive"
	log "getting mimetype... "
	get_local_archive_mimetype ${INPUT_ARCHIVE}
	if [ $? -ne 0 ]; then
	    echo "CRITICAL: Failed getting S3 archive mimetype"
	    exit 1
	fi
	log "local archive inspection completed."
	;;
    "http://"* )
	echo "http/https not yet supported"
	exit 1
	;;
     * )
	echo "Not supported input archive format"
	exit 1
	;;
esac

log "Input archive mimetype: $MIME"
log "Opening input archive... "

# Now we a local archive and a corresponding mimetpye
case $MIME in
    *"application/x-gzip"* | *"application/x-tar"* )
	log "opening .tgz archive... "
	# open_tar_archive ${INPUT_ARCHIVE}
	open_tar_archive ${WORKING_DIR}.tgz
    	if [ $? -ne 0 ]; then
    	    echo "Failed opening input archive"
    	    exit 1
    	fi
	log ".tgz archive open completed."
    	;;
    *"application/zip"* )
	log "opening .zip archive... "
	# open_zip_archive ${INPUT_ARCHIVE}
	open_zip_archive ${WORKING_DIR}.zip
    	if [ $? -ne 0 ]; then
    	    echo "Failed opening input archive"
    	    exit 1
    	fi
	log ".zip archive open completed."
    	;;
    *)
    	echo "$MIME not supported"
    	exit 1
    	;;
esac

# Print actual configuration options
echo "Configuration options:"
echo "----------------------"

echo -e "Input archive:\t$INPUT_ARCHIVE"
echo -e "Mimetype:\t$MIME"
echo -e "Working dir:\t$WORKING_DIR"
echo -e "Output URL:\t$OUTPUT_URL"
echo -e "Grok:\t\t$GROK"
echo -e "HGS:\t\t$HGS"
echo -e "\n\n"

## Execute the Grok and HGS code ##

echo "Run simulation:"
echo "---------------"

echo "GROK"

# Check whether 'grok' and 'hgs' have been defined
# Check whether grok command has been set
if [ -z "$GROK" ]; then
    echo "CRITICAL: No 'grok' command found"
    exit 1
elif [ ! -x "$GROK" ]; then
    echo "CRITICAL: grok file $GROK not executable"
    exit 1
fi
# Check whether grok command has been set
if [ -z "$HGS" ]; then
    echo "CRITICAL: No 'hgs' command found"
    exit 1
elif [ ! -x "$HGS" ]; then
    echo "CRITICAL: hgs file $HGS not executable"
    exit 1
fi

GROK_LOG=grok.log
HGS_LOG=hgs.log

cd "${WORKING_DIR}"
echo "working in $PWD"

# run grok
log "$GROK 1>${GROK_LOG} 2>&1"
time `$GROK 1>${GROK_LOG} 2>&1`
RET=$?
echo "Grok exit code: [$RET]"
if [ $RET -ne 0 ]; then
    exit $RET
fi

# run hgs
echo "HGS"
log "$HGS 1>${HGS_LOG} 2>&1"
time `$HGS 1>${HGS_LOG} 2>&1`
RET=$?
echo "HGS exit code: [$RET]"
if [ $RET -ne 0 ]; then
    exit $RET
fi

echo -e "\n\n"

echo "Postprocessing:"
echo -e "---------------\n"

# Return to original location
cd "${CUR_DIR}"

# generate output archive
echo "Generating output archive... "
# e.g. tar cfz testo.results.tgz testo.*
log "${TAR} cfz ${OUTPUT_ARCHIVE} ${WORKING_DIR}"
${TAR} cfz ${OUTPUT_ARCHIVE} ${WORKING_DIR}
RET=$?
echo "[$RET]"
if [ $RET -ne 0 ]; then
    exit $RET
fi

log "Uploading output archive to S3 container... "
# Check whether output needs to be uploaded to an ObjectStore

case $OUTPUT_URL in
    "s3://"* )
	# Upload to S3 repo
	log "Checking whether output-data already exists... "
	$S3CMD -c $S3CFG -q info ${OUTPUT_URL}
	if [ $? -eq 0 ]; then
	    # data exists on S3
	    # postfix hours-minutes-secs
	    POSTFIX="-`date +%H%M%S`"
	    log "Output archive already exists; creating postfix ${POSTFIX}"
	else
	    POSTFIX=""
	fi
	log "Uploading result..."
	put_s3_archive ${OUTPUT_ARCHIVE} ${OUTPUT_URL}${POSTFIX}
	if [ $? -ne 0 ]; then
	    echo "ERROR: failed to upload output archive"
	    FAILED=1
	fi
	log "Result upload completed."
	;;
    "./"* )
	# keep output archive local
	# will be retrieved in a separate step
	log "Result kep local"
	;;
    * )
	echo "ERROR: expected output URL of type s3://"
	FAILED=1
	;;
esac

# exit
echo "[`date +%Y-%m-%d" "%H:%M:%S`] Done"
log "exiting with value $FAILED"
exit $FAILED
