#!/bin/csh -f
#
#  runOneGatherer.csh 
###########################################################################
#
#  Purpose:
#
#      This script is a wrapper around the step that
#      runs a single gatherer.
#
#  Usage:
#
#      runOneGatherer.csh gatherer_name
#      Example: runOneGatherer.csh mp_annotation
#
#  Env Vars:
#
#      - See Configuration file (loadadmin product)
#
#      - See master.config.csh (mgiconfig product)
#
#  Inputs:
#
#      - Gatherer name
#
#  Outputs:
#
#      - Log file for the script (${LOG})
#
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  Fatal error occurred
#
#  Assumes:  Nothing 
#
#  Implementation:
#
#      This script will perform the following steps:
#
#      1) Source the configuration file to establish the environment.
#      2) Build the frontend schema for the specified gatherer.
#
#  Notes:  None
#
###########################################################################

cd `dirname $0` && source ./Configuration

setenv SCRIPT_NAME `basename $0`


setenv LOG ${LOGSDIR}/${SCRIPT_NAME}.log
rm -f ${LOG}
touch ${LOG}

echo "$0" >> ${LOG}
env | sort >> ${LOG}


#
# Build the frontend schema.
#
date | tee -a ${LOG}
#
# Make sure there is at least one argument to the script.
#
if ( $# != 1 ) then
    echo "********************************************************"
    echo ""
    echo "Usage: $0 gatherer_name(the name of the gatherer to run)"
    echo ""
    echo "Example: $0 mp_annotation"
    echo "To run the mp_annotation gatherer"
    echo ""
    echo "********************************************************"
    exit 1
endif

echo "Building the frontend schema for $1" | tee -a ${LOG}
echo ${FEMOVER}/control/buildDB.sh postgres -G $1 >>& ${LOG}
${FEMOVER}/control/buildDB.sh postgres -G $1 >>& ${LOG}
if ( $status != 0 ) then
    echo "${SCRIPT_NAME} failed" | tee -a ${LOG}
    date | tee -a ${LOG}
    exit 1
endif

echo "${SCRIPT_NAME} completed successfully" | tee -a ${LOG}
date | tee -a ${LOG}
exit 0
