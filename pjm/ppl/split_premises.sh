#!/bin/sh

# split_premises:  split premises into 2 files
#
#   File splitting is limited to 3 files due to the 
# size MEM/CPU demands of the Wolfram Kernel.
# Each of the three files will be passed to the predict
# script.

function add_date(){
    echo "$(date '+%H:%M:%S')"
}



if [ -f "$1" ] && [[ ! -z "${2}" ]] ; then
    LINE_COUNT="$(wc -l < ${1})"
    SPLIT_SIZE="$((( ${LINE_COUNT} / 2) + 1))"
    split -a 1 -l ${SPLIT_SIZE} --additional-suffix=.tmp "${1}" "${2}_"
fi
