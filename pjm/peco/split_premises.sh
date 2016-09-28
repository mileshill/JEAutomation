#!/bin/sh

# split_premises:  split premises into 3 files
#
#   File splitting is limited to 3 files due to the 
# size MEM/CPU demands of the Wolfram Kernel.
# Each of the three files will be passed to the predict
# script.

function add_date(){
    echo "$(date '+%H:%M:%S')"
}


if [ -f "$1" ]; then
    echo "$(add_date) Splitting file: ${1}"
    LINE_COUNT="$(wc -l < ${1})"
    echo "$(add_date) The line count is: ${LINE_COUNT}"
    SPLIT_SIZE="$(( ${LINE_COUNT} / 3 ))"
    echo "$(add_date) The split size is: ${SPLIT_SIZE}"
    split -a 1 -l ${SPLIT_SIZE} $1 prem_
    tree
fi
