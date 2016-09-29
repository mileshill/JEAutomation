#!/bin/sh

# split_premises:  split premises into 2 files
#
#   File splitting is limited to 3 files due to the 
# size MEM/CPU demands of the Wolfram Kernel.
# Each of the three files will be passed to the predict
# script.


if [ -f "$1" ]; then
    LINE_COUNT="$(wc -l < ${1})"
    SPLIT_SIZE="$(( ${LINE_COUNT} / 2 ))"
    split -a 1 -l ${SPLIT_SIZE} $1 prem_
fi
