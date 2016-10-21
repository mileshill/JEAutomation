#!/usr/bin/env bash

# run_all_calcs
#   Locates all run_calcs.sh scripts in utility directories.
#   Navigates into utility directories and runs run_calcs.sh
#   Completes by running 'jeUpdate' which syncs S3, rebuilds output JSON for UI
#       and updates github repo

OLD_DIR=$(pwd)
JE_DIR=~/Projects/JustEnergyCalcs

# Check for running WolframKernels. If found, do not run computation.
WOLF_KERN=$(pgrep WolframKernel | wc -l)
if [[ "$WOLF_KERN" -gt 0 ]]; then
    echo "WolframKernels currently active: ${WOLF_KERN}" >&2  
    echo "Must wait until kernel(s) completes" >&2
    exit 1
fi


# Locate all run_calcs.sh and run inside their local directory
SCRIPTS=$(cd $JE_DIR ; find . -type f -name "run_calcs.sh"; cd $OLD_DIR)
for RUN_SCRIPT in $SCRIPTS
do
    FILE_PATH=$(dirname $RUN_SCRIPT)   # the full directory path
    FILE_NAME=$(basename $RUN_SCRIPT)  # the file name  

    cd $FILE_PATH           # move into utility directory
    bash $FILE_NAME         # run the 'run_calc.sh' script in utility directory
done

# run the update function
jeUpdate

# return to original directory
cd $OLD_DIR
