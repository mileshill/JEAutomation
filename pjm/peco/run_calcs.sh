#!/bin/bash
# Use -gt 1 to consume two arguments per pass in the loop (e.g. each
# argument has a corresponding value to got with it.)
while [[ $# -gt 1 ]]
do
    key="$1"
    case $key in
        -u |--utility)
            UTILITY="$2"
            shift
            ;;
        -o |--overwrite)
            OVERWRITE=true
            shift
            ;;
        *)
            ;;
    esac
    shift
done

# Clean directory
if [ "${OVERWRITE}" ]; then
    echo "Removing previous results."
    rm *.csv *.txt
fi
echo "${OVERWRITE}"

RECIPE="${UTILITY}_rec.m"
# Does the recipe exists?
if [ ! -e "${RECIPE}" ]; then
    echo "${RECIPE} not found."
    exit
fi

# Has the recipe been run?
if [ ! -e "${UTILITY}_rec.csv" ]; then
    echo "Running recipe predictions"
    math -script ${RECIPE} > ${UTILITY}_rec.csv
fi
# Store the unique premises
if [ ! -e "${UTILITY}_premises.txt" ]; then
    echo "Storing unique premises for prediction"
    cat ${UTILITY}_rec.csv | awk -F ',' '{print $1 }' | uniq > ${UTILITY}_premises.txt
fi

# Run the prediction script on unique premises
if [ -e ${UTILITY}_pred.m ] && [ ! -e ${UTILITY}_pred.csv ]; then
    echo "Running predictions on unique premises"
    echo "***** script goes here *****"
else
    echo "Script not found or prediction results exist"
fi


echo "Recipe complete"
echo
ls -lth
