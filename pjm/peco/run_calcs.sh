#!/bin/bash
# Use -gt 1 to consume two arguments per pass in the loop (e.g. each
# argument has a corresponding value to got with it.)
while [[ $# -gt 0 ]]
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
# Auxiliary functions
function add_date {
    echo "$(date '+%H:%M:%S')"
}
# If no utility given
if [ -z "${UTILITY}" ]; then
    UTILITY=$(basename $(pwd))
    echo "$(add_date) ${UTILITY} calculations started"
fi

# If clean directory
if [ "${OVERWRITE}" ]; then
    echo "$(add_date) Removing previous results."
    rm *.csv *.txt
fi

# Variable declarition
RECIPE="${UTILITY}_rec.m"
RECIPE_RESULT="${UTILITY}_rec.csv"
PREDICTION="${UTILITY}_pred.m"
PREDICTION_RESULT="${UTILITY}_pred.csv"

# Does the recipe exists?
if [ ! -e "${RECIPE}" ]; then
    echo "$(add_date) ${RECIPE} not found."
    exit
fi

# Has the recipe been run?
if [ ! -e "${UTILITY}_rec.csv" ]; then
    echo "$(add_date) Running recipe predictions"
    math -script ${RECIPE} > ${RECIPE_RESULT}
fi
# Store the unique premises
if [ ! -e "${UTILITY}_premises.txt" ]; then
    echo "$(add_date) Storing unique premises for prediction"
    cat ${RECIPE_RESULT} | awk -F ',' '{print $1 }' | uniq > ${UTILITY}_premises.txt
fi

# Run the prediction script on unique premises
if [ -e "${PREDICTION}" ] && [ ! -e ${UTILITY}_pred.csv ]; then
    echo "$(add_date) Running predictions on unique premises"
    math -script ${PREDICTION} > ${PREDICTION_RESULT}
    echo "$( cat ${PREDICTION_RESULT} | wc -l ) predictios made."
fi


echo "$(add_date) $(UTILITY) Recipe complete."
echo
ls -lth
