#!/bin/bash
# Use -gt 1 to consume two arguments per pass in the loop (e.g. each
# argument has a corresponding value to got with it.)

OVERWRITE=false
while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        -o |--overwrite)
            OVERWRITE=true
            shift
            ;;
        -p|--predict)
            ONLY_PREDICT=true
            shift
            ;;
        -r|--recipe)
            ONLY_RECIPE=true
            shift
            ;;
        -u |--utility)
            UTILITY="$2"
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
    rm *pred.csv *.txt
fi

# Variable declarition
RECIPE="${UTILITY}_rec.m"
RECIPE_RESULT="${UTILITY}_rec.csv"
UNIQ_PREM="${UTILITY}_premises.txt"
PREDICTION="${UTILITY}_pred.m"
PREDICTION_RESULT="${UTILITY}_pred.csv"
COMPARE_SCRIPT="pred_compare.m"
COMPARE_RESULT="${UTILITY}_pred_compare.csv"

# Does the recipe and prediction scripts exist?
if [ ! -e "${RECIPE}" ] && [ ! -e "${PREDICTION}" ] ; then
    echo "$(add_date) Scripts not found."
    exit
fi

# Has the recipe been run?
if [ ! -e "${RECIPE_RESULT}" ]; then 
    # run recipe and store result
    echo "$(add_date) Running recipe calculations."
    MathKernel -script ${RECIPE} > ${RECIPE_RESULT}
    # store unique premises
    echo "$(add_date) Storing unique premises for prediction"
    cat ${RECIPE_RESULT} | awk -F ',' '{print $1 }' | uniq > ${UNIQ_PREM}
    echo "$(add_date) $(cat ${UNIQ_PREM} | wc -l) unique premises"
fi

# Run the prediction script on unique premises
if [ ! -e "${PREDICTION_RESULT}" ]; then 
    echo "$(add_date) Running predictions on unique premises"
    MathKernel -script ${PREDICTION} > ${PREDICTION_RESULT}
fi

# Comparison against available historical
if [ -e "${COMPARE_SCRIPT}" ]; then
    echo "$(add_date) Running ${COMPARE_SCRIPT}"
    MathKernel -script ${COMPARE_SCRIPT} > ${COMPARE_RESULT} 
fi

echo "$(add_date) ${UTILITY} Recipe complete."
echo
ls -lth
