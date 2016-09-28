#!/bin/bash
# Use -gt 1 to consume two arguments per pass in the loop (e.g. each
# argument has a corresponding value to got with it.)

# Logging and overwrite vars

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        -o |--overwrite)
            OVERWRITE=1
            shift
            ;;
        -p|--predict)
            ONLY_PREDICT=1
            shift
            ;;
        -r|--recipe)
            ONLY_RECIPE=1
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

# If no utility given
if [ -z "${UTILITY}" ]; then
    UTILITY=$(basename $(pwd))
fi


# Auxiliary functions
function add_date {
    echo "$(date '+%Y_%m_%d_%H_%M_%S')"
}

# Variable declarition
RECIPE="${UTILITY}_rec.m"
RECIPE_RESULT="${UTILITY}_rec.csv"
UNIQ_PREM="${UTILITY}_premises.txt"
PREDICTION="${UTILITY}_pred_most.m"
PREDICTION_RESULT="${UTILITY}_pred_most.csv"
LOGFILE="./log/${UTILITY}.log"
OVERWRITE=0;
COMPARE="pred_compare.m"
COMPARE_RESULT="${UTILITY}_pred_compare.csv"

# If clean directory
if [ "${OVERWRITE}" ]; then
    echo "$(add_date) Removing previous results." >> ${LOGFILE}
    find . -type f -not \( -name "*.m" -o -name "*.sh" \) -print0 | xargs -0 rm -f 
fi

# Does the recipe and prediction scripts exist?
if [ ! -e "${RECIPE}" ] && [ ! -e "${PREDICTION}" ] && [ ! -e "${COMPARE}" ] ; then
    echo "$(add_date) Scripts not found." >> ${LOGFILE}
    exit
fi

# Has the recipe been run?
if [ ! -e "${RECIPE_RESULT}" ]; then 
    # run recipe and store result
    echo "$(add_date) ${UTILITY} calculations started." >> ${LOGFILE}
    echo "$(add_date) Running ${RECIPE}." >> ${LOGFILE}
    MathKernel -script ${RECIPE} > ${RECIPE_RESULT}
    # store unique premises
    echo "$(add_date) Storing unique premises for prediction" >> ${LOGFILE}
    cat ${RECIPE_RESULT} | awk -F ',' '{print $1}' | uniq > ${UNIQ_PREM}
    echo "$(add_date) $(cat ${UNIQ_PREM} | wc -l) unique premises" >> ${LOGFILE}
fi

# Split the premise data and run predictions 
if [ -e "${UNIQ_PREM}" ] ; then
    echo "$(add_date) Premise split for prediciton" >> ${LOGFILE}
    sh split_premises.sh ${UNIQ_PREM}
    touch ${PREDICTION_RESULT}
    find . -name "prem_*" -print | xargs -I {} sh -c "MathKernel -script ${PREDICTION} {} >> ${PREDICTION_RESULT}"
    # check to ensure WolframKernels have terminated; remove split premise files; prem_*
    if [ ! "$(pgrep Wolfram)" ]; then
        rm prem_*
    fi
    echo "$(add_date) Prediciton count: $(wc -l < ${PREDICTION_RESULT})" >> ${LOGFILE}
fi

:<<'END'
# Run the prediction script on unique premises
if [ ! -e "${PREDICTION_RESULT}" ]; then 
    echo "$(add_date) Running ${PREDICTION}"
    MathKernel -script ${PREDICTION} > ${PREDICTION_RESULT}
fi

# Comparison against available historical
if [ ! -e "${COMPARE_RESULT}" ] ||  [ "${PREDICTION_RESULT}" -nt  "${COMPARE_RESULT}" ]; then
    echo "$(add_date) Running ${COMPARE}"
    MathKernel -script ${COMPARE} > ${COMPARE_RESULT} 
fi
END

echo "$(add_date) ${UTILITY} Recipe complete." >> ${LOGFILE}
echo
ls -lth
