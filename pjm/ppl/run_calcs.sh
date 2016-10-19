#!/bin/bash
# Use -gt 1 to consume two arguments per pass in the loop (e.g. each
# argument has a corresponding value to got with it.)

# Logging and overwrite vars

UTILITY=$(basename $(pwd))
# Auxiliary functions
function record {
    echo "CST:$(date '+%Y_%m_%d_%H_%M_%S') UTILITY=${UTILITY} DESC=$1" >> ${LOGFILE}
}

# Variable declarition
RECIPE="${UTILITY}_rec.m"
RECIPE_RESULT="${UTILITY}_rec.csv"
UNIQ_PREM="${UTILITY}_premises.txt"
PREDICTION="${UTILITY}_pred_most.m"
PREDICTION_RESULT="${UTILITY}_pred_most.csv"
LOGFILE="./log/${UTILITY}.log"
OVERWRITE=0;

RESULT_DIR=./results/
COMPARE="pred_compare.m"
COMPARE_RESULT="${UTILITY}_pred_compare.csv"



:<<'END'
# If clean directory
if [ "${OVERWRITE}" ]; then
    find . -type f -not \( -name "*.m" -o -name "*.sh" \) -print0 | xargs -0 rm -f 
fi
END

# Does the recipe and prediction scripts exist?
if [ ! -e "${RECIPE}" ] && [ ! -e "${PREDICTION}" ] ; then
    $(record "No scripts found")
    exit
fi


# Has the recipe been run?
if [ ! -e "${RECIPE_RESULT}" ]; then 
    # run recipe and store result
    $(record "Recipe start")
    MathKernel -script ${RECIPE} > ${RECIPE_RESULT}
    # store unique premises
    $(record "Recipe end")
    cat ${RECIPE_RESULT} | awk -F ',' '{print $3}' | sort -n | uniq  > ${UNIQ_PREM}
    $(record "Unique premises: $(wc -l < ${UNIQ_PREM})")
fi

# Split the premise data and run predictions 
if [ -e "${UNIQ_PREM}" ] ; then
    $(record "Prediction start")
    $(record "Splitting premises")
    sh split_premises.sh ${UNIQ_PREM}
    touch ${PREDICTION_RESULT}
    find . -maxdepth 1 -name "prem_*" -print \
        | xargs -n 1 -P 2 -I {} sh -c "MathKernel -script ${PREDICTION} {} >> ${PREDICTION_RESULT}" 
    # check to ensure WolframKernels have terminated; remove split premise files; prem_*
    if [ ! "$(pgrep Wolfram)" ]; then
        rm prem_*
    fi
    echo "pred end"
    $(record "Prediction end")
fi

# Move results to ./Results
if [ ! -d "${RESULT_DIR}" ]; then
   $(record "Create ${RESULT_DIR}; move *.csv *.txt")
   mkdir ${RESULT_DIR} && mv -u *.csv *.txt ${RESULT_DIR}
else
    $(record "Move *.csv *.txt to ${RESULT_DIR}")
    mv -u *.csv *.txt ${RESULT_DIR}
fi

$(record "Complete")

echo >> ${LOGFILE}
