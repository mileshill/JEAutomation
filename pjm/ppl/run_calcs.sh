#!/bin/bash
# Use -gt 1 to consume two arguments per pass in the loop (e.g. each
# argument has a corresponding value to got with it.)

# Logging and overwrite vars

:<<'END'
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
END
#################### Var Declaration #################### 
# Utility Level Vars
UTILITY=$(basename $(pwd))
LOGFILE="./log/${UTILITY}.log"
OVERWRITE=0;
RESULT_DIR=./results/

RECIPE_RESULTS="${UTILITY}_rec.csv"

# Interval
INT_REC="${UTILITY}_rec_int.m"
INT_REC_RES="${UTILITY}_rec_int.csv"
INT_PRED="${UTILITY}_pred.m"
INT_PRED_RES="${UTILITY}_pred.csv"
INT_UNIQ="${UTILITY}_premises_int.txt"
INT_ALIAS="INT"

# Could this be junked?
COMPARE="pred_compare.m"
COMPARE_RESULT="${UTILITY}_pred_compare.csv"

#################### Auxiliary Functions ####################  
# Auxiliary functions
function record {
    echo "CST:$(date '+%Y_%m_%d_%H_%M_%S') UTILITY=${UTILITY} DESC=$1" >> ${LOGFILE}
}

# recipe_calc
# Computes RECIPE, stores in RESULT, and determines UNIQue premies; UNQ passed to predict  
function recipe_calc {
    RECIPE="$1"
    RESULT="$2"
    UNQ="$3"
    if [ ! -e "${RESULT}" ]; then 
        # run recipe and store result
        $(record "Recipe start")
        MathKernel -script ${RECIPE} >> ${RESULT}
        # store unique premises
        $(record "Recipe end")
        # print premise id and store uniques
        cat ${RESULT} | awk -F ',' '{print $5}' | sort -n | uniq  > ${UNQ}
        $(record "Unique premises: $(wc -l < ${UNQ})")
        cat ${RESULT} >> ${RECIPE_RESULTS}
    fi
}

# predict:
# PREDicts  icap UNIQue for premises and pipes into RESULT
function predict {
    PRED="$1"
    RESULT="$2"
    UNQ="$3"
    ALIAS="$4"
    if [ -e "${UNQ}" ] ; then
        $(record "Prediction start")
        $(record "Splitting premises")
        sh split_premises.sh ${UNQ} ${ALIAS}
        touch ${RESULT}
        find . -maxdepth 1 -name "${ALIAS}*.tmp" -print0 | xargs -0 -n 1 -P 2 -I {} sh -c "MathKernel -script ${PRED} {} >> ${RESULT}" 
        $(record "Prediction end")
    fi
}

#################### Recipe #################### 
# Interval recipe and uniq premises
$(recipe_calc ${INT_REC} ${INT_REC_RES} ${INT_UNIQ})

#################### Predictions #################### 
# Interval 
$(predict ${INT_PRED} ${INT_PRED_RES} ${INT_UNIQ} ${INT_ALIAS})

# Monthly with demand
#$(predict ${DMD_PRED} ${DMD_PRED_RES} ${DMD_UNIQ})

# Monthly consumption
#$(predict ${CON_PRED} ${CON_PRED_RES} ${CON_UNIQ})

#################### Cleanup and S3 Export #################### 
# cat recipe results; cat prediction results; move to ./results; send to S3

if [ ! -d "${RESULT_DIR}" ]; then
   $(record "Create ${RESULT_DIR}; move *.csv *.txt")
   mkdir ${RESULT_DIR} && mv -u *.csv *.txt ${RESULT_DIR}
else
    $(record "Move *.csv *.txt to ${RESULT_DIR}")
    mv -u *.csv *.txt ${RESULT_DIR}
    find . -maxdepth 1 -type f -name "*.tmp" -print0 | xargs -0 -I {} sh -c "rm {}"
fi

$(record "Complete")
echo >> ${LOGFILE}
