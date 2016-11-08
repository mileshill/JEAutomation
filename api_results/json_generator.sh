#!/usr/bin/env bash

# json_generator
#   Calls the Wolfram Language routine that converts all local CSV files into JSON for export
#   to the UI. 
#
#   INPUT: mma_csv_json.m locates all necessary files
#   OUTPUT: writes output to /usr/share/tomcat6/webapps/json/<fileName>

:<<'END'
if [[ ! -z $1 ]] && [[ -f $1 ]]; then
    JSON_PATH=/usr/share/tomcat6/webapps/json/premise_records.json
    MathKernel -script $1 > ${JSON_PATH}
else
    echo "Usage: $0 script.m"  >&2
fi
END
#################### Transformations
#
# Input:
#   Multiple CSV files with varying NF
#
# Output:
#   JSON
#       { ISO: {
#            Util: 
#                recipe:{}, 
#                forecast{},
#                premise:{
#                    premId:{
#                        recipe:{}, 
#                        forecast:{}, 
#                        records{
#                            year_1:{}, 
#                            year_2:{}
#                        }
#                    }
#                }
#            }
#        } 
#
# File transforms by file:
#   Files will be grouped using their first three columns; Util, Prem, Year. 
# All input files need matched against their file type and then truncated into
# correct form. Column transformations are listed below.
#
#   *_rec.csv:  (Util, Prem, Year, RecICap, ISO, RateClass, Strata) =  $4 $5 $6 $10 $3 $7 $8            // len=7
#   *_pred.csv: (Util, Prem, Year, PredICap, ICapUnc, YearCount, NumSamp) = $4 $5 $6 $9 $10 $11 $12     // len=7
#   historical.csv: (Util, Prem, Year, HistICap) = $1 $2 $3 $4                                          // len=4


# Steps to Completion
#   1) What type of file?
#   2) Apply the transform
#   3) Concatenate results
#   4) Clean up temp 
#   5) Launch the aggregation process
#   6) Move results to tomacat directory for web requests

# Loop over files
for file in "$@"; do
    if [[ $file =~ ^.*pred\.csv$ ]]; then
        echo "Prediction: $file"
        #$(predict_transform $file)
    elif [[ $file =~ ^.*rec\.csv$ ]]; then
        echo "Recipe: $file"
        #$(recipe_transform $file))
    elif [[ $file ]]
        echo "No match $file"
    fi
done
