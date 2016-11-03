#!/usr/bin/env bash

# json_generator
#   Calls the Wolfram Language routine that converts all local CSV files into JSON for export
#   to the UI. 
#
#   INPUT: mma_csv_json.m locates all necessary files
#   OUTPUT: writes output to /usr/share/tomcat6/webapps/json/<fileName>

if [[ ! -z $1 ]] && [[ -f $1 ]]; then
    JSON_PATH=/usr/share/tomcat6/webapps/json/premise_records.json
    MathKernel -script $1 > ${JSON_PATH}
else
    echo "Usage: $0 script.m"  >&2
fi
