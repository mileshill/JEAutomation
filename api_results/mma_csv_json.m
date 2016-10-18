#! /usr/bin/local/WolframScript -script

BeginPackage["CSVtoJSON`"];

(*#################### Import all CSV Files and Join #################### *)
CSVImport = Import[#, "CSV", "Numeric" -> False]&;
fileNames =  FileNames["*.csv"];

allData = Map[CSVImport, fileNames]//
    Flatten[#, 1]& //
    Select[#, Length @ # == 8&]&;

(* ExportString is restrictive with output. Filter is used to clean up
the last argument of each record. Anything that is non-numeric will be
converted to a zero.
*)
Filter[{first__, last_}] /; Not[NumericQ @ ToExpression @ last] := {first, 0.0};
Filter[{first__, last_}]:= {first, ToExpression @ last} 

newData = Map[Filter, allData];
(*#################### Transform into Records and Group for Export #################### *)

header = {"RunDate", "RunTime", "UtilityId", "PremiseId", "Year", "RateClass", "Strata", "RecipeICap"};
records = Map[AssociationThread[header -> #]&, newData];

exportString = GroupBy[records, {#UtilityId&, #PremiseId&, #Year&}]//
    ExportString[<|"PJM" -> #|>, "JSON", "Compact" -> True]&;

Print @ exportString;

EndPackage[];
Quit[];
