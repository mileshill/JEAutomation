#!/usr/local/bin/WolframScript -script

BeginPackage["recPECO`",{"DatabaseLink`","DBConnect`"}];

If[ Length @ $CommandLine != 4,
    Throw[$Failed]; Return[1],
    Nothing
];

fileName = $CommandLine[[4]];


conn =  JEConnection[];
If[ Not@ MatchQ[ conn, _SQLConnection ],
    Throw[$Failed]; Return[1],
    Nothing
];



(* Import the Utility Parameters *)
SQLExecute[conn,"select  
        Cast(Year(pv.StartDate)-1 as VARCHAR), 
        Replace(pv.RateClass,' ',''), 
        Cast(pv.Strata as VARCHAR), pv.ParameterID, pv.ParameterValue
        from UtilityParameterValue as pv
        where pv.UtilityID = 'PECO'
        and (pv.ParameterId = 'NCRatio'
        or  pv.ParameterId = 'RateClassLoss'
        or  pv.ParameterId = 'NormalKW'
        or  pv.ParameterId = 'CoincidentKW')"]//
    <|"Year"->#,"RateClass"->#2,"Strata"->#3,#4->#5|>&@@@#&//
    GroupBy[#, {#Year, #RateClass, #Strata}&]&//
    Map[Merge[Identity]]//
    Select[#, KeyExistsQ[#, "RateClassLoss"] && KeyExistsQ[#, "NCRatio"]&]&//
    Select[#, Length @ #NCRatio == 5&]&//
    Map[#NCRatio * (1 + #RateClassLoss[[1]] / 100.)&]//
    (utilParams=#)&;

(* 
Premises are generated from the recipe calculations. BASH script determines
all unique premises ids and stores them in local file.
*) 
premises = Rest @ StringSplit @ Import[fileName, "Text"];

(* Template accepts PremiseID as parameter. Selects all years of summer usage data *)
queryTemp = StringTemplate[
    "select YEAR(h.UsageDate), 
        MONTH(h.UsageDate), 
        Day(h.UsageDate), 
        h.HourEnding, 
        DatePart(weekday, h.UsageDate), 
        h.Usage,
        p.RateClass, p.Strata
     from HourlyUsage as h
     inner join Premise as p
        on p.UtilityId = h.UtilityId
        and p.premiseId = h.Premiseid
     where h.UtilityId = 'PECO'
        and h.PremiseId = '`premise`'
        and MONTH(h.UsageDate) in (6,7,8,9)"];

(* Assign the stdout stream and Row 1 for the CSV outputfile *)
stdout = Streams[][[1]];
labels = {
    "Premise", "Year", "RateClass", "Strata", 
    "PredictedICap", "Uncertainty", "TrainingYears", "TrainingSamples"};

Write[stdout, StringRiffle[ labels, ", "]]; 



(* Loop over premises; train predictor and predict summer values *)
Do[

    records = SQLExecute[conn, queryTemp[<|"premise"-> premItr|>]];
    sampleCount = Length @ records;
    yearCount = Length @ Union @ records[[All,1]];
    
    (* only train for premises with 2 years *)
    If[ yearCount != 2, Continue[]];

    maxYear = Union[ records[[All, 1]] ] // Last; 
    {rateClass, strata} = records[[1, -2;;]];
   

    
    (* drop all maxYear records *)
    (* Print[premIter," Original record count: ", Length @ records];*)

    newRecords = DeleteCases[records, {maxYear,__}];

    (* Print[premIter," New record count: ", Length @ newRecords]; *)
    (* Print[premIter," Year Count Old: ", yearCount, " Year Count New: ", Length @ Union @ newRecords[[All,1]]];*)
    (* Print["Old max year: ", maxYear, " New max: ", Max @ Union @ newRecords[[All, 1]]]; *)
    
    trainingData = N[ #[[All,;;5]] -> #[[All,6]] ]& @  newRecords;
    yearCount = Length @ Union @ newRecords[[All, 1]];

    (* Build predictors *)
    predictTREE = Predict[ trainingData, Method -> "RandomForest", PerformanceGoal->"TrainingSpeed"];

    (* predictNN   = Predict[ trainingData, Method -> "RandomForest", PerformanceGoal->"Memory"];*)

    maxYear = Max @ Union @ newRecords[[All, 1]];
    ClearAll @ buildSummer;
    Attributes[buildSummer] = HoldFirst;
    buildSummer[func_]:= Outer[
        func[{maxYear+1, #, #2, #3, #4}, "Distribution"]&,
        Range[6.,9],
        Range[1.,31],
        Range[1.,7],
        Range[13.,19]
    ];

    {summerTREEPred, summerTREEUnc} = buildSummer[ predictTREE ] // 
        Flatten // TakeLargestBy[#, First, 5]& // List @@@ # & // Transpose ;

    (*
    {summerNNPred, summerNNUnc}  = buildSummer[ predictNN   ] // 
        Flatten // TakeLargestBy[#, First, 5]& // List @@@ # & // Transpose ;
    *) 

    (* Utility Vector *)
    utilVector = utilParams[ToString /@ {maxYear+1, rateClass, strata}];

    (* Compute *)
    icapTREE = Mean /@ {summerTREEPred * utilVector, summerTREEUnc * utilVector};

    (* icapNN  = Mean  /@ {summerNNPred * utilVector, summerNNUnc * utilVector };*)

    {icap, icapUnc} = If[Head@#===Times,First@#,#]& /@ icapTREE; 
    (*Mean[ {icapTREE, icapNN} ];*)

    results = {premItr, maxYear+1, rateClass, Sequence @ strata, Sequence @ icap, icapUnc, yearCount, sampleCount};
    Write[stdout, StringRiffle[ results, ", " ]];

,{premItr, premises}]




EndPackage[];
Quit[];
