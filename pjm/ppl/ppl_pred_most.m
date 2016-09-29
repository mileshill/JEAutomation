#!/usr/local/bin/WolframScript -script
(* ppl_pred_most.m:
    Loops over premises passed in `fileName`:
        1) If premise has usage history greater than 1 year then make icap prediciton
        2) write out results to file

*)
BeginPackage["recPPL`",{"DatabaseLink`","DBConnect`"}];

(* MathKernel -script scriptName fileName *)
If[Length @ $CommandLine != 4,
    Throw[$Failed]; Return[1],
    Nothing
];

(* File contains only premisesIds. See run_calcs.sh for kernel invocation *)
fileName = $CommandLine[[4]];


(* Ensure connection to the database  *)
conn = JEConnection[];
If[ Not @ MatchQ[conn, _SQLConnection],
    Throw[$Failed]; Return[1],
    Nothing
];


(******************** Queries ********************)

(* load reconcilation factor: year -> 5 vector *)
SQLExecute[conn,"select CAST(CPYearID-1 as VARCHAR), ParameterValue
    from SystemLoad
    where UtilityID = 'PPL'"]//
    <|"Year"->#,"ReconFactor"->#2|>&@@@#&//
    GroupBy[#,#Year&]&//
    Map[Merge[Identity]]//
    Map[#ReconFactor&]//
    (Clear@reconFactor;reconFactor=#)&;


(* load loss factor: {year, rateClass} -> scalar *)
SQLExecute[conn,"select CAST(c.CPYearID-1 as VARCHAR), u.RateClass, u.Strata, 
    u.ParameterID, u.ParameterValue
    from UtilityParameterValue u
    inner join CoincidentPeak as c
      on c.CPID = u.CPID
      and c.UtilityID = u.UtilityID
    where u.UtilityID = 'PPL'
        and u.ParameterID = 'Loss Factor'
    "]//
    <|"Year"->#,"RateClass"->#2,"Strata"->#3,"LossFactor"->#5|>&@@@#&//
    GroupBy[#,{#Year,#RateClass}&]&//
    Map[KeyDrop[{"Year","RateClass","Strata"}]]//
    Map[#LossFactor&@First@#&]//
    (Clear@lossFactor;lossFactor=#)&;

(* premise history query for loop:
    {year, month, day, hourEnding, weekdayQ?, usage, rateClass, strata}
    
    weekdayQ: (day of week is Mon-Friday)? 1 : 0;
    
    *)
queryTemp = StringTemplate[
        "select Year(h.UsageDate),
            Month(h.UsageDate),
            Day(h.UsageDate),
            h.HourEnding,
            iif(DatePart(weekday, h.Usage) in (1, 7), 1, 0),
            h.Usage,
            p.RateClass, p.Strata
        from HourlyUsage as h
        inner join Premise as p
            on p.UtilityId = h.UtilityId
            and p.PremiseId = h.PremiseId
        where h.UtilityId = 'PPL'
            and h.PremiseId = '`premise`'
            and Month(h.UsageDate) in (6,7,8,9)"];

(******************** Premise Import ********************)
premises = Rest @ StringSplit @ Import[fileName,"Text"];


(******************** Prediction Loop ********************)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];

stdout = Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#, ", "]]&;
labels = {
    "RunDate", "RunTime","Premise", "Year", "RateClass", "Strata",
    "PredictedICap", "Uncertainty", "TrainingYears", "TrainingSamples"};

(* Write the header *)
writeFunc @ labels;

Do[
    records = SQLExecute[conn, queryTemp[<|"premise" -> premItr|>]];
    sampleCount = Length @ records;
    yearCount = Length @ Union @ records[[All, 1]];
    
    If[ yearCount >= 2, Continue[]];
    
    maxYear = Last @ Union @ records[[All, 1]];
    {rateClass, strata} = {#, Sequence @ #2}& @@ records[[1, -2;;]];
    
    (* drop all maxYear records *)
    newRecords = DeleteCases[records, {maxYear, __}];
    trainingData = N[ #[[All, ;;5]] -> #[[All, 6]] ]& @ newRecords;
    yearCount = Length @ Union @ newRecords[[All, 1]];
    maxYear = Max @ Union @ newRecords[[All, 1]];

    utilVector = reconFactor[ToString @ maxYear] * lossFactor[{ToString @ maxYear, rateClass}];
    predictTREE = Predict[trainingData, Method -> "RandomForest", PerformanceGoal -> "Quality"];

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
    (* Compute *)
    icapTREE = Mean /@ {summerTREEPred * utilVector, summerTREEUnc * utilVector};

    {icap, icapUnc} = {Sequence @ #, Sequence @ #2}& @@ If[Head@#===Times,First@#,#]& /@ icapTREE; 
    results = {runDate, runTime, premItr, maxYear+1, rateClass,  strata, icap, icapUnc, yearCount, sampleCount};
    writeFunc @ results;

    ,{premItr, premises[[;;10]]}
 ];
    
EndPackage[];
Quit[];
