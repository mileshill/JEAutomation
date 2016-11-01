#!/usr/bin/env/WolframScript -script
BeginPackage["pplPredict`"];

If[ Length @ $CommandLine != 4,
    Throw[$Failed]; Return[1],
    Nothing
];

fileName = $CommandLine[[4]]

Needs @ "DatabaseLink`";
Get @ "DBConnect.m";

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
        where pv.UtilityID = 'PPL'
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
     where h.UtilityId = 'PPL'
        and h.PremiseId = '`premise`'
        and MONTH(h.UsageDate) in (6,7,8,9)"];

(* time stamp *)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];

labels = {"RunDate", "RunTime", "PremiseId", "Year", "RateClass", "Strata", "PredictedIcap", "IcapUnc", "YearCount", "SampleCount"};
stdout=Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#,","]]&;

writeFunc @ labels;
(* Loop over premises; train predictor and predict summer values *)
Do[

    records = SQLExecute[conn, queryTemp[<|"premise"-> premItr|>]];
    sampleCount = Length @ records;
    yearCount = Length @ Union @ records[[All,1]];
    maxYear = Union[ records[[All,1]] ] // Last;
    {rateClass, strata} = records[[1,-2;;]];
    
    If[ maxYear != 2016, Continue[]];

    trainingData = N[ #[[All,;;5]] -> #[[All,6]] ]& @ records;
    predictTREE = Predict[ trainingData, Method -> "RandomForest", PerformanceGoal->"TrainingSpeed" ];
    (*predictNN   = Predict[ trainingData, Method -> "NeuralNetwork", PerformanceGoal->"TrainingSpeed"]; *)

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

    (*{summerNNPred, summerNNUnc}  = buildSummer[ predictNN   ] // 
        Flatten // TakeLargestBy[#, First, 5]& // List @@@ # & // Transpose ;*)
    
    (* Utility Vector *)
    utilVector = utilParams[ToString /@ {maxYear-2, rateClass, strata}];

    (* Compute *)
    icapTREE = Mean /@ {summerTREEPred * utilVector, summerTREEUnc * utilVector};
    (*icapNN  = Mean  /@ {summerNNPred * utilVector, summerNNUnc * utilVector };*)

    {icap, icapUnc} = If[Head@#===Times,First@#,#]& /@ icapTREE;

    results = {runDate, runTime, premItr, maxYear+1, rateClass, Sequence @ strata, Sequence @ icap, icapUnc, yearCount, sampleCount};
    writeFunc @ results;

,{premItr, premises}]




Return[0];
EndPackage[];
Quit[];
