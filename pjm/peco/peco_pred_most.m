#!/usr/bin/env/WolframScript -script

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
premises = Rest @ StringSplit @ Import["peco_premises.txt","Text"];

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
        and MONTH(h.UsageDate) in (6,7,8,9)
     order by Year(h.UsageDate), Month(h.UsageDate), Day(h.UsageDate)"];

(* Auxiliary function for summer predictions *)
ClearAll @ buildSummer;
Attributes[buildSummer] = HoldFirst;
buildSummer[func_]:= Outer[
    func[{maxYear+1, #, #2, #3, #4}, "Distribution"]&,
    Range[6.,9],
    Range[1.,31],
    Range[1.,7],
    Range[13.,19]
];


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
    uniqueYears = Union @ records[[All, 1]];
    yearCount = Length @ uniqueYears;
   
    Print[ records[[All, 1]] ];
    Break[];
    (* If only 1 year of data, break the cycle, else predict year 2 *)
    If[ yearCount == 1, Print[premItr, " Single Year of data; skipping"]; Continue[]];

    maxYear = Last @ uniqueYears; 
    {rateClass, strata} = records[[1, -2;;]];
    
    Print[premItr, " Sufficient Data"];


    (* split the records by year *)
    trainYears = Most @ uniqueYears;
    testYear = Last @ uniqueYears;
    trainingData = Cases[records, {Alternatives @@ trainYears,__}];
    testingData  = Complement[records, trainingData];

    Print["Training Length :", Length @ trainingData];
    Print["Testing  Length :", Length @ testingData];
    Print["Years       :", uniqueYears];

    Continue[];
   
    SQLExecute[conn, 
        StringTemplate[
            "select h.Usage
            from HourlyUsage as h
            inner join CoincidentPeak as cp
                on cp.UtilityId = h.UtilityId
                and cp.CPDate = h.UsageDate
                and cp.HourEnding = h.hourEnding
            where Cast(Year(cp.CPDate) as varchar) = '`maxYear`'
                and h.PremiseId = '`premise`'"
                    ][<|"maxYear"-> ToString @ maxYear, "premise"-> premItr|>]]//
        Print["Premise: ", premItr," CPValues ", #]&;

    Continue[];
    
    trainingData = N[ #[[All,;;5]] -> #[[All,6]] ]& @ records;
    predictTREE = Predict[ trainingData, Method -> "RandomForest", PerformanceGoal->"TrainingSpeed" ];
    predictNN   = Predict[ trainingData, Method -> "NeuralNetwork", PerformanceGoal->"TrainingSpeed"]; 

    {summerTREEPred, summerTREEUnc} = buildSummer[ predictTREE ] // 
    Flatten // TakeLargestBy[#, First, 5]& // List @@@ # & // Transpose ;

    {summerNNPred, summerNNUnc}  = buildSummer[ predictNN   ] // 
        Flatten // TakeLargestBy[#, First, 5]& // List @@@ # & // Transpose ;
    
    (* Utility Vector *)
    utilVector = utilParams[ToString /@ {maxYear-2, rateClass, strata}];

    (* Compute *)
    icapTREE = Mean /@ {summerTREEPred * utilVector, summerTREEUnc * utilVector};
    icapNN  = Mean  /@ {summerNNPred * utilVector, summerNNUnc * utilVector };

    {icap, icapUnc} = If[Head@#===Times,First@#,#]& /@ Mean[ {icapTREE, icapNN} ];

    results = {premItr, maxYear+1, rateClass, Sequence @ strata, Sequence @ icap, icapUnc, yearCount, sampleCount};
    Write[stdout, StringRiffle[ results, ", " ]];

,{premItr, premises[[;;50]]}]




Return[0]
Quit[];
