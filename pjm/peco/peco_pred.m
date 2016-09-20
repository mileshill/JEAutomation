#!/usr/bin/env/WolframScript -script

Needs @ "DatabaseLink`";
Get @ "DBConnect.m";

conn =  JEConnection[];
If[ Not@ MatchQ[ conn, _SQLConnection ],
    Throw[$Failed]; Return[1],
    Nothing
];

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
                GroupBy[#,{#Year,#RateClass,#Strata}&]&//
                Map[Merge[Identity]]//
                Select[#,KeyExistsQ[#,"RateClassLoss"]&&KeyExistsQ[#,"NCRatio"]&]&//
                Select[#,Length@#NCRatio==5&]&//
                Map[#NCRatio* (1+#RateClassLoss[[1]]/100.)&]//
                (utilParams=#)&;

premises = Rest @ StringSplit @ Import["peco_premises.txt","Text"];

queryTemp = StringTemplate[
    "select YEAR(h.UsageDate), MONTH(h.UsageDate), Day(h.UsageDate), h.HourEnding, DatePart(weekday, h.UsageDate), h.Usage,
        p.RateClass, p.Strata
     from HourlyUsage as h
     inner join Premise as p
        on p.UtilityId = h.UtilityId
        and p.premiseId = h.Premiseid
     where h.UtilityId = 'PECO'
        and h.PremiseId = '`premise`'
        and MONTH(h.UsageDate) in (6,7,8,9)"];


stdout = Streams[] // First;
labels = {"Premise", "Year", "RateClass", "Strata", "PredictedICap", "Uncertainty", "TrainingYears", "TrainingSamples"};
Write[stdout, StringRiffle[ labels, ", "]]; 

Do[

    records = SQLExecute[conn, queryTemp[<|"premise"-> premItr|>]];
    sampleCount = Length @ records;
    yearCount = Length @ Union @ records[[All,1]];
    maxYear = Union[ records[[All,1]] ] // Last;
    {rateClass, strata} = records[[1,-2;;]];
    
    trainingData = (#[[All,;;5]] -> #[[All,6]])& @ records;
    predictTREE = Predict[ trainingData, Method -> "RandomForest" ];
    predictNN   = Predict[ trainingData, Method -> "NeuralNetwork"]; 

    ClearAll @ buildSummer;
    Attributes[buildSummer] = HoldFirst;
    buildSummer[func_]:= Outer[
        func[{maxYear+1, #, #2, #3, #4}, "Distribution"]&,
        Range[{6,9}],
        Range[31],
        Range[7],
        Range[{13,19}]
    ];

    {summerTREEPred, summerTREEUnc} = buildSummer[ predictTREE ] // 
        Flatten // TakeLargestBy[#, First, 5]& // List @@@ # & // Transpose ;

    {summerNNPred, summerNNUnc}  = buildSummer[ predictNN   ] // 
        Flatten // TakeLargestBy[#, First, 5]& // List @@@ # & // Transpose ;
    
    (* Utility Vector *)
    utilVector = utilParams[ToString /@ {maxYear-2, rateClass, strata}];

    (* Compute *)
    icapTREE = Mean /@ {summerTREEPred * utilVector, summerTREEUnc * utilVector};
    icapNN  = Mean/@ {summerNNPred * utilVector, summerNNUnc * utilVector };

    {icap, icapUnc} = Mean @ {icapTREE, icapNN};

    results = {premItr, maxYear+1, rateClass, strata, icap, icapUnc, yearCount, sampleCount};
    Write[stdout, StringRiffle[ results, ", " ]];

,{premItr, premises[[;;1]]}]




Return[0]
Quit[];
