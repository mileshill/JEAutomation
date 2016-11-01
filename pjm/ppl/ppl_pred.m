BeginPackage["pplPredict`", {"DatabaseLink`", "DBConnect`"}];

If[ Length @ $CommandLine != 4,
    Return[1]
];
fileName = $CommandLine[[4]];
premises = Import[fileName, "Text", "Numeric"-> False] // StringSplit;

conn = JEConnection[];
If[ Not @ MatchQ[conn, _SQLConnection],
    Return[1]
];



(*#################### Query Strings ####################*)
recordQry = StringTemplate["select 
		Year(h.UsageDate),
		Month(h.UsageDate),
		Day(h.UsageDate),
		h.HourEnding,
		DatePart(weekday, h.UsageDate),
		h.Usage,
		p.RateClass, p.Strata
	from HourlyUsage as h
	inner join Premise as p
		on p.UtilityId = h.UtilityId
		and p.PremiseId = h.PremiseId
	where h.UtilityId = 'PPL'
		and h.PremiseId = '`premise`'
		and Month(h.Usage) in (6,7,8,9)
			"][<|"premise" -> #|>]&;

reconQry = "select 
        CAST(CPYearId-1 as varchar), 
        ParameterValue
    from SystemLoad
    where UtilityId = 'PPL'";

loadlossQry = "select 
		Cast(cp.CPYearID-1 as varchar),
		RTrim(pv.RateClass), 
		pv.ParameterValue
	from UtilityParameterValue as pv
	inner join CoincidentPeak as cp
		on cp.CPID = pv.CPID
		and cp.UtilityId = pv.UtilityId
	where pv.UtilityId = 'PPL'
		and pv.ParameterId = 'Loss Factor'";


(*#################### Query Execution ####################*)
(* System and Utility queries only. Premises queries called in loop *)

(* <|{year} -> {f_1, ..., f_5} |> *)
reconFactor = SQLExecute[conn, reconQry] //
    <|"Year" -> #, "Value" -> #2|>& @@@ #& //
    GroupBy[#, #Year&]& //
    Map[Merge[Identity]] //
    Map[#Value&];

(* <|{year, rateclass} -> lossFactor  |>  *)
loadlossFactor = SQLExecute[conn, loadlossQry]//
	Rule[{#, #2}, #3]& @@@ #&//
	Association;



(*#################### Icap Logic Loop ####################*)
(* time stamp *)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];

labels = {"RunDate", "RunTime", "ISO", "Utility", "PremiseId", "Year", "RateClass", "Strata", "PredictedICap",
    "ICapUnc", "YearCount", "NumSamples"};

util = "PPL";
iso = "PJM";
stdout=Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#,","]]&;

writeFunc @ labels;
Do[
	records = SQLExecute[conn, recordQry[premId]];
	sampleCount = Length @ records;

    If[ sampleCount == 0, Continue[]];
    
	yearCount = Length @ Union @ records[[All, 1]];
	maxYear = Last @ Union @ records[[All, 1]];
	{rateClass, strata} = records[[1, -2;;]];

    (* check for the correct years  *)
	If[maxYear != 2016, Continue[]];

    (* if utility vector fails, skip premise *)
    maxYearKey = ToString @ maxYear;
    localRecon = Lookup[reconFactor, {maxYearKey}, ConstantArray[0.,5]] // First;
    localLoss = Lookup[loadlossFactor, {{maxYearKey, rateClass}}, 0.] // First;


    utilVector = If[ Length @ localRecon == 5 && localLoss != 0.,
        localRecon * localLoss,
        $Failed];
    
    If[ FailureQ @ utilVector, Continue[]];
    
    (* prep the training data; convert to numeric;
        train the predictor;
        map predictor over summer;
        reap top 5 usage values;
    *)
	trainingData = N[#[[All, ;;5]] -> #[[All, 6]]]& @ records;

	predictTREE = Predict[trainingData, Method -> "RandomForest", PerformanceGoal -> "TrainingSpeed"];

    ClearAll @ buildSummer;
    Attributes[buildSummer] = HoldFirst;
    buildSummer[func_]:= Outer[
        func[{maxYear+1, #, #2, #3, #4}, "Distribution"]&,
        Range[6., 9],
        Range[1., 31],
        Range[1., 7],
        Range[13., 19]
    ];

    {summerTREEPred, summerTREEUnc} = buildSummer[predictTREE] //
        Flatten// TakeLargestBy[#, First, 5]& // List @@@ #& // Transpose;

  
    icapTREE = Mean /@ {summerTREEPred * utilVector, summerTREEUnc * utilVector};
    {icap, icapUnc} = If[Head @ # === Times, First @ #, #]& /@ icapTREE;

    results = {runDate, runTime, iso, util, premId, maxYear+1, rateClass, strata,
        icap, icapUnc, yearCount, sampleCount};

    writeFunc @ results;
,{premId, premises}]

EndPackage[];
Quit[];

