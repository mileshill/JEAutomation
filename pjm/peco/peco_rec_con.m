#!/usr/local/bin/WolframScript -script

BeginPackage["PecoConsumption`",{"DatabaseLink`", "DBConnect`"}];

(*
Consumption meters require four factors to compute the Icap.
Factors are keyed by RateClass, Strata, and Year.

icap = strataLoadShape * rateClassLossFactor * summerScaling * PLCScaling 

*)


(* connect to the database *)
conn = JEConnection[];
If[Not @ MatchQ[conn, _SQLConnection],
    Throw[$Failed]; Return[1],
    Nothing
];

(* list of queries *)

(* sls: {RateClass, Strata} -> Loadshape *)
strataLoadShapeQ = "select distinct 
	p.RateClass, p.Strata,
	(sls.ConstantCoefficient + 99 * sls.LinearCoefficient) as LoadShape
from SeasonLoadShape as sls
inner join Premise as p
	on p.RateClass = sls.RateClass
	and p.Strata = sls.Strata
where sls.DayType = 'WEEKDAY'
	and sls.Season = 'Summer'
	and sls.HourEnding = 17
	and sls.Segment = 4
	and sls.UpBandEffTemp = 200";

(* rclf: {Year, RateClass, Strata} -> rclf *)
rclfQ = "select distinct 
	Cast(Year(StartDate) as varchar), 
	u.RateClass, u.Strata, 
	(1 + u.ParameterValue/100.) as RCLF
from UtilityParameterValue as u
inner join CoincidentPeak as c
	on c.CPID = u.CPID
where u.ParameterId = 'RateClassLoss'
	and u.UtilityId = 'PECO'";

(* summerSclaing: {Year, RateClass, Strata} -> summerScaling *)
summerScalingQ = "select distinct
	Cast(Year(StartDate) as varchar), 
	RateClass, Strata, 
	ParameterValue
from UtilityParameterValue
where UtilityId = 'PECO'
	and ParameterId = 'StrataSummerScale'";

(* PLC: {Year} -> ParameterValue *)
plcScalingQ = "select 
	Cast(CPYearId as varchar), ParameterValue
from SystemLoad
where UtilityId = 'PECO'
	and ParameterId = 'PLCScaleFactor'";

(* consumption records *)
recordsQ = "select distinct
	m.PremiseId, Cast((Year(m.StartDate) + 1) as varchar),
	p.RateClass, p.Strata
	from MonthlyUsage as m
	inner join Premise as p
		on p.UtilityId = m.UtilityId
		and p.PremiseId = m.PremiseId
	where m.UtilityId = 'PECO'";

(* execute queries and group *)
strataLoadShape = SQLExecute[conn, strataLoadShapeQ]//
	MapAt[StringTrim @ ToString @ #&, {All, ;;2}]//
	<| "RateClass" -> #1, "Strata" -> #2, "SLS" -> #3 |> & @@@ #&//
	GroupBy[#, {#RateClass, #Strata}&]& //
	Map[First] //
	Map[#SLS&];

rclf = SQLExecute[conn, rclfQ] //
	MapAt[StringTrim @ ToString @ #&, {All, ;;3}]//
	<|"Year"-> #1, "RateClass" -> #2, "Strata" -> #3, "SLS" -> #4 |> & @@@ #&//
	GroupBy[#, {#Year, #RateClass, #Strata}&]& //
	Map[First] //
	Map[#SLS&];

summerScaling = SQLExecute[conn, summerScalingQ] //
	MapAt[StringTrim @ ToString @ #&, {All, ;;3}]//
	<|"Year"-> #1, "RateClass" -> #2, "Strata" -> #3, "SS" -> #4 |>& @@@ #&//
	GroupBy[#, {#Year, #RateClass, #Strata}&]& //
	Map[First] //
	Map[#SS&];

plcScaling = SQLExecute[conn, plcScalingQ]//
	MapAt[StringTrim @ ToString @ #&, {All, 1}]//
	<|"Year" -> #, "PLC" ->#2|>& @@@ #&//
	GroupBy[#, #Year&]&//
	Map[First]//
	Map[#PLC&];

records = SQLExecute[conn, recordsQ] //
	Map[StringTrim @ ToString @ #&, #, {-1}]&;

stdout = Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#, ","]]&;

(* compute icap *)

(* time stamp *)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];
stdout=Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#,","]]&;

labels = {"RunDate", "RunTime", "ISO", "Utility", "PremiseId", "Year", "RateClass", "Strata", "MeterType", "RecipeICap"};
iso = "PJM";
utility = "PECO";
mType = "CON";

Do[
	{premId, year, rateClass, strata} = record;

	localSLS = Lookup[strataLoadShape, {{rateClass, strata}}, 0.] // First;
	localRCLF = Lookup[rclf, {{year, rateClass, strata}}, 0.] // First; 
	localSS = Lookup[summerScaling, {{year,rateClass,strata}}, 0.] // First; 
	localPLC = Lookup[plcScaling, year, 0.]; 
    
    icap = localSLS * localRCLF * localSS * localPLC;

    yearADJ = ToExpression[year] + 1; 
    results = {runDate, runTime, iso, utility, premId, yearADJ, rateClass, strata, mType, icap};

    writeFunc @ results;
	,{record, records}];



EndPackage[];
