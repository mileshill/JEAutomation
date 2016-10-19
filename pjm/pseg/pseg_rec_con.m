#!/usr/bin/local/WolframScript -script

BeginPackage["PSEG`", {"DatabaseLink`", "DBConnect`"}];


(* #################### Queries #################### *)
recordQuery = "select 
    m.PremiseId, 
	Cast(Year(m.EndDate) as varchar), 
	RTrim(p.RateClass), RTrim(p.Strata),
    m.Usage,
	DateDiff(hour, m.StartDate, m.EndDate) as NumHour
from MonthlyUsage as m
inner join Premise as p
	on p.UtilityId = m.UtilityId
	and p.PremiseId = m.PremiseId
where m.UtilityId = 'PSEG'
    and (m.Demand = 0 or m.Demand is Null)
	and Month(m.EndDate) in (6,7,8,9)
    --and m.PremiseId = 'PE000008120852292456'
    --and m.PremiseId = 'PE000007912140663664'
    --and m.PremiseId = 'PE000008711916061850'";

utilityQuery = "select distinct 
		CAST((c.CPYearID - 1) as varchar), 
		RTrim(u.RateClass), RTrim(u.Strata),
		u.ParameterValue
	from UtilityParameterValue as u
	inner join CoincidentPeak as c
		on c.CPID = u.CPID
        and c.UtilityId = u.UtilityId
	where u.UtilityId = 'PSEG'
        and u.ParameterId in ('GenCapScale', 'LossExpanFactor', 'CapProfPeakRatio')";

systemQuery = "select 
	Cast(CPYearId-1 as varchar) as Year, 
	Exp(Sum(Log(ParameterValue))) as PFactor
from SystemLoad
where UtilityId = 'PSEG'
	and ParameterId in ('CapObligScale', 'ForecastPoolResv', 'FinalRPMZonal')
group by Cast(CPYearId-1 as varchar)"

(* #################### Execute Queries #################### *)
conn = JEConnection[];
If[Not @ MatchQ[conn, _SQLConnection],
    Throw[$Failed]; Return[1],
    Nothing
];

records = SQLExecute[conn, recordQuery]//
	<|
		"Premise" -> #, "Year" -> #2, "RateClass" -> #3, "Strata" -> #4, 
		"Usage" -> #5, "NumHour" -> #6 
	|>& @@@ #& //
	GroupBy[#, {#Premise, #Year, #RateClass, #Strata}&]& //
	Map[Merge[Identity]] //
	Map[{#Usage, #NumHour}&]// Quiet //
	Normal //
	# /. Rule[key_, usage_] :> Flatten[{key, usage}, 1]&;


(* {year, rateclass, strata} -> paramvalue *)
util = SQLExecute[conn, utilityQuery]//
    (*MapAt[First @ StringSplit[#,"-"]&, #, {All, 2}]& //*)
	<|"Year" -> #1, "RateClass" -> #2, "Strata" -> #3, "PV" -> ToExpression @ #4|>& @@@ #& //
	GroupBy[#, {#Year, #RateClass}&]& //
	Map[Merge[Identity]]//
	Map[(Times @@ #PV)&];

(* {year} -> paramvalue *)
sys = SQLExecute[conn, systemQuery]//
	Rule[#, #2]& @@@ #&//
	Association;

missingUtil = <| "2014" -> (1.0913 * 1.02800111), "2015"-> (1.0952 * 1.06246338)|>;

(* #################### Compute ICap #################### *)
(* time stamp *)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];
stdout=Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#,","]]&;

labels = {"RunDate", "RunTime", "ISO", "Utility", "PremiseId", "Year", "RateClass", "Strata", "MeterType", "RecipeICap"};
iso = "PJM";
utility = "PSEG";
mType = "CON";

writeFunc @ labels;
Do[
    {premId, year, rc, st, usage, numHours} = record // Quiet; 
    (*If[Length @ usage != 4, Continue[]];*)
    
	utilFactor = Lookup[util, {{year, rc}}, 0.] // If[MatchQ[#, _List], First @ #]&;
	sysFactor = Lookup[sys, year, 0.];
    (*missingFactor = Lookup[missingUtil, year, 0.];*)

     
    normalizedUsage = Total[usage] / Total[numHours];
	scalar = Times @@ {utilFactor, sysFactor};
	icap = normalizedUsage * scalar;
    yearADJ = ToExpression[year] + 1;
	results = {runDate, runTime, iso, utility, premId, yearADJ, rc, st, mType, icap};
	
	writeFunc @ results;
    ,{record, records}];

EndPackage[];

Quit[];
