#!/usr/bin/local/WolframScript -script

BeginPackage["PSEG`", {"DatabaseLink`", "DBConnect`"}];


(* #################### Queries #################### *)
recordQuery = "select h.PremiseId, 
		Cast(Year(h.UsageDate) as varchar) as Year, 
		RTrim(p.RateClass), RTrim(p.Strata),
		h.Usage
	from HourlyUsage as h
	inner join CoincidentPeak as c
		on  c.UtilityId = h.UtilityId 
		and c.CPDate = h.UsageDate
		and c.HourEnding = h.HourEnding
	inner join Premise as p
		on p.UtilityID = h.UtilityId
		and p.PremiseId = h.PremiseID
	where h.UtilityId = 'PSEG'
	order by h.PremiseId, Year";


utilityQuery = "select distinct 
		CAST((c.CPYearID - 1) as varchar), 
		RTrim(u.RateClass), RTrim(u.Strata),
		u.ParameterValue
	from UtilityParameterValue as u
	inner join CoincidentPeak as c
		on c.CPID = u.CPID
	where u.UtilityId = 'PSEG'
		and u.RateClass like '%-INT'";

systemQuery = "select 
	Cast(CPYearId as varchar) as Year, 
	Exp(Sum(Log(ParameterValue))) as PFactor
from SystemLoad
where UtilityId = 'PSEG'
	and ParameterId in ('CapObligScale', 'ForecastPoolResv', 'FinalRPMZonal')
group by Cast(CPYearId as varchar)"

(* #################### Execute Queries #################### *)
conn = JEConnection[];
If[Not @ MatchQ[conn, _SQLConnection],
    Throw[$Failed]; Return[1],
    Nothing
];

records = SQLExecute[conn, recordQuery]//
	<|"Premise" -> #, "Year" -> #2, "RateClass" -> #3, "Strata" -> #4, "Usage" -> #5|>& @@@ #& //
	GroupBy[#, {#Premise, #Year, #RateClass, #Strata}&]& //
	Map[Merge[Identity]] //
	Map[#Usage&]//
	Normal //
	# /. Rule[key_, usage_] :> Flatten[{key, usage}]& //
	Select[#, Length @ # == 9&]&;


(* {year, rateclass, strata} -> paramvalue *)
util = SQLExecute[conn, utilityQuery]//
	MapAt[First @ StringSplit[#,"-"]&, #, {All, 2}]& //
	<|"Year" -> #1, "RateClass" -> #2, "Strata" -> #3, "PV" -> ToExpression @ #4|>& @@@ #& //
	GroupBy[#, {#Year, #RateClass}&]& //
	Map[Merge[Identity]]//
	Map[(Times @@ #PV)&];

(* {year} -> paramvalue *)
sys = SQLExecute[conn, systemQuery]//
	Rule[#, #2]& @@@ #&//
	Association;

(*missingUtil = <| "2014" -> (1.0913 * 1.02800111), "2015"-> (1.0952 * 1.06246338)|>;*)

(* #################### Compute ICap #################### *)
(* time stamp *)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];

labels = {"RunDate", "RunTime", "PremiseId", "Year", "RateClass", "Strata", "RecipeICap"};
stdout=Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#,","]]&;

Do[

    {premId, year, rc, st, usage} = {#, #2, StringSplit[#3,"-"][[1]], #4, {##5}}& @@ record // Quiet;
    utility = "PSEG";

	utilFactor = Lookup[util, {{year, rc}}, 0.] // If[MatchQ[#, _List], First @ #]&;
	sysFactor = Lookup[sys, year, 0.];
    (*missingFactor = Lookup[missingUtil, year, 0.];i*)
	
	scalar = Times @@ {utilFactor, sysFactor};
	
	icap = Mean[usage * scalar];
	results = {runDate, runTime, utility, premId, year, rc, st, icap};
	
	writeFunc @ results;

    ,{record, records}];

EndPackage[];

