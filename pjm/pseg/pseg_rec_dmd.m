#!/usr/bin/local/WolframScript -script

BeginPackage["PSEG`", {"DatabaseLink`", "DBConnect`"}];

(* #################### Queries #################### *)
recordQuery = "select 
	distinct m.PremiseId,										--1 
	Cast(Year(m.EndDate) as varchar) as Year,					--2 
	Month(m.EndDate) as Month,									--3
	RTrim(p.RateClass), RTrim(p.Strata),						--4,5
    m.Demand,
	--(DateDiff(day, m.StartDate, m.EndDate) * m.Demand) as BDxD, --6
	DateDiff(day, m.StartDate, m.EndDate) as NumDays			--7
from MonthlyUsage as m
inner join Premise as p
	on p.UtilityId = m.UtilityId
	and p.PremiseId = m.PremiseId
where m.UtilityId = 'PSEG'
	--and (m.Demand = 0 or m.Demand is Null)
	and (m.Demand > 0 and m.Demand is not Null)
	and Month(m.EndDate) in (6,7,8,9)
    --and m.PremiseId = 'PE000011707310605787'  /* test case: icap 2016 = 128.080 */
order by m.PremiseId, Year, Month";


utilityQuery = "select distinct 
		CAST(c.CPYearID - 1 as varchar), 
		RTrim(u.RateClass), RTrim(u.Strata),
		u.ParameterValue
	from UtilityParameterValue as u
	inner join CoincidentPeak as c
		on c.CPID = u.CPID
        and c.UtilityId = u.UtilityId
	where u.UtilityId = 'PSEG'";

systemQuery = "select 
	Cast(CPYearId-1  as varchar) as Year, 
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
		"Premise" -> #, "Year" -> #2, "RateClass" -> #4, "Strata" -> #5, 
		"demand" -> #6, "NumDays" -> #7 
	|>& @@@ #&// Quiet //
	GroupBy[#, {#Premise, #Year, #RateClass, #Strata}&]& //
	Map[Merge[Identity]] //
	Map[{#demand, #NumDays}&] //
	Normal //
	# /. Rule[key_, values_] :> Flatten[{key, values}, 1]&;

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

(* #################### Compute ICap #################### *)
(* time stamp *)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];

labels = {"RunDate", "RunTime", "UtilityId", "PremiseId", "Year", "RateClass", "Strata", "RecipeICap"};
stdout=Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#,","]]&;

utility = "PSEG";

Do[

    {premId, year, rc, st, demand, days} = {#, #2, #3, #4, #5, #6}& @@ record; 
    numMonths = Length @ demand;
    avgDailyDemand = Mean[demand / days] // Quiet;
    totalDays = Total @ days;
    genCapLoad = (avgDailyDemand * totalDays) / numMonths;

	utilFactor = Lookup[util, {{year, rc}}, 0.] // If[MatchQ[#, _List], First @ #]&;
	sysFactor = Lookup[sys, year, 0.];
    
	scalar = Times @@ {utilFactor, sysFactor};
	
	icap = genCapLoad * scalar;

    yearADJ = ToExpression[year] + 1;
	results = {runDate, runTime, utility, premId, yearADJ, rc, st, icap};
	
	writeFunc @ results;

    ,{record, records}];

EndPackage[];

