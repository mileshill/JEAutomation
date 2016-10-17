#!/usr/bin/local/WolframScript -script

BeginPackage["CENTHUD`", {"DatabaseLink`", "DBConnect`"}];


(* #################### Queries #################### *)
recordQuery = "select m.PremiseId, 
		Cast(Year(m.EndDate) as varchar) as Year,
		RTrim(p.RateClass), RTrim(p.Strata),
		Avg(m.Demand)
	from MonthlyUsage as m
	inner join Premise as p
		on p.UtilityId = m.UtilityId
		and p.PremiseId = m.PremiseId
	where m.UtilityId = 'CENTHUD'
		and (m.Demand > 0 or m.Demand is not NULL)
		and Month(m.EndDate) in (6,7,8)
		--and m.PremiseId = '5616019800'    /* test case; 2015 = 91.6 */
		--and m.PremiseId = '5609061901'    /* consumption test case; should not come through */
	group by m.PremiseId, Cast(Year(m.EndDate) as varchar), 
		RTrim(p.RateClass), RTrim(p.Strata)
	having Count(m.EndDate) = 3
	order by m.PremiseId, Year";

(* The FactorAdjust is bad; this query causes bad results.
utilityQuery = "select distinct Cast(cp.CPYearID-1 as varchar), 
		RTrim(upv.RateClass), RTrim(upv.Strata), 
		Exp(Sum(Log(upv.ParameterValue)))
	from UtilityParameterValue as upv
	inner join CoincidentPeak as cp
		on cp.CPID = upv.CPID
	where upv.UtilityId = 'CENTHUD'
	group by Cast(cp.CPYearID-1 as varchar), 
		RTrim(upv.RateClass), RTrim(upv.Strata)";
*)
utilityQuery = "select distinct Cast(cp.CPYearID-1 as varchar),
        RTrim(upv.RateClass), RTrim(upv.Strata),
        upv.ParameterValue
    from UtilityParameterValue as upv
    inner join CoincidentPeak as cp
        on cp.CPID = upv.CPID
    where upv.UtilityId = 'CENTHUD'
        and upv.ParameterId = 'WeatherNormalFactor'";

loadProfileQuery = "select RTrim(lp.Strata), lp.AVGKwHourlyLoad
	from CENTHUD_AVGkWHourly as lp
	inner join CoincidentPeak as cp
		on cp.UtilityID = 'CENTHUD'
		and Month(cp.CPDate) = lp.Month
		and cp.HourEnding = lp.Hour
	where DayType = 'WKDAY'";

(* #################### Execute Queries #################### *)
conn = JEConnection[];
If[Not @ MatchQ[conn, _SQLConnection],
    Throw[$Failed]; Return[1],
    Nothing
];

(* {{prem, year, rateclass, strata, avgDmd},...} *)
records = SQLExecute[conn, recordQuery];

(* <|{year, rateClass, strata}-> factor, ... |> *)
util = SQLExecute[conn, utilityQuery]//
    <| "Year" -> #1, "RateClass" -> #2, "Strata" -> #3, "Factor" -> #4 |>& @@@ #&//
    GroupBy[#, {#Year, #RateClass, #Strata}&]& //
    Map[#Factor&[First @ #]&];

(* <| rateClass -> loadFactor |> *)
loadProfile = SQLExecute[conn, loadProfileQuery]//
	Rule @@@ #& //
	Association;

(* #################### Compute ICap #################### *)
(* time stamp *)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];

labels = {"RunDate", "RunTime", "PremiseId", "Year", "RateClass", "Strata", "RecipeICap"};
stdout=Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#,","]]&;
Do[

    {premId, year, rc, st, avg} = record;


	utilFactor = Lookup[util, {{year, rc, st}}, 0.] // If[MatchQ[#, _List], First @ #, #]&;
    loadFactor = Lookup[loadProfile, st, 0.];	
	scalar = Times[avg, utilFactor];
	
	icap = scalar + loadFactor;
	
    utility = "CENTHUD";
    yearADJ = ToExpression[year] + 1;
	results = {runDate, runTime, utility, premId, yearADJ, rc, st, scalar};
	writeFunc @ results;

    ,{record, records}];

EndPackage[];

