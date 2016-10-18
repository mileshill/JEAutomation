#!/usr/bin/local/WolframScript -script

(*
2016/10/13
Still missing the customer usage factor! Without that factor, the numbers will be off.
The current usage factor in the test case was obtained from the Central Hudson website by Earl.
We have not yet resolved this issue.
*)


BeginPackage["CENTHUD`", {"DatabaseLink`", "DBConnect`"}];
(* #################### Queries #################### *)
recordQuery = "select m.PremiseId, 
		Cast(Year(m.EndDate) as varchar) as Year,
		RTrim(p.RateClass) as RateClass, RTrim(p.Strata) as Strata,
		lp.AvgHourlyLoad_kw as LoadProfile
	from MonthlyUsage as m
	inner join Premise as p
		on p.UtilityId = m.UtilityId
		and p.PremiseId = m.PremiseId
	inner join CoincidentPeak as cp
		on cp.UtilityId = m.UtilityId
	inner join CENTHUD_Load_Profile as lp
		on p.Strata = lp.Stratum
		and Month(cp.CPDate) = Cast(lp.Month as int)
		and cp.HourEnding = Cast(lp.Hour as int)
	where m.UtilityID = 'CENTHUD'
		and (m.Demand is NULL or m.Demand = 0)
		and (
			select CPDate
			from CoincidentPeak
			where UtilityId = 'CENTHUD'
                and Year(CPDate) = Year(m.EndDate)
		) between m.StartDate and m.EndDate
		and lp.DayType = 'WKDAY'
        --and m.PremiseId = '5609061901' /* test case: icap = 7.790 */";

utilityQuery = "select distinct Cast(cp.CPYearID-1 as varchar), 
		RTrim(upv.RateClass), RTrim(upv.Strata), 
		Exp(Sum(Log(upv.ParameterValue)))
	from UtilityParameterValue as upv
	inner join CoincidentPeak as cp
		on cp.CPID = upv.CPID
	where upv.UtilityId = 'CENTHUD'
	group by Cast(cp.CPYearID-1 as varchar), 
		RTrim(upv.RateClass), RTrim(upv.Strata)";

loadProfileQuery = "waiting on logic for this!";

(* #################### Execute Queries #################### *)
conn = JEConnection[];
If[Not @ MatchQ[conn, _SQLConnection],
    Throw[$Failed]; Return[1],
    Nothing
];

(* {{prem, year, rateclass, strata, loadProfile},...} *)
records = SQLExecute[conn, recordQuery];


(* <|{year, rateClass, strata}-> factor, ... |> *)
util = SQLExecute[conn, utilityQuery]//
    <| "Year" -> #1, "RateClass" -> #2, "Strata" -> #3, "Factor" -> #4 |>& @@@ #&//
    GroupBy[#, {#Year, #RateClass, #Strata}&]& //
    Map[#Factor&[First @ #]&];

(* <| rateClass -> loadFactor |> *)
(*loadProfile = SQLExecute[conn, loadProfileQuery]//
	Rule @@@ #& //
	Association;
    *)

(* #################### Compute ICap #################### *)
(* time stamp *)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];
stdout=Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#,","]]&;

labels = {"RunDate", "RunTime", "ISO", "Utility", "PremiseId", "Year", "RateClass", "Strata", "MeterType", "RecipeICap"};
iso = "PJM";
utility = "CENTHUD";
mType = "CON";

writeFunc @ labels;
Do[

    {premId, year, rc, st, loadProfile} = record;

	utilFactor = Lookup[util, {{year, rc, st}}, 0.] // If[MatchQ[#, _List], First @ #, #]&;
    (*loadFactor = Lookup[loadProfile, st, 0.];	*)
	scalar = Times[loadProfile, utilFactor];
	
    (*icap = scalar + loadFactor;*)
    utility = "CENTHUD";
    yearADJ = ToExpression[year] + 1;
	results = {runDate, runTime, iso, utility, premId, yearADJ, rc, st, mType, scalar};
	
	writeFunc @ results;

    ,{record, records}];

EndPackage[];

