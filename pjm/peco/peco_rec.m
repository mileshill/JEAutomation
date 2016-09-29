#!/usr/local/bin/WolframScript -script


(* imports *)
BeginPackage["PecoScript`",{"DatabaseLink`","DBConnect`"}];

conn = JEConnection[];
If[Not @ MatchQ[conn, _SQLConnection],
    Throw[$Failed]; Return[1],
    Nothing
];

(* Query and filter for premises *)
With[
	{labels={"PremiseId","Year","Usage","RateClass","Strata"}},
	
	SQLExecute[conn,
	"select distinct h.PremiseId, 
        Cast(Year(h.Usagedate) as VARCHAR), 
        h.Usage,
		p.RateClass, p.Strata
	from HourlyUsage as h
	inner join Premise as p
		on p.UtilityId = h.UtilityID
		and p.PremiseID = h.PremiseID
	inner join CoincidentPeak as cp
		on cp.UtilityID = h.UtilityID
		and cp.CPDate = h.UsageDate
		and cp.HourEnding = h.HourEnding
	where h.UtilityId = 'PECO'
	order by h.premiseID, 
        Cast(Year(h.UsageDate) as VARCHAR), 
        p.RateClass, p.Strata"]//
		AssociationThread[labels -> #]& /@ #&//
		GroupBy[#,{#PremiseId, #Year, #RateClass, #Strata}&]&//
		Map[Merge[Identity]]//
		Map[#Usage&]//
		Select[#,Length @ # == 5&]&//
        Normal//
        # /. Rule[key_, usage_] :> Flatten[{key, usage}]&//
		(records=#)&
];

(* Query for utility/system parameters *)
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
	<|"Year" -> #,"RateClass" -> #2,"Strata" -> #3,#4 -> #5|>& @@@ #&//
		GroupBy[#,{#Year,#RateClass,#Strata}&]&//
		Map[Merge[Identity]]//
		Select[#, KeyExistsQ[#, "RateClassLoss"] && KeyExistsQ[#, "NCRatio"]&]&//
		Select[#, Length @ #NCRatio == 5&]&//
		Map[#NCRatio * (1 + #RateClassLoss[[1]] / 100.)&]//
		(utilParams=#)&;
 
(*
records//
 	Normal//
	#/.Rule[{prem_,yr_,rc_,st_}, usage_List] :> {prem, ToExpression[yr]+1, rc, st, Mean[usage * Lookup[utilParams, {{yr,rc,st}}, ConstantArray[0., 5]][[1]]]}&//
	(icapValues = #)&;
*)

(* time stamp *)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];

labels = {"RunDate", "RunTime", "PremiseId", "Year", "RateClass", "Strata", "RecipeICap"};
stdout=Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#,","]]&;
Do[
    
    {premId, year, rateClass, strata, usage} = {#, #2, #3, #4, {##5}}& @@ premItr;

    localUtil = Lookup[ utilParams, {{year, rateClass, strata}}, ConstantArray[0.,5]] // Flatten;

    iCap = Mean @ (usage * localUtil);

    writeFunc @ {runDate, runTime, premId, ToExpression[year] + 1, rateClass, strata, iCap};
    ,{premItr, records}]


JECloseConnection[];
EndPackage[];
Quit[];
