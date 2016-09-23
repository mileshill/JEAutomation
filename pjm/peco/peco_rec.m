#!/usr/bin/env/WolframScript -script

(* imports *)
BeginPackage["PecoScript`",{"DatabaseLink`","DBConnect`"}];

conn = JEConnection[];

(* connected to database *)
With[
	{labels={"PremiseId","Year","Usage","RateClass","Strata"}},
	
	SQLExecute[conn,
	"select distinct h.PremiseId, Cast(Year(h.Usagedate) as VARCHAR), h.Usage,
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
	order by h.premiseID, Cast(Year(h.UsageDate) as VARCHAR), p.RateClass, p.Strata"]//
		AssociationThread[labels-> #]&/@#&//
		GroupBy[#,{#PremiseId,#Year,#RateClass,#Strata}&]&//
		Map[Merge[Identity]]//
		Map[#Usage&]//
		Select[#,Length @ # == 5&]&//
		(results=#)&
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
	<|"Year" -> #,"RateClass" -> #2,"Strata" -> #3,#4 -> #5|>& @@@ #&//
		GroupBy[#,{#Year,#RateClass,#Strata}&]&//
		Map[Merge[Identity]]//
		Select[#, KeyExistsQ[#, "RateClassLoss"] && KeyExistsQ[#, "NCRatio"]&]&//
		Select[#, Length @ #NCRatio == 5&]&//
		Map[#NCRatio * (1 + #RateClassLoss[[1]] / 100.)&]//
		(utilParams=#)&;
  
 results//
 	Normal//
	#/.Rule[{prem_,yr_,rc_,st_}, usage_List] :> {prem, ToString[ToExpression[yr] + 1], rc, st, Mean[usage * Lookup[utilParams, {{yr,rc,st}}, ConstantArray[0., 5]][[1]]]}&//
	Prepend[#,{"PremiseId", "Year", "RateClass", "Strata", "RecipeICap"}]&//
	(icapValues = #)&;
	
stdout=Streams[][[1]];
Map[Write[stdout, StringRiffle[#,", "]]&, icapValues]

JECloseConnection[];
EndPackage[];
