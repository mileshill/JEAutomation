#!/usr/bin/local WolframScript -script

BeginPackage["SystemLoadCorrelation`",{"DatabaseLink`","DBConnect`"}];

(* open connection to Just Energy Database *)
conn = JEConnection[];
If[ Not @ MatchQ[conn, _SQLConnection],
    Throw[$Failed]; Return[1]
];

(* This is a large query! 3 to 4 minute run time. May cause JDBC error *)
aggregateQuery =  "select 
		UtilityId, 
		PremiseId, 
		Cast(Year(EndDate) as varchar) as Year, 
		DateName(month, EndDate) as Month, 
		Usage
	from MonthlyUsage
	where (Usage > 0 or Demand > 0)
		and UtilityId = 'CENTHUD'
	UNION
	select 
		UtilityId, 
		PremiseId, 
		Cast(Year(UsageDate) as varchar) as Year, 
		DateName(month, UsageDate) as Month, 
		Sum(Usage) as Usage
	from HourlyUsage
	where UtilityId = 'CENTHUD'
	group by UtilityId, PremiseId, Cast(Year(UsageDate) as varchar), DateName(month, UsageDate)
	order by UtilityId, PremiseId, Year, Month";

rawData = SQLExecute[conn, aggregateQuery];

(* Association for population of default values *)
months = {"January", "February", "March", "April", "May", "June", 
    "July", "August", "September", "October", "November", "December"};

default = AssociationThread[months -> ConstantArray[0.0, Length @ months]];


recordTransform = <|"UtilityId" -> #, "PremiseId" -> #2, "Year" -> #3, #4 -> #5|>&;
groupTransfrom = GroupBy[#, {#UtilityId&, #PremiseId&, #Year&}]&;
joinOnDefault = Join[default, #]&;
JSONconvert = ExportString[#, "JSON"]&;

rawData//
	recordTransform @@@ #&//
	groupTransform  //
	Map[Merge[Identity], #, {3}]&
    
    (*//
    Map[joinOnDefault, #, {3}]& //
	JSONconvert	//
	Print @ #&;
*)
JECloseConnection[];
EndPackage[];
