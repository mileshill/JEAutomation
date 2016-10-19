#!/usr/bin/local WolframScript -script

BeginPackage["SystemLoadCorrelation`",{"DBConnect`", "JLink`"}];

(* open connection to Just Energy Database *)
InstallJava[];
ReinstallJava[JVMArguments -> "-Xmx512m"];

Needs @ "DatabaseLink`";
conn = JEConnection[];
If[ Not @ MatchQ[conn, _SQLConnection],
    Throw[$Failed]; Return[1]
];


utilityQuery = "select distinct UtilityId
    from CoincidentPeak
    where UtilityId != 'COMED'";

(* This is a large query! 3 to 4 minute run time. May cause JDBC error *)
aggregateTemp = StringTemplate[ "select 
		UtilityId, 
		PremiseId, 
		Cast(Year(EndDate) as varchar) as Year, 
		DateName(month, EndDate) as Month, 
		Usage
	from MonthlyUsage
	where (Usage > 0 or Demand > 0)
		and UtilityId = '`utility`'
	UNION
	select 
		UtilityId, 
		PremiseId, 
		Cast(Year(UsageDate) as varchar) as Year, 
		DateName(month, UsageDate) as Month, 
		Sum(Usage) as Usage
	from HourlyUsage
	where UtilityId = '`utility`'
	group by UtilityId, PremiseId, Cast(Year(UsageDate) as varchar), DateName(month, UsageDate)
	order by UtilityId, PremiseId, Year, Month"][<|"utility" -> #|>]&;

(* Association for population of default values *)
RawToASC[data:{{__}..}]:= Block[
    {months, default, recordTransform, groupTransform, joinOnDefault},

    months = {"January", "February", "March", "April", "May", "June", 
        "July", "August", "September", "October", "November", "December"};

    default = AssociationThread[months -> ConstantArray[0.0, Length @ months]];

    recordTransform = <|"UtilityId" -> #, "PremiseId" -> #2, "Year" -> #3, #4 -> #5|>& @@@ #&;
    groupTransform = GroupBy[#, {#UtilityId&, #PremiseId&, #Year&}]&;
    joinOnDefault = Join[default, #]&;

    data //
        recordTransform //
        groupTransform //
        Map[Merge[Identity], #, {3}]& //
        Map[joinOnDefault, #, {3}]& //
        Map[First, #, {-2}]&
];

utilities = SQLExecute[conn, utilityQuery] // Flatten;
output = <||>;
Do[
    rawData = SQLExecute[conn, aggregateTemp @ utilItr];
    utilRule = (utilItr ->  RawToASC @ rawData);
    AssociateTo[output, utilRule];
    
    ,{utilItr, utilities}];

ExportString[output, "JSON", "Compact" -> True] //
    Print @ #&;


Quit[];
JECloseConnection[];
EndPackage[];
