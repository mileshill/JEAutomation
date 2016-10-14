#!/usr/bin/env/WolframScript -script
(* imports *)
BeginPackage["CONEDScript`",{"DatabaseLink`","DBConnect`"}];

(* Open stdout for write operations *)
stdout = Streams[][[1]];





(* #################### Queries #################### *)
(* ::Section:: *)
(* METER TYPE FILTER *)
(****************** MCD - Interval*******************
Select and group values by variance test.

if VarTest = 1; then 
    MCD = CPHourUsage 
else 
    MCD = MIN(Normalized Usage, Billed Demand) 
fi
*)

(* Import interval meters and determine variance test; includes RateClass and Stratum; 
- Variance test is computed in SQL; 
*)
intervalQry = "select h.PremiseId, 
		p.RateClass, p.Strata,
		ce.[Zone Code] as ZoneCode, ce.[Stratum Variable] as Stratum, ce.[Time of Day Code] as TOD,
		Year(h.UsageDate),
		 m.StartDate, m.EndDate,
		m.Usage as BilledUsage, 
		m.Demand as BilledDemand,   
		Round(Sum(h.Usage), 0) as CPHourUsage,                          -- sum all usage in cp bill cycle
		iif( Abs((m.Usage - (Sum(h.Usage))) / m.Usage) <= 0.04, 1, 0)   -- varince test 
	from HourlyUsage as h
	inner join MonthlyUsage as m                                        -- join adds BilledUsage, BilledDemand
		on m.PremiseId = h.PremiseID
	inner join CoincidentPeak as cp         
		on cp.UtilityId = h.UtilityId
		and (cp.CPDate between m.StartDate and m.EndDate)               -- select days in coincident peak bill cycle
	inner join Premise as p                                             -- join adds RateClass and Stata
		on p.PremiseId = h.PremiseId
	inner join ConED as ce                                              -- join adds ZoneCode, Stratum, and TOD
		on CAST(ce.[Account Number] as varchar) = h.PremiseId
	where h.UtilityId = 'CONED'
	group by h.PremiseId, 
		p.RateClass, p.Strata, 
		ce.[Zone Code], ce.[Stratum Variable], ce.[Time of Day Code],
		Year(h.UsageDate), 
		m.StartDate, m.EndDate,
		m.Usage, m.Demand";

(* Filter meterTypes for Demand and Consumption  *)
monthlyQry = "select m.PremiseId,
        p.RateClass, p.Strata,
        ce.[Zone Code] as ZoneCode, ce.[Stratum Variable] as Stratum, ce.[Time of Day Code] as TOD,
        Year(m.StartDate) as Year,
        m.StartDate, m.EndDate,
        m.Usage as BilledUsage, 
        m.Demand as BilledDemand,
        iif(m.Demand = 0 or m.Demand is null, 'CSP','DMD') as MeterType
    from MonthlyUsage as m
    inner join CoincidentPeak as cp
        on cp.UtilityId = m.UtilityId
        and (cp.CPDate between m.StartDate and m.EndDate)     -- select days in coincident peak bill cycle
    inner join Premise as p                                   -- join adds RateClass and Stata
        on p.PremiseId = m.PremiseId
    inner join ConED as ce                                    -- join adds ZoneCode, Stratum, and TOD
        on CAST(ce.[Account Number] as varchar) = m.PremiseId
    where m.UtilityId = 'CONED'";

(* rateClass/serviceClass mapping and TODQ value *)
rateClassMapQry = "select distinct 
		RTrim(sm.PremiseSrvClass) as RateClass, 
		RTrim(sm.LoadShapeTblSrvClass) as Map, 
		iif(t.TODQ is NULL, 0, iif(t.TODQ = 'Yes', 1, 0)) as TODQ --TOD logic: (TODQ)? 1 : 0
	from Premise as p
	full outer join CONED_TOD as t
		on Cast(t.TODCode as int) = Cast(p.RateClass as int)
	full outer join CONED_SClass_Map as sm
		on Cast(p.RateClass as int) = Cast(sm.PremiseSrvClass as int)
	where sm.PremiseSrvClass is not NULL
		and p.UtilityId = 'CONED'
	order by RateClass";

(* coincident peak information *)
coincidentPeakQry = "select CPDate, Cast(HourEnding as int) 
    from CoincidentPeak
    where UtilityId = 'CONED'";

(*  *)
dailyAvgQry = "select Convert(date, c1.ObservedDate),         -- keep ISO date 
        Upper(SubString(DateName(weekday, c1.ObservedDate), 1, 3)), -- format to match load adj table   
        c1.Hour, 
        ((0.25) * (
                    c1.Temperature + c1.WetBulbTemperature + 
                    c2.Temperature + c2.WetBulbTemperature
                    )
        ) as HourTempAvg
    from CONED_NYWeatherData as c1
    inner join CONED_NYWeatherData as c2
        on c1.ObservedDate = c2.ObservedDate
        and c1.Hour = c2.Hour
        and c1.StationCode != c2.StationCode
    order by Convert(date, c1.ObservedDate)";


(* #################### Query Execution #################### *)
(* Connection test *)
conn = JEConnection[];
If[ Not @ MatchQ[conn, _SQLConnection], Write[stdout, "Connection Failed"]; Return[1]];

(* interval meters *)
intervalMeters = SQLExecute[conn, intervalQry];
intervalVarTrue = Select[intervalMeters, #[[-1]] == 1& ];
intervalVarFalse = Complement[intervalMeters, intervalVarTrue ][[All,;;-2]];

(* demand and consumption meters *)
Clear[demandMeters, consumpMeters];
SQLExecute[conn, monthlyQry]//
    (demandMeters = Select[#, StringMatchQ[Last @ #, "DMD"]&];
     consumpMeters = Select[#, StringMatchQ[Last @ #, "CSP"]&]
    )&;

(* records to test/use normalized usage *)
Clear @ allPremisesForNormalizedUsage;
Join[intervalVarFalse, demandMeters, consumpMeters]//
    (allPremisesForNormalizedUsage = #)&;


(* coincident peak and rateClass map *)
{cpDate, cpHour} = SQLExecute[conn, coincidentPeakQry] // First;

(* <| rateclass -> <|Mapping, TODQ|>, ...|> *)
rateClassMap = SQLExecute[conn, rateClassMapQry] //
	<|"RateClass" ->#1,  "Mapping"-> #2, "TODQ" -> #3|>& @@@ #& //
	GroupBy[#, #RateClass&]& //
    Map[First];

(* TEMP VARIANTS TABLE *)
(* compute daily-hour avg in SQL *)
SQLExecute[conn, dailyAvgQry]//
    GroupBy[#, #[[{1, 2}]]&]& (* group by date and weekday *)//
    Map[Last, #, {-2}]& (* keep only hourly temp avg *)//
    Map[(Max @ MovingAverage[#, 3])&] (* max of 3 hour moving avg per day *)//
        (* daily avgs complete; now compute weighted sum over 3 days *)
        Normal//
        # /. Rule -> List &//
        Transpose//
        MapAt[MovingAverage[#, {0.1, 0.2, 0.7}]&, #, {2}]& (* compute weighted sum *)//
        {#1[[3;;]], #2}& @@ #&//
        Thread @ (Rule @@ #)&//
        (# /. Rule[key_List, temp_] :> Flatten[{key, temp}])&//
    (* {dateString, daytype} -> tempAdj *)
    (tempVariant = #)&;  
Print @ "Temp Variant Start:End";
Print @ First @ tempVariant;
Print @ Last @ tempVariant;

(*#################### Logic Loop ####################*)
(* Build load shape adjustment table; per premise id *)
(* time stamp *)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];

labels = {"RunDate", "RunTime", "PremiseId", "Year", "RateClass", "Strata", "RecipeICap"};
stdout=Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#,","]]&;

Print @ Position[tempVariant, {cpDate, __}];
Do[
    (* premItr is an entire record!
        1) premid   2) rateClass    3) strata (don't use)   4) zone code
        5) stratum  6) tod code     7) year                 8) bill cycle start
        9) bill cycle end           10) billed usage        11) billed demand
        12) if interval then CPHourUsage else CMD || DMD
    *)
    {   premId, rateClass, zoneCode, 
        stratum, tod, year, billStart, billEnd,
        billUsage, billDemand, useOrMType
    } = {
            #, rateClassMap[#2]["Mapping"], #4, ToExpression @  #5, rateClassMap[#2]["TODQ"], 
            #7, #8, #9, #10, #11, #12
        }& @@ premItr;

       
    (* select temperature variants by bill cycle  *)
    billCycle = {billStart, billEnd};
    tempVarSelect =  ( 
        Position[tempVariant, {Alternatives @@ billCycle, __}]//
            Take[tempVariant, Flatten @ #]&
    );
    writeFunc @ {premId, tod, billStart, billEnd, useOrMType};
    Print @ Position[tempVariant, {billStart, __}|{billEnd, __}];
    Print[];
    Continue[];
    
    (* conditional template for Load Shape Adjustment Table  *)
    (********************************************************* 
    NEED TO DETERMINE WHAT <STRATA> means in the LoadShapeTempAdj TABLE BEFORE
    FINAL <LOADPROFILE> CAN BE BUILt.
    *)
    boundsTemp = StringTemplate["(
        SC = `rateClass`
        and `stratum` between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = '`day`' 
        and `tempVar` between [TEMP L BOUND] and [TEMP U BOUND])"
        ][<|"rateClass" -> #1, "stratum" -> #2, "day" -> #3, "tempVar" -> #4|>]&;

    (* apply template to days in bill cycle *)
    boundsTemp @@@ Flatten[{{rateClass, stratum, ##}& @@@ tempVarSelect[[All, 2;;]]}, 1] //
        StringRiffle[#, " or "]&//
        (boundsTempString = #)&;

    (* construct the sql query to return Load Shape Adjustment Table *)
    loadAdjTableTemp = StringTemplate["
       select KW1, KW2, KW3, KW4 ,KW5 ,KW6 ,KW7 ,KW8 , KW9, KW10, KW11, KW12,
            KW13, KW14, KW15, KW16, KW17, KW18, KW19, KW20, KW21, KW22, KW23, KW24
       from CONED_LoadShapeTempAdj
       where STRATA != ''
         and (`conditions`)
    "][<|"conditions" -> boundsTempString|>];


    SQLExecute[conn, loadAdjTableTemp]//
        (loadProfile = #)&;


    
    (*  Map[Print[StringRiffle[#, " "]]&, loadProfile];*)
    Print["LP Dimensions : ",  Dimensions @ loadProfile];

    csf = Total @ Flatten @ loadProfile;
    Print["CSF: ", csf];

    cpDay = Position[tempVariant, cpDate][[1,1]];
    Print @ cpDay;
    
    Continue[];
    lp = loadProfile[[ cpDay, cpHour ]];
    Print["Load profile: ",lp];
,{premItr, allPremisesForNormalizedUsage[[;;30]]}
];

(* Normalized Usage *)

(* MCD - Interval/Demand - MIN(Normalized Usage, Billed Demand) *)

(* MCD - Consumpiton - Normalized Usage *)

JECloseConnection[];
EndPackage[];
Quit[];
