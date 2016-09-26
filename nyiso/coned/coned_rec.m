(* imports *)
BeginPackage["CONEDScript`",{"DatabaseLink`","DBConnect`"}];

(* Open stdout for write operations *)
stdout = Streams[][[1]];


(* Connection test *)
conn = JEConnection[];
If[ Not @ MatchQ[conn, _SQLConnection], Write[stdout, "Connection Failed"]];




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
queryInterval = "select h.PremiseId, 
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
    m.Usage, m.Demand"

intervalMeters = SQLExecute[conn, queryInterval];

intervalVarTrue = Select[ intervalMeters, #[[-1]] == 1& ];
intervalVarFalse = Complement[ intervalMeters, intervalVarTrue ][[All,;;-2]];



(* Filter meterTypes for Demand and Consumption  *)
queryMonthly = "select m.PremiseId,
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


Clear[demandMeters, consumpMeters];
SQLExecute[conn, queryMonthly]//
    (demandMeters = Select[#, StringMatchQ[Last @ #, "DMD"]&];
     consumpMeters = Select[#, StringMatchQ[Last @ #, "CSP"]&]
    )&;

Join[intervalVarFalse, demandMeters, consumpMeters]//
    (allPremisesForNormalizedUsage = #)&;

(* :: Section:: *)
(* Load the cpDate, cpHourEnding *)
{cpDate, cpHour} = SQLExecute[conn, "select CPDate, Cast(HourEnding as int) 
    from CoincidentPeak
    where UtilityId = 'CONED'"] // First;

(* ::Section::  *)
(* TEMP VARIANTS TABLE *)

(* compute daily-hour avg in SQL *)
queryDailyAvg = "select Convert(date, c1.ObservedDate),         -- keep ISO date 
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

SQLExecute[conn, queryDailyAvg]//
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
        (tempVariant = #)&;  (* {dateString, daytype} -> tempAdj *)

(* ::Section:: *)
(* Build load shape adjustment table; per premise id *)
testPrem = First @ allPremisesForNormalizedUsage; (* this is an interval meter type *)


{serviceClass, stratum} = ToExpression[{#2, #5}]& @@ testPrem;
billCycle ={#8, #9}& @@ testPrem ;
tempVarSelect =  ( (* returns {{dayType, temp}..} for day in bill cycle *)
    Position[tempVariant, {Alternatives @@ billCycle, __}]//
        Take[tempVariant, Flatten @ #]&
);

boundsTemp = StringTemplate["(DayType = '`day`' 
        and `tempVar` between [TEMP L BOUND] and [TEMP U BOUND])"][<|"day" -> #1, "tempVar" -> #2|>]&;

boundsTemp @@@ tempVarSelect[[All, 2;;]] //
    StringRiffle[#, " or "]&//
    (boundsTempString = #)&;

loadAdjTableTemp = StringTemplate["
   select distinct KW1, KW2, KW3, KW4 ,KW5 ,KW6 ,KW7 ,KW8 , KW9, KW10, KW11, KW12,
        KW13, KW14, KW15, KW16, KW17, KW18, KW19, KW20, KW21, KW22, KW23, KW24
   from CONED_LoadShapeTempAdj
   where SC = `serviceClass`
    and `stratum` between [STRAT L BOUND] and [STRAT U BOUND]
    and `tempCycle`
"][<|"serviceClass" -> serviceClass, "stratum" -> stratum, "tempCycle" -> boundsTempString |>];

Print @ loadAdjTableTemp;

SQLExecute[conn, loadAdjTableTemp]//
    (loadProfile = #)&;

Map[Print[StringRiffle[#, " "]]&, loadProfile];
Print["LP Dimensions : ",  Dimensions @ loadProfile];

csf = Total @ Flatten @ loadProfile;
Print["CSF: ", csf];

cpDay = Position[tempVariant, cpDate][[1,1]];
Print @ cpDay;

lp = loadProfile[[ cpDay, cpHour ]];
Print["Load profile: ",lp];
Quit[];










(* Normalized Usage *)

(* MCD - Interval/Demand - MIN(Normalized Usage, Billed Demand) *)

(* MCD - Consumpiton - Normalized Usage *)

JECloseConnection[];
EndPackage[];
Quit[];
