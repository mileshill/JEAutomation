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
    Year(h.UsageDate),
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
where h.UtilityId = 'CONED'
group by h.PremiseId, p.RateClass, p.Strata, Year(h.UsageDate), m.Usage, m.Demand"

intervalMeters = SQLExecute[conn, queryInterval];

intervalVarTrue = Select[ intervalMeters, #[[-1]] == 1& ];
intervalVarFalse = Complement[ intervalMeters, intervalVarTrue ];



(* Filter meterTypes for Demand and Consumption  *)
queryMonthly = "select m.PremiseId,
    p.RateClass, p.Strata,
    ce.[Zone Code] as ZoneCode, ce.[Stratum Variable] as Stratum, ce.[Time of Day Code] as TOD,
    Year(m.StartDate),
    m.Usage as BilledUsage, 
    m.Demand as BilledDemand,
    iif(m.Demand = 0 or m.Demand is null, 'CSP','DMD') as MeterType
from MonthlyUsage as m
inner join CoincidentPeak as cp
    on cp.UtilityId = m.UtilityId
    and (cp.CPDate between m.StartDate and m.EndDate)               -- select days in coincident peak bill cycle
inner join Premise as p                                             -- join adds RateClass and Stata
    on p.PremiseId = m.PremiseId
inner join ConED as ce                                              -- join adds ZoneCode, Stratum, and TOD
    on CAST(ce.[Account Number] as varchar) = m.PremiseId
where m.UtilityId = 'CONED'";

Clear[demandMeters, consumpMeters];
SQLExecute[conn, queryMonthly]//
    (demandMeters = Select[#, StringMatchQ[Last @ #, "DMD"]&];
     consumpMeters = Select[#, StringMatchQ[Last @ #, "CSP"]&]
    )&;


(* ::Section::  *)
(* TEMP VARIANTS TABLE *)

(* compute daily-hour avg in SQL *)
queryDailyAvg = "select Convert(date, c1.ObservedDate), 
    DateName(weekday, c1.ObservedDate), 
    c1.Hour, 
    -- average weather station temps
    ((0.25) * (
                c1.Temperature + c1.WetBulbTemperature + 
                c2.Temperature + c2.WetBulbTemperature
                )
    ) as HourTempAvg
from CONED_NYWeatherData as c1
inner join CONED_NYWeatherData as c2
    on c1.ObservedDate = c2.ObservedDate
    and c1.Hour = c2.Hour
    and c1.StationCode != c2.StationCode";

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
        Map[Print]






(* Normalized Usage *)

(* MCD - Interval/Demand - MIN(Normalized Usage, Billed Demand) *)

(* MCD - Consumpiton - Normalized Usage *)

JECloseConnection[];
EndPackage[];
Quit[];
