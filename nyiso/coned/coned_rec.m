(* imports *)
BeginPackage["CONEDScript`",{"DatabaseLink`","DBConnect`"}];

(* Open stdout for write operations *)
stdout = Streams[][[1]];


(* Connection test *)
conn = JEConnection[];
If[ Not @ MatchQ[conn, _SQLConnection], Write[stdout, "Connection Failed"]];





(****************** MCD - Interval*******************
Select and group values by variance test.

if VarTest = 1; then 
    MCD = CPHourUsage 
else 
    MCD = MIN(Normalized Usage, Billed Demand) 
fi
*)

(* Import interval meters and determine variance test; includes RateClass and Stratum  *)
query = "select h.PremiseId, 
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

intervalMeters = SQLExecute[conn, query];

intervalVarTrue = Select[ intervalMeters, #[[-1]] == 1& ];
intervalVarFalse = Complement[ intervalMeters, intervalVarTrue ];











(* Normalized Usage *)

(* MCD - Interval/Demand - MIN(Normalized Usage, Billed Demand) *)

(* MCD - Consumpiton - Normalized Usage *)

JECloseConnection[];
EndPackage[];
Quit[];
