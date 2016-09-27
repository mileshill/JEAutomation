(* ::Package:: *)

(* :Title: recPECO *)

(* :Author: miles *)

(* :Created on: 2016/06/19/20/21/52 *)

(* ::Summary::
    Calculates pjm/peco/monthly_with_Demand icaps for all given years
*)

(* :Context: recPECO` *)

(* :Mathematica Version: *)

BeginPackage["recPECO`",{"DatabaseLink`", "DBConnect`"}];

(* open/verify connection to database *)

(* 
query to load avg demand per premise per year;
 
format: {premId_String, year_String,rateClass_String, strata_String, avg_Real}

*)
monthlyWithDemandQuery = "
    select m.PremiseId, 
        Cast(Year(m.StartDate) as varchar), 
        RTrim(p.RateClass), RTrim(p.Strata),
        Avg(m.Demand) as AvgDmd
    from MonthlyUsage as m
    inner join CoincidentPeak as cp
        on cp.UtilityId = m.UtilityId
        and cp.CPDate between m.StartDate and m.EndDate
    inner join Premise as p
        on p.PremiseId = m.PremiseId
        and p.UtilityId = m.UtilityId
    where m.UtilityId = 'PECO'
        and Month(m.EndDate) in (6,7,8,9)
        and m.Demand is not Null
    group by m.PremiseId,
        Cast(Year(m.StartDate) as varchar),
        RTrim(p.RateClass), RTrim(p.Strata),
        m.EndDate
    having Count(m.Demand) = 4";

weatherCorrectionFactorQuery = "
    select Cast(Year(upv.StartDate)-1 as varchar), 
        RTrim(upv.RateClass), RTrim(upv.Strata), 
        Count(Year(upv.StartDate)), Avg(upv.ParameterValue)
    from UtilityParameterValue as upv
    inner join CoincidentPeak as cp
        on cp.UtilityId = upv.UtilityId
        and cp.CPID = upv.CPID
    where upv.UtilityId = 'PECO'
        and upv.ParameterId = 'NARatio'
    group by Cast(Year(upv.StartDate)-1 as varchar), 
        RTrim(upv.RateClass), 
        RTrim(upv.Strata)
    having Count(Year(upv.StartDate)) = 5";


rateClassLossFactorQuery = "
    select distinct Cast(Year(upv.StartDate)-1 as varchar), 
        RTrim(upv.RateClass), RTrim(upv.Strata),
        upv.ParameterId, (1 - upv.ParameterValue/100.)
    from UtilityParameterValue as upv
    inner join CoincidentPeak as cp
        on cp.UtilityId = upv.UtilityId
        and cp.CPID = upv.CPID
    where upv.UtilityId = 'PECO'
        and upv.ParameterId = 'RateClassLoss'    
";


plcScaleFactorQuery = "
    select Cast((CPYearID - 1) as varchar), ParameterValue
    from SystemLoad
    where UtilityId = 'PECO'
        and ParameterId = 'PLCScaleFactor'
";

conn = JEConection[];
If[Not @ MatchQ[conn, _SQLConnection],
    Throw[$Failed]; Return[1],
    Nothing];

records = SQLExecute[conn, monthlyWithDemandQuery];
wcf = SQLExecute[conn, weatherCorrectionFactorQuery];
rclf = SQLExecute[conn, rateClassLossFactorQuery];
plcf = SQLExecute[conn, plcScaleFactorQuery]

EndPackage[];
Quit[];
