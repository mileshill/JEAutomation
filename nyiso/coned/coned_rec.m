#!/usr/bin/env/WolframScript -script
(* imports *)
BeginPackage["CONEDScript`",{"DatabaseLink`","DBConnect`"}];

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
		p.RateClass, ce.[Service Classification],
		ce.[Zone Code] as ZoneCode, ce.[Stratum Variable] as Stratum, ce.[Time of Day Code] as TOD,
		Year(m.EndDate) as Year,
		DateAdd(day, 0,  m.StartDate) as StartDate, 
		m.EndDate,
		m.Usage as BilledUsage, 
		m.Demand as BilledDemand,	
		Round(Sum(h.Usage), 0) as CPHourUsage,							-- sum all usage in cp bill cycle
		iif( Abs((m.Usage - (Sum(h.Usage))) / m.Usage) <= 0.04, 1, 0) as VarTest-- varince test 
		--Abs((m.Usage - (Sum(h.Usage))) / m.Usage) as var,
		--Count(h.Usage) as UseCount,
		--DateDiff(hour, DateAdd(day, 1, m.StartDate), EndDate)
	from HourlyUsage as h
	inner join MonthlyUsage as m										-- join adds BilledUsage, BilledDemand
		on m.PremiseId = h.PremiseID
		and m.UtilityID = h.UtilityId
	inner join CoincidentPeak as cp			
		on cp.UtilityId = h.UtilityId
		and Year(cp.CPDate) = Year(m.EndDate)
		and (cp.CPDate between m.StartDate and m.EndDate)				-- select days in coincident peak bill cycle	
		and (h.UsageDate between m.StartDate and m.EndDate)
	inner join Premise as p												-- join adds RateClass and Stata
		on p.PremiseId = h.PremiseId
		and p.PremiseId = '590003513000000'
	inner join ConED as ce												-- join adds ZoneCode, Stratum, and TOD
		on CAST(ce.[Account Number] as varchar) = h.PremiseId
	where 
		h.UtilityId = 'CONED'
		and h.HourEnding between 1 and 24
	group by h.PremiseId, 
		p.RateClass, ce.[Service Classification], 
		ce.[Zone Code], ce.[Stratum Variable], ce.[Time of Day Code],
		Year(m.EndDate), 
		m.StartDate, m.EndDate,
		m.Usage, 
		m.Demand
	having
		Count(h.Usage) = (DateDiff(hour, m.StartDate, m.EndDate) + 24)";

(* Filter meterTypes for Demand and Consumption  *)
monthlyQry = "select m.PremiseId,
        p.RateClass, p.Strata,
        ce.[Zone Code] as ZoneCode, ce.[Stratum Variable] as Stratum, ce.[Time of Day Code] as TOD,
        Year(m.EndDate) as Year,
        m.StartDate, m.EndDate,
        m.Usage as BilledUsage, 
        m.Demand as BilledDemand,
        iif(m.Demand = 0 or m.Demand is null, 'Scalar','Demand') as MeterType
    from MonthlyUsage as m
    inner join CoincidentPeak as cp
        on cp.UtilityId = m.UtilityId
        and Year(cp.CPDate) = Year(m.EndDate)
        and (cp.CPDate between m.StartDate and m.EndDate)     -- select days in coincident peak bill cycle
    inner join Premise as p                                   -- join adds RateClass and Stata
        on p.PremiseId = m.PremiseId
    inner join ConED as ce                                    -- join adds ZoneCode, Stratum, and TOD
        on CAST(ce.[Account Number] as varchar) = m.PremiseId
    where m.UtilityId = 'CONED'
        and m.PremiseId not in (
                select distinct PremiseId
                from HourlyUsage
                where UtilityId = 'CONED'
        )
        --and (
        --    m.PremiseId = '401126045000005' 
        --   or m.PremiseId = '444011303900006'
        --    or m.PremiseId = '266138069200019')";

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
coincidentPeakQry = "select 
        Cast(CPYearID-1 as varchar),
        CPDate, 
        Cast(HourEnding as int) 
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
intervalVarFalse = Select[intervalMeters,#[[-1]] == 0& ][[All,;;-2]];

(* demand and consumption meters *)
Clear[demandMeters, consumpMeters];
SQLExecute[conn, monthlyQry]//
    (demandMeters = Select[#, StringMatchQ[Last @ #, "Demand"]&];
     consumpMeters = Select[#, StringMatchQ[Last @ #, "Scalar"]&]
    )&;

(* records to test/use normalized usage *)
Clear @ allPremisesForNormalizedUsage;
Join[intervalVarFalse, demandMeters, consumpMeters]//
    (allPremisesForNormalizedUsage = #)&;


(* coincident peak and rateClass map *)
(* <| year_String -> <|Date, HourEnding|>|> *)
coincidentPeakASC = SQLExecute[conn, coincidentPeakQry] // 
    (# -> <|"Date" -> #2, "HourEnding" -> #3|>)& @@@ #&//
    Association;
   
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
(*#################### Aux: Meter Logic ####################*)
(* SQL templates *)
MeterLogic[_, 1] := "VTOU";
MeterLogic[x_String, 0]/; StringMatchQ[x, Alternatives @@ {"Demand","Scalar"}]:= x;
MeterLogic[x_?NumericQ, 0] := "Interval";

(* OUTPUT *)
MeterLogic[_, 1,"OUTPUT"] := "VTOU";
MeterLogic["Demand", 0, "OUTPUT"] := "DMD";
MeterLogic["Scalar", 0, "OUTPUT"]:= "CON";
MeterLogic[x_?NumericQ, 0, "OUTPUT"]:= "INT";

(*#################### Logic Loop ####################*)
(* Build load shape adjustment table; per premise id *)
(* time stamp *)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];

stdout=Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#,","]]&;
labels = {"RunDate", "RunTime", "ISO", "UtilityId", "PremiseId", "Year", "RateClass", "Strata", 
    "MType", "RecipeICap"};
iso = "NYISO";
utility = "CONED";

writeFunc @ labels;
(* Loop handles all values that require use of Normalized Usage. That is Consumption, Demand, and
Interval Meters; interval meters must have varinace from billed usage > 4% else they are handled
in the second logic loop below.
*)
(* loadProfileASC caches daily load profiles; see inner `Do` *)
loadProfileASC = <||>;
Do[
    (*#################### Initialization #################### *)
    (* premItr is an entire record!
        1) premid   2) rateClass    3) strata (don't use)   4) zone code
        5) stratum  6) tod code     7) year                 8) bill cycle start
        9) bill cycle end           10) billed usage        11) billed demand
        12) if interval then CPHourUsage else Scalar || Demand
    *)
    Clear[premId, rateClass, zoneCode, stratum, tod, year];
    Clear[billStart, billEnd, billUsage, billDemand, useOrMType];
    {   premId, rateClass, zoneCode, 
        stratum, tod, year, billStart, billEnd,
        billUsage, billDemand, useOrMType
    } = {
            #, rateClassMap[#2]["Mapping"], #4, ToExpression @  #5, rateClassMap[#2]["TODQ"], 
            ToString[#7], #8, #9, #10, #11, #12
        }& @@ premItr;

    (* pull the correct CPDate/Hour from the association *)
    Clear[localCPDate, localCPHour];
    localCPDate = coincidentPeakASC[year]["Date"];
    localCPHour = coincidentPeakASC[year]["HourEnding"];

    (*#################### Temperature variant Table ####################*) 
    (* select temperature variants by bill cycle  *)
    Clear[billCycle];
    billCycle = {billStart, billEnd};
    tempVarSelect =  ( 
        Position[tempVariant, {Alternatives @@ billCycle, __}]//
            If[ Length @ # == 2,
                Take[tempVariant, Flatten @ #],
                $Failed
            ]&
    );
    If[ FailureQ @ tempVarSelect, Continue[]];
    (* if localCP in tempVarSelect for premise, the store the index; else continue to next premise *)
    localCPIdx = If[# =!= {}, First @ #, Continue[]]& @ (Flatten @ Position[tempVarSelect, {localCPDate, __}]);

    (*#################### Load Profile from Temperature Variant Table ####################*)
	(* loop over each day in tempVarSelect to create load profile correctly *)
	todCondition = If[tod == 1, "Strata = 'T'", "Strata != 'T'"];
	loadProfileQuery = StringTemplate["select 
        kw1,kw2,kw3,kw4,kw5,kw6,kw7,kw8,kw9,kw10,kw11,kw12,
        kw13,kw14,kw15,kw16,kw17,kw18,kw19,kw20,kw21,kw22,kw23,kw24
	from CONED_LoadShapeTempAdj
	where
        (
        Strata != ''
		and `todCondition`
		and SC = `rateClass`
		and (`stratum` between [Strat L Bound] and [Strat U Bound])
		and DayType = '`day`'
		and (`temp` between [Temp L Bound] and [Temp U Bound])
        )
	"][<|"todCondition" -> #, "rateClass" -> #2, "stratum" -> #3, "day" -> #4, "temp" -> #5|>]&;
    Clear[loadProfile];
    loadProfile = {}; 
	Do[
        {date, dayType, temp} = day;
        keyPattern = {day, todCondition, rateClass, stratum, dayType, temp};
        If[ KeyExistsQ[loadProfileASC, keyPattern],
            result = loadProfileASC[keyPattern],
 		    result = SQLExecute[conn, loadProfileQuery[todCondition, rateClass, stratum, dayType, temp]] // Flatten;
            AssociateTo[loadProfileASC, keyPattern -> result]
        ];
		AppendTo[loadProfile, result];
	,{day, tempVarSelect}];
    
    (*#################### Compute Normalized Usage ####################*)
	(* compute factors for normalized usage *) 
    csf = billUsage / N[Total @ Flatten @ loadProfile]//Quiet;
    If[ Not @ NumericQ @ csf, Continue[]];

    lp = loadProfile[[ localCPIdx, localCPHour ]];
	normalizedUsage = csf * lp;
	localMCD = If[ useOrMType === "Scalar", normalizedUsage, Min[normalizedUsage, billDemand]];
    
        (*#################### Subzone and Forecast Trueup Factors ####################*)
	
	utilityFactorQuery = StringTemplate["select  Factor
		from CONED_UtilityParameters
		where 
			Cast(CPYear-1 as varchar) = '`year`'
			and (MeterType = '`meterType`' or MeterType = 'All-Meter-Types')
			and Zone = '`zone`'"][<|"year"-> year, "meterType" -> MeterLogic[useOrMType, tod], "zone" -> zoneCode |>];	
    utilFactors = SQLExecute[conn, utilityFactorQuery]// Flatten;
	utilProduct = If[ Length @ utilFactors == 2, Times @@(utilFactors + 1.), 0.];
	
	icap = localMCD * utilProduct;
	
    yearADJ = ToExpression[year] + 1;
	results = {runDate, runTime, iso, utility, premId, yearADJ, rateClass, stratum, MeterLogic[useOrMType, tod, "OUTPUT"], icap};
    (*
	Print["\nPremise: ", premId];
    Print["Year: ", yearADJ];
    Print["RateClassMap: ", rateClass];
    Print["Service Class: ", premItr[[2]]];
    Print["Stratum: ", stratum];
    Print["TOD: ", tod];
    Print["Billed Usage: ", billUsage];
    Print["Billed Demand: ", billDemand];
    Print["Sum over LP: ", N[Total @ Flatten @ loadProfile]]
    Print["Load Profile (cp): ", lp];
    Print["CSF: ", csf];
    Print["Normalized Usage: ", normalizedUsage];
    Print["MCD :", localMCD];
    Print["ICap: ", icap];
    Print["Temp Var Length: ", Length@tempVarSelect];
    Print["Load Profile Length: ", Length@loadProfile];
    Print[""];
    (*writeFunc /@ tempVarSelect;*)
    writeFunc /@ loadProfile;
    Continue[];
    *)
    writeFunc @ results;

,{premItr, allPremisesForNormalizedUsage}
](* end Normalized Usage Loop *);


(* loop to handle the interval meters where variance is < 0.04 from billed usage. 
The CPHourUsage must be selected before proceeding.
*)
intervalCPHourQuery = "select 
        h.PremiseId, 
        Cast(Year(h.UsageDate) as varchar), 
        Usage
    from HourlyUsage as h
    inner join CoincidentPeak as cp
        on cp.UtilityId = h.UtilityId
        and cp.CPDate = h.UsageDate
        and cp.HourEnding = h.HourEnding
    where h.UtilityId = 'CONED'";

(* dictionary to hold {premId, year}-> cp_date_hour_usage *)
intervalMCD = SQLExecute[conn, intervalCPHourQuery]//
    ({#, #2} -> #3)& @@@ #& // Association;


(* loop logic *)
Do[
	(*#################### Initialization #################### *)
    (* premItr is an entire record!
        1) premid   2) rateClass    3) strata (don't use)   4) zone code
        5) stratum  6) tod code     7) year                 8) bill cycle start
        9) bill cycle end           10) billed usage        11) billed demand
        12) if interval then CPHourUsage else Scalar || Demand
    *)
    {   premId, rateClass, zoneCode, 
        stratum, tod, year, billStart, billEnd,
        billUsage, billDemand, useOrMType
    } = {
            #, rateClassMap[#2]["Mapping"], #4, ToExpression @  #5, rateClassMap[#2]["TODQ"], 
            ToString[#7], #8, #9, #10, #11, #12
        }& @@ premItr[[;;-2]];

    yearADJ = ToExpression[year] + 1;
	utilityFactorQuery = StringTemplate["select  Factor
		from CONED_UtilityParameters
		where 
			Cast(CPYear as varchar) = '`year`'
			and (MeterType = '`meterType`' or MeterType = 'All-Meter-Types')
			and Zone = '`zone`'"][<|"year"-> yearADJ, "meterType" -> MeterLogic[useOrMType, tod], "zone" -> zoneCode |>];	
    
    utilFactors = SQLExecute[conn, utilityFactorQuery]// Flatten;
	utilProduct = If[ Length @ utilFactors == 2, Times @@(utilFactors + 1.), 0.];

    localMCD = Lookup[intervalMCD, {{premId, year}}, 0.] //If[Head@#===List, First@#, #]&;
	icap = localMCD * utilProduct;
    (*
    Print["Year :", year];
    Print["bill demand: ", billDemand];
    Print["bill usage: ", billUsage];
    Print["idr sum: ", useOrMType];
    Print["util factors: ", utilFactors];
    Print["interval mcd: ", localMCD];
    Print["meter type: ", MeterLogic[useOrMType, tod]];
    Print["zone: ", zoneCode];
*)
    yearADJ = ToExpression[year] + 1;
	results = {runDate, runTime, iso, utility, premId, yearADJ, rateClass, stratum, MeterLogic[useOrMType, tod, "OUTPUT"], icap};
	writeFunc @ results;

,{premItr, intervalVarTrue}
];
JECloseConnection[];
EndPackage[];
Quit[];
