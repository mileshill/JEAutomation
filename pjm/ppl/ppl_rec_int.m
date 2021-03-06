BeginPackage["recPPL`",{"DatabaseLink`","DBConnect`"}];

(* PPL only has interval meters! *)
conn = JEConnection[];

SQLExecute[conn,"select h.PremiseID, CAST(YEAR(h.UsageDate) as VARCHAR), h.Usage
        , p.RateClass, p.Strata
    from HourlyUsage as h
    inner join CoincidentPeak as c
        on c.UtilityId = h.UtilityId
        and c.CPDate = h.UsageDate
        and c.HourEnding = h.HourEnding
    inner join Premise as p
        on p.PremiseID = h.PremiseID
    where h.UtilityID = 'PPL'"]//
    <|"Premise"-> #,"Year"-> #2,"Usage"-> #3,"RateClass"-> #4,"Strata"-> #5|>&@@@#&//
    GroupBy[#,{#Premise,#Year,#RateClass,#Strata}&]&//
    Select[#,Length@#==5&]&//
    Map[Merge[Identity]]//
    Map[KeyDrop[#,{"Premise","Year", "RateClass","Strata"}]&]//
    Map[#Usage&]//
    Normal// 
    # /. Rule[key_, usage_] :> Flatten[{key, usage}] &//
    (Clear@records; records=#)&;

(* load reconcilation factor: year -> 5 vector *)
SQLExecute[conn,"select CAST(CPYearID-1 as VARCHAR), ParameterValue
    from SystemLoad
    where UtilityID = 'PPL'"]//
    <|"Year"->#,"ReconFactor"->#2|>&@@@#&//
    GroupBy[#,#Year&]&//
    Map[Merge[Identity]]//
    Map[#ReconFactor&]//
    (Clear@reconFactor;reconFactor=#)&;

(* load loss factor: {year, rateClass} -> scalar *)
SQLExecute[conn,"select CAST(c.CPYearID-1 as VARCHAR), u.RateClass, u.Strata, 
    u.ParameterID, u.ParameterValue
    from UtilityParameterValue u
    inner join CoincidentPeak as c
      on c.CPID = u.CPID
      and c.UtilityID = u.UtilityID
    where u.UtilityID = 'PPL'
        and u.ParameterID = 'Loss Factor'
    "]//
    <|"Year"->#,"RateClass"->#2,"Strata"->#3,"LossFactor"->#5|>&@@@#&//
    GroupBy[#,{#Year,#RateClass}&]&//
    Map[KeyDrop[{"Year","RateClass","Strata"}]]//
    Map[#LossFactor&@First@#&]//
    (Clear@lossFactor;lossFactor=#)&;

(* time stamp *)
runDate = DateString[{"Year", "-", "Month", "-", "Day"}];
runTime = DateString[{"Hour24", ":", "Minute"}];
stdout=Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#,","]]&;

labels = {"RunDate", "RunTime", "ISO", "Utility", "PremiseId", "Year", "RateClass", "Strata", "MeterType", "RecipeICap"};
iso = "PJM";
utility = "PPL";
mType = "INT";

writeFunc @ labels;
Do[
    {premId, year, rateClass, strata, usageVec} = {#, #2, #3, #4, {##5}}& @@ premItr;

    localRF = Lookup[reconFactor, year, ConstantArray[0., 5]] // Flatten; 
    localLF = Lookup[lossFactor, {{year, rateClass}}, 0.] // First;
    
    icap = Mean[ usageVec * localRF * localLF ];

    yearADJ = ToExpression[year] + 1;
    results = {runDate, runTime, iso, utility, premId, yearADJ, rateClass, strata, mType, icap};
    
    writeFunc @ results;
    ,{premItr, records}]

Quit[];
