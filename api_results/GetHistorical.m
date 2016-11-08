BeginPackage["GetHistorical`",{"DatabaseLink`", "DBConnect`"}];

(* Make the connection *)
conn = JEConnection[];
If[ Not @ MatchQ[conn, _SQLConnection],
    Return[1] 
    ];

(* Link to STDOUT and create CSV writer  *)
stdout = Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#, ","]]&;


(* query for all historical data *)
historicalQry = "select 
        UtilityId, 
        PremiseId, 
        Cast(CPYearId as varchar), 
        CapacityTagValue
    from CapacityTagHistorical
    order by UtilityId, PremiseId, CPYearId";


(* Execute query and write to stdout *)
SQLExecute[conn, historicalQry] //
    Map[writeFunc, #]&;

EndPackage[];
Quit[];
