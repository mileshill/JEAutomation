(* Imports *)
Needs @ "DatabaseLink`";
Get @ "DBConnect.m";

(* Import predictions *)
With[{fileName = "peco_pred.csv"},

    If[FileExistsQ @ fileName,
        Clear @ predictions;
        predictions = (
            Import[fileName, "CSV", "Numeric" -> False ]//
                Map[StringTrim, #, {-1}]&
        ),
        Return[1]
        ]

];


(* Import the historical results *)
conn = JEConnection[];
If[Not @ MatchQ[conn, _SQLConnection], Return[2]];
 

Clear @ historical;
historical = (
    SQLExecute[
        conn, 
        "select PremiseId, Cast(CPYearID as int), CapacityTagValue
        from CapacityTagHistorical
        where UtilityId = 'PECO'"
    ]//
    Map[StringTrim @ ToString @ #&, #, {-1}]&
    );


predictions ~ Join ~ historical //
    GroupBy[#, #[[{1,2}]]&]&//
    Select[#, Length @ # > 1&]&//
    Values//
    Flatten[{#1, #2[[-1]]}]& @@@ #&//
    (comparison = Sort @ #)&;

labels = {"PremiseId", "Year", "RateClass", "Strata",
    "PredictionICap", "Uncertainty", "NumTrainYears", "NumTrainSamp",
    "HistoricalICap"};

stdout = Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#, ", "]]&;

Map[writeFunc, PrependTo[comparison, labels]];


Quit[];
