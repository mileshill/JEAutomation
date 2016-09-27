#!/usr/bin/env/WolframScript -script

Needs @ "DatabaseLink`";
Get @ "DBConnect.m";

(* open connection to db *)
conn = JEConnection[];

(* database servcie class for coned *)
dbsc = SQLExecute[conn,
    "select distinct Cast(SC as int)
    from CONED_LoadShapeTempAdj"] // 
    Flatten // ToExpression;

(* premise service class from premise profiles *)
psc = Import["uniq_servclass.csv","CSV"]//
    Flatten // ToExpression;

(* name the stream and write function *)
stdout = Streams[][[1]];
writeFunc = Write[stdout, StringRiffle[#, ", "]]&;


PcD = Complement[psc, dbsc];

DcP = Complement[dbsc, psc];

PiD = Intersection[dbsc, psc];

results = {PcD, DcP, PiD};
max = Max[Length /@ results];

output = Transpose @ Map[ArrayPad[#, {0, max - (Length @ #)}, Null]&, results];

writeFunc @ {"PremiseNotDatabase", "DatabaseNotPremise", "PremiseAndDatabase"};

writeFunc /@ output;


Quit[];
