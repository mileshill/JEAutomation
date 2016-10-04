BeginPackage["DBConnect`",{"DatabaseLink`"}];

Clear@JEConnection;
JEConnection::usage="Connection to Just Energy MS SQL Database";
JEConnection::NoLink="Link could not be established.";

Clear@JECloseConnection;
JECloseConnection::usage="Close the SQL Connection";

Begin["`Private`"];

Needs["DatabaseLink`"];


JEConnection[]:=Block[{conn},
	conn=OpenSQLConnection[
	  JDBC["Microsoft SQL Server(jTDS)", 
	  "ec2-52-71-54-106.compute-1.amazonaws.com"]
	  , "Username" -> "CapTagModel"
	  , "Password" -> "8usL@816"];
	  
	If[
		Not@MatchQ[conn,_SQLConnection]
		,(Message[Connect::NoLink];Throw[$Failed])
		,conn
	]
](* endJEConnect *);

JECloseConnection[]:=CloseSQLConnection/@(Flatten@SQLConnections[]);

End[];
EndPackage[];