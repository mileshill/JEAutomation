#!/bin/awk -f
BEGIN {FS=","; print }

{
    if ($NF == 0.)
        {count +=1}
}

END{
    print FILENAME
    print "RECORDS :" NR
    print "ZEROS :"   count
    print "%ZERO :"   count / NR
}
