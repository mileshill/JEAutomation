
       select KW1, KW2, KW3, KW4 ,KW5 ,KW6 ,KW7 ,KW8 , KW9, KW10, KW11, KW12,
            KW13, KW14, KW15, KW16, KW17, KW18, KW19, KW20, KW21, KW22, KW23, KW24
       from CONED_LoadShapeTempAdj
       where STRATA != ''
         and ((
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'WED' 
        and 81.9833 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'THU' 
        and 80.2333 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'FRI' 
        and 78.425 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'SAT' 
        and 77.8167 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'SUN' 
        and 77.1333 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'MON' 
        and 78.6833 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'TUE' 
        and 78.8583 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'WED' 
        and 76.9333 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'THU' 
        and 73.675 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'FRI' 
        and 73.2083 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'SAT' 
        and 72.7083 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'SUN' 
        and 73.1167 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'MON' 
        and 72.9583 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'TUE' 
        and 75.1667 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'WED' 
        and 74.5417 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'THU' 
        and 72.875 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'FRI' 
        and 75.5583 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'SAT' 
        and 78.6667 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'SUN' 
        and 80.4667 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'MON' 
        and 81.75 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'TUE' 
        and 81.0667 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'WED' 
        and 80.1917 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'THU' 
        and 79.2667 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'FRI' 
        and 77.1917 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'SAT' 
        and 74.8417 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'SUN' 
        and 75.4833 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'MON' 
        and 76.2167 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'TUE' 
        and 78.7417 between [TEMP L BOUND] and [TEMP U BOUND]) or (
        SC = 9
        and 1798.7 between [STRAT L BOUND] and [STRAT U BOUND]
        and DayType = 'WED' 
        and 74.3667 between [TEMP L BOUND] and [TEMP U BOUND]))
    
