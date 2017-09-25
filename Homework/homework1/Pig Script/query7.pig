bag1= load 'fbexp/Mypage.csv' using PigStorage(',')as (ID:int, Name:Chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);
bag2= load 'fbexp/AccessLog.csv' using PigStorage(',')as (AccessId:int, Bywho:int, WhatPage:int,TypeofAccess:chararray, AccessTime:int);
join1= join Mypage by ID, AccessLog BY Bywho;
bag3 = foreach join1 GENERATE Name, Bywho, AccessTime;
bag4 = foreach bag3 generate group AS Bywho, MAX(AccessTime) AS Lastdate, MIN(AccessTime) AS Firstdate;
bag5 = foreach bag4 generate group AS Bywho, DIFF(Lastdate - Firstdate);
bag 6 = LIMIT bag5 > 10;
dump bag6;




