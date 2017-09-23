MyPage = LOAD 'AssignmentInput/myPage.csv' USING PigStorage(',') AS (ID,Name,Nationality,CountryCode,Hobby);
AccessLog = LOAD 'AssignmentInput/AccessLog.csv' USING PigStorage(',') AS (AccessId,ByWho,WhatPage,TypeOfAccess,AccessTime);
step1 = JOIN MyPage BY ID, AccessLog by WhatPage;
step2 = GROUP step1 by ID;
step3 = foreach step2 generate group,COUNT(step1.WhatPage) AS cnt;
step4 = order step3 by cnt DESC;
step5 = limit step4 10;
store step5 into 'pigoutput3' using PigStorage(',');
