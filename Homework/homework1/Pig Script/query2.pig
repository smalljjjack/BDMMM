step1 = LOAD 'AssignmentInput/myPage.csv' using PigStorage(',') AS (id,name,nationality,countryCode,hobby);
step2 = GROUP step1 BY(nationality);
step3 = foreach step2 GENERATE group,COUNT(step1.nationality);
store step3 into 'pigoutput2' using PigStorage(',');
