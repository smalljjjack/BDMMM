step1 = LOAD 'AssignmentInput/myPage.csv' USING PigStorage(',') AS (id,name,nationality,countryCode,hobby);
step2 = filter step1 by nationality == '$country';
step3 = foreach step2 generate name,hobby;
store step3 into 'pigoutput1' using PigStorage(',');
