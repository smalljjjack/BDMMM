MyPage = LOAD 'AssignmentInput/myPage.csv' USING PigStorage(',') AS (ID,Name,Nationality,CountryCode,Hobby);
Friends = LOAD 'AssignmentInput/AccessLog.csv' USING PigStorage(',') AS (FriendRel,PersonID,MyFriend,DateOfFriendship,Description);
step1 = JOIN MyPage by ID,Friends by MyFriend;
step2 = foreach step1 generate Name,PersonID;
step3 = GROUP step2 by Name;
step4 = foreach step3 generate group as Name,COUNT($1) AS cnt;
step5 = foreach step4 generate Name,cnt;
store step5 into 'pigoutput4' using PigStorage(',');
