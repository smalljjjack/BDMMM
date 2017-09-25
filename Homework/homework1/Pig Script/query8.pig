Friends = load '/user/hadoop/input/Mydata/Friends' USING PigStorage(',') AS (Rec:int, ID:int, FriendID:int, Date:chararray, Desc:int);

A = GROUP Friends BY ID;

B = foreach A {
	generate group as ID, (double)COUNT(Friends) as (sum:double);
};
C = GROUP B ALL;

AV = FOREACH C GENERATE group as ID, (double)AVG(B.sum) as (avg:double);

D = FILTER B BY sum > AV.avg;

E = GROUP D BY ID;

STORE E INTO 'output/Pig/try6';


