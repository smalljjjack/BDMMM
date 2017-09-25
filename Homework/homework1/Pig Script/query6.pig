Friends = load'input/Mydata/Friends' using PigStorage(',') as (Rec:int, ID:int, Page:int, Data:chararray, Desc: chararray);
Access = LOAD'input/Mydata/AccessLog' USING PigStorage(',') AS(Rec:int, ID:int, Page:int, Type:chararray, Time:chararray);

A = FOREACH Friends generate ID, Page;	


Friends_gro = group A BY ID;

Friend_count = foreach Friends_gro{
	generate group as ID, (int)COUNT(A) AS (friendNUM:int);	
};

STORE Friend_count INTO 'output/Pig/Friend_count';

B = FOREACH Access generate ID, Page;

D = JOIN A BY (ID, Page), B By (ID, Page); 

STORE D INTO 'output/Pig/D';

E = FOREACH D GENERATE A::ID AS ID, A::Page AS Page;

STORE E INTO  'output/Pig/E';

F = GROUP E BY ID;

G = foreach F{
 generate group AS ID, (int)COUNT(E) AS (intersection:int);
};

STORE G INTO 'output/Pig/G';

H = JOIN Friend_count BY ID, G BY ID;

I = FOREACH H GENERATE G::ID AS ID, Friend_count::friendNUM AS (friendNUM:int), G::intersection as (intersection:int);

J = FILTER I BY friendNUM > intersection;

L = FOREACH J GENERATE ID AS ID, (friendNUM-intersection) AS (FriendsNOAccessed:int); 

STORE L INTO 'output/Pig/Job6';


