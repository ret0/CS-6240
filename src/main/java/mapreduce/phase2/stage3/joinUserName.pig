
P2S3Out  = LOAD '$input1' AS (hashTag:chararray, uid:int, category:chararray, tc:int, oc:int, relTc:int, relOc:int);
User  = LOAD '$input2' AS (uid:int, uname:chararray);

JoinRecord = JOIN P2S3Out BY $1, User BY $0;
X = FOREACH JoinRecord GENERATE hashTag, uname, category, tc, oc, relTc, relOc;
OutRecord = ORDER X BY hashTag;

STORE OutRecord INTO '$output';


