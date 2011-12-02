/**
 * Phase 1, Stage 3: Count intersection size of the wordsets of the top words in two different hashtags 
 * See COMPUTE_SCORE.java for code of UDF
 */

REGISTER twitterProject-0.0.1-SNAPSHOT-job.jar;

DataRecordA  = LOAD '$input' AS (hashTagA:chararray, wordsA: chararray);
DataRecordB  = LOAD '$input' AS (hashTagB:chararray, wordsB: chararray);

CrossProductRecord = CROSS DataRecordA, DataRecordB PARALLEL 40;

Combination = FILTER CrossProductRecord BY hashTagA < hashTagB;
-- TupledCombination = FOREACH Combination GENERATE c AS (tuple(hashTagA,wordsA,hashTagB,wordsB);

Result = FOREACH Combination GENERATE mapreduce.stage3.pigudfs.COMPUTE_SCORE(*);

STORE Result INTO '$output';
