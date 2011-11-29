

REGISTER twitterProject-0.0.1-SNAPSHOT-job.jar;

TrendyTag  = LOAD 'data/taggedtweetsample.txt' AS (clusterSize: int, clusterTag :chararray);
Tweet  = LOAD 'data/clsters.txt' AS (
	tweetId: int,
	userId: int,
	timestamp:  chararray,
	replyTweetId : int
	replyUserId: int
	source: chararray
	isTruncated: chararray,
	isFavorited: chararray,
	location: chararray,
	text: chararray);
	
			 
--CrossProductRecord = CROSS TrendyTag, Tweet PARALLEL 40;

--Combination = FILTER CrossProductRecord BY mapreduce.phase2.stage1.pigudfs. hashTagA < hashTagB;
-- TupledCombination = FOREACH Combination GENERATE c AS (tuple(hashTagA,wordsA,hashTagB,wordsB);

Result = empty.bag;
FOREACH Tweet{
	Result = UNION Result, GENERATE mapreduce.phase2.stage1.pigudfs.FIND_MONTHLY_SIZE(*);
}

DUMP Result;

STORE Result INTO '$output';
