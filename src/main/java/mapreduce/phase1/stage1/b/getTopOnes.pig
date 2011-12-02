---------------------------------------------------------------------------------------- 
-- Phase 1 Stage 1b : Get Top 100 Results by Cluster-Size (number of hashtag mentions) 
---------------------------------------------------------------------------------------- 

dataRecords  = LOAD '$input' AS (clusterSize:int, hashTag:chararray);

filteredRecords = FILTER dataRecords BY clusterSize >= $lowerBound;
sortedRecords = ORDER filteredRecords BY clusterSize DESC;
results = limit sortedRecords 100;

STORE results INTO '$output';
