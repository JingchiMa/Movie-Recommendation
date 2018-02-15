

rm results/part-r-00002
hdfs dfs -get /Sum/part-r-000000 results/part-r-00002

diff results/part-t-00000 results/part-r-00002
