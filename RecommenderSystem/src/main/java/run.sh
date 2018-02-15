hdfs dfs -rm -r /dataDividedByUser
hdfs dfs -rm -r /coOccurrenceMatrix
hdfs dfs -rm -r /Normalize
hdfs dfs -rm -r /Multiplication
hdfs dfs -rm -r /Sum
hdfs dfs -mkdir /input
hdfs dfs -mkdir /mysql

hdfs dfs -put mysqlConnector/mysql-connector-java-*.jar /mysql/

hdfs dfs -put ~/src/Project4/RecommenderSystem/input/* /input
hdfs dfs -ls /input
hdfs dfs -ls /
