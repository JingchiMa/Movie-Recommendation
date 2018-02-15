hadoop com.sun.tools.javac.Main *.java
jar cf recommender.jar *.class
hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrix /Normalize /Multiplication /Sum
hdfs dfs -cat /Sum/* 
hdfs dfs -ls /

