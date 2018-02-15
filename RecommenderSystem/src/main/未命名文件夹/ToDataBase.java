import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ToDataBase {
    public static class ToDataBaseMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // inputValue: userId:movieId \t expectedRating
            String line = value.toString().trim();
            String[] inputs = line.split("\t");
            String[] user_movie = inputs[0].split(":");
            double expectedRating = Double.parseDouble(inputs[1]);
            String userId = user_movie[0];
            String movieId = user_movie[1];

            context.write(new Text(userId + ":" + movieId), new DoubleWritable(expectedRating));
        }
    }

    public static class ToDataBaseReducer extends Reducer<Text, DoubleWritable, DBOutputWritable, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
            // inputKey: userId:movieId
            // inputValue: expectedRating
            // outputKey: new DBWritable(userId, movieId, expectedRating)
            // outputValue: NullWritable
            String[] user_movie = key.toString().trim().split(":");
            String userId = user_movie[0];
            String movieId = user_movie[1];
            double expectedRating;
            for (DoubleWritable value: values) {
                expectedRating = value.get();
                context.write(new DBOutputWritable(userId, movieId, expectedRating), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //Use dbConfiguration to configure all the jdbcDriver, db user, db password, database
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://164.67.226.196:8889/MovieRecommenderSystem",
                "root",
                "root");
        Job job = Job.getInstance(conf);
        job.setJobName("ToDatabase");
        job.setJarByClass(ToDataBase.class);
        job.setMapperClass(ToDataBaseMapper.class);
        job.setReducerClass(ToDataBaseReducer.class);

        //How to add external dependency to current project?
		/*
		  1. upload dependency to hdfs
		  2. use this "addArchiveToClassPath" method to define the dependency path on hdfs
		 */
        job.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));
        //Why do we add map outputKey and outputValue?
        //Because map output key and value are inconsistent with reducer output key and value

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(DBOutputWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        TextInputFormat.setInputPaths(job, args[0]);

        //use dbOutputformat to define the table name and columns

        DBOutputFormat.setOutput(job, "ExpectedMovieRating", new String[] {"userId", "movieId", "expectedRating"});
        job.waitForCompletion(true);
    }
}
