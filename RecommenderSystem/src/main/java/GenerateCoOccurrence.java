import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GenerateCoOccurrence {
	public static class GenerateCoOccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			// inputValue: user1 \t movie1:rating1,movie2:rating2...
			String line = value.toString().trim();
			String[] keyValuePair = line.split("\t"); // key value splited by '\t' by default
			if (keyValuePair.length != 2) { // might be a new user, temporarily no operations
				return;
			}
			String[] movieRatingPairs = keyValuePair[1].trim().split(",");
			if (movieRatingPairs.length == 0) {
				throw new IOException("No Value for this user");
			}
			// making moviePairs: movie1:movie1, movie1:movie2,...etc
			for (int i = 0; i < movieRatingPairs.length; i++) {
				String movieA = movieRatingPairs[i].split(":")[0];
				for (int j = 0; j < movieRatingPairs.length; j++) {
					String movieB = movieRatingPairs[j].split(":")[0];
					context.write(new Text(movieA + ":" + movieB), new IntWritable(1));
				}
			}
		}
	}

	public static class GenerateCoOccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// reducer method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

			int relationNum = 0;
			for (IntWritable value: values) {
				relationNum += value.get();
			}
			context.write(key, new IntWritable(relationNum));
		}
	}
	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setMapperClass(GenerateCoOccurrenceMapper.class);
		job.setReducerClass(GenerateCoOccurrenceReducer.class);
		job.setJarByClass(GenerateCoOccurrence.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}
}