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

public class DataDividedByUser {
	public static class DataDivideMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// input value = user,movie,rating
			// outputKey: user
			// outputValue: movie:rating
			String line = value.toString().trim();
			String[] input = line.split(",");

			if (input.length < 3) {
				throw new IOException("Invalid Input");
			}
			int userId = Integer.parseInt(input[0]);
			String movieId = input[1];
			String rating = input[2];
			context.write(new IntWritable(userId), new Text(movieId + ":" + rating));
		}
	}

	public static class DataDivideReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		// reducer method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			// outputKey key
			// outputValue <movie1:rating, movie2: rating....>
			StringBuilder sequence = new StringBuilder();
			for (Text value: values) {
				sequence.append(value.toString().trim() + ",");
			}
			sequence.deleteCharAt(sequence.length() - 1);
			context.write(key, new Text(sequence.toString()));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDivideMapper.class);
		job.setReducerClass(DataDivideReducer.class);

		job.setJarByClass(DataDividedByUser.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
