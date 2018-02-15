import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiplication {
	// CoOccurrence matrix
	public static class CoOccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			// inputValue: movie2\tmovie1=relativeRelation
			String line = value.toString().trim();
			String[] keyValue = line.split("\t");
			if (keyValue.length != 2) {
				throw new IOException("Invalid Input from Normalizer");
			}
			context.write(new Text(keyValue[0]), new Text(keyValue[1]));
		}
	}
	// Rating Column Vector
	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text>  {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			// inputValue: userId,movie,rating
			// outputKey: movie
			// outputValue: userId:rating
			String line = value.toString().trim();
			String[] keyValue = line.split(",");
			if (keyValue.length != 3) {
				throw new IOException("Rating input doesn't contain 3 inputs");
			}
			String movie = keyValue[1];
			String userId = keyValue[0];
			String rating = keyValue[2];
			context.write(new Text(movie), new Text(userId + ":" + rating));
		}
	}

	public static class MulitplicationReducer extends Reducer<Text, Text, Text, DoubleWritable>  {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// inputKey: movie2
			// inputValue: movie1=relativeRelation or userId:rating
			// outputKey: ueserId:movie2
			// outputValue: rating

			Map<String, Double> relationMap = new HashMap<>(); // movie1 -> relativeRelation
			Map<String, Double> ratingMap = new HashMap<>(); // userId -> rating
			for (Text value: values) {
				String curValue = value.toString().trim();
				if (curValue.contains("=")) { // movie1=relativeRelation
					String[] movieRelation = curValue.split("=");
					relationMap.put(movieRelation[0], Double.parseDouble((movieRelation[1])));
				} else { // userId:rating
					String[] userRating = curValue.split(":");
					ratingMap.put(userRating[0], Double.parseDouble(userRating[1]));
				}
			}

			for (Map.Entry<String, Double> entry: relationMap.entrySet()) {
				String outputMovie = entry.getKey();
				Double relativeRelation = entry.getValue();
				for (Map.Entry<String, Double> entry2: ratingMap.entrySet()) {
					String userId = entry2.getKey();
					Double rating = entry2.getValue();
					context.write(new Text(userId + ":" + outputMovie), new DoubleWritable(relativeRelation * rating));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// jarClass + mapper class + reducer class
		job.setJarByClass(Multiplication.class);
		ChainMapper.addMapper(job, CoOccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CoOccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);
		job.setReducerClass(MulitplicationReducer.class);

		// why the following two lines are needless?
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(OutputFormat.class);

		// output key value class
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// input and output paths
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CoOccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}
}
