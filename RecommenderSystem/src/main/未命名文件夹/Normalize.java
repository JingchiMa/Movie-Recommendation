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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // mapper method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // inputValue: movie1:movie2 \t relationNum
            // outputKey: movie2
            // outputValue: movie1=relation
            String line = value.toString().trim();
            String[] input = line.split("\t");
            if (input.length != 2) {
                throw new IOException("InValid Input from GenerateCoOccurrence");
            }
            String[] movies = input[0].split(":");
            String relation = input[1];
            if (movies.length != 2) {
                throw new IOException("Invalid MoviePair from generateCoOccurrence");
            }
            String movie2 = movies[1];
            String movie1 = movies[0];
            context.write(new Text(movie1), new Text(movie2 + "=" + relation));
            context.getCounter("MapperResultForNormalize", movie2 + " " + relation);
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // note the iterable values can only be iterated only once! Therefore we have to store the values as iterating.
            Map<String, Integer> map = new HashMap<>();
            // inputKey: movie1
            // inputValue: <movie2=relation2, movie3=relation3...>
            // outputKey: movie2, movie3, ...
            // outputValue: movie1=relativeRelation
            int sum = 0;
            for (Text value: values) {
                String inputValue = value.toString().trim();
                String[] movie_relation = inputValue.split("=");
                if (movie_relation.length < 2) {
                    throw new IOException("NormalizeReducer Error");
                }
                String movie2 = movie_relation[0];
                int relation = Integer.parseInt(movie_relation[1]);
                sum += relation;
                map.put(movie2, relation);
            }

            for (Map.Entry<String, Integer> entry: map.entrySet()) {
                double relativeRelation = entry.getValue() * 1.0 / sum;
                context.write(new Text(entry.getKey()), new Text(key + "=" + relativeRelation));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // set class
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);
        job.setJarByClass(Normalize.class);

        // set input and output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //set input and output key value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set input and output paths
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
