import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Sum {

    public static class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            // inputValue: userId:movieA \t subrating
            // outputKey: userId:movieA
            // outputValue: subrating

            String line = value.toString().trim();
            String[] keyValue = line.split("\t");
            context.write(new Text(keyValue[0]), new DoubleWritable(Double.parseDouble(keyValue[1])));
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
            // inputKey: userId:movieA
            // inputValue: subrating
            // outputKey: userId:movieA
            // outputValue: sum of subrating

            double rating = 0;
            for (DoubleWritable value: values) {
                double subrating = value.get(); // use get() to get double value
                rating += subrating;
            }
            context.write(key, new DoubleWritable(rating));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // class
        job.setJarByClass(Sum.class);
        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        // input and output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // key value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // input and output paths
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
