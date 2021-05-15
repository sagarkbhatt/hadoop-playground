package in.sagarkbhatt.playground.hadoop.movielens;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PopularMovie {

    public static class MapperGetRating extends Mapper<Object, Text, Text, LongWritable> {

        private final LongWritable one = new LongWritable(1);
        private Text movieId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            movieId.set(data[1]);
            context.write(movieId, one);
        }
    }

    public static class CombinerGetRating extends Reducer<Text, IntWritable, Text, LongWritable> {

        private LongWritable movieCount = new LongWritable();

        public void reduce(Text movieId, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            movieCount.set(sum);
            context.write(movieId, movieCount);
        }
    }

    public static class ReducerGroupByRatingCount extends Reducer<Text, LongWritable, LongWritable, Text> {
        private LongWritable movieCount = new LongWritable();

        public void reduce(Text movieId, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            movieCount.set(sum);
            context.write(movieCount, movieId);
        }
    }

    public static class MapKey extends Mapper<Object, Text, LongWritable, Text> {

        private final LongWritable ratingCount = new LongWritable();
        private Text movieId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            movieId.set(data[1]);
            ratingCount.set(Long.parseLong(data[0]));
            context.write(ratingCount, movieId);
        }
    }

    public static class ReducerSortedOutput extends Reducer<LongWritable, Text, Text, LongWritable> {

        public void reduce(LongWritable ratingCount, Iterable<Text> movies, Context context) throws IOException, InterruptedException {
            for (Text movie : movies) {
                context.write(movie, ratingCount);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "popular movie");
        job1.setJarByClass(PopularMovie.class);

        job1.setMapperClass(MapperGetRating.class);
        job1.setCombinerClass(CombinerGetRating.class);
        job1.setReducerClass(ReducerGroupByRatingCount.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean isJobCompleted = job1.waitForCompletion(true);
        if(!isJobCompleted) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "popular movie");
        job2.setJarByClass(PopularMovie.class);

        job2.setMapperClass(MapKey.class);
        job2.setReducerClass(ReducerSortedOutput.class);

        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
