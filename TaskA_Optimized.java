import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskA_Optimized {

    // CircleNetPage: ID,NickName,JobTitle,RegionCode,FavoriteHobby
    public static class HobbyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text hobby = new Text();
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] f = value.toString().split(",", -1);
            if (f.length < 5) return;

            String fav = f[4].trim();
            if (fav.isEmpty() || "FavoriteHobby".equalsIgnoreCase(fav)) return;

            hobby.set(fav);
            context.write(hobby, ONE);
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TaskA_Optimized <CircleNetPage.csv> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(new Configuration(), "TaskA_Optimized_HobbyCount");
        job.setJarByClass(TaskA_Optimized.class);

        job.setMapperClass(HobbyMapper.class);
        job.setCombinerClass(SumReducer.class);   // Optimization: local aggregation
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
