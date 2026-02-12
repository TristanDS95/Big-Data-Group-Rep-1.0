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

import java.io.IOException;
import java.util.HashSet;

public class TaskE_Optimized {

    // ActivityLog: ActionId, ByWho, WhatPage, ActionType, ActionTime
    public static class LogMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final IntWritable outKey = new IntWritable();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] cols = line.split(",", -1);
            if (cols.length < 3) return;

            try {
                int byWho = Integer.parseInt(cols[1].trim());
                int whatPage = Integer.parseInt(cols[2].trim());

                outKey.set(byWho);

                // total actions
                outVal.set("A|1");
                context.write(outKey, outVal);

                // distinct pages token
                outVal.set("P|" + whatPage);
                context.write(outKey, outVal);

            } catch (NumberFormatException ignored) { }
        }
    }

    // Combiner: partial aggregation
    public static class AggCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int actionSum = 0;
            HashSet<String> pages = new HashSet<>();

            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("A|")) {
                    actionSum += Integer.parseInt(s.substring(2));
                } else if (s.startsWith("P|")) {
                    pages.add(s); // keep unique page tokens
                }
            }

            if (actionSum > 0) {
                outVal.set("A|" + actionSum);
                context.write(key, outVal);
            }
            for (String p : pages) {
                outVal.set(p);
                context.write(key, outVal);
            }
        }
    }

    // Reducer: final aggregation
    public static class AggReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int totalActions = 0;
            HashSet<Integer> distinctPages = new HashSet<>();

            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("A|")) {
                    totalActions += Integer.parseInt(s.substring(2));
                } else if (s.startsWith("P|")) {
                    distinctPages.add(Integer.parseInt(s.substring(2)));
                }
            }

            outVal.set(totalActions + "\t" + distinctPages.size());
            context.write(key, outVal);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TaskE_Optimized <ActivityLog> <Output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskE_Optimized_TotalAndDistinct_WithCombiner");
        job.setJarByClass(TaskE_Optimized.class);

        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(AggCombiner.class);
        job.setReducerClass(AggReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class); // ID (ByWho)
        job.setOutputValueClass(Text.class);      // totalActions \t distinctPages

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

