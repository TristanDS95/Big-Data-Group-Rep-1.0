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

public class TaskE_Simple {

    // ActivityLog: ActionId, ByWho, WhatPage, ActionType, ActionTime :contentReference[oaicite:2]{index=2}
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

                // 1) Count total actions
                outVal.set("A");
                context.write(outKey, outVal);

                // 2) Track distinct pages
                outVal.set("P|" + whatPage);
                context.write(outKey, outVal);

            } catch (NumberFormatException ignored) { }
        }
    }

    public static class AggReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int totalActions = 0;
            HashSet<Integer> distinctPages = new HashSet<>();

            for (Text t : values) {
                String s = t.toString();
                if (s.equals("A")) totalActions += 1;
                else if (s.startsWith("P|")) distinctPages.add(Integer.parseInt(s.substring(2)));
            }

            outVal.set(totalActions + "\t" + distinctPages.size());
            context.write(key, outVal);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TaskE_Simple <ActivityLog> <Output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskE_Simple_TotalAndDistinct");
        job.setJarByClass(TaskE_Simple.class);

        job.setMapperClass(LogMapper.class);
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
