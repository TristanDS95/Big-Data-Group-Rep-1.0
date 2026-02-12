import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskB_Simple {

    // =========================
    // JOB 1: Count accesses per page
    // =========================
    public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text pageId = new Text();

        // ActivityLog: ActionId,ByWho,WhatPage,ActionType,ActionTime
        @Override
        public void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",", -1);
            if (fields.length < 5) return;

            String whatPage = fields[2].trim();
            if (whatPage.isEmpty() || "WhatPage".equalsIgnoreCase(whatPage)) return; // skip header

            pageId.set(whatPage);
            ctx.write(pageId, ONE);
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context ctx)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            ctx.write(key, new IntWritable(sum));
        }
    }

    // =========================
    // JOB 2: Join + Top 10
    // =========================
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outVal = new Text();
        private boolean isPageFile;

        @Override
        protected void setup(Context ctx) throws IOException, InterruptedException {
            String path = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) ctx.getInputSplit())
                    .getPath().toString();
            isPageFile = path.contains("CircleNetPage");
        }

        @Override
        public void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {

            if (isPageFile) {
                // CircleNetPage: ID,NickName,JobTitle,RegionCode,FavoriteHobby
                String[] f = value.toString().split(",", -1);
                if (f.length < 5) return;

                String id = f[0].trim();
                if (id.isEmpty() || "ID".equalsIgnoreCase(id)) return; // skip header

                String nickname = f[1].trim();
                String job = f[2].trim();

                outKey.set(id);
                outVal.set("P\t" + nickname + "\t" + job);
                ctx.write(outKey, outVal);

            } else {
                // counts output: pageId \t count
                String[] parts = value.toString().split("\t", -1);
                if (parts.length != 2) return;

                String id = parts[0].trim();
                String count = parts[1].trim();
                if (id.isEmpty() || count.isEmpty()) return;

                outKey.set(id);
                outVal.set("C\t" + count);
                ctx.write(outKey, outVal);
            }
        }
    }

    public static class Top10Reducer extends Reducer<Text, Text, Text, Text> {

        private static class Row {
            String id, nickname, job;
            int count;
            Row(String id, String nickname, String job, int count) {
                this.id = id;
                this.nickname = nickname;
                this.job = job;
                this.count = count;
            }
        }

        private final PriorityQueue<Row> pq = new PriorityQueue<>(Comparator.comparingInt(r -> r.count));

        @Override
        public void reduce(Text key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {

            String nickname = null;
            String job = null;
            Integer count = null;

            for (Text t : values) {
                String[] p = t.toString().split("\t", -1);
                if (p.length < 2) continue;

                if ("P".equals(p[0]) && p.length >= 3) {
                    nickname = p[1];
                    job = p[2];
                } else if ("C".equals(p[0])) {
                    try {
                        count = Integer.parseInt(p[1]);
                    } catch (NumberFormatException e) {
                        // ignore bad count
                    }
                }
            }

            if (nickname != null && job != null && count != null) {
                pq.offer(new Row(key.toString(), nickname, job, count));
                if (pq.size() > 10) pq.poll();
            }
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            List<Row> top = new ArrayList<>();
            while (!pq.isEmpty()) top.add(pq.poll());
            Collections.reverse(top);

            for (Row r : top) {
                // Output: ID \t NickName \t JobTitle \t AccessCount
                ctx.write(new Text(r.id), new Text(r.nickname + "\t" + r.job + "\t" + r.count));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: TaskB_Simple <ActivityLog> <CircleNetPage> <Output>");
            System.exit(1);
        }

        String activity = args[0];
        String pages = args[1];
        String output = args[2];
        String temp = output + "_tmp_counts";

        // ---- Job 1 ----
        Job job1 = Job.getInstance(new Configuration(), "TaskB_Simple_Count");
        job1.setJarByClass(TaskB_Simple.class);

        job1.setMapperClass(CountMapper.class);
        job1.setReducerClass(SumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(activity));
        FileOutputFormat.setOutputPath(job1, new Path(temp));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // ---- Job 2 ----
        Job job2 = Job.getInstance(new Configuration(), "TaskB_Simple_Join_Top10");
        job2.setJarByClass(TaskB_Simple.class);

        job2.setMapperClass(JoinMapper.class);
        job2.setReducerClass(Top10Reducer.class);
        job2.setNumReduceTasks(1); // global top-10

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(temp));
        FileInputFormat.addInputPath(job2, new Path(pages));
        FileOutputFormat.setOutputPath(job2, new Path(output));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
