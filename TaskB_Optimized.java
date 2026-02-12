import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskB_Optimized {

    // =========================
    // JOB 1: Count accesses per page (with Combiner)
    // =========================
    public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text pageId = new Text();

        @Override
        public void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",", -1);
            if (fields.length < 5) return;

            String whatPage = fields[2].trim();
            if (whatPage.isEmpty() || "WhatPage".equalsIgnoreCase(whatPage)) return;

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
    // JOB 2: Reduce-side join, emit (count + profile), local top10 per reducer
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
                if (id.isEmpty() || "ID".equalsIgnoreCase(id)) return;

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

    // Job2 reducer: does join and keeps top-10 *per reducer*
    public static class LocalTop10JoinReducer extends Reducer<Text, Text, NullWritable, Text> {

        private static class Row {
            String id, nickname, job;
            int count;
            Row(String id, String nickname, String job, int count) {
                this.id = id; this.nickname = nickname; this.job = job; this.count = count;
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
                    try { count = Integer.parseInt(p[1]); }
                    catch (NumberFormatException e) { /* ignore */ }
                }
            }

            if (nickname != null && job != null && count != null) {
                pq.offer(new Row(key.toString(), nickname, job, count));
                if (pq.size() > 10) pq.poll();
            }
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            // Emit local top10 rows as plain text:
            // id \t nickname \t job \t count
            List<Row> top = new ArrayList<>();
            while (!pq.isEmpty()) top.add(pq.poll());
            Collections.reverse(top);

            for (Row r : top) {
                ctx.write(NullWritable.get(),
                        new Text(r.id + "\t" + r.nickname + "\t" + r.job + "\t" + r.count));
            }
        }
    }

    // =========================
    // JOB 3: Global top10 from Job2 partial top10s
    // =========================
    public static class GlobalTop10Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {
            // Pass through
            ctx.write(NullWritable.get(), value);
        }
    }

    public static class GlobalTop10Reducer extends Reducer<NullWritable, Text, Text, Text> {

        private static class Row {
            String id, nickname, job;
            int count;
            Row(String id, String nickname, String job, int count) {
                this.id = id; this.nickname = nickname; this.job = job; this.count = count;
            }
        }

        private final PriorityQueue<Row> pq = new PriorityQueue<>(Comparator.comparingInt(r -> r.count));

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {

            for (Text t : values) {
                String[] p = t.toString().split("\t", -1);
                if (p.length < 4) continue;

                String id = p[0].trim();
                String nickname = p[1].trim();
                String job = p[2].trim();

                int count;
                try { count = Integer.parseInt(p[3].trim()); }
                catch (NumberFormatException e) { continue; }

                pq.offer(new Row(id, nickname, job, count));
                if (pq.size() > 10) pq.poll();
            }
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            List<Row> top = new ArrayList<>();
            while (!pq.isEmpty()) top.add(pq.poll());
            Collections.reverse(top);

            for (Row r : top) {
                // Final output: ID \t NickName \t JobTitle \t AccessCount
                ctx.write(new Text(r.id), new Text(r.nickname + "\t" + r.job + "\t" + r.count));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: TaskB_Optimized <ActivityLog> <CircleNetPage> <Output>");
            System.exit(1);
        }

        String activity = args[0];
        String pages = args[1];
        String output = args[2];

        String tmpCounts = output + "_tmp_counts";
        String tmpLocalTop = output + "_tmp_localtop";

        Configuration conf = new Configuration();

        // ---- Job 1 (count + combiner) ----
        Job job1 = Job.getInstance(conf, "TaskB_Optimized_Count");
        job1.setJarByClass(TaskB_Optimized.class);

        job1.setMapperClass(CountMapper.class);
        job1.setCombinerClass(SumReducer.class);   // Optimization
        job1.setReducerClass(SumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(activity));
        FileOutputFormat.setOutputPath(job1, new Path(tmpCounts));

        if (!job1.waitForCompletion(true)) System.exit(1);

        // ---- Job 2 (join + local top10 per reducer) ----
        Job job2 = Job.getInstance(conf, "TaskB_Optimized_Join_LocalTop10");
        job2.setJarByClass(TaskB_Optimized.class);

        job2.setMapperClass(JoinMapper.class);
        job2.setReducerClass(LocalTop10JoinReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setNumReduceTasks(2); // optimization: parallel join reducers

        FileInputFormat.addInputPath(job2, new Path(tmpCounts));
        FileInputFormat.addInputPath(job2, new Path(pages));
        FileOutputFormat.setOutputPath(job2, new Path(tmpLocalTop));

        if (!job2.waitForCompletion(true)) System.exit(1);

        // ---- Job 3 (global top10) ----
        Job job3 = Job.getInstance(conf, "TaskB_Optimized_GlobalTop10");
        job3.setJarByClass(TaskB_Optimized.class);

        job3.setMapperClass(GlobalTop10Mapper.class);
        job3.setReducerClass(GlobalTop10Reducer.class);

        job3.setMapOutputKeyClass(NullWritable.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setNumReduceTasks(1); // global aggregation

        FileInputFormat.addInputPath(job3, new Path(tmpLocalTop));
        FileOutputFormat.setOutputPath(job3, new Path(output));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
