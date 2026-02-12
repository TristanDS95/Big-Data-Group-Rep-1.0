import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskH {

    // Simple CSV split (assumes no embedded commas in fields, per your generator constraints)
    private static String[] split(String line) {
        return line.split(",", -1);
    }

    private static String t(String s) {
        return s == null ? "" : s.trim();
    }

    // ============================================================
    // JOB 1 – Attach follower region (reduce-side join)
    // Output: follower,followee,followerRegion
    // Join key: followerId
    // ============================================================

    public static class J1_PageMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String[] p = split(value.toString());
            if (p.length < 4) return;

            String id = t(p[0]);
            String region = t(p[3]);
            if (id.isEmpty() || region.isEmpty()) return;

            ctx.write(new Text(id), new Text("P|" + region));
        }
    }

    public static class J1_FollowsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String[] p = split(value.toString());
            if (p.length < 2) return;

            String follower = t(p[0]);
            String followee = t(p[1]);
            if (follower.isEmpty() || followee.isEmpty()) return;
            if (follower.equals(followee)) return;

            ctx.write(new Text(follower), new Text("F|" + followee));
        }
    }

    public static class J1_Reducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        public void reduce(Text followerId, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {

            String followerRegion = null;
            List<String> followees = new ArrayList<>();

            for (Text tv : values) {
                String v = tv.toString();
                if (v.startsWith("P|")) {
                    followerRegion = v.substring(2);
                } else if (v.startsWith("F|")) {
                    followees.add(v.substring(2));
                }
            }

            if (followerRegion == null) return;

            String follower = followerId.toString();
            for (String followee : followees) {
                ctx.write(NullWritable.get(), new Text(follower + "," + followee + "," + followerRegion));
            }
        }
    }

    // ============================================================
    // JOB 2 – Attach followee region (reduce-side join)
    // Input: Job1 output + Page
    // Output: follower,followee,followerRegion,followeeRegion
    // Join key: followeeId
    // ============================================================

    public static class J2_PageMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String[] p = split(value.toString());
            if (p.length < 4) return;

            String id = t(p[0]);
            String region = t(p[3]);
            if (id.isEmpty() || region.isEmpty()) return;

            ctx.write(new Text(id), new Text("P|" + region));
        }
    }

    public static class J2_Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // follower,followee,followerRegion
            String[] p = split(value.toString());
            if (p.length < 3) return;

            String follower = t(p[0]);
            String followee = t(p[1]);
            String followerRegion = t(p[2]);

            if (follower.isEmpty() || followee.isEmpty() || followerRegion.isEmpty()) return;

            // join key is followee id
            ctx.write(new Text(followee), new Text("E|" + follower + "|" + followerRegion));
        }
    }

    public static class J2_Reducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        public void reduce(Text followeeId, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {

            String followeeRegion = null;
            List<String> edges = new ArrayList<>();

            for (Text tv : values) {
                String v = tv.toString();
                if (v.startsWith("P|")) {
                    followeeRegion = v.substring(2);
                } else if (v.startsWith("E|")) {
                    edges.add(v);
                }
            }

            if (followeeRegion == null) return;

            String followee = followeeId.toString();
            for (String e : edges) {
                // E|follower|followerRegion
                String[] parts = e.split("\\|", -1);
                if (parts.length < 3) continue;

                String follower = parts[1];
                String followerRegion = parts[2];

                ctx.write(NullWritable.get(),
                        new Text(follower + "," + followee + "," + followerRegion + "," + followeeRegion));
            }
        }
    }

    // ============================================================
    // JOB 3 – Filter same-region (map-only)
    // Input: follower,followee,followerRegion,followeeRegion
    // Output: follower,followee
    // ============================================================

    public static class J3_FilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String[] p = split(value.toString());
            if (p.length < 4) return;

            String follower = t(p[0]);
            String followee = t(p[1]);
            String fr = t(p[2]);
            String er = t(p[3]);

            if (follower.isEmpty() || followee.isEmpty() || fr.isEmpty() || er.isEmpty()) return;

            if (fr.equals(er)) {
                ctx.write(NullWritable.get(), new Text(follower + "," + followee));
            }
        }
    }

    // ============================================================
    // JOB 4 – One-sided relationship detection (unordered pair + direction)
    // Input: follower,followee
    // Output: followerId (the one who follows in the one-sided relationship)
    // ============================================================

    public static class J4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String[] p = split(value.toString());
            if (p.length < 2) return;

            String followerS = t(p[0]);
            String followeeS = t(p[1]);
            if (followerS.isEmpty() || followeeS.isEmpty()) return;

            long follower, followee;
            try {
                follower = Long.parseLong(followerS);
                followee = Long.parseLong(followeeS);
            } catch (NumberFormatException ex) {
                return;
            }
            if (follower == followee) return;

            long lo = Math.min(follower, followee);
            long hi = Math.max(follower, followee);

            // AB means lo -> hi ; BA means hi -> lo
            String dir = (follower == lo && followee == hi) ? "AB" : "BA";

            ctx.write(new Text(lo + "," + hi), new Text(dir + "|" + follower));
        }
    }

    public static class J4_Reducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        public void reduce(Text pairKey, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {

            boolean sawAB = false, sawBA = false;
            String followerAB = null, followerBA = null;

            for (Text tv : values) {
                String[] p = tv.toString().split("\\|", -1);
                if (p.length < 2) continue;

                String dir = p[0];
                String followerId = p[1];

                if ("AB".equals(dir)) {
                    sawAB = true;
                    followerAB = followerId;
                } else if ("BA".equals(dir)) {
                    sawBA = true;
                    followerBA = followerId;
                }
            }

            // one-sided only
            if (sawAB ^ sawBA) {
                String followerOut = sawAB ? followerAB : followerBA;
                if (followerOut != null) {
                    ctx.write(NullWritable.get(), new Text(followerOut));
                }
            }
        }
    }

    // ============================================================
    // JOB 5 – Attach nickname (reduce-side join)
    // Input: Page + Job4 output (followerId)
    // Output: followerId,nickname
    // Join key: followerId
    // ============================================================

    public static class J5_PageMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String[] p = split(value.toString());
            if (p.length < 2) return;

            String id = t(p[0]);
            String nickname = t(p[1]);
            if (id.isEmpty() || nickname.isEmpty()) return;

            ctx.write(new Text(id), new Text("P|" + nickname));
        }
    }

    public static class J5_IdMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String id = t(value.toString());
            if (id.isEmpty()) return;

            ctx.write(new Text(id), new Text("I"));
        }
    }

    public static class J5_Reducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        public void reduce(Text id, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {

            String nickname = null;
            boolean want = false;

            for (Text tv : values) {
                String v = tv.toString();
                if ("I".equals(v)) {
                    want = true;
                } else if (v.startsWith("P|")) {
                    nickname = v.substring(2);
                }
            }

            if (want && nickname != null) {
                ctx.write(NullWritable.get(), new Text(id.toString() + "," + nickname));
            }
        }
    }

    // ============================================================
    // DRIVER
    // ============================================================

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: TaskH <PageCSV> <FollowsCSV> <OutputBaseDir>");
            System.exit(2);
        }

        String pagePath = args[0];
        String followsPath = args[1];
        String outBase = args[2];

        Configuration conf = new Configuration();

        Path out1 = new Path(outBase + "/job1");
        Path out2 = new Path(outBase + "/job2");
        Path out3 = new Path(outBase + "/job3");
        Path out4 = new Path(outBase + "/job4");
        Path out5 = new Path(outBase + "/final");

        // ---------------- JOB 1 ----------------
        Job job1 = Job.getInstance(conf, "TaskH-Job1-AttachFollowerRegion");
        job1.setJarByClass(TaskH.class);

        // IMPORTANT: mapper outputs Text,Text
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);

        job1.setReducerClass(J1_Reducer.class);

        MultipleInputs.addInputPath(job1, new Path(pagePath), TextInputFormat.class, J1_PageMapper.class);
        MultipleInputs.addInputPath(job1, new Path(followsPath), TextInputFormat.class, J1_FollowsMapper.class);

        FileOutputFormat.setOutputPath(job1, out1);

        if (!job1.waitForCompletion(true)) System.exit(1);

        // ---------------- JOB 2 ----------------
        Job job2 = Job.getInstance(conf, "TaskH-Job2-AttachFolloweeRegion");
        job2.setJarByClass(TaskH.class);

        // IMPORTANT: mapper outputs Text,Text
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setReducerClass(J2_Reducer.class);

        MultipleInputs.addInputPath(job2, new Path(pagePath), TextInputFormat.class, J2_PageMapper.class);
        MultipleInputs.addInputPath(job2, out1, TextInputFormat.class, J2_Job1Mapper.class);

        FileOutputFormat.setOutputPath(job2, out2);

        if (!job2.waitForCompletion(true)) System.exit(1);

        // ---------------- JOB 3 (map-only) ----------------
        Job job3 = Job.getInstance(conf, "TaskH-Job3-FilterSameRegion");
        job3.setJarByClass(TaskH.class);

        job3.setMapperClass(J3_FilterMapper.class);
        job3.setNumReduceTasks(0);

        // mapper outputs NullWritable,Text
        job3.setOutputKeyClass(NullWritable.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, out2);
        FileOutputFormat.setOutputPath(job3, out3);

        if (!job3.waitForCompletion(true)) System.exit(1);

        // ---------------- JOB 4 ----------------
        Job job4 = Job.getInstance(conf, "TaskH-Job4-OneSided");
        job4.setJarByClass(TaskH.class);

        job4.setMapperClass(J4_Mapper.class);
        job4.setReducerClass(J4_Reducer.class);

        // IMPORTANT: mapper outputs Text,Text
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);

        job4.setOutputKeyClass(NullWritable.class);
        job4.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job4, out3);
        FileOutputFormat.setOutputPath(job4, out4);

        if (!job4.waitForCompletion(true)) System.exit(1);

        // ---------------- JOB 5 ----------------
        Job job5 = Job.getInstance(conf, "TaskH-Job5-JoinNickname");
        job5.setJarByClass(TaskH.class);

        // IMPORTANT: mapper outputs Text,Text
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);

        job5.setOutputKeyClass(NullWritable.class);
        job5.setOutputValueClass(Text.class);

        job5.setReducerClass(J5_Reducer.class);

        MultipleInputs.addInputPath(job5, new Path(pagePath), TextInputFormat.class, J5_PageMapper.class);
        MultipleInputs.addInputPath(job5, out4, TextInputFormat.class, J5_IdMapper.class);

        FileOutputFormat.setOutputPath(job5, out5);

        System.exit(job5.waitForCompletion(true) ? 0 : 1);
    }
}
