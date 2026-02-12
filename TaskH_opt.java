package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TaskH_opt extends Configured implements Tool {

    private static String[] splitCsv(String line) {
        // NOTE: follows file has a description that can include commas.
        // So we will NOT use this helper for follows; we will split with a limit there.
        return line.split(",", -1);
    }

    // ============================================================
    // JOB 1: Find follower IDs who follow same-region but NOT back
    // Input: CircleNetFollows CSV
    // Cache: CircleNetPage CSV (ID -> region)
    // Output (temp): follower_ID
    // ============================================================

    public static class UnrecipSameRegionMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        private final Map<Integer, Integer> regionById = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Distributed cache file should be symlinked to "CircleNetPage.csv"
            File pageFile = new File("CircleNetPage.csv");
            if (!pageFile.exists()) {
                throw new IOException("Missing cache file CircleNetPage.csv. Did you addCacheFile(pagePath + \"#CircleNetPage.csv\")?");
            }

            try (BufferedReader br = new BufferedReader(new FileReader(pageFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    String[] p = splitCsv(line);
                    // Expect: page_ID,nickname,jobtitle,regioncode,hobby
                    if (p.length < 4) continue;

                    try {
                        int id = Integer.parseInt(p[0].trim());
                        int region = Integer.parseInt(p[3].trim());
                        regionById.put(id, region);
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // follows: follow_ID,follower_ID,followee_ID,date(unix),description(with possible commas)
            String[] p = line.split(",", 5);
            if (p.length < 3) return;

            int followerId, followeeId;
            try {
                followerId = Integer.parseInt(p[1].trim());
                followeeId = Integer.parseInt(p[2].trim());
            } catch (NumberFormatException e) {
                return;
            }

            Integer r1 = regionById.get(followerId);
            Integer r2 = regionById.get(followeeId);
            if (r1 == null || r2 == null) return;

            // same region only
            if (!r1.equals(r2)) return;

            int a = Math.min(followerId, followeeId);
            int b = Math.max(followerId, followeeId);

            // key = unordered pair, value = directed edge
            outKey.set(a + "," + b);
            outVal.set(followerId + "->" + followeeId);
            ctx.write(outKey, outVal);
        }
    }

    public static class UnrecipSameRegionReducer extends Reducer<Text, Text, Text, NullWritable> {
        private final Text out = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String[] ab = key.toString().split(",", -1);
            if (ab.length != 2) return;

            int a, b;
            try {
                a = Integer.parseInt(ab[0].trim());
                b = Integer.parseInt(ab[1].trim());
            } catch (NumberFormatException e) {
                return;
            }

            boolean aToB = false;
            boolean bToA = false;

            for (Text t : values) {
                String s = t.toString();
                String[] xy = s.split("->", -1);
                if (xy.length != 2) continue;

                int from, to;
                try {
                    from = Integer.parseInt(xy[0].trim());
                    to = Integer.parseInt(xy[1].trim());
                } catch (NumberFormatException e) {
                    continue;
                }

                if (from == a && to == b) aToB = true;
                else if (from == b && to == a) bToA = true;

                if (aToB && bToA) break; // reciprocated; done
            }

            // Emit the follower of the one-way edge
            if (aToB && !bToA) {
                out.set(String.valueOf(a));
                ctx.write(out, NullWritable.get());
            } else if (bToA && !aToB) {
                out.set(String.valueOf(b));
                ctx.write(out, NullWritable.get());
            }
        }
    }

    // ============================================================
    // JOB 2: Attach nicknames (ID -> nickname) to the IDs from Job 1
    // Input: temp output from Job 1 (follower_ID)
    // Cache: CircleNetPage CSV (ID -> nickname)
    // Output (final): follower_ID \t nickname
    // ============================================================

    public static class AttachNicknameMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private final Text out = new Text();
        private final Map<Integer, String> nicknameById = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            File pageFile = new File("CircleNetPage.csv");
            if (!pageFile.exists()) {
                throw new IOException("Missing cache file CircleNetPage.csv. Did you addCacheFile(pagePath + \"#CircleNetPage.csv\")?");
            }

            try (BufferedReader br = new BufferedReader(new FileReader(pageFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    String[] p = splitCsv(line);
                    // Expect: page_ID,nickname,jobtitle,regioncode,hobby
                    if (p.length < 2) continue;

                    try {
                        int id = Integer.parseInt(p[0].trim());
                        String nick = p[1].trim();
                        nicknameById.put(id, nick);
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String raw = value.toString().trim();
            if (raw.isEmpty()) return;

            // job1 writes "id" as key, NullWritable as value -> lines often look like: "123"
            // If your environment writes "123\t", this still works:
            String idStr = raw.split("\\s+")[0];

            int id;
            try {
                id = Integer.parseInt(idStr);
            } catch (NumberFormatException e) {
                return;
            }

            String nick = nicknameById.getOrDefault(id, "UNKNOWN");
            out.set(id + "\t" + nick);
            ctx.write(out, NullWritable.get());
        }
    }

    // Optional reducer to dedupe IDs (recommended).
    public static class DedupReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context ctx) throws IOException, InterruptedException {
            ctx.write(key, NullWritable.get()); // one line per unique key
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: TaskH_opt <circleNetPageInput> <circleNetFollowsInput> <tempOutDir> <finalOutDir>");
            System.err.println("Example: hadoop jar project1.jar org.example.TaskH_opt " +
                    "/user/ds503/input/CircleNetPage_full_42.csv " +
                    "/user/ds503/input/CircleNetFollows_full_42.csv " +
                    "/user/ds503/output/taskH_tmp " +
                    "/user/ds503/output/taskH_out");
            return 2;
        }

        Path pagePath = new Path(args[0]);
        Path followsIn = new Path(args[1]);
        Path tmpOut = new Path(args[2]);
        Path finalOut = new Path(args[3]);

        Configuration conf = getConf();

        // ---------------- Job 1 ----------------
        Job job1 = Job.getInstance(conf, "TaskH_opt-UnrecipSameRegion-IDs");
        job1.setJarByClass(TaskH_opt.class);

        // Cache CircleNetPage -> local name CircleNetPage.csv
        job1.addCacheFile(new URI(pagePath.toString() + "#CircleNetPage.csv"));

        job1.setMapperClass(UnrecipSameRegionMapper.class);
        job1.setReducerClass(UnrecipSameRegionReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);

        TextInputFormat.addInputPath(job1, followsIn);
        FileOutputFormat.setOutputPath(job1, tmpOut);

        if (!job1.waitForCompletion(true)) return 1;

        // ---------------- Job 2 ----------------
        Job job2 = Job.getInstance(conf, "TaskH_opt-AttachNicknames");
        job2.setJarByClass(TaskH_opt.class);

        job2.addCacheFile(new URI(pagePath.toString() + "#CircleNetPage.csv"));

        job2.setMapperClass(AttachNicknameMapper.class);

        // Dedupe final lines (so each ID shows once even if multiple one-way follows)
        job2.setReducerClass(DedupReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(NullWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        TextInputFormat.addInputPath(job2, tmpOut);
        FileOutputFormat.setOutputPath(job2, finalOut);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TaskH_opt(), args));
    }
}
