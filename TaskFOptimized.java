import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Task F (Optimized with Combiner) - UPDATED OUTPUT
 *
 * Now outputs: followeeID \t followerCount
 * for followees whose followerCount > average followers across all owners.
 *
 * Pipeline:
 *   Job1 (map-only): Count total pages (owners) from CircleNetPage using a counter.
 *   Job2 (MR + combiner): Count followers per followee (owner) from CircleNetFollows; also count total follows using a counter.
 *   Job3 (map-only): Filter followees whose followerCount > avgFollowers (avg passed via config),
 *                   and output (followeeID, followerCount).
 *
 * Args:
 *   args[0] = input path to CircleNetPage CSV
 *   args[1] = input path to CircleNetFollows CSV
 *   args[2] = temp output path for Job2 (followers per owner)
 *   args[3] = final output path for Job3 (popular owners + follower counts)
 *
 * Assumptions:
 * - No headers.
 * - CSV fields do not contain commas inside fields (your generator removes commas in strings).
 */
public class TaskFOptimized extends Configured implements Tool {

    // Hadoop counters (globally aggregated)
    public enum GlobalCounters {
        TOTAL_PAGES,
        TOTAL_FOLLOWS
    }

    /**
     * JOB 1: Count CircleNetPage owners (total pages)
     * Input: CircleNetPage CSV lines
     * Output: none (map-only, uses counter)
     */
    public static class CountPagesMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            if (value == null) return;
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            context.getCounter(GlobalCounters.TOTAL_PAGES).increment(1);
        }
    }

    /**
     * JOB 2: Count followers per followee (owner)
     * Mapper emits: (followeeID, 1)
     * Combiner/Reducer sums: (followeeID, followerCount)
     *
     * Also increments TOTAL_FOLLOWS counter once per follow edge.
     */
    public static class FollowCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final IntWritable outKey = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            if (value == null) return;
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // CircleNetFollows schema (no header):
            // follow_ID,follower_ID,followee_ID,date(unix),description
            String[] parts = line.split(",", 5);
            if (parts.length < 3) return;

            String followeeStr = parts[2].trim();
            if (followeeStr.isEmpty()) return;

            int followeeId;
            try {
                followeeId = Integer.parseInt(followeeStr);
            } catch (NumberFormatException e) {
                return;
            }

            // Count total follow edges globally
            context.getCounter(GlobalCounters.TOTAL_FOLLOWS).increment(1);

            // Emit per-followee count contribution
            outKey.set(followeeId);
            context.write(outKey, ONE);
        }
    }

    /**
     * Combiner: local sum of counts per followeeID
     */
    public static class SumIntCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private final IntWritable outVal = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws java.io.IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            outVal.set(sum);
            context.write(key, outVal);
        }
    }

    /**
     * Reducer: global sum of counts per followeeID
     */
    public static class SumIntReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private final IntWritable outVal = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws java.io.IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            outVal.set(sum);
            context.write(key, outVal);
        }
    }

    /**
     * JOB 3: Filter followees whose followerCount > avgFollowers (map-only)
     * Input: Job2 output (tab-separated): followeeID \t followerCount
     *
     * Uses KeyValueTextInputFormat so:
     *   key = "followeeID"
     *   value = "followerCount"
     *
     * Output NOW: (followeeID, followerCount) as IntWritable, IntWritable
     */
    public static class AboveAverageFilterMapper extends Mapper<Text, Text, IntWritable, IntWritable> {
        private final IntWritable outKey = new IntWritable();
        private final IntWritable outVal = new IntWritable();
        private double avgFollowers;

        @Override
        protected void setup(Context context) {
            avgFollowers = context.getConfiguration().getDouble("taskf.avgFollowers", 0.0);
        }

        @Override
        protected void map(Text key, Text value, Context context) throws java.io.IOException, InterruptedException {
            if (key == null || value == null) return;

            String idStr = key.toString().trim();
            String cntStr = value.toString().trim();
            if (idStr.isEmpty() || cntStr.isEmpty()) return;

            int id, cnt;
            try {
                id = Integer.parseInt(idStr);
                cnt = Integer.parseInt(cntStr);
            } catch (NumberFormatException e) {
                return;
            }

            if (cnt > avgFollowers) {
                outKey.set(id);
                outVal.set(cnt);
                context.write(outKey, outVal);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: TaskFOptimized <CircleNetPage.csv> <CircleNetFollows.csv> <tmpOutJob2> <finalOutJob3>");
            return 2;
        }

        Path pagesInput = new Path(args[0]);
        Path followsInput = new Path(args[1]);
        Path tmpOutJob2 = new Path(args[2]);
        Path finalOutJob3 = new Path(args[3]);

        Configuration conf = getConf();

        // -------------------
        // Job 1: Count pages
        // -------------------
        Job job1 = Job.getInstance(conf, "TaskF - Job1 Count Total Pages");
        job1.setJarByClass(TaskFOptimized.class);

        job1.setMapperClass(CountPagesMapper.class);
        job1.setNumReduceTasks(0);

        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job1, pagesInput);

        // Needs an output directory even though we emit nothing.
        Path job1Out = new Path(tmpOutJob2.toString() + "_job1_pagecount");
        FileOutputFormat.setOutputPath(job1, job1Out);

        if (!job1.waitForCompletion(true)) return 1;

        long totalPages = job1.getCounters().findCounter(GlobalCounters.TOTAL_PAGES).getValue();
        if (totalPages <= 0) {
            System.err.println("ERROR: totalPages <= 0; cannot compute average.");
            return 1;
        }

        // --------------------------------------------
        // Job 2: Count followers per followee (+combiner)
        // --------------------------------------------
        Job job2 = Job.getInstance(conf, "TaskF - Job2 Followers Per Owner (with Combiner)");
        job2.setJarByClass(TaskFOptimized.class);

        job2.setMapperClass(FollowCountMapper.class);
        job2.setCombinerClass(SumIntCombiner.class);   // optimization: combiner
        job2.setReducerClass(SumIntReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, followsInput);
        FileOutputFormat.setOutputPath(job2, tmpOutJob2);

        if (!job2.waitForCompletion(true)) return 1;

        long totalFollows = job2.getCounters().findCounter(GlobalCounters.TOTAL_FOLLOWS).getValue();

        // Compute average in the driver
        double avgFollowers = ((double) totalFollows) / ((double) totalPages);

        // -------------------
        // Job 3: Filter above average (map-only) and output (ID, followerCount)
        // -------------------
        Configuration conf3 = new Configuration(conf);
        conf3.setDouble("taskf.avgFollowers", avgFollowers);

        Job job3 = Job.getInstance(conf3, "TaskF - Job3 Filter Above Average (Map-Only)");
        job3.setJarByClass(TaskFOptimized.class);

        job3.setInputFormatClass(KeyValueTextInputFormat.class);

        job3.setMapperClass(AboveAverageFilterMapper.class);
        job3.setNumReduceTasks(0);

        // Output types (NOW includes followerCount)
        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job3, tmpOutJob2);
        FileOutputFormat.setOutputPath(job3, finalOutJob3);

        return job3.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TaskFOptimized(), args);
        System.exit(exitCode);
    }
}
