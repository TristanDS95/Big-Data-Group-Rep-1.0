import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaskF extends Configured implements Tool {

    public enum GlobalCounters {
        TOTAL_PAGES,
        TOTAL_FOLLOWS
    }

    /**
     * JOB 1: Count total pages (owners) from CircleNetPage.
     * Map-only job using a counter.
     */
    public static class CountPagesMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {

            if (value == null) return;
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            context.getCounter(GlobalCounters.TOTAL_PAGES).increment(1);
        }
    }

    /**
     * JOB 2: Count followers per followee (owner).
     * Mapper emits (followeeID, 1)
     * Reducer sums counts per followee.
     * NO COMBINER (baseline version).
     */
    public static class FollowCountMapper
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final IntWritable outKey = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {

            if (value == null) return;
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // CircleNetFollows format:
            // follow_ID,follower_ID,followee_ID,date,description
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

            // Increment total follow counter
            context.getCounter(GlobalCounters.TOTAL_FOLLOWS).increment(1);

            outKey.set(followeeId);
            context.write(outKey, ONE);
        }
    }

    public static class SumReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private final IntWritable outVal = new IntWritable();

        @Override
        protected void reduce(IntWritable key,
                              Iterable<IntWritable> values,
                              Context context)
                throws java.io.IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }

            outVal.set(sum);
            context.write(key, outVal);
        }
    }

    /**
     * JOB 3: Filter owners whose follower count > average.
     * Map-only job.
     */
    public static class AboveAverageFilterMapper
            extends Mapper<Text, Text, IntWritable, NullWritable> {

        private final IntWritable outKey = new IntWritable();
        private double avgFollowers;

        @Override
        protected void setup(Context context) {
            avgFollowers = context.getConfiguration()
                    .getDouble("taskf.avgFollowers", 0.0);
        }

        @Override
        protected void map(Text key, Text value, Context context)
                throws java.io.IOException, InterruptedException {

            if (key == null || value == null) return;

            String idStr = key.toString().trim();
            String cntStr = value.toString().trim();

            if (idStr.isEmpty() || cntStr.isEmpty()) return;

            int id, count;
            try {
                id = Integer.parseInt(idStr);
                count = Integer.parseInt(cntStr);
            } catch (NumberFormatException e) {
                return;
            }

            if (count > avgFollowers) {
                outKey.set(id);
                context.write(outKey, NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.println("Usage: TaskF <CircleNetPage> <CircleNetFollows> <tmpOut> <finalOut>");
            return 2;
        }

        Path pagesInput = new Path(args[0]);
        Path followsInput = new Path(args[1]);
        Path tmpOut = new Path(args[2]);
        Path finalOut = new Path(args[3]);

        Configuration conf = getConf();

        // -------------------
        // Job 1: Count pages
        // -------------------
        Job job1 = Job.getInstance(conf, "TaskF - Count Pages");
        job1.setJarByClass(TaskF.class);

        job1.setMapperClass(CountPagesMapper.class);
        job1.setNumReduceTasks(0);

        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job1, pagesInput);
        Path job1Out = new Path(tmpOut.toString() + "_pagecount");
        FileOutputFormat.setOutputPath(job1, job1Out);

        if (!job1.waitForCompletion(true)) return 1;

        long totalPages = job1.getCounters()
                .findCounter(GlobalCounters.TOTAL_PAGES)
                .getValue();

        // -------------------
        // Job 2: Followers per owner
        // -------------------
        Job job2 = Job.getInstance(conf, "TaskF - Followers Per Owner");
        job2.setJarByClass(TaskF.class);

        job2.setMapperClass(FollowCountMapper.class);
        job2.setReducerClass(SumReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, followsInput);
        FileOutputFormat.setOutputPath(job2, tmpOut);

        if (!job2.waitForCompletion(true)) return 1;

        long totalFollows = job2.getCounters()
                .findCounter(GlobalCounters.TOTAL_FOLLOWS)
                .getValue();

        double avgFollowers = (double) totalFollows / (double) totalPages;

        // -------------------
        // Job 3: Filter above average
        // -------------------
        Configuration conf3 = new Configuration(conf);
        conf3.setDouble("taskf.avgFollowers", avgFollowers);

        Job job3 = Job.getInstance(conf3, "TaskF - Filter Above Average");
        job3.setJarByClass(TaskF.class);

        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setMapperClass(AboveAverageFilterMapper.class);
        job3.setNumReduceTasks(0);

        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(NullWritable.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job3, tmpOut);
        FileOutputFormat.setOutputPath(job3, finalOut);

        return job3.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TaskF(), args);
        System.exit(exitCode);
    }
}
