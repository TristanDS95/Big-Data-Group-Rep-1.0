import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TaskD_Optimized {

    // CircleNetPage: ID, NickName, JobTitle, RegionCode, FavoriteHobby :contentReference[oaicite:2]{index=2}
    public static class PageMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final IntWritable outKey = new IntWritable();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] cols = line.split(",", -1);
            if (cols.length < 2) return;

            String idStr = cols[0].trim();
            String nick  = cols[1].trim();

            // skip non-numeric id lines
            int id;
            try { id = Integer.parseInt(idStr); }
            catch (NumberFormatException e) { return; }

            outKey.set(id);
            outVal.set("P|" + nick);
            context.write(outKey, outVal);
        }
    }

    // Follows: ColRel, ID1, ID2, DateofRelation, Desc :contentReference[oaicite:3]{index=3}
    // We need followers per owner => count by ID2 (the followed person).
    public static class FollowsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final IntWritable outKey = new IntWritable();
        private static final Text ONE = new Text("F|1");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] cols = line.split(",", -1);
            if (cols.length < 3) return;

            String id2Str = cols[2].trim(); // ID2 = person being followed
            int id2;
            try { id2 = Integer.parseInt(id2Str); }
            catch (NumberFormatException e) { return; }

            outKey.set(id2);
            context.write(outKey, ONE);
        }
    }

    // Combiner: sum F counts locally; pass through P record.
    public static class JoinCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sumF = 0;
            String pageRec = null;

            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("F|")) {
                    sumF += Integer.parseInt(s.substring(2));
                } else if (s.startsWith("P|")) {
                    pageRec = s; // keep nickname
                }
            }

            if (pageRec != null) {
                outVal.set(pageRec);
                context.write(key, outVal);
            }
            if (sumF > 0) {
                outVal.set("F|" + sumF);
                context.write(key, outVal);
            }
        }
    }

    // Reducer: left join — output every page owner with follower count (0 if none)
    public static class JoinReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        private final Text outNick = new Text();
        private final IntWritable outCount = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String nickname = null;
            int followers = 0;

            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("P|")) {
                    nickname = s.substring(2);
                } else if (s.startsWith("F|")) {
                    followers += Integer.parseInt(s.substring(2));
                }
            }

            // Only output people who have a CircleNetPage (should be all IDs 1..200k)
            if (nickname != null) {
                outNick.set(nickname);
                outCount.set(followers);
                context.write(outNick, outCount);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TaskD_Optimized <CircleNetPage> <Follows> <Output>");
            System.exit(2);
        }

        String pagePath = args[0];
        String followsPath = args[1];
        String outPath = args[2];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskD_Optimized_PopularityFactor");
        job.setJarByClass(TaskD_Optimized.class);

        MultipleInputs.addInputPath(job, new Path(pagePath), TextInputFormat.class, PageMapper.class);
        MultipleInputs.addInputPath(job, new Path(followsPath), TextInputFormat.class, FollowsMapper.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(JoinCombiner.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);       // NickName
        job.setOutputValueClass(IntWritable.class); // follower count

        FileOutputFormat.setOutputPath(job, new Path(outPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
