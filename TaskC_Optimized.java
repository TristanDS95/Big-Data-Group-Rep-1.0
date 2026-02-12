import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TaskC_Optimized {

    // Minimal CSV split (handles quotes)
    private static String[] splitCSV(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') inQuotes = !inQuotes;
            else if (c == ',' && !inQuotes) {
                out.add(cur.toString().trim());
                cur.setLength(0);
            } else cur.append(c);
        }
        out.add(cur.toString().trim());
        return out.toArray(new String[0]);
    }

    enum Ctr { MATCHES, BAD_LINES, HEADER_LINES }

    public static class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final Text out = new Text();
        private String hobby;

        // cheap suffix patterns
        private String suffixPlain;
        private String suffixQuoted;

        @Override
        protected void setup(Context context) {
            hobby = context.getConfiguration().get("taskc.hobby", "").trim();
            suffixPlain = "," + hobby;
            suffixQuoted = ",\"" + hobby + "\"";
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line == null) return;
            line = line.trim();
            if (line.isEmpty()) return;

            // Skip header quickly
            if (line.toLowerCase().startsWith("id,")) {
                context.getCounter(Ctr.HEADER_LINES).increment(1);
                return;
            }

            // FAST FILTER: hobby is last column (FavoriteHobby)
            // If it doesn’t end with hobby, skip without splitting.
            if (!hobby.isEmpty()) {
                if (!(line.endsWith(suffixPlain) || line.endsWith(suffixQuoted))) {
                    return;
                }
            }

            // Now parse only candidates
            String[] cols = splitCSV(line);
            // Expected: ID, NickName, JobTitle, RegionCode, FavoriteHobby
            if (cols.length < 5) {
                context.getCounter(Ctr.BAD_LINES).increment(1);
                return;
            }

            String nick = cols[1].replace("\"", "").trim();
            String job  = cols[2].replace("\"", "").trim();
            String fav  = cols[4].replace("\"", "").trim();

            if (!hobby.isEmpty() && fav.equalsIgnoreCase(hobby)) {
                out.set(nick + "\t" + job);
                context.write(out, NullWritable.get());
                context.getCounter(Ctr.MATCHES).increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TaskC_Optimized <CircleNetPage> <Output> <Hobby>");
            System.exit(2);
        }

        String input = args[0];
        String output = args[1];
        String hobby = args[2];

        Configuration conf = new Configuration();
        conf.set("taskc.hobby", hobby);

        Job job = Job.getInstance(conf, "TaskC_Optimized_MapOnly_FilterHobby");
        job.setJarByClass(TaskC_Optimized.class);

        job.setMapperClass(FilterMapper.class);
        job.setNumReduceTasks(0); // map-only

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
