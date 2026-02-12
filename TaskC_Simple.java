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

public class TaskC_Simple {

    // simple CSV split (good enough since generator avoids commas in fields)
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

    public static class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final Text out = new Text();
        private String targetHobby;

        @Override
        protected void setup(Context context) {
            targetHobby = context.getConfiguration().get("taskc.hobby", "").trim();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // CircleNetPage columns: ID, NickName, JobTitle, RegionCode, FavoriteHobby :contentReference[oaicite:2]{index=2}
            String[] cols = splitCSV(line);
            if (cols.length < 5) return;

            String nick = cols[1].replace("\"", "").trim();
            String job  = cols[2].replace("\"", "").trim();
            String hobby = cols[4].replace("\"", "").trim();

            if (!targetHobby.isEmpty() && hobby.equalsIgnoreCase(targetHobby)) {
                out.set(nick + "\t" + job);
                context.write(out, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TaskC_Simple <CircleNetPage> <Output> <Hobby>");
            System.exit(2);
        }

        String input = args[0];
        String output = args[1];
        String hobby = args[2];

        Configuration conf = new Configuration();
        conf.set("taskc.hobby", hobby);

        Job job = Job.getInstance(conf, "TaskC_Simple_MapOnly_FilterHobby");
        job.setJarByClass(TaskC_Simple.class);

        job.setMapperClass(FilterMapper.class);
        job.setNumReduceTasks(0); // map-only

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
