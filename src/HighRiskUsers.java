import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HighRiskUsers
{
    public static class SortMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] buf = value.toString().split("\"\\s");
            context.write(new Text(buf[0]), new Text(buf[1]));
        }
    }

    public static class SortReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Vector<Integer> statuses = new Vector<>();
            Vector<Float> errorRates = new Vector<>();
            Vector<Float> avgTxSizes = new Vector<>();
            Vector<Integer> indices = new Vector<>();
            int index = 0;
            for (Text value : values) {
                String[] buf = value.toString().split("\\s");
                int status = Integer.parseInt(buf[0]);
                float errorRate = Float.parseFloat(buf[1]);
                float avgTxSize = Float.parseFloat(buf[2]);
                statuses.add(status);
                errorRates.add(errorRate);
                avgTxSizes.add(avgTxSize);
                indices.add(index);
                index++;
            }
            indices.sort((Integer i1, Integer i2) -> {return errorRates.get(i2).compareTo(errorRates.get(i1));});
            StringBuilder sb = new StringBuilder();
            sb.append(key.toString()).append("->[");
            for (int i = 0; i < indices.size(); i++) {
                if (i != 0) sb.append("; ");
                int idx = indices.get(i);
                sb.append(statuses.get(idx)).append(": ").append(errorRates.get(idx)).append(", ").append(avgTxSizes.get(idx));
            }
            sb.append("]");
            context.write(new Text(sb.toString()), NullWritable.get());
        }
    }
   
    public static class HighRiskUsersMapper extends Mapper<Object, Text, Text, Text> {
        private Set<String> top10Agents = new HashSet<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(conf.get("visitagent")))));
            String line;
            int i = 10;
            while (i > 0 && (line = br.readLine()) != null) {
                String[] buf = line.trim().split("\\s+[0-9]+$");
                if (buf[0].equals("-")) continue;
                top10Agents.add(buf[0] + "\"");
                i--;
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String logEntry = value.toString();
            String logPattern = "^\\S+ \\S+ \\S+ \\[.*?\\] \".*?\" \\[(\\d+)\\] (\\d+) \".*?\" \"(.*?\")$";
            Pattern pattern = Pattern.compile(logPattern);
            Matcher matcher = pattern.matcher(logEntry);

            if (matcher.find()) {
                String status = matcher.group(1);
                String body_bytes_sent = matcher.group(2);
                String http_user_agent = matcher.group(3);
                if (!top10Agents.contains((http_user_agent))) return;
                if (!http_user_agent.equals("-\"")) context.write(new Text(http_user_agent), new Text(status + " " + body_bytes_sent));
            }
        }
    }

    public static class HighRiskUsersReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<Integer, Integer> map_cnt = new HashMap<>();
            HashMap<Integer, Integer> map_error = new HashMap<>();
            HashMap<Integer, Integer> map_txsize = new HashMap<>();
            int totalcnt = 0;
            for (Text value : values) {
                String[] buf = value.toString().split("\\s");
                int status = Integer.parseInt(buf[0]);
                int body_bytes_sent = Integer.parseInt(buf[1]);
                totalcnt++;
                if (status != 200) {
                    map_cnt.put(status, map_cnt.getOrDefault(status, 0) + 1);
                    map_error.put(status, map_error.getOrDefault(status, 0) + 1);
                    map_txsize.put(status, map_txsize.getOrDefault(status, 0) + body_bytes_sent);
                }
            }
            for (int status : map_cnt.keySet()) {
                int cnt = map_cnt.get(status);
                int error = map_error.getOrDefault(status, 0);
                int txsize = map_txsize.getOrDefault(status, 0);
                float errorRate = (float) error / totalcnt;
                float avgTxSize = (float) txsize / cnt;
                context.write(new Text(key), new Text(status + " " + errorRate + " " + avgTxSize));
            }
        }
    }
    public static void main( String[] args ) throws Exception {
        Configuration conf1 = new Configuration();
        conf1.set("visitagent", args[1]);
        Job job1 = Job.getInstance(conf1, "HighRiskUsers");
        job1.setJarByClass(HighRiskUsers.class);
        job1.setMapperClass(HighRiskUsersMapper.class);
        job1.setReducerClass(HighRiskUsersReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2] + "_tmp"));
        job1.waitForCompletion(true);
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "HighRiskUsersSort");
        job2.setJarByClass(HighRiskUsers.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(args[2] + "_tmp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        int status = job2.waitForCompletion(true) ? 0 : 1;
        FileSystem fs = FileSystem.get(conf2);
        if (fs.exists(new Path(args[2] + "_tmp"))) {
            fs.delete(new Path(args[2] + "_tmp"), true);
        }
        System.exit(status);
    }
}