import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Log2Vector
{
    public static class Log2VectorMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String logEntry = value.toString();
            String logPattern = "^(\\S+) (\\S+ \\S+) \\[(.*?)\\] \"(.*?)\" \\[(\\d+)\\] (\\d+) \"(.*?)\" \"(.*?)\"$";
            Pattern pattern = Pattern.compile(logPattern);
            Matcher matcher = pattern.matcher(logEntry);

            if (matcher.find()) {
                String remote_addr = matcher.group(1);
                String time_local = matcher.group(3);
                String http_user_agent = matcher.group(8);
                if (http_user_agent.equals("-")) return;

                String[] buf = time_local.split(":|/");
                int h = Integer.parseInt(buf[3]);
                int at_night = (h >= 6 && h < 18) ? 0 : 1;

                context.write(new Text(remote_addr), new Text(at_night + " \"" + http_user_agent));
            }
        }
    }

    public static class Log2VectorReducer extends Reducer<Text, Text, Text, Text> {
        String[] mobileKeywords = {
            "mobile", "android", "iphone", "ipad"
        };
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            int at_night_cnt = 0;
            int mobile_cnt = 0;
            for (Text value : values) {
                cnt++;
                String[] buf = value.toString().split(" \"");
                if (buf[0].equals("1")) at_night_cnt++;
                String agent = buf[1].toLowerCase();
                for (String keyword : mobileKeywords) {
                    if (agent.contains(keyword)) {
                        mobile_cnt++;
                        break;
                    }
                }
            }
            double visit = (double)cnt;
            double at_night_portion = (double)at_night_cnt / (double)cnt;
            double mobile_portion = (double)mobile_cnt / (double)cnt;
            context.write(key, new Text(visit + " " + at_night_portion + " " + mobile_portion));
        }
    }
    public static void main( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log2Vector");
        job.setJarByClass(Log2Vector.class);
        job.setMapperClass(Log2VectorMapper.class);
        job.setReducerClass(Log2VectorReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}