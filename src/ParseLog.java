import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ParseLog
{
    public static class ParseLogMapper extends Mapper<Object, Text, Text, NullWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String logEntry = value.toString();
            String logPattern = "^(\\S+) (\\S+ \\S+) \\[(.*?)\\] \"(.*?)\" \\[(\\d+)\\] (\\d+) \"(.*?)\" \"(.*?)\"$";
            Pattern pattern = Pattern.compile(logPattern);
            Matcher matcher = pattern.matcher(logEntry);

            if (matcher.find()) {
                String remote_addr = matcher.group(1);
                String remote_user = matcher.group(2);
                String time_local = matcher.group(3);
                String request = matcher.group(4);
                String status = matcher.group(5);
                String body_bytes_sent = matcher.group(6);
                String http_referer = matcher.group(7);
                String http_user_agent = matcher.group(8);

                StringBuilder parsed = new StringBuilder();
                parsed.append("remote_addr: ").append(remote_addr).append("\n");
                parsed.append("remote_user: ").append(remote_user).append("\n");
                parsed.append("time_local: ").append(time_local).append("\n");
                parsed.append("request: ").append(request).append("\n");
                parsed.append("status: ").append(status).append("\n");
                parsed.append("body_bytes_sent: ").append(body_bytes_sent).append("\n");
                parsed.append("http_referer: ").append(http_referer).append("\n");
                parsed.append("http_user_agent: ").append(http_user_agent).append("\n");
                context.write(new Text(parsed.toString()), NullWritable.get());
            }
        }
    }

    public static class ParseLogReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
    public static void main( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ParseLog");
        job.setJarByClass(ParseLog.class);
        job.setMapperClass(ParseLogMapper.class);
        job.setReducerClass(ParseLogReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}