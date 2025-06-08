import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgTxSizeForStatus
{
    public static class AvgTxSizeForStatusMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String logEntry = value.toString();
            String logPattern = "^(\\S+) (\\S+ \\S+) \\[(.*?)\\] \"(.*?)\" \\[(\\d+)\\] (\\d+) \"(.*?)\" \"(.*?)\"$";
            Pattern pattern = Pattern.compile(logPattern);
            Matcher matcher = pattern.matcher(logEntry);

            if (matcher.find()) {
                String status = matcher.group(5);
                String body_bytes_sent = matcher.group(6);
                context.write(new Text(status), new IntWritable(Integer.parseInt(body_bytes_sent)));
            }
        }
    }

    public static class AvgTxSizeForStatusReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            int sum = 0;
            for (IntWritable value : values) {
                cnt++;
                sum += value.get();
            }
            float avg = (float) sum / cnt;
            context.write(key, new FloatWritable(avg));
        }
    }
    public static void main( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AvgTxSizeForStatus");
        job.setJarByClass(AvgTxSizeForStatus.class);
        job.setMapperClass(AvgTxSizeForStatusMapper.class);
        job.setReducerClass(AvgTxSizeForStatusReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}