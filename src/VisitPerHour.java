import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class VisitPerHour
{
    public static class DescendingIntComparator extends WritableComparator {
        protected DescendingIntComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable key1 = (IntWritable) w1;
            IntWritable key2 = (IntWritable) w2;
            return key2.compareTo(key1); // Reverse the order
        }
    }

    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] buf = value.toString().split("\\s");
            context.write(new IntWritable(Integer.parseInt(buf[1])), new Text(buf[0]));
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val: values) {
                context.write(val, key);
            }
        }
    }

    public static class VisitPerHourMapper extends Mapper<Object, Text, Text, IntWritable> {
        public String month2num(String month) {
            switch (month) {
                case "Jan": return "01";
                case "Feb": return "02";
                case "Mar": return "03";
                case "Apr": return "04";
                case "May": return "05";
                case "Jun": return "06";
                case "Jul": return "07";
                case "Aug": return "08";
                case "Sep": return "09";
                case "Oct": return "10";
                case "Nov": return "11";
                case "Dec": return "12";
                default: return null;
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String logEntry = value.toString();
            String logPattern = "^\\S+ \\S+ \\S+ \\[(.*?)\\] \".*?\" \\[\\d+\\] \\d+ \".*?\" \".*?\"$";
            Pattern pattern = Pattern.compile(logPattern);
            Matcher matcher = pattern.matcher(logEntry);

            if (matcher.find()) {
                String time_local = matcher.group(1);
                String[] timeParts = time_local.split(":|/");
                context.write(new Text(timeParts[2] + month2num(timeParts[1]) + timeParts[0] + timeParts[3]), new IntWritable(1));
            }
        }
    }

    public static class VisitPerHourReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    public static void main( String[] args ) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "VisitPerHour");
        job1.setJarByClass(VisitPerHour.class);
        job1.setMapperClass(VisitPerHourMapper.class);
        job1.setReducerClass(VisitPerHourReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_tmp"));
        job1.waitForCompletion(true);
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "VisitPerHourSort");
        job2.setJarByClass(VisitPerHour.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setSortComparatorClass(DescendingIntComparator.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(args[1] + "_tmp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        int status = job2.waitForCompletion(true) ? 0 : 1;
        FileSystem fs = FileSystem.get(conf2);
        if (fs.exists(new Path(args[1] + "_tmp"))) {
            fs.delete(new Path(args[1] + "_tmp") ,true);
        }
        System.exit(status);
    }
}