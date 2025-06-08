import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans
{
    public static class Centroid {
        double x;
        double y;
        double z;
        int id;

        Centroid(double x, double y, double z, int id) {
            this.x = x;
            this.y = y;
            this.z = z;
            this.id = id;
        }
    }

    public static class MeanMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] buf = value.toString().split("\\s");
            if (buf.length < 4) return;
            double x = Double.parseDouble(buf[1]);
            double y = Double.parseDouble(buf[2]);
            double z = Double.parseDouble(buf[3]);
            context.write(new IntWritable(0), new DoubleWritable(x));
            context.write(new IntWritable(1), new DoubleWritable(y));
            context.write(new IntWritable(2), new DoubleWritable(z));
        }
    }

    public static class MeanReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int cnt = 0;
            for (DoubleWritable val: values) {
                sum += val.get();
                cnt++;
            }
            context.write(key, new DoubleWritable(sum / (double)cnt));
        }
    }

    public static class StdDevMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        double avg0, avg1, avg2;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            URI[] cacheFiles = context.getCacheFiles();
            for (URI file : cacheFiles) {
                FileSystem fs = FileSystem.get(conf);
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(file))))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] buf = line.split("\\s");
                        if (buf.length < 2) continue;
                        int dim = Integer.parseInt(buf[0]);
                        double avg = Double.parseDouble(buf[1]);
                        switch (dim) {
                        case 0:
                            avg0 = avg;
                            break;
                        case 1:
                            avg1 = avg;
                            break;
                        case 2:
                            avg2 = avg;
                            break;
                        default:
                            break;
                        }
                    }
                    br.close();
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] buf = value.toString().split("\\s");
            if (buf.length < 4) return;
            double x = Math.pow(Double.parseDouble(buf[1]) - avg0, 2);
            double y = Math.pow(Double.parseDouble(buf[2]) - avg1, 2);
            double z = Math.pow(Double.parseDouble(buf[3]) - avg2, 2);
            context.write(new IntWritable(0), new DoubleWritable(x));
            context.write(new IntWritable(1), new DoubleWritable(y));
            context.write(new IntWritable(2), new DoubleWritable(z));
        }
    }

    public static class StdDevReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int cnt = 0;
            for (DoubleWritable val: values) {
                sum += val.get();
                cnt++;
            }
            context.write(new IntWritable(key.get() + 3), new DoubleWritable(Math.sqrt(sum /(double)cnt)));
        }
    }

    public static class NormalizeMapper extends Mapper<Object, Text, Text, Text> {
        double avg0, avg1, avg2;
        double stddev0, stddev1, stddev2;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            URI[] cacheFiles = context.getCacheFiles();
            for (URI file : cacheFiles) {
                FileSystem fs = FileSystem.get(conf);
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(file))))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] buf = line.split("\\s");
                        if (buf.length < 2) continue;
                        int dim = Integer.parseInt(buf[0]);
                        double val = Double.parseDouble(buf[1]);
                        switch (dim) {
                        case 0:
                            avg0 = val;
                            break;
                        case 1:
                            avg1 = val;
                            break;
                        case 2:
                            avg2 = val;
                            break;
                        case 3:
                            stddev0 = val;
                            break;
                        case 4:
                            stddev1 = val;
                            break;
                        case 5:
                            stddev2 = val;
                            break;
                        default:
                            break;
                        }
                    }
                    br.close();
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] buf = value.toString().split("\\s");
            if (buf.length < 4) return;
            double x = (Double.parseDouble(buf[1]) - avg0) / stddev0;
            double y = (Double.parseDouble(buf[2]) - avg1) / stddev1;
            double z = (Double.parseDouble(buf[3]) - avg2) / stddev2;
            context.write(new Text(buf[0]), new Text(x + " " + y + " " + z));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val: values) {
                context.write(key, val);
            }
        }
    }

    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
        private List<Centroid> centroids = new ArrayList<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            // Load centroids from HDFS
            Configuration conf = context.getConfiguration();
            URI[] cacheFiles = context.getCacheFiles();
            for (URI file : cacheFiles) {
                FileSystem fs = FileSystem.get(conf);
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(file))))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] buf = line.split("\\s");
                        if (buf.length < 4) continue;
                        centroids.add(new Centroid(
                            Double.parseDouble(buf[1]),
                            Double.parseDouble(buf[2]),
                            Double.parseDouble(buf[3]),
                            Integer.parseInt(buf[0])
                        ));
                    }
                    br.close();
                }
            }
            
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] buf = value.toString().split("\\s");
            if (buf.length < 4) return;
            double x = Double.parseDouble(buf[1]);
            double y = Double.parseDouble(buf[2]);
            double z = Double.parseDouble(buf[3]);
            double minDistance = Double.MAX_VALUE;
            int id = -1;
            for (Centroid centroid: centroids) {
                double distance = Math.sqrt(Math.pow(x - centroid.x, 2) + Math.pow(y - centroid.y, 2) + Math.pow(z - centroid.z, 2));
                if (distance < minDistance) {
                    minDistance = distance;
                    id = centroid.id;
                }
            }
            context.write(new IntWritable(id), new Text(buf[1] + " " + buf[2] + " " + buf[3]));
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumX = 0;
            double sumY = 0;
            double sumZ = 0;
            int count = 0;

            for (Text value : values) {
                String[] buf = value.toString().split("\\s");
                if (buf.length < 3) continue;
                sumX += Double.parseDouble(buf[0]);
                sumY += Double.parseDouble(buf[1]);
                sumZ += Double.parseDouble(buf[2]);
                count++;
            }

            if (count > 0) {
                context.write(key, new Text(sumX / count + " " + sumY / count + " " + sumZ / count));
            }
        }
    }

    public static class DistributionMapper extends Mapper<Object, Text, IntWritable, Text> {
        private List<Centroid> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load centroids from HDFS
            Configuration conf = context.getConfiguration();
            URI[] cacheFiles = context.getCacheFiles();
            for (URI file : cacheFiles) {
                FileSystem fs = FileSystem.get(conf);
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(file))))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] buf = line.split("\\s");
                        if (buf.length < 4) continue;
                        centroids.add(new Centroid(
                            Double.parseDouble(buf[1]),
                            Double.parseDouble(buf[2]),
                            Double.parseDouble(buf[3]),
                            Integer.parseInt(buf[0])
                        ));
                    }
                    br.close();
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] buf = value.toString().split("\\s");
            if (buf.length < 4) return;
            double x = Double.parseDouble(buf[1]);
            double y = Double.parseDouble(buf[2]);
            double z = Double.parseDouble(buf[3]);
            double minDistance = Double.MAX_VALUE;
            int id = -1;
            for (Centroid centroid: centroids) {
                double distance = Math.sqrt(Math.pow(x - centroid.x, 2) + Math.pow(y - centroid.y, 2) + Math.pow(z - centroid.z, 2));
                if (distance < minDistance) {
                    minDistance = distance;
                    id = centroid.id;
                }
            }
            context.write(new IntWritable(id), value);
        }
    }

    public static class DistributionReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            for (Text val: value) {
                context.write(val, key);
            }
        }
    }

    public static void main( String[] args ) throws Exception {
        int k = Integer.parseInt(args[2]);
        int maxIterations = Integer.parseInt(args[3]);
        double threshold = Double.parseDouble(args[4]);

        FileSystem fs = FileSystem.get(new Configuration());

        //Normalize vectors
        Configuration meanConf = new Configuration();
        Job meanJob = Job.getInstance(meanConf, "GetMean");
        meanJob.setJarByClass(KMeans.class);
        meanJob.setMapperClass(MeanMapper.class);
        meanJob.setReducerClass(MeanReducer.class);
        meanJob.setMapOutputKeyClass(IntWritable.class);
        meanJob.setMapOutputValueClass(DoubleWritable.class);
        meanJob.setOutputKeyClass(IntWritable.class);
        meanJob.setOutputValueClass(DoubleWritable.class);
        meanJob.setNumReduceTasks(4);
        FileInputFormat.addInputPath(meanJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(meanJob, new Path(args[1], "mean"));
        meanJob.waitForCompletion(true);
        Configuration stddevConf = new Configuration();
        Job stddevJob = Job.getInstance(stddevConf, "GetStddev");
        stddevJob.setJarByClass(KMeans.class);
        stddevJob.setMapperClass(StdDevMapper.class);
        stddevJob.setReducerClass(StdDevReducer.class);
        stddevJob.setMapOutputKeyClass(IntWritable.class);
        stddevJob.setMapOutputValueClass(DoubleWritable.class);
        stddevJob.setOutputKeyClass(IntWritable.class);
        stddevJob.setOutputValueClass(DoubleWritable.class);
        stddevJob.setNumReduceTasks(4);
        FileInputFormat.addInputPath(stddevJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(stddevJob, new Path(args[1], "stddev"));
        for (FileStatus file: fs.listStatus(new Path(args[1], "mean"))) {
            stddevJob.addCacheFile(file.getPath().toUri());
        }
        stddevJob.waitForCompletion(true);
        Configuration normalizeConf = new Configuration();
        Job normalizeJob = Job.getInstance(normalizeConf, "NormalizeVectors");
        normalizeJob.setJarByClass(KMeans.class);
        normalizeJob.setMapperClass(NormalizeMapper.class);
        normalizeJob.setReducerClass(NormalizeReducer.class);
        normalizeJob.setMapOutputKeyClass(Text.class);
        normalizeJob.setMapOutputValueClass(Text.class);
        normalizeJob.setOutputKeyClass(Text.class);
        normalizeJob.setOutputValueClass(Text.class);
        normalizeJob.setNumReduceTasks(4);
        FileInputFormat.addInputPath(normalizeJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(normalizeJob, new Path(args[1], "normalized_vectors"));
        for (FileStatus file: fs.listStatus(new Path(args[1], "mean"))) {
            normalizeJob.addCacheFile(file.getPath().toUri());
        }
        for (FileStatus file: fs.listStatus(new Path(args[1], "stddev"))) {
            normalizeJob.addCacheFile(file.getPath().toUri());
        }
        normalizeJob.waitForCompletion(true);

        Vector<Centroid> centroids = new Vector<>();
        Path directory = new Path(args[1], "normalized_vectors");
        FileStatus[] files = fs.listStatus(directory);
        String line;
        int centroid_cnt = 0;
        for (FileStatus file: files) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())))) {
                while ((line = br.readLine()) != null && centroid_cnt < k) {
                    String[] buf = line.split("\\s");
                    if (buf.length < 4) continue;
                    centroids.add(new Centroid(
                        Double.parseDouble(buf[1]),
                        Double.parseDouble(buf[2]),
                        Double.parseDouble(buf[3]),
                        centroid_cnt++
                    ));
                }
                br.close();
            }
        }

        Path centroidPath = new Path(args[1], "centroids_init");
        
        try (BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(args[1], "centroids_init/centroids"))))) {
            for (Centroid centroid : centroids) {
                br.write(centroid.id + " " + centroid.x + " " + centroid.y + " " + centroid.z + "\n");
            }
            br.close();
        }

        int total_iterations = 1;
        for (; total_iterations <= maxIterations; total_iterations++) {
            // Run KMeans iteration
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "KMeans Iteration " + total_iterations);
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(4);
            FileInputFormat.addInputPath(job, new Path(args[1], "normalized_vectors"));
            FileOutputFormat.setOutputPath(job, new Path(args[1], "iter_" + total_iterations));
            for (FileStatus file: fs.listStatus(centroidPath)) {
                job.addCacheFile(file.getPath().toUri());
            }
            job.waitForCompletion(true);

            // Check for convergence
            Path newCentroidPath = new Path(args[1], "iter_" + total_iterations);

            Vector<Centroid> newCentroids = new Vector<>(centroids.size());
            for (int j = 0; j < centroids.size(); j++) {
                newCentroids.add(null);
            }
            double maxChange = 0;
            for (FileStatus file: fs.listStatus(newCentroidPath)) {
                try (BufferedReader newReader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())))) {
                    String newLine;
                    while ((newLine = newReader.readLine()) != null) {
                        String[] buf = newLine.split("\\s");
                        if (buf.length < 4) continue;
                        int centroidId = Integer.parseInt(buf[0]);
                        newCentroids.set(centroidId, new Centroid(
                            Double.parseDouble(buf[1]),
                            Double.parseDouble(buf[2]),
                            Double.parseDouble(buf[3]),
                            centroidId
                        ));
                        Centroid oldCentroid = centroids.get(centroidId);
                        Centroid newCentroid = newCentroids.get(centroidId);
                        double change = Math.sqrt(
                            Math.pow(newCentroid.x - oldCentroid.x, 2) +
                            Math.pow(newCentroid.y - oldCentroid.y, 2) +
                            Math.pow(newCentroid.z - oldCentroid.z, 2)
                        );
                        if (change > maxChange) {
                            maxChange = change;
                        }
                    }
                }
            }
            if (maxChange < threshold) {
                break;
            }

            // Update centroids
            centroidPath = newCentroidPath;
            centroids = newCentroids;
        }

        // Distribute centroids
        Configuration distConf = new Configuration();
        Job distJob = Job.getInstance(distConf, "Distribution");
        distJob.setJarByClass(KMeans.class);
        distJob.setMapperClass(DistributionMapper.class);
        distJob.setReducerClass(DistributionReducer.class);
        distJob.setMapOutputKeyClass(IntWritable.class);
        distJob.setMapOutputValueClass(Text.class);
        distJob.setOutputKeyClass(IntWritable.class);
        distJob.setOutputValueClass(Text.class);
        distJob.setNumReduceTasks(4);
        FileInputFormat.addInputPath(distJob, new Path(args[1], "normalized_vectors"));
        FileOutputFormat.setOutputPath(distJob, new Path(args[1], "distribution_result"));
        for (FileStatus file: fs.listStatus(centroidPath)) {
            distJob.addCacheFile(file.getPath().toUri());
        }
        distJob.waitForCompletion(true);
    }
}