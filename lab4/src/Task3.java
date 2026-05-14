import java.io.*;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {

    public static class HotMovieMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Set<String> top100Set = new HashSet<>();
        private Text movieIdOut = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles != null) {
                for (Path path : cacheFiles) {
                    if (path.getName().equals("top100.csv")) {
                        try (BufferedReader br = new BufferedReader(new FileReader(path.toString()))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                String[] fields = line.split(",");
                                if (fields.length > 0) {
                                    top100Set.add(fields[0].trim());
                                }
                            }
                        }
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                try {
                    float rating = Float.parseFloat(fields[2]);
                    String movieId = fields[1];
                    if (rating >= 4.0 && top100Set.contains(movieId)) {
                        movieIdOut.set(movieId);
                        context.write(movieIdOut, one);
                    }
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, Text> {

        private Text outputKey = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // 输出格式 F:N
            outputKey.set(key.toString() + ":" + sum);
            context.write(outputKey, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Task3 <ratings.csv> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Task3 Movie Hotness");
        job.setJarByClass(Task3.class);

        job.setMapperClass(HotMovieMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // Reducer 输出 key 为 Text，value 也为 Text（实际为空）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}