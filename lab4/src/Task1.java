// Task1.java
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class Task1 {

    // Job 1: 生成每对用户一部共同喜欢电影的一条记录
    public static class UserPairMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieId = new Text();
        private Text userId = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                try {
                    float rating = Float.parseFloat(fields[2]);
                    if (rating >= 4.0) {
                        movieId.set(fields[1]);
                        userId.set(fields[0]);
                        context.write(movieId, userId);
                    }
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    public static class UserPairReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> users = new ArrayList<>();
            for (Text v : values) {
                users.add(v.toString());
            }
            Collections.sort(users);
            for (int i = 0; i < users.size(); i++) {
                for (int j = i + 1; j < users.size(); j++) {
                    String pair = users.get(i) + "-" + users.get(j);
                    context.write(new Text(pair), one);
                }
            }
        }
    }

    // Job 2: 对相同的用户对数求和
    public static class SumMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text pair = new Text();
        private IntWritable count = new IntWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                pair.set(parts[0]);
                count.set(Integer.parseInt(parts[1]));
                context.write(pair, count);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Job1
        Job job1 = Job.getInstance(conf, "Task1 Step1 Generate Pairs");
        job1.setJarByClass(Task1.class);
        job1.setMapperClass(UserPairMapper.class);
        job1.setReducerClass(UserPairReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_tmp"));

        // Job2
        Job job2 = Job.getInstance(conf, "Task1 Step2 Sum");
        job2.setJarByClass(Task1.class);
        job2.setMapperClass(SumMapper.class);
        job2.setReducerClass(SumReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1] + "_tmp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        // 串联执行
        ControlledJob cJob1 = new ControlledJob(conf);
        cJob1.setJob(job1);
        ControlledJob cJob2 = new ControlledJob(conf);
        cJob2.setJob(job2);
        cJob2.addDependingJob(cJob1);

        JobControl jc = new JobControl("Task1 chain");
        jc.addJob(cJob1);
        jc.addJob(cJob2);
        Thread t = new Thread(jc);
        t.start();
        while (!jc.allFinished()) {
            Thread.sleep(1000);
        }
        System.exit(jc.getFailedJobList().isEmpty() ? 0 : 1);
    }
}