import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task2FromTask1 {

    // ===== Mapper：读取任务1输出，生成双向 <IntWritable, Text> =====
    public static class UserPairMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable uid = new IntWritable();
        private Text fid = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // 输入格式：A-B \t N
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\\t");
            if (parts.length < 1) return;

            String pair = parts[0];
            String[] users = pair.split("-");
            if (users.length == 2) {
                int userA = Integer.parseInt(users[0]);
                int userB = Integer.parseInt(users[1]);

                uid.set(userA);
                fid.set(users[1]);
                context.write(uid, fid);

                uid.set(userB);
                fid.set(users[0]);
                context.write(uid, fid);
            }
        }
    }

    // ===== Reducer：聚合每个用户的同好列表，输出 A:B,C,D =====
    public static class FriendGatherReducer
            extends Reducer<IntWritable, Text, Text, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> friendSet = new TreeSet<>();   // 自动去重、排序
            for (Text val : values) {
                friendSet.add(val.toString());
            }
            String result = key.get() + ":" + String.join(",", friendSet);
            context.write(new Text(result), new Text(""));
        }
    }

    // ===== 主程序 =====
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Task2FromTask1 <task1 output> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task2 (sorted) from Task1 output");
        job.setJarByClass(Task2FromTask1.class);

        job.setMapperClass(UserPairMapper.class);
        job.setReducerClass(FriendGatherReducer.class);

        // Map 输出类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置一个 Reducer，保证全局按用户ID数值排序
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}