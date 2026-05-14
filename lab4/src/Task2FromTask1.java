import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    /**
     * Mapper: 读入任务1的输出 "A-B \t N"，生成双向用户关系
     * Key:   IntWritable (用户A)
     * Value: Text (用户B)
     */
    public static class UserPairMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable uid = new IntWritable();
        private Text fid = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // 按制表符分割，第一部分为 "A-B"
            String[] parts = line.split("\\t");
            if (parts.length < 1) return;

            String pair = parts[0];
            String[] users = pair.split("-");
            if (users.length == 2) {
                int userA = Integer.parseInt(users[0]);
                int userB = Integer.parseInt(users[1]);

                // 双向输出
                uid.set(userA);
                fid.set(users[1]);
                context.write(uid, fid);

                uid.set(userB);
                fid.set(users[0]);
                context.write(uid, fid);
            }
        }
    }

    /**
     * Reducer: 聚合同一用户的所有好友，去重，数值排序，输出 "A:B,C,D"
     */
    public static class FriendGatherReducer
            extends Reducer<IntWritable, Text, Text, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<Integer> friendSet = new TreeSet<>();  // 自动升序
            for (Text val : values) {
                friendSet.add(Integer.parseInt(val.toString()));
            }
            // 构造输出字符串
            List<String> sortedFriends = new ArrayList<>();
            for (Integer id : friendSet) {
                sortedFriends.add(id.toString());
            }
            String result = key.get() + ":" + String.join(",", sortedFriends);
            context.write(new Text(result), new Text("")); // 整行作为key，value为空
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Task2FromTask1 <task1_output> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task2 from Task1 output (sorted)");
        job.setJarByClass(Task2FromTask1.class);

        job.setMapperClass(UserPairMapper.class);
        job.setReducerClass(FriendGatherReducer.class);

        // Map 输出类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 使用一个 Reducer，保证全局按用户ID排序
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}