import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PlayDurationSum {

    // Mapper: 过滤播放行为，输出 <video_category, play_duration>
    public static class PlayDurationMapper
            extends Mapper<LongWritable, Text, Text, LongWritable> {

        private Text category = new Text();
        private LongWritable duration = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(" ");
            if (fields.length >= 10 && "1".equals(fields[4])) {
                try {
                    String cat = fields[5];          // video_category
                    long dur = Long.parseLong(fields[1]); // play_duration
                    category.set(cat);
                    duration.set(dur);
                    context.write(category, duration);//(种类，时长)
                } catch (NumberFormatException e) {

                }
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable result = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PlayDurationSum <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Play Duration Sum by Category");
        job.setJarByClass(PlayDurationSum.class);

        job.setMapperClass(PlayDurationMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}