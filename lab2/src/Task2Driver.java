import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task2Driver {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Task2Driver <input> <output> <stopword_file>");
            System.exit(-1);
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        String stopPath = args[2];
        Path temp = new Path(output.getParent(), "temp_" + System.currentTimeMillis());

        Configuration conf = new Configuration();
        conf.set("stopword.path", stopPath);

        Job job1 = Job.getInstance(conf, "Task2_WithStopWords");
        job1.setJarByClass(Task2Driver.class);
        job1.setMapperClass(Task2MapperWithStopWords.class);
        job1.setCombinerClass(Task1Combiner.class);
        job1.setReducerClass(Task1Reducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, temp);
        if (!job1.waitForCompletion(true)) System.exit(1);

        Job job2 = Job.getInstance(conf, "Task2_Sort");
        job2.setJarByClass(Task2Driver.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        FileInputFormat.addInputPath(job2, temp);
        FileOutputFormat.setOutputPath(job2, output);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}