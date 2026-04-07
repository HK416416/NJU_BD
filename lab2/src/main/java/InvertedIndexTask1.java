import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexTask1 {

    // Job1 Mapper：输出 <词语#文档名, 1>
    public static class Step1Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text wordDoc = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获取当前文档名（如video1.csv → video1）
            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();
            fileName = fileName.substring(0, fileName.lastIndexOf("."));

            // 解析行：跳过评论ID，只取分词内容
            String line = value.toString();
            String[] parts = line.split(",", 2);
            if (parts.length < 2) return;
            String content = parts[1];

            // 按空格分词并输出
            StringTokenizer st = new StringTokenizer(content);
            while (st.hasMoreTokens()) {
                String word = st.nextToken().trim();
                if (word.isEmpty()) continue;
                wordDoc.set(word + "#" + fileName);
                context.write(wordDoc, one);
            }
        }
    }

    // Job1 Combiner：Map端本地聚合
    public static class Step1Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    // Job1 Reducer：输出 <词语, 文档名:词频>
    public static class Step1Reducer extends Reducer<Text, IntWritable, Text, Text> {
        private Text word = new Text();
        private Text docFreq = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] parts = key.toString().split("#", 2);
            String wordStr = parts[0];
            String docName = parts[1];

            int sum = 0;
            for (IntWritable val : values) sum += val.get();

            word.set(wordStr);
            docFreq.set(docName + ":" + sum);
            context.write(word, docFreq);
        }
    }

    // Job2 Mapper：输入 <词语\t文档名:词频> → 输出 <词语, 文档名:词频>
    public static class Step2Mapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t", 2);
            if (parts.length < 2) return;
            context.write(new Text(parts[0]), new Text(parts[1]));
        }
    }

    // Job2 Reducer：计算总词频并按总词频降序输出
    public static class Step2Reducer extends Reducer<Text, Text, IntWritable, Text> {
        private IntWritable totalFreq = new IntWritable();
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            int total = 0;

            // 拼接所有文档词频并计算总词频
            for (Text val : values) {
                String docFreq = val.toString();
                sb.append(docFreq).append("\t");
                int freq = Integer.parseInt(docFreq.split(":")[1]);
                total += freq;
            }

            // 去掉末尾多余制表符
            if (sb.length() > 0) sb.deleteCharAt(sb.length() - 1);

            // 构造最终输出行
            String outputLine = key.toString() + "\t" + total + "\t" + sb.toString();
            totalFreq.set(total);
            result.set(outputLine);
            context.write(totalFreq, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: InvertedIndexTask1 <inputPath> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Path tempOutput = new Path(args[1] + "_temp");
        Path finalOutput = new Path(args[1]);

        // 清理已存在的输出路径
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(tempOutput)) fs.delete(tempOutput, true);
        if (fs.exists(finalOutput)) fs.delete(finalOutput, true);

        // Job1：计算词-文档词频
        Job job1 = Job.getInstance(conf, "InvertedIndex-Step1");
        job1.setJarByClass(InvertedIndexTask1.class);
        job1.setMapperClass(Step1Mapper.class);
        job1.setCombinerClass(Step1Combiner.class);
        job1.setReducerClass(Step1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tempOutput);

        if (!job1.waitForCompletion(true)) System.exit(1);

        // Job2：计算总词频并排序
        Job job2 = Job.getInstance(conf, "InvertedIndex-Step2");
        job2.setJarByClass(InvertedIndexTask1.class);
        job2.setMapperClass(Step2Mapper.class);
        job2.setReducerClass(Step2Reducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setSortComparatorClass(IntWritable.DecreasingComparator.class); // 降序排序
        FileInputFormat.addInputPath(job2, tempOutput);
        FileOutputFormat.setOutputPath(job2, finalOutput);

        if (job2.waitForCompletion(true)) {
            fs.delete(tempOutput, true); // 删除临时文件
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}