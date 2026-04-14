import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Task2MapperWithStopWords extends Mapper<LongWritable, Text, Text, Text> {
    private String filename;
    private Set<String> stopWords;
    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
        filename = ((FileSplit) ctx.getInputSplit()).getPath().getName();
        String stopPath = ctx.getConfiguration().get("stopword.path");
        try {
            stopWords = StopWordLoader.load(ctx.getConfiguration(), stopPath);
        } catch (Exception e) {
            stopWords = new HashSet<>();   
        }
    }
    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",", 2);
        if (parts.length < 2) return;
        String[] words = parts[1].split("\\s+");
        for (String w : words) {
            w = w.trim();
            if (w.isEmpty() || stopWords.contains(w)) continue;
            ctx.write(new Text(w), new Text(filename + ":1"));
        }
    }
}