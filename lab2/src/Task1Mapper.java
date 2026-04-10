import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class Task1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private String filename;
    @Override
    protected void setup(Context ctx) {
        filename = ((FileSplit) ctx.getInputSplit()).getPath().getName();
    }
    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",", 2);
        if (parts.length < 2) return;
        String[] words = parts[1].split("\\s+");
        for (String w : words) {
            if (w.trim().isEmpty()) continue;
            ctx.write(new Text(w), new Text(filename + ":1"));
        }
    }
}