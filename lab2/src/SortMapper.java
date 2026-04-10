import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");
        if (parts.length < 2) return;
        long total = Long.parseLong(parts[1]);
        ctx.write(new LongWritable(total), new Text(line));
    }
}