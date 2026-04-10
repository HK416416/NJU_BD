import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SortReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        for (Text val : values) {
            ctx.write(val, NullWritable.get());
        }
    }
}