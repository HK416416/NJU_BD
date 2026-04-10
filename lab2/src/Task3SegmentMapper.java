import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import java.util.List;

public class Task3SegmentMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private String filename;
    @Override
    protected void setup(Context ctx) {
        filename = ((FileSplit) ctx.getInputSplit()).getPath().getName();
    }
    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.trim().isEmpty()) return;
        List<Term> terms = HanLP.segment(line);
        StringBuilder sb = new StringBuilder();
        for (Term t : terms) {
            String word = t.word;
            if (word.length() < 2) continue;
            if (word.matches("\\d+")) continue;
            sb.append(word).append(" ");
        }
        if (sb.length() > 0) {
            String virtualId = filename + "_" + key.get();
            String outLine = virtualId + "," + sb.toString().trim();
            ctx.write(new Text(outLine), NullWritable.get());
        }
    }
}