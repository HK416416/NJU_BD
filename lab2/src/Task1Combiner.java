import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Task1Combiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        Map<String, Integer> map = new HashMap<>();
        for (Text val : values) {
            String[] kv = val.toString().split(":");
            String doc = kv[0];
            int cnt = Integer.parseInt(kv[1]);
            map.put(doc, map.getOrDefault(doc, 0) + cnt);
        }
        for (Map.Entry<String, Integer> e : map.entrySet()) {
            ctx.write(key, new Text(e.getKey() + ":" + e.getValue()));
        }
    }
}