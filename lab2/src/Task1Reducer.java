import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Task1Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        Map<String, Integer> map = new HashMap<>();
        int total = 0;
        for (Text val : values) {
            String[] kv = val.toString().split(":");
            String doc = kv[0];
            int cnt = Integer.parseInt(kv[1]);
            map.put(doc, map.getOrDefault(doc, 0) + cnt);
            total += cnt;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(total);
        for (Map.Entry<String, Integer> e : map.entrySet()) {
            sb.append("\t").append(e.getKey()).append(":").append(e.getValue());
        }
        ctx.write(key, new Text(sb.toString()));
    }
}