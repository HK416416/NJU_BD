import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class StopWordLoader {
    public static Set<String> load(Configuration conf, String pathStr) throws Exception {
        Set<String> set = new HashSet<>();
        Path path = new Path(pathStr);
        FileSystem fs = FileSystem.get(conf);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String w = line.trim();
                if (!w.isEmpty()) set.add(w);
            }
        }
        return set;
    }
}