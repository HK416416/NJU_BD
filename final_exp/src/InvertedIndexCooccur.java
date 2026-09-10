import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 任务二：基于行为标签的倒排索引与共现分析
 *
 * 输入：train.csv + test.csv（无表头，逗号分隔）
 * 输出：
 *   - item_user_inverted_index.txt  商品→用户倒排索引
 *   - item_co_occurrence.txt        商品共现 Top 1000
 *
 * 用法（本地）：spark-submit --class InvertedIndexCooccur jar \
 *              file:///path/train.csv file:///path/test.csv file:///path/output/
 * 用法（集群）：spark-submit --class InvertedIndexCooccur jar \
 *              /user/root/final_exp/exp1/train.csv /user/root/final_exp/exp1/test.csv /user/xxx/output/
 */
public class InvertedIndexCooccur {

    // ── 行合法性校验 ──
    private static boolean isValid(String[] fields) {
        if (fields.length < 5) return false;
        for (int i = 0; i < 5; i++) {
            if (fields[i] == null || fields[i].trim().isEmpty()) return false;
        }
        return true;
    }

    // ── 将 RDD 输出为单个文件 ──
    private static void saveAsSingleFile(JavaRDD<String> rdd, String outputPath) throws Exception {
        String tmpDir = outputPath + "_tmp";
        rdd.coalesce(1).saveAsTextFile(tmpDir);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path(tmpDir);
        Path destPath = new Path(outputPath);

        if (fs.exists(destPath)) fs.delete(destPath, true);

        FileStatus[] files = fs.listStatus(tmpPath);
        for (FileStatus f : files) {
            if (f.getPath().getName().startsWith("part-")) {
                fs.rename(f.getPath(), destPath);
                break;
            }
        }
        fs.delete(tmpPath, true);
    }

    // ── 是否为深度交互 ──
    private static boolean isDeepInteraction(String type) {
        return type.equals("cart") || type.equals("fav") || type.equals("buy");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: InvertedIndexCooccur <train_path> <test_path> <output_dir>");
            System.exit(1);
        }

        String trainPath = args[0];
        String testPath  = args[1];
        String outputDir = args[2];

        SparkConf conf = new SparkConf().setAppName("InvertedIndexCooccur");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            // ────────────────────────────────────────────
            //  Step 1：加载数据 + 过滤深度交互
            // ────────────────────────────────────────────
            JavaRDD<String> trainData = sc.textFile(trainPath);
            JavaRDD<String> testData  = sc.textFile(testPath);
            JavaRDD<String> allData   = trainData.union(testData);

            // 清洗 + 只保留深度交互
            JavaPairRDD<String, String> userItemPairs = allData
                .filter(line -> {
                    String[] f = line.split(",", -1);
                    return isValid(f) && isDeepInteraction(f[3].trim());
                })
                .mapToPair(line -> {
                    String[] f = line.split(",");
                    return new Tuple2<>(f[0].trim(), f[1].trim()); // (user_id, item_id)
                })
                .distinct()  // 去重：同一用户对同一商品的多次交互只算一次
                .cache();

            long recordCount = userItemPairs.count();
            System.out.println("[INFO] 深度交互 (user, item) 去重后: " + recordCount + " 对");

            // ────────────────────────────────────────────
            //  Step 2：构建商品→用户倒排索引
            //          (item_id, [user_id_list])
            // ────────────────────────────────────────────
            JavaPairRDD<String, Iterable<String>> item2users = userItemPairs
                .mapToPair(Tuple2::swap)  // 交换 → (item_id, user_id)
                .groupByKey();            // 按 item 聚合

            // 格式化为: item_id \t user1,user2,user3...
            JavaRDD<String> invertedIndex = item2users
                .map(t -> {
                    String itemId = t._1;
                    String users = StreamSupport.stream(t._2.spliterator(), false)
                        .collect(Collectors.joining(","));
                    return itemId + "\t" + users;
                });

            // 输出文件 1：item_user_inverted_index.txt
            saveAsSingleFile(invertedIndex, outputDir + "/item_user_inverted_index.txt");

            System.out.println("[INFO] 倒排索引构建完成，商品数: " + item2users.count());

            // ────────────────────────────────────────────
            //  Step 3：计算商品共现次数（Jaccard 分子）
            //          对每个用户，对其所有深度交互商品两两配对
            // ────────────────────────────────────────────
            JavaPairRDD<String, Long> cooccurPairs = userItemPairs
                .groupByKey()                     // (user_id, [item_ids])
                .flatMapToPair(t -> {
                    // 收集该用户的所有商品
                    List<String> items = new ArrayList<>();
                    t._2.forEach(items::add);

                    List<Tuple2<String, Long>> pairs = new ArrayList<>();
                    int n = items.size();
                    for (int i = 0; i < n; i++) {
                        for (int j = i + 1; j < n; j++) {
                            String a = items.get(i);
                            String b = items.get(j);
                            // 统一排序，保证 a < b（字典序即可，ID 为数字串）
                            if (a.compareTo(b) < 0) {
                                pairs.add(new Tuple2<>(a + "," + b, 1L));
                            } else {
                                pairs.add(new Tuple2<>(b + "," + a, 1L));
                            }
                        }
                    }
                    return pairs.iterator();
                })
                .reduceByKey(Long::sum);  // 按商品对聚合共现次数

            long totalPairs = cooccurPairs.count();
            System.out.println("[INFO] 共现商品对总数: " + totalPairs);

            // 取 Top 1000（按共现次数降序）
            List<Tuple2<String, Long>> top1000 = cooccurPairs
                .mapToPair(t -> new Tuple2<>(t._2, t._1))  // (count, pair) — 便于排序
                .sortByKey(false)                            // 按 count 降序
                .mapToPair(t -> new Tuple2<>(t._2, t._1))  // 换回 (pair, count)
                .take(1000);

            System.out.println("[INFO] Top 1000 共现对已筛选，第1名: "
                               + (top1000.isEmpty() ? "N/A" : top1000.get(0)));

            // 格式化输出: itemA,itemB \t count
            JavaRDD<String> top1000Rdd = sc.parallelize(
                top1000.stream()
                    .map(t -> t._1 + "\t" + t._2)
                    .collect(Collectors.toList())
            );

            // 输出文件 2：item_co_occurrence.txt
            saveAsSingleFile(top1000Rdd, outputDir + "/item_co_occurrence.txt");

            System.out.println("[INFO] 任务二完成");
            System.out.println("[INFO] item_user_inverted_index → " + outputDir + "/item_user_inverted_index.txt");
            System.out.println("[INFO] item_co_occurrence       → " + outputDir + "/item_co_occurrence.txt");
        }
    }
}
