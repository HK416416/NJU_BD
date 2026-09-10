import java.io.Serializable;

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
 * 任务一：数据清洗与用户统计特征提取
 *
 * 输入：train.csv + test.csv（无表头，逗号分隔）
 * 输出：
 *   - clean_behavior_log.csv  清洗后的原始数据
 *   - user_features.csv       归一化后的三维用户特征
 *
 * 用法（本地）：spark-submit --class DataCleanFeature jar \
 *              file:///path/train.csv file:///path/test.csv file:///path/output/
 * 用法（集群）：spark-submit --class DataCleanFeature jar \
 *              /user/root/final_exp/exp1/train.csv /user/root/final_exp/exp1/test.csv /user/xxx/output/
 */
public class DataCleanFeature {

    // ── 用户原始特征容器（可序列化） ──
    public static class UserStats implements Serializable {
        long lastTs;   // 最近一次行为时间戳
        long count;    // 总交互次数
        long score;    // 加权得分

        public UserStats(long ts, long cnt, long sc) {
            this.lastTs = ts;
            this.count = cnt;
            this.score = sc;
        }

        /** 合并两段统计（reduceByKey 的合并函数） */
        public static UserStats merge(UserStats a, UserStats b) {
            return new UserStats(
                Math.max(a.lastTs, b.lastTs),
                a.count + b.count,
                a.score + b.score
            );
        }
    }

    // ── 行为类型 → 分值映射 ──
    private static int behaviorScore(String type) {
        switch (type) {
            case "buy":  return 4;
            case "cart": return 3;
            case "fav":  return 2;
            case "pv":   return 1;
            default:     return 0; // 不合法类型，后续过滤
        }
    }

    // ── 行合法性校验 ──
    private static boolean isValid(String[] fields) {
        if (fields.length < 5) return false;                        // 字段数不足
        for (int i = 0; i < 5; i++) {
            if (fields[i] == null || fields[i].trim().isEmpty()) return false; // 缺失值
        }
        String behavior = fields[3].trim();
        if (!behavior.equals("pv") && !behavior.equals("cart") &&
            !behavior.equals("fav") && !behavior.equals("buy")) return false;   // 非法行为类型

        try {
            Long.parseLong(fields[4].trim());  // 确保 timestamp 是合法数值
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    // ── 将 RDD 输出为单个文件（saveAsTextFile 生成目录，此方法取出 part-00000 重命名为目标文件） ──
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

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: DataCleanFeature <train_path> <test_path> <output_dir>");
            System.exit(1);
        }

        String trainPath = args[0];
        String testPath  = args[1];
        String outputDir = args[2];

        SparkConf conf = new SparkConf().setAppName("DataCleanFeature");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            // ────────────────────────────────────────────
            //  Step 1：加载并合并 train + test
            // ────────────────────────────────────────────
            JavaRDD<String> trainData = sc.textFile(trainPath);
            JavaRDD<String> testData  = sc.textFile(testPath);
            JavaRDD<String> allData   = trainData.union(testData);

            // ────────────────────────────────────────────
            //  Step 2：数据清洗（过滤 + 缓存，后续复用）
            // ────────────────────────────────────────────
            JavaRDD<String> cleanData = allData
                .filter(line -> {
                    String[] fields = line.split(",", -1); // -1 保留尾部空串
                    return isValid(fields);
                })
                .cache();  // 缓存，避免后续两次 action 重复计算

            long totalBefore = allData.count();
            long totalAfter  = cleanData.count();
            System.out.println("[INFO] 清洗前: " + totalBefore + " 条, 清洗后: " + totalAfter + " 条"
                               + ", 丢弃: " + (totalBefore - totalAfter) + " 条");

            // 输出文件 1：clean_behavior_log.csv
            saveAsSingleFile(cleanData, outputDir + "/clean_behavior_log.csv");

            // ────────────────────────────────────────────
            //  Step 3：计算全局最大时间戳（F₁ 的截止时间）
            // ────────────────────────────────────────────
            long globalMaxTs = cleanData
                .map(line -> Long.parseLong(line.split(",")[4].trim()))
                .reduce(Math::max);
            System.out.println("[INFO] 全局最大时间戳: " + globalMaxTs
                               + " (" + new java.util.Date(globalMaxTs * 1000) + ")");

            // ────────────────────────────────────────────
            //  Step 4：按 user_id 聚合三维原始特征
            // ────────────────────────────────────────────
            JavaPairRDD<String, UserStats> userStats = cleanData
                .mapToPair(line -> {
                    String[] f = line.split(",");
                    String uid = f[0].trim();
                    long ts = Long.parseLong(f[4].trim());
                    int  bScore = behaviorScore(f[3].trim());
                    return new Tuple2<>(uid, new UserStats(ts, 1, bScore));
                })
                .reduceByKey(UserStats::merge);

            // 转换为 double[3]：{ F1_raw, F2_raw, F3_raw }
            // F1_raw = 距离截止时间的天数
            JavaPairRDD<String, double[]> rawFeatures = userStats
                .mapValues(s -> new double[]{
                    (globalMaxTs - s.lastTs) / 86400.0,  // F1: 时效(天)
                    (double) s.count,                      // F2: 活跃度
                    (double) s.score                       // F3: 价值分
                });

            // ────────────────────────────────────────────
            //  Step 5：求全局极值（Min-Max 归一化所需）
            // ────────────────────────────────────────────
            JavaRDD<double[]> vals = rawFeatures.values();

            double f1min = vals.mapToDouble(f -> f[0]).min();
            double f1max = vals.mapToDouble(f -> f[0]).max();
            double f2min = vals.mapToDouble(f -> f[1]).min();
            double f2max = vals.mapToDouble(f -> f[1]).max();
            double f3min = vals.mapToDouble(f -> f[2]).min();
            double f3max = vals.mapToDouble(f -> f[2]).max();

            System.out.printf("[INFO] F1 范围: %.4f ~ %.4f (天)%n", f1min, f1max);
            System.out.printf("[INFO] F2 范围: %.0f ~ %.0f (次)%n", f2min, f2max);
            System.out.printf("[INFO] F3 范围: %.0f ~ %.0f (分)%n", f3min, f3max);

            // 计算各维度的极差
            double r1 = f1max - f1min;
            double r2 = f2max - f2min;
            double r3 = f3max - f3min;

            // ────────────────────────────────────────────
            //  Step 6：Min-Max 归一化 + 格式化输出
            // ────────────────────────────────────────────
            JavaRDD<String> normFeatures = rawFeatures
                .map(t -> {
                    double[] f = t._2;
                    // 若极差为 0（所有用户该特征值相同），归一化为 0
                    double n1 = (r1 == 0) ? 0.0 : (f[0] - f1min) / r1;
                    double n2 = (r2 == 0) ? 0.0 : (f[1] - f2min) / r2;
                    double n3 = (r3 == 0) ? 0.0 : (f[2] - f3min) / r3;
                    return String.format("%s,%.4f,%.4f,%.4f", t._1, n1, n2, n3);
                });

            // 输出文件 2：user_features.csv
            saveAsSingleFile(normFeatures, outputDir + "/user_features.csv");

            System.out.println("[INFO] 任务一完成");
            System.out.println("[INFO] clean_behavior_log → " + outputDir + "/clean_behavior_log.csv");
            System.out.println("[INFO] user_features       → " + outputDir + "/user_features.csv");
        }
    }
}
