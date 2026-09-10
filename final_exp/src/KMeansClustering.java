import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * 任务三：K-Means 用户聚类
 *
 * 输入：任务一的 user_features.csv（user_id,F1_norm,F2_norm,F3_norm）
 * 输出：
 *   - cluster_centers.txt          K 个聚类中心的三维坐标
 *   - user_cluster_labels.csv      每个用户的聚类标签
 *
 * 用法：spark-submit --class KMeansClustering jar \
 *       <user_features_path> <output_dir> [K]
 */
public class KMeansClustering {

    private static final int DEFAULT_K = 3;
    private static final int MAX_ITER = 20;
    private static final double THRESHOLD = 1e-4;

    // ── 欧氏距离平方（避免开方，仅用于比较） ──
    private static double squaredDist(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double d = a[i] - b[i];
            sum += d * d;
        }
        return sum;
    }

    // ── 找到最近的中心点索引 ──
    private static int nearestCenter(double[] point, double[][] centers) {
        int best = 0;
        double minDist = Double.MAX_VALUE;
        for (int i = 0; i < centers.length; i++) {
            double dist = squaredDist(point, centers[i]);
            if (dist < minDist) {
                minDist = dist;
                best = i;
            }
        }
        return best;
    }

    // ── 中心点移动量（欧氏距离） ──
    private static double centerMove(double[] a, double[] b) {
        return Math.sqrt(squaredDist(a, b));
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

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: KMeansClustering <user_features_path> <output_dir> [K]");
            System.exit(1);
        }

        String inputPath  = args[0];
        String outputDir  = args[1];
        int K = (args.length >= 3) ? Integer.parseInt(args[2]) : DEFAULT_K;

        SparkConf conf = new SparkConf().setAppName("KMeansClustering");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            // ────────────────────────────────────────────
            //  Step 1：加载用户特征 → (user_id, double[3])
            // ────────────────────────────────────────────
            JavaPairRDD<String, double[]> userFeatures = sc.textFile(inputPath)
                .mapToPair(line -> {
                    String[] f = line.split(",");
                    double[] feats = {
                        Double.parseDouble(f[1]),
                        Double.parseDouble(f[2]),
                        Double.parseDouble(f[3])
                    };
                    return new Tuple2<>(f[0], feats);
                })
                .cache();

            long userCount = userFeatures.count();
            System.out.println("[INFO] 加载用户数: " + userCount + ", K = " + K);

            // ────────────────────────────────────────────
            //  Step 2：随机抽样 K 个用户作为初始中心
            // ────────────────────────────────────────────
            List<Tuple2<String, double[]>> samples = userFeatures.takeSample(false, K);
            double[][] centers = new double[K][3];
            System.out.print("[INFO] 初始中心: ");
            for (int i = 0; i < K; i++) {
                centers[i] = samples.get(i)._2.clone();
                System.out.printf("C%d=(%.4f,%.4f,%.4f) ",
                    i, centers[i][0], centers[i][1], centers[i][2]);
            }
            System.out.println();

            // ────────────────────────────────────────────
            //  Step 3：迭代 K-Means
            // ────────────────────────────────────────────
            int iter;
            for (iter = 0; iter < MAX_ITER; iter++) {
                // 广播当前中心到所有 Executor
                Broadcast<double[][]> bc = sc.broadcast(centers);

                // 分配每个点到最近中心，按 cluster_id 聚合 (坐标和, 点数)
                JavaPairRDD<Integer, Tuple2<double[], Long>> clusterAgg = userFeatures
                    .mapToPair(t -> {
                        double[][] c = bc.value();
                        int nearest = nearestCenter(t._2, c);
                        return new Tuple2<>(nearest, new Tuple2<>(t._2, 1L));
                    })
                    .reduceByKey((a, b) -> {
                        double[] sum = new double[3];
                        for (int d = 0; d < 3; d++) sum[d] = a._1[d] + b._1[d];
                        return new Tuple2<>(sum, a._2 + b._2);
                    });

                // 收集新中心
                Map<Integer, Tuple2<double[], Long>> clusterMap = clusterAgg.collectAsMap();

                double[][] newCenters = new double[K][3];
                double maxMove = 0.0;

                for (int i = 0; i < K; i++) {
                    Tuple2<double[], Long> agg = clusterMap.get(i);
                    if (agg != null && agg._2 > 0) {
                        for (int d = 0; d < 3; d++) {
                            newCenters[i][d] = agg._1[d] / agg._2;  // 均值 = 坐标和 / 点数
                        }
                    } else {
                        // 空簇：保持原中心
                        newCenters[i] = centers[i].clone();
                    }
                    double move = centerMove(newCenters[i], centers[i]);
                    if (move > maxMove) maxMove = move;
                }

                centers = newCenters;
                System.out.printf("[INFO] Iter %2d: maxMove=%.6f%n", iter, maxMove);

                if (maxMove < THRESHOLD) {
                    System.out.println("[INFO] 收敛，迭代终止");
                    break;
                }
            }

            // ────────────────────────────────────────────
            //  Step 4：最终分配 + 输出
            // ────────────────────────────────────────────

            // 输出文件 1：cluster_centers.txt
            StringBuilder centersStr = new StringBuilder();
            for (int i = 0; i < K; i++) {
                centersStr.append(String.format("%d\t%.4f,%.4f,%.4f%n",
                    i, centers[i][0], centers[i][1], centers[i][2]));
            }
            JavaRDD<String> centersRdd = sc.parallelize(
                java.util.Arrays.asList(centersStr.toString().trim().split("\n")));
            saveAsSingleFile(centersRdd, outputDir + "/cluster_centers.txt");
            System.out.println("[INFO] cluster_centers.txt 已保存");

            // 输出文件 2：user_cluster_labels.csv
            Broadcast<double[][]> bcFinal = sc.broadcast(centers);
            JavaRDD<String> labels = userFeatures
                .map(t -> {
                    int cluster = nearestCenter(t._2, bcFinal.value());
                    return t._1 + "," + cluster;
                });

            saveAsSingleFile(labels, outputDir + "/user_cluster_labels.csv");
            System.out.println("[INFO] user_cluster_labels.csv 已保存");

            // 打印各簇样本数
            Map<Integer, Long> clusterSizes = labels
                .mapToPair(line -> {
                    String[] f = line.split(",");
                    return new Tuple2<>(Integer.parseInt(f[1]), 1L);
                })
                .countByKey();
            for (int i = 0; i < K; i++) {
                System.out.printf("[INFO] 簇 %d: %d 用户%n", i, clusterSizes.getOrDefault(i, 0L));
            }

            System.out.println("[INFO] 任务三完成，迭代次数: " + (iter + 1));
        }
    }
}
