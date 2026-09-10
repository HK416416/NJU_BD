import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * 任务四：基于二分图随机游走的 PersonalRank 推荐
 *
 * 输入：train.csv + test.csv，指定目标用户
 * 输出：personal_rank_recommendations.txt
 *
 * 用法：spark-submit --class PersonalRank jar \
 *       <train_path> <test_path> <target_user_id> <output_dir>
 */
public class PersonalRank {

    private static final double ALPHA = 0.85;      // 阻尼系数
    private static final int MAX_ITER = 20;         // 最大迭代次数

    // ── 行校验 ──
    private static boolean isValid(String[] fields) {
        if (fields.length < 5) return false;
        for (int i = 0; i < 5; i++) {
            if (fields[i] == null || fields[i].trim().isEmpty()) return false;
        }
        return true;
    }

    // ── 所有合法行为类型（扩大二分图覆盖面） ──
    private static boolean isValidBehavior(String type) {
        return type.equals("pv") || type.equals("cart") || type.equals("fav") || type.equals("buy");
    }

    // ── 保存为单文件 ──
    private static void saveAsSingleFile(JavaRDD<String> rdd, String outPath) throws Exception {
        String tmpDir = outPath + "_tmp";
        rdd.coalesce(1).saveAsTextFile(tmpDir);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpP = new Path(tmpDir), dstP = new Path(outPath);
        if (fs.exists(dstP)) fs.delete(dstP, true);
        FileStatus[] files = fs.listStatus(tmpP);
        for (FileStatus f : files) {
            if (f.getPath().getName().startsWith("part-")) {
                fs.rename(f.getPath(), dstP);
                break;
            }
        }
        fs.delete(tmpP, true);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: PersonalRank <train> <test> <target_user> <output_dir>");
            System.exit(1);
        }

        String trainPath  = args[0];
        String testPath   = args[1];
        String targetUser = args[2];
        String outputDir  = args[3];

        SparkConf conf = new SparkConf().setAppName("PersonalRank");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            // ════════════════════════════════════════════
            //  Step 1：加载数据 + 过滤深度交互 + 去重
            // ════════════════════════════════════════════
            JavaRDD<String> allData = sc.textFile(trainPath).union(sc.textFile(testPath));

            // (user_id, item_id) 去重后的深度交互对
            JavaPairRDD<String, String> userItemEdges = allData
                .filter(line -> {
                    String[] f = line.split(",", -1);
                    return isValid(f) && isValidBehavior(f[3].trim());
                })
                .mapToPair(line -> {
                    String[] f = line.split(",");
                    return new Tuple2<>(f[0].trim(), f[1].trim());
                })
                .distinct()
                .cache();

            long edgeCount = userItemEdges.count();
            System.out.println("[INFO] 二分图边数: " + edgeCount);

            // ── 目标用户已交互的商品（后续过滤用）────
            Set<String> targetItems = new HashSet<>(
                userItemEdges.filter(t -> t._1.equals(targetUser))
                             .map(t -> t._2)
                             .collect()
            );
            System.out.println("[INFO] 目标用户 " + targetUser
                               + " 已交互商品数: " + targetItems.size());

            // ════════════════════════════════════════════
            //  Step 2：计算度 + 构建逆边 RDD
            // ════════════════════════════════════════════

            // 出度 Map：user → degree,  item → degree（广播用）
            Map<String, Integer> userDegMap = userItemEdges
                .mapToPair(t -> new Tuple2<>(t._1, 1))
                .reduceByKey(Integer::sum)
                .collectAsMap();

            Map<String, Integer> itemDegMap = userItemEdges
                .mapToPair(t -> new Tuple2<>(t._2, 1))
                .reduceByKey(Integer::sum)
                .collectAsMap();

            Broadcast<Map<String, Integer>> bcUserDeg = sc.broadcast(userDegMap);
            Broadcast<Map<String, Integer>> bcItemDeg = sc.broadcast(itemDegMap);

            // 逆边：item → user
            JavaPairRDD<String, String> itemUserEdges = userItemEdges
                .mapToPair(t -> new Tuple2<>(t._2, t._1));

            // ════════════════════════════════════════════
            //  Step 3：初始化 PR 分数
            // ════════════════════════════════════════════
            JavaPairRDD<String, Double> prUsers = userItemEdges
                .keys()
                .distinct()
                .mapToPair(u -> new Tuple2<>(u, u.equals(targetUser) ? 1.0 : 0.0));

            JavaPairRDD<String, Double> prItems = userItemEdges
                .values()
                .distinct()
                .mapToPair(i -> new Tuple2<>(i, 0.0));

            // 目标用户重启 RDD
            double restart = 1.0 - ALPHA;
            JavaPairRDD<String, Double> restartRdd = sc.parallelizePairs(
                Arrays.asList(new Tuple2<>(targetUser, restart)));

            // ════════════════════════════════════════════
            //  Step 4：迭代 PersonalRank
            // ════════════════════════════════════════════
            for (int iter = 0; iter < MAX_ITER; iter++) {

                // ── 4a：用户 PR → 商品 PR ──
                // PR_item = α × Σ(PR_user / out_degree(user))
                prItems = prUsers
                    .join(userItemEdges)                        // (user, (pr, item))
                    .mapToPair(t -> {
                        int deg = bcUserDeg.value().getOrDefault(t._1, 1);
                        return new Tuple2<>(t._2._2, t._2._1 / deg);  // (item, contribution)
                    })
                    .reduceByKey(Double::sum)
                    .mapValues(s -> s * ALPHA);

                // ── 4b：商品 PR → 用户 PR ──
                // PR_user = α × Σ(PR_item / out_degree(item))
                prUsers = prItems
                    .join(itemUserEdges)                        // (item, (pr, user))
                    .mapToPair(t -> {
                        int deg = bcItemDeg.value().getOrDefault(t._1, 1);
                        return new Tuple2<>(t._2._2, t._2._1 / deg);  // (user, contribution)
                    })
                    .reduceByKey(Double::sum)
                    .mapValues(s -> s * ALPHA);

                // ── 4c：给目标用户加重启概率 (1-α) ──
                prUsers = prUsers
                    .leftOuterJoin(restartRdd)
                    .mapValues(t -> t._1 + (t._2.isPresent() ? t._2.get() : 0.0));

                System.out.printf("[INFO] Iter %2d: PR_users=%d, PR_items=%d%n",
                    iter, prUsers.count(), prItems.count());
            }

            // ════════════════════════════════════════════
            //  Step 5：过滤已交互商品，取 Top 10
            // ════════════════════════════════════════════
            Broadcast<Set<String>> bcTargetItems = sc.broadcast(targetItems);

            // 诊断：查看非零 PR 值分布
            double maxScore = prItems.values().reduce(Math::max);
            long nonZeroCount = prItems.filter(t -> t._2 > 1e-15).count();
            System.out.printf("[INFO] PR物品最大值=%.6f, 非零物品数=%d%n", maxScore, nonZeroCount);

            // (score, item) 降序排列
            List<Tuple2<Double, String>> top10 = prItems
                .filter(t -> !bcTargetItems.value().contains(t._1))  // 排除已交互
                .mapToPair(Tuple2::swap)                              // (score, item)
                .sortByKey(false)                                     // 降序
                .take(10);

            System.out.println("[INFO] Top 10 推荐 (过滤已交互后):");
            for (int i = 0; i < top10.size(); i++) {
                System.out.printf("  %d. item=%s  score=%.6f%n",
                    i + 1, top10.get(i)._2, top10.get(i)._1);
            }

            // ════════════════════════════════════════════
            //  Step 6：输出
            // ════════════════════════════════════════════
            StringBuilder outStr = new StringBuilder();
            outStr.append(targetUser).append(" \t ");
            for (int i = 0; i < top10.size(); i++) {
                if (i > 0) outStr.append(",");
                outStr.append(String.format("%s:%.6f",
                    top10.get(i)._2, top10.get(i)._1));
            }

            JavaRDD<String> output = sc.parallelize(
                Arrays.asList(outStr.toString()));
            saveAsSingleFile(output, outputDir + "/personal_rank_recommendations.txt");

            System.out.println("[INFO] 任务四完成");
            System.out.println("[INFO] personal_rank_recommendations.txt → "
                               + outputDir + "/personal_rank_recommendations.txt");
        }
    }
}
