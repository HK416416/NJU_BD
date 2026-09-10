## 期末项目：天猫复购预测

### 目录结构

```
final_exp/
├── data/                  # 测试数据
│   ├── train.csv
│   └── test.csv
├── src/                   # 源代码
│   ├── DataCleanFeature.java        (任务一)
│   ├── InvertedIndexCooccur.java    (任务二)
│   ├── KMeansClustering.java        (任务三)
│   └── PersonalRank.java            (任务四)
├── jar/                   # 可执行 JAR
│   ├── task1.jar
│   ├── task2.jar
│   ├── task3.jar
│   └── task4.jar
├── output/                # 本地测试输出
│   ├── task1/
│   │   ├── clean_behavior_log.csv
│   │   └── user_features.csv
│   ├── task2/
│   │   ├── item_user_inverted_index.txt
│   │   └── item_co_occurrence.txt
│   ├── task3/
│   │   ├── cluster_centers.txt
│   │   └── user_cluster_labels.csv
│   └── task4/
│       └── personal_rank_recommendations.txt
├── script/                # 本地测试脚本
│   ├── run_task1.sh
│   ├── run_task2.sh
│   ├── run_task3.sh
│   └── run_task4.sh
├── build/                 # 编译中间文件
└── README.md
```

### 环境要求
- Hadoop 3.2.1
- Spark 3.3.x（RDD API）
- JDK 1.8

### 任务依赖关系

```
任务一 (user_features.csv) → 任务三 (K-Means 聚类)
任务一 (交互关系)         → 任务四 (PersonalRank 二分图)
```

### 集群执行命令

**任务一：**
```bash
spark-submit --class DataCleanFeature --master yarn \
    task1.jar \
    /user/root/final_exp/exp1/train.csv \
    /user/root/final_exp/exp1/test.csv \
    /user/231220082a/final_exp_output/task1
```

**任务二：**
```bash
spark-submit --class InvertedIndexCooccur --master yarn \
    task2.jar \
    /user/root/final_exp/exp1/train.csv \
    /user/root/final_exp/exp1/test.csv \
    /user/231220082a/final_exp_output/task2
```

**任务三：**
```bash
spark-submit --class KMeansClustering --master yarn \
    task3.jar \
    /user/231220082a/final_exp_output/task1/user_features.csv \
    /user/231220082a/final_exp_output/task3 \
    3
```
> 最后一个参数 K 可省略，默认 K=3。

**任务四：**
```bash
spark-submit --class PersonalRank --master yarn \
    task4.jar \
    /user/root/final_exp/exp1/train.csv \
    /user/root/final_exp/exp1/test.csv \
    <target_user_id> \
    /user/231220082a/final_exp_output/task4
```
> 第三个参数为目标用户 ID，第四个为输出路径。本地测试使用 `100004`。

### 输出文件格式

| 任务 | 输出文件 | 格式 |
|------|---------|------|
| 任务一 | `clean_behavior_log.csv` | `user_id,item_id,category_id,behavior_type,timestamp` |
| 任务一 | `user_features.csv` | `user_id,F1_norm,F2_norm,F3_norm` (4位小数) |
| 任务二 | `item_user_inverted_index.txt` | `item_id \t user1,user2,...` |
| 任务二 | `item_co_occurrence.txt` | `itemA,itemB \t count` (Top 1000 降序) |
| 任务三 | `cluster_centers.txt` | `cluster_id \t F1,F2,F3` |
| 任务三 | `user_cluster_labels.csv` | `user_id,cluster_id` |
| 任务四 | `personal_rank_recommendations.txt` | `target_user \t item:score,item:score,...` (Top 10) |
