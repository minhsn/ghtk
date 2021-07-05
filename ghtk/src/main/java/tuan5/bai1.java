package tuan5;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class bai1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("appName");
        SparkSession spark = SparkSession.builder().config(conf)
                .getOrCreate();
        Dataset<Row> dataset=spark.read().parquet("hdfs:/user/minhnd85/inputTuan5/1.parquet");
        dataset.createOrReplaceTempView("data");

        Dataset dataset1 =spark.sql("select device_model,user_id,button_id from data");
//        dataset1.show();

        Dataset<Row> device_model_num_user= dataset1
                .filter(dataset1.col("device_model").isNotNull())
                .groupBy("device_model")
                .agg(countDistinct(dataset.col("user_id")).alias("count"));
//        device_model_num_user.show();
        device_model_num_user.repartition(1).write().parquet("hdfs:/user/minhnd85/tuan5/device_model_num_user");

        Dataset<Row> device_model_list_user=dataset1
                .filter(dataset1.col("device_model").isNotNull())
                .groupBy("device_model")
                .agg(collect_list("user_id").alias("list_user_id"));
        device_model_list_user.repartition(1).write().orc("hdfs:/user/minhnd85/tuan5/device_model_list_user");

        Dataset<Row> user_id_device = spark.sql("select concat(user_id,'_',ifnull(device_model,'')) user_id_device_model,button_id,count(*) count " +
                "from data " +
                "where button_id is not null " +
                "group by user_id_device_model,button_id");
//        user_id_device.show();
        user_id_device.repartition(1).write().parquet("hdfs:/user/minhnd85/tuan5/button_count_by_user_id_device_model");
    }
}
