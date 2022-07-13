import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, lit}

object Task23 {
    def main(args: Array[String]): Unit = {
//        Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
        val spark = SparkSession
            .builder()
            .appName("Task23-manhnk9")
            .getOrCreate()


        m2_3(spark, args)
        spark.stop()
    }

    private def m2_3(spark: SparkSession, args: Array[String]): Unit = {
        //        val myPath = "data/sample/big.parquet"
        val myPath = args(0)
        var parquetFileDF = spark.read.parquet(myPath)

        // remove rows which have "device_model" is null
        parquetFileDF = parquetFileDF.filter(parquetFileDF("device_model").isNotNull)
        // distinct device_model and user_id
        val distinctDF = parquetFileDF.select(col("device_model"), col("user_id")).distinct()

        //group by device_model and count user_id
        val device_model_num_user = distinctDF.groupBy(col("device_model")).count()
        device_model_num_user.coalesce(1)
            .write
            .format("parquet")
            .option("compression", "snappy")
            .mode("overwrite")
            .save(args(1) + "/device_model_num_user")

        // group by device_model and list all user_id in one column
        val device_model_list_user = distinctDF.groupBy("device_model").agg("user_id" -> "collect_list")
        device_model_list_user.coalesce(1)
            .write
            .format("orc")
            .mode("overwrite")
            .save(args(1) + "/device_model_list_user")
    }
}