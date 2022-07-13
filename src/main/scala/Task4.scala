import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, lit}

object Task4 {
    def main(args: Array[String]): Unit = {
//        Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
        val spark = SparkSession
            .builder()
            .appName("Task4-manhnk9")
            .getOrCreate()

        m4(spark, args)
        spark.stop()
    }

    private def m4(spark: SparkSession, args: Array[String]): Unit = {
        var parquetFileDF = spark.read.parquet(args(0))

        // remove rows which have null values
        parquetFileDF = parquetFileDF.filter(
            parquetFileDF("button_id").isNotNull &&
            parquetFileDF("device_model").isNotNull &&
            parquetFileDF("user_id").isNotNull)

        // concat user_id column with device_model column to create new column user_id_device_model
        var action_by_button_id = parquetFileDF.withColumn(
            "user_id_device_model",
            functions.concat(
                col("user_id"),
                lit("_"),
                col("device_model")
            )
        )

        // group by user_id_device_model, button_id and count
        action_by_button_id = action_by_button_id.groupBy("user_id_device_model", "button_id").count()

        action_by_button_id.coalesce(1)
            .write.format("parquet")
            .mode("overwrite")
            .save(args(1) + "/action_by_button_id")
    }
}