# sparksql-parquet-masterdev
# Chạy job trên YARN
```
ssh hadoop@172.17.80.27
Password:1
```
```
spark-submit --deploy-mode client --class Task23 ~/manhnk9/spark-demo/target/sample-1.0-SNAPSHOT.jar /user/manhnk9/Sample_data/big.snappy.parquet /user/manhnk9
spark-submit --deploy-mode client --class Task4 ~/manhnk9/spark-demo/target/sample-1.0-SNAPSHOT.jar /user/manhnk9/Sample_data/big.snappy.parquet /user/manhnk9
```
Output trên HDFS
```
/user/manhnk9/
```
# Chạy local với Intellij
Sửa Spark Session thành
```
val spark = SparkSession
            .builder()
            .appName("App Name")
            .master("local[*]")
            .getOrCreate()
```
Edit Run Configuration và thêm Program Arguments
```
Sample_data/small.snappy.parquet output
```
