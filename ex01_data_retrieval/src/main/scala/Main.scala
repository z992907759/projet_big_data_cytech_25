import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    // 默认用 2025-01，可以通过命令行参数覆盖
    val year  = if (args.length > 0) args(0) else "2025"
    val month = if (args.length > 1) args(1) else "01"

    val fileName  = s"yellow_tripdata_${year}-${month}.parquet"
    val localPath = s"data/raw/$fileName"
    val s3Path    = s"s3a://nyc-raw/$fileName"

    val spark = SparkSession.builder()
      .appName("Ex01DataRetrieval")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()

    val df = spark.read.parquet(localPath)

    df.printSchema()
    df.show(5)

    df.write
      .mode("overwrite")
      .parquet(s3Path)

    spark.stop()
  }
}