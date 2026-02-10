import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import java.util.Properties

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ex02DataIngestion_FullYear")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4") 
      .getOrCreate()

    // On boucle sur toute l'année 2024
    val yearToProcess = 2024
    (1 to 12).foreach { m =>
      val monthStr = f"$m%02d"
      println(s"\n>>> PROCESSING: $yearToProcess-$monthStr <<<")
      try {
        processMonth(spark, yearToProcess.toString, monthStr)
      } catch {
        case e: Exception => 
          println(s"[ERROR] Skip $yearToProcess-$monthStr: ${e.getMessage}")
      }
    }
    spark.stop()
  }

  def processMonth(spark: SparkSession, year: String, month: String): Unit = {
    import spark.implicits._

    val fileName = s"yellow_tripdata_$year-$month.parquet"
    val rawPath = s"s3a://nyc-raw/$fileName"
    val cleanedPath = s"s3a://nyc-cleaned/year=$year/month=$month/"

    // ÉTAPE 1 : LECTURE
    val rawDF = spark.read.parquet(rawPath)

    // ÉTAPE 2 : NETTOYAGE
    val cleanedDF = rawDF.filter(
      $"passenger_count" > 0 && $"trip_distance" > 0 && $"fare_amount" > 0 &&
      $"tpep_pickup_datetime".isNotNull && $"tpep_dropoff_datetime".isNotNull &&
      $"tpep_pickup_datetime" < $"tpep_dropoff_datetime" &&
      $"PULocationID".isNotNull && $"DOLocationID".isNotNull
    ).withColumn("source_file", lit(fileName))
     .withColumn("ingestion_year", lit(year.toInt))
     .withColumn("ingestion_month", lit(month.toInt))

    // ÉTAPE 2.a : EXPORT SILVER (MinIO)
    cleanedDF.write.mode(SaveMode.Append).parquet(cleanedPath)

    // ÉTAPE 3 : DWH POSTGRES
    val jdbcUrl  = sys.env.getOrElse("JDBC_URL", "jdbc:postgresql://localhost:5432/taxidb")
    val jdbcUser = sys.env.getOrElse("JDBC_USER", "myuser")
    val jdbcPass = sys.env.getOrElse("JDBC_PASSWORD", "mypassword")
    val props = new Properties()
    props.setProperty("user", jdbcUser); props.setProperty("password", jdbcPass); props.setProperty("driver", "org.postgresql.Driver")

    def readTable(table: String): DataFrame = spark.read.jdbc(jdbcUrl, table, props)
    def isNotEmpty(df: DataFrame): Boolean = df.take(1).nonEmpty

    // ÉTAPE 3.a : DIMS INCREMENTAL
    // VENDOR
    val existingVendor = readTable("dwh.dim_vendor").select($"vendor_id")
    val newVendor = cleanedDF.select($"VendorID".cast("int").as("vendor_id")).na.drop().distinct()
      .join(existingVendor, Seq("vendor_id"), "left_anti")
    if (isNotEmpty(newVendor)) newVendor.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_vendor", props)

    // RATE CODE
    val existingRate = readTable("dwh.dim_rate_code").select($"rate_code_id")
    val newRate = cleanedDF.select($"RatecodeID".cast("int").as("rate_code_id")).na.drop().distinct()
      .join(existingRate, Seq("rate_code_id"), "left_anti")
    if (isNotEmpty(newRate)) newRate.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_rate_code", props)

    // PAYMENT
    val existingPay = readTable("dwh.dim_payment_type").select($"payment_type_id")
    val newPay = cleanedDF.select($"payment_type".cast("int").as("payment_type_id")).na.drop().distinct()
      .join(existingPay, Seq("payment_type_id"), "left_anti")
    if (isNotEmpty(newPay)) newPay.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_payment_type", props)

    // LOCATION (PU + DO)
    val existingLoc = readTable("dwh.dim_location").select($"location_id")
    val allLoc = cleanedDF.select($"PULocationID".cast("int").as("location_id"))
      .union(cleanedDF.select($"DOLocationID".cast("int").as("location_id"))).na.drop().distinct()
    val newLoc = allLoc.join(existingLoc, Seq("location_id"), "left_anti")
    if (isNotEmpty(newLoc)) newLoc.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_location", props)

    // DATETIME
    val existingDt = readTable("dwh.dim_datetime").select($"ts")
    val allTsDF = cleanedDF.select($"tpep_pickup_datetime".cast("timestamp").as("ts"))
      .union(cleanedDF.select($"tpep_dropoff_datetime".cast("timestamp").as("ts"))).na.drop().distinct()
    val newTs = allTsDF.join(existingDt, Seq("ts"), "left_anti")
      .withColumn("date", to_date($"ts"))
      .withColumn("year", org.apache.spark.sql.functions.year($"ts"))
      .withColumn("month", org.apache.spark.sql.functions.month($"ts"))
      .withColumn("day", dayofmonth($"ts"))
      .withColumn("hour", hour($"ts"))
      .withColumn("dow", dayofweek($"ts"))
    if (isNotEmpty(newTs)) newTs.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_datetime", props)

    // ÉTAPE 4 : FACT TABLE
    val dimVendor = readTable("dwh.dim_vendor").select($"vendor_key", $"vendor_id")
    val dimRate   = readTable("dwh.dim_rate_code").select($"rate_code_key", $"rate_code_id")
    val dimPay    = readTable("dwh.dim_payment_type").select($"payment_type_key", $"payment_type_id")
    val dimLoc    = readTable("dwh.dim_location").select($"location_key", $"location_id")
    val dimDt     = readTable("dwh.dim_datetime").select($"datetime_key", $"ts")

    val factTripDF = cleanedDF
      .withColumn("vendor_id", $"VendorID".cast("int"))
      .withColumn("rate_code_id", $"RatecodeID".cast("int"))
      .withColumn("payment_type_id", $"payment_type".cast("int"))
      .withColumn("pu_location_id", $"PULocationID".cast("int"))
      .withColumn("do_location_id", $"DOLocationID".cast("int"))
      .withColumn("pickup_ts", $"tpep_pickup_datetime".cast("timestamp"))
      .withColumn("dropoff_ts", $"tpep_dropoff_datetime".cast("timestamp"))
      .join(dimVendor, Seq("vendor_id"), "left")
      .join(dimRate, Seq("rate_code_id"), "left")
      .join(dimPay, Seq("payment_type_id"), "left")
      .join(dimLoc.withColumnRenamed("location_id", "pu_location_id").withColumnRenamed("location_key", "pu_location_key"), Seq("pu_location_id"), "left")
      .join(dimLoc.withColumnRenamed("location_id", "do_location_id").withColumnRenamed("location_key", "do_location_key"), Seq("do_location_id"), "left")
      .join(dimDt.withColumnRenamed("ts", "pickup_ts").withColumnRenamed("datetime_key", "pickup_datetime_key"), Seq("pickup_ts"), "left")
      .join(dimDt.withColumnRenamed("ts", "dropoff_ts").withColumnRenamed("datetime_key", "dropoff_datetime_key"), Seq("dropoff_ts"), "left")
      .select(
        $"pickup_datetime_key", $"dropoff_datetime_key", $"vendor_key", $"rate_code_key",
        $"payment_type_key", $"pu_location_key", $"do_location_key", $"passenger_count",
        $"trip_distance", $"fare_amount", $"extra", $"mta_tax", $"tip_amount",
        $"tolls_amount", $"improvement_surcharge", $"total_amount", $"source_file"
      )

    factTripDF.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.fact_trip", props)
    println(s"[SUCCESS] Month $month processed and inserted.")
  }
}
