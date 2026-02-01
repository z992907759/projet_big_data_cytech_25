import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import java.util.Properties

object Main {

  def main(args: Array[String]): Unit = {

    val year  = if (args.length > 0) args(0) else "2025"
    val month = if (args.length > 1) args(1) else "01"

    val fileName = s"yellow_tripdata_${year}-${month}.parquet"

    val rawPath = s"s3a://nyc-raw/$fileName"
    // Branche 1: on écrit dans un dossier partitionné pour un historique ML
    val cleanedPath = s"s3a://nyc-cleaned/year=$year/month=$month/"

    val spark = SparkSession.builder()
      .appName("Ex02DataIngestion")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    // 1) LECTURE (Source Minio Bronze)
    println(s"[EX2] Reading raw parquet: $rawPath")
    val rawDF = spark.read.parquet(rawPath)

    // 2) NETTOYAGE / VALIDATION (Branche 1 ML-ready + base pour la Branche 2)
    val cleanedDF = rawDF
      .filter(
        $"passenger_count" > 0 &&
        $"trip_distance" > 0 &&
        $"fare_amount" > 0 &&
        $"tpep_pickup_datetime".isNotNull &&
        $"tpep_dropoff_datetime".isNotNull &&
        $"tpep_pickup_datetime" < $"tpep_dropoff_datetime" &&
        $"PULocationID".isNotNull &&
        $"DOLocationID".isNotNull
      )
      // ajout de metadata utiles
      .withColumn("source_file", lit(fileName))
      .withColumn("ingestion_year", lit(year.toInt))
      .withColumn("ingestion_month", lit(month.toInt))

    // 2.a) EXPORT BRANCHE 1 : Minio Silver (historique)
    println(s"[EX2] Writing cleaned parquet to: $cleanedPath (APPEND)")
    cleanedDF.write
      .mode(SaveMode.Append)
      .parquet(cleanedPath)

    // 3) BRANCHE 2 : DWH Postgres
    val jdbcUrl = sys.env.getOrElse("JDBC_URL", "jdbc:postgresql://localhost:5432/postgres")
    val jdbcUser = sys.env.getOrElse("JDBC_USER", "postgres")
    val jdbcPass = sys.env.getOrElse("JDBC_PASSWORD", "password")

    val props = new Properties()
    props.setProperty("user", jdbcUser)
    props.setProperty("password", jdbcPass)
    props.setProperty("driver", "org.postgresql.Driver")

    // -- Helper: safe table read (fail with explicit message)
    def readTable(table: String): DataFrame = {
      try {
        spark.read.jdbc(jdbcUrl, table, props)
      } catch {
        case e: Exception =>
          throw new RuntimeException(
            s"[EX2] Cannot read table '$table'. Did you run EX3 creation.sql first? " +
            s"JDBC_URL=$jdbcUrl. Root error: ${e.getMessage}",
            e
          )
      }
    }

    // 3.a) Charger les dims existantes (natural ids) pour l'incremental insert
    val existingVendor = readTable("dwh.dim_vendor").select($"vendor_id")
    val existingRate   = readTable("dwh.dim_rate_code").select($"rate_code_id")
    val existingPay    = readTable("dwh.dim_payment_type").select($"payment_type_id")
    val existingLoc    = readTable("dwh.dim_location").select($"location_id")
    val existingDt     = readTable("dwh.dim_datetime").select($"ts")

    // 3.b) Insérer uniquement les nouvelles valeurs (left_anti)
    val newVendor = cleanedDF.select($"VendorID".cast("int").as("vendor_id"))
      .na.drop().distinct()
      .join(existingVendor, Seq("vendor_id"), "left_anti")
    if (!newVendor.isEmpty) newVendor.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_vendor", props)

    val newRate = cleanedDF.select($"RatecodeID".cast("int").as("rate_code_id"))
      .na.drop().distinct()
      .join(existingRate, Seq("rate_code_id"), "left_anti")
    if (!newRate.isEmpty) newRate.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_rate_code", props)

    val newPay = cleanedDF.select($"payment_type".cast("int").as("payment_type_id"))
      .na.drop().distinct()
      .join(existingPay, Seq("payment_type_id"), "left_anti")
    if (!newPay.isEmpty) newPay.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_payment_type", props)

    val allLoc = cleanedDF
      .select($"PULocationID".cast("int").as("location_id"))
      .union(cleanedDF.select($"DOLocationID".cast("int").as("location_id")))
      .na.drop().distinct()

    val newLoc = allLoc.join(existingLoc, Seq("location_id"), "left_anti")
    if (!newLoc.isEmpty) newLoc.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_location", props)

    // dim_datetime : pickup + dropoff timestamps
    val pickupTsDF  = cleanedDF.select($"tpep_pickup_datetime".cast("timestamp").as("ts"))
    val dropoffTsDF = cleanedDF.select($"tpep_dropoff_datetime".cast("timestamp").as("ts"))
    val allTsDF = pickupTsDF.union(dropoffTsDF).na.drop().distinct()

    val newTs = allTsDF.join(existingDt, Seq("ts"), "left_anti")
      .withColumn("date", to_date($"ts"))
      .withColumn("year", year($"ts"))
      .withColumn("month", month($"ts"))
      .withColumn("day", dayofmonth($"ts"))
      .withColumn("hour", hour($"ts"))
      .withColumn("dow", dayofweek($"ts"))

    if (!newTs.isEmpty) newTs.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dwh.dim_datetime", props)

    // 3.c) Recharger dims avec surrogate keys
    val dimVendor = readTable("dwh.dim_vendor").select($"vendor_key", $"vendor_id")
    val dimRate   = readTable("dwh.dim_rate_code").select($"rate_code_key", $"rate_code_id")
    val dimPay2   = readTable("dwh.dim_payment_type").select($"payment_type_key", $"payment_type_id")
    val dimLoc    = readTable("dwh.dim_location").select($"location_key", $"location_id")
    val dimDt2    = readTable("dwh.dim_datetime").select($"datetime_key", $"ts")

    // 3.d) Préparer fact table (natural ids ==> surrogate keys)
    val baseFact = cleanedDF
      .withColumn("vendor_id", $"VendorID".cast("int"))
      .withColumn("rate_code_id", $"RatecodeID".cast("int"))
      .withColumn("payment_type_id", $"payment_type".cast("int"))
      .withColumn("pu_location_id", $"PULocationID".cast("int"))
      .withColumn("do_location_id", $"DOLocationID".cast("int"))
      .withColumn("pickup_ts", $"tpep_pickup_datetime".cast("timestamp"))
      .withColumn("dropoff_ts", $"tpep_dropoff_datetime".cast("timestamp"))

    // Joins dimensions
    val withVendor = baseFact.join(dimVendor, Seq("vendor_id"), "left")
    val withRate   = withVendor.join(dimRate, Seq("rate_code_id"), "left")
    val withPay3   = withRate.join(dimPay2, Seq("payment_type_id"), "left")

    val puDim = dimLoc.withColumnRenamed("location_id", "pu_location_id")
      .withColumnRenamed("location_key", "pu_location_key")
    val doDim = dimLoc.withColumnRenamed("location_id", "do_location_id")
      .withColumnRenamed("location_key", "do_location_key")

    val withPuLoc = withPay3.join(puDim, Seq("pu_location_id"), "left")
    val withDoLoc = withPuLoc.join(doDim, Seq("do_location_id"), "left")

    val pickupDim = dimDt2.withColumnRenamed("ts", "pickup_ts")
      .withColumnRenamed("datetime_key", "pickup_datetime_key")
    val dropoffDim = dimDt2.withColumnRenamed("ts", "dropoff_ts")
      .withColumnRenamed("datetime_key", "dropoff_datetime_key")

    val withPickupKey = withDoLoc.join(pickupDim, Seq("pickup_ts"), "left")
    val withDropoffKey = withPickupKey.join(dropoffDim, Seq("dropoff_ts"), "left")

    // Sélection finale fact_trip (selon  l'ex03)
    val factTripDF = withDropoffKey.select(
      $"pickup_datetime_key",
      $"dropoff_datetime_key",
      $"vendor_key",
      $"rate_code_key",
      $"payment_type_key",
      $"pu_location_key",
      $"do_location_key",
      $"passenger_count",
      $"trip_distance",
      $"fare_amount",
      $"extra",
      $"mta_tax",
      $"tip_amount",
      $"tolls_amount",
      $"improvement_surcharge",
      $"total_amount",
      $"source_file"
    )

    // 3.e) Export fact_trip (utilisation de Append pour garder un historique)
    println(s"[EX2] Writing fact_trip to Postgres (APPEND)")
    factTripDF.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "dwh.fact_trip", props)

    println(s"[EX2] Job terminé: Branche 1 (Minio Silver) + Branche 2 (Postgres DWH) synchronisées.")
    spark.stop()
  }
}
