import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object CrimeAnalysis {
  def main(args: Array[String]): Unit = {
    // Check if the required arguments are provided
    if (args.length < 3) {
      println(
        "Usage: CrimeAnalysis <crime_csv_path> <offense_codes_csv_path> <output_parquet_path>"
      )
      sys.exit(1)
    }

    val crimeCsvPath        = args(0)
    val offenseCodesCsvPath = args(1)
    val outputParquetPath   = args(2)

    val spark = SparkSession
      .builder()
      .appName("CrimeAnalysis")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Read the crime.csv file
    val crimeDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(crimeCsvPath)
      .select(
        "INCIDENT_NUMBER",
        "DISTRICT",
        "OFFENSE_CODE",
        "YEAR",
        "MONTH",
        "Lat",
        "Long"
      )

    // Read the offense_codes.csv file and select unique codes
    val offenseCodesDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(offenseCodesCsvPath)
      .select("CODE", "NAME")
      .distinct()

    // Check data for correctness and clean duplicates
    val cleanedCrimeDf = crimeDf.na
      .drop()           // Remove rows with any null or NaN values
      .dropDuplicates() // Remove duplicate rows
      .withColumn("OFFENSE_CODE_INT", 'OFFENSE_CODE.cast(IntegerType))
      .withColumn(
        "YEAR_MON",
        concat('YEAR, lit("-"), 'MONTH)
      )                      // Concatenate year and month
      .drop('OFFENSE_CODE) // Drop the original OFFENSE_CODE
      .drop('YEAR)
      .drop('MONTH)

    // Extract the crime_type from NAME
    val crimeTypeDf = offenseCodesDf
      .withColumn("crime_name_part", split('NAME, " - ").getItem(0))

    //Prepare and combine Df's
    val combinedDf = cleanedCrimeDf.join(
      broadcast(crimeTypeDf),
      cleanedCrimeDf("OFFENSE_CODE_INT") === crimeTypeDf("CODE")
    )
    val countedCombinedDf = combinedDf
      .groupBy("DISTRICT", "YEAR_MON")
      .agg(count('INCIDENT_NUMBER).alias("incident_count"))

    // Calculate metrics
    //Top 3 crimes per district
    val windowSpec0 = Window.partitionBy("DISTRICT").orderBy(desc("count"))
    val countsDF    = combinedDf.groupBy("DISTRICT", "crime_name_part").count()
    val rankedDF    = countsDF.withColumn("rank", dense_rank().over(windowSpec0))
    val top3DF = rankedDF
      .filter(col("rank") <= 3)
      .select("DISTRICT", "crime_name_part", "count")
    val metricsDf0 = top3DF
      .groupBy("DISTRICT")
      .agg(
        collect_list('crime_name_part).as("crime_name_top3")
      )
      .withColumnRenamed("DISTRICT", "DISTRICT0")

    //Median crimes per district monthly
    val windowSpec1   = Window.partitionBy("DISTRICT").orderBy("YEAR_MON")
    val windowSpecAgg = Window.partitionBy("DISTRICT")
    val metricsDf1 = countedCombinedDf
      .withColumn("row", row_number.over(windowSpec1))
      .withColumn(
        "crimes_monthly_median",
        expr("percentile_approx(incident_count, 0.5)").over(windowSpecAgg)
      )
      .withColumnRenamed("DISTRICT", "DISTRICT1")
      .where('row === 1)
      .select("DISTRICT1", "crimes_monthly_median")

    //Average geospatial coordinates per district
    val metricsDf2 = combinedDf
      .groupBy("DISTRICT")
      .agg(
        count("INCIDENT_NUMBER").alias("crimes_total"),
        round(avg('Lat), 8).alias("lat_avg"),
        round(avg('Long), 8).alias("lng_avg")
      )

    //Join all together
    val metricsDf = metricsDf2
      .join(metricsDf0, metricsDf0("DISTRICT0") === metricsDf2("DISTRICT"))
      .join(metricsDf1, metricsDf1("DISTRICT1") === metricsDf2("DISTRICT"))
      .drop("DISTRICT1", "DISTRICT0")

    // Save output dataset as a single Parquet file
    metricsDf.repartition(1).write.mode("overwrite").parquet(outputParquetPath)
    spark.stop()
  }
}
