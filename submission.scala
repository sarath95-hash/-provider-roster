// Imports
import org.apache.spark.sql.functions._

// Load the providers and visits DataFrames
val file_location_visits = "./data/visits.csv"
val visits_df = spark.read.format("csv")
  .option("inferSchema", "false")
  .option("header", "false")
  .option("sep", ",")
  .load(file_location_visits)
  .toDF("visit_id", "provider_id", "service_date")

val file_location_providers = "./data/providers.csv"
val providers_df = spark.read.format("csv")
  .option("inferSchema", "false")
  .option("header", "true")
  .option("sep", "|")
  .load(file_location_providers)

// Combine first, middle, and last name into provider_name
val providers_df_with_name = providers_df.withColumn(
  "provider_name", concat_ws(" ", col("first_name"), col("middle_name"), col("last_name"))
).drop("first_name", "middle_name", "last_name")

// Join DataFrames on provider_id
val visits_per_provider = visits_df
  .join(providers_df_with_name, "provider_id")
  .groupBy("provider_id", "provider_specialty", "provider_name")
  .agg(count("visit_id").alias("total_visits"))

// Write output in JSON, partitioned by specialty
visits_per_provider.write
  .format("json")
  .mode("overwrite")
  .partitionBy("provider_specialty")
  .save("/FileStore/output/visits_per_provider")

// Display the DataFrame
visits_per_provider.show()

// Extract month from the service_date
val visits_with_month = visits_df.withColumn("month", date_format(to_date(col("service_date"), "yyyy-MM-dd"), "yyyy-MM"))

// Calculate total number of visits per provider per month
val visits_per_provider_per_month = visits_with_month
  .groupBy("provider_id", "month")
  .agg(count("visit_id").alias("total_visits"))

// Write output in JSON
visits_per_provider_per_month.write
  .format("json")
  .mode("overwrite")
  .save("/FileStore/output/visits_per_provider_per_month")

visits_per_provider_per_month.show()