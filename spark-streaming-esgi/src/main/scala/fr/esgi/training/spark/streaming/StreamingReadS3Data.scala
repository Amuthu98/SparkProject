package fr.esgi.training.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, dayofweek, month, date_format, hour, minute, round, split, when, _}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.streaming.Seconds
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StreamingReadS3Data {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    import spark.sqlContext.implicits._

    // Replace Key with your AWS account key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "")
    // Replace Key with your AWS secret key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3-eu-west-3.amazonaws.com")

    // classic log files
    val userSchema_log_files = new StructType().add("Latitude", "string")
      .add("Longitude", "string")
      .add("Height", "string")
      .add("Timestamp", "string")
      .add("Id Drone", "string")
      .add("Status", "string")

    val csv_log_DF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema_log_files)      // Specify schema of the csv files
      .csv("s3a://projetspark4iabd2ana2/raw_data/drone_log_*")    // Equivalent to format("csv").load("/path/to/directory")

    // violation log
    val userSchema_violation_files = new StructType().add("Latitude", "string")
      .add("Longitude", "string")
      .add("Height", "string")
      .add("Timestamp", "string")
      .add("Id Drone", "string")
      .add("Status", "string")
      .add("Violation Code", "string")
      .add("Image Id", "string")

    val csv_violation_DF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema_violation_files)      // Specify schema of the csv files
      .csv("s3a://projetspark4iabd2ana2/raw_data/drone_violation_*")    // Equivalent to format("csv").load("/path/to/directory")

    // violation image log
    val userSchema_violation_image_files = new StructType().add("Latitude", "string")
      .add("Timestamp", "string")
      .add("Image Id", "string")
      .add("Base 64", "string")

    val csv_violation_image_DF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema_violation_image_files)      // Specify schema of the csv files
      .csv("s3a://projetspark4iabd2ana2/raw_data/drone_image_*")    // Equivalent to format("csv").load("/path/to/directory")

    // Historical violation data
    val userSchema_historical = new StructType().add("Latitude", "string")
      .add("Longitude", "string")
      .add("Height", "string")
      .add("Timestamp", "string")
      .add("Id Drone", "string")
      .add("Status", "string")
      .add("Violation Code", "string")
      .add("Image Id", "string")

    var csv_historical_DF = spark
      .readStream
      .option("sep", ";")
      // Specify schema of the csv files
      .schema(userSchema_historical)
      // Equivalent to format("csv").load("/path/to/directory")
      .csv("s3a://projetspark4iabd2ana2/raw_historical_data_2")

    // change content of timestamp column to print datetime
    csv_historical_DF = csv_historical_DF
      .withColumn("Timestamp", from_unixtime($"Timestamp"))
      .withColumn("SparkLoadedAt", current_timestamp().as("SparkLoadedAt"))
      //.withColumn("Timestamp", date_format($"Timestamp".cast(DataTypes.TimestampType), "yyyy-MM-dd hh:mm:ss"))

    csv_historical_DF.printSchema()

    // Answer questions
    var test = current_timestamp()
    print(test)
    //Q1 Plus de violations le matin ou l'après midi ?
    print("Q1 Plus de violations le matin ou l'après midi ?")

    var csv_historical_DF_Q1 = csv_historical_DF
      .withColumn("Morning_or_afternoon",
        when(hour($"Timestamp") >= 6 && hour($"Timestamp") < 13, "morning_6-12h59")
          .when(hour($"Timestamp") >= 13 && hour($"Timestamp") < 20, "afternoon_13-19h59")
          .otherwise("another_time")
      )
      .select($"Morning_or_afternoon")//, $"SparkLoadedAt")
      //.withWatermark("SparkLoadedAt", "60 minutes")
      .groupBy($"Morning_or_afternoon")//, window($"SparkLoadedAt", "60 minutes", "30 minutes"))
      //.groupBy($"Morning_or_afternoon")
      .count()

    //Q2 Plus de violation le weeked / journées normales ?
    print("Q2 Plus de violations le weekend ou en semaine ?")

    var csv_historical_DF_Q2 = csv_historical_DF
      .withColumn("normal_day_or_weekend",
        when(dayofweek($"Timestamp") >= 2 && dayofweek($"Timestamp") < 7, "normal_day")
          .otherwise("weekend")
      )
      .select($"normal_day_or_weekend", $"SparkLoadedAt")
      .withWatermark("SparkLoadedAt", "2 minutes")
      .groupBy($"normal_day_or_weekend", $"SparkLoadedAt")//window($"SparkLoadedAt", "60 minutes", "30 minutes"))
      .count()

    csv_historical_DF_Q2 = csv_historical_DF_Q2
        .withColumn("count_avg",
          when($"normal_day_or_weekend" === "normal_day", $"count" / 5)
            .otherwise($"count" / 2)
        )

    //Q3 Nombre de violations par trimestres (janvier + fevrier + mars = 1 trimestre)
    print("Q3 Plus de violations selon les trimestres ? (janvier + fevrier + mars = 1 trimestre)")

    var csv_historical_DF_Q3 = csv_historical_DF
      .withColumn("infractions_by_quarter",
        when(month($"Timestamp") >= 1 && month($"Timestamp") < 4, "quarter_1")
          .when(month($"Timestamp") >= 4 && month($"Timestamp") < 7, "quarter_2")
          .when(month($"Timestamp") >= 7 && month($"Timestamp") < 10, "quarter_3")
          .otherwise("quarter_4")
      )
      .select($"infractions_by_quarter")
      .groupBy($"infractions_by_quarter")
      .count()

    //Q4 Quels sont les  5 codes de violation les plus récurrents ?
    print("Q4 Quels sont les codes de violation les plus récurrents ?")

    var csv_historical_DF_Q4 = csv_historical_DF
      .select($"Violation Code")
      .groupBy($"Violation Code")
      .count()
      .orderBy(col("count").desc)
      .limit(5)

    // choose the dataframe to read (between historical and drone logs), here we read them all
    val query_drone_log = csv_log_DF.writeStream
      .outputMode("append")
      .format("console")
      .start()
    val query_violation_log = csv_violation_DF.writeStream
      .outputMode("append")
      .format("console")
      .start()

    val query_image_violation_log = csv_violation_image_DF.writeStream
      .outputMode("append")
      .format("console")
      //.option("numRows", 50)
      //.trigger(Trigger.ProcessingTime("10 seconds")) //==> used before windows
      .start()

    val query_1 = csv_historical_DF_Q1.writeStream
      .outputMode("complete")
      .format("console")
      //.option("numRows", 50)
      //.trigger(Trigger.ProcessingTime("10 seconds")) //==> used before windows
      .start()
      //.awaitTermination()

    val query_2 = csv_historical_DF_Q2.select($"normal_day_or_weekend", $"count")
    .writeStream
      .format("parquet")
      /*.format("csv")
      .option("header", true)
      .option("sep", ",")
      */
      .outputMode("append")
      //.option("path", "s3a://projetspark4iabd2ana/result_data")
      //.option("checkpointLocation", "/s3-checkpointing")
      .option("checkpointLocation", "/checkpoints6")
      .option("path", "/results6")
      //.trigger(Trigger.ProcessingTime("20 seconds"))
      .start("my_parquet.parquet")

    val query_3 = csv_historical_DF_Q3.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    //.awaitTermination()

    val query_4 = csv_historical_DF_Q4.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()

  }

}
