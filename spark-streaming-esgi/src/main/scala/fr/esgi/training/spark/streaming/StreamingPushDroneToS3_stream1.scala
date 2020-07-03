package fr.esgi.training.spark.streaming

import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object StreamingPushDroneToS3_stream1 {
  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder()
    val spark = SparkUtils.spark()
    import spark.implicits._

    // AWS CREDENTIALS
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    var df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9997)
      .option("includeTimestamp", false) //pratique pour créer directement une colonne de timestamps utilisable !
      .load()
      //.selectExpr("CAST(value AS STRING)", "timestamp")

    //val stock_df = df
    df.printSchema
    // basic
    //var new_df = df
    //new_df = new_df.withColumn("stream_number", lit("1"))

    // complete data transformation for df 1
    var new_df = df
    new_df = new_df.withColumn("latitude", split($"value", ";").getItem(0))
    new_df = new_df.withColumn("longitude", split($"value", ";").getItem(1))
    new_df = new_df.withColumn("height", split($"value", ";").getItem(2))
    new_df = new_df.withColumn("timestamp", split($"value", ";").getItem(3))
    new_df = new_df.withColumn("id_drone", split($"value", ";").getItem(4))
    new_df = new_df.withColumn("status_drone", split($"value", ";").getItem(5))
    df = df.drop("value")
    //split data into columns

    /*df = df.withColumn("UUID_iot", split($"value", ";").getItem(0))
    df = df.withColumn("temp", split($"value", ";").getItem(1).cast("Float"))
    // get the timestamp from the file, we could also the use the generated timestamp available
    df = df.withColumn("time", date_format(split($"value", ";").getItem(2).cast("Int").cast(DataTypes.TimestampType), "yyyy-MM-dd hh:mm:ss"))
    df = df.withColumn("sensors_id", split($"value", ";").getItem(3))
    df = df.drop("value")*/

    //df.printSchema
    // OLD METHOD
    //split data into columns
    /*df.withColumn("value_splited",  split($"value", ";"))
    df.withColumn("tmp", col("value_splited")).select(
      $"tmp".getItem(0).as("UUID_iot").cast(DataTypes.StringType),
      $"tmp".getItem(1).as("temp").cast("Float"),
      $"tmp".getItem(2).as("time").cast("Int"),
      $"tmp".getItem(3).as("sensors").cast(DataTypes.StringType)
    )*/

    // Window for question 1
    //df = df.groupBy($"UUID_iot", window($"time", "60 seconds").as("window")).avg("temp")

    // Window for question 2
    //df = df.groupBy($"UUID_iot", window($"time", "60 seconds", "30 seconds").as("window")).avg("temp")

    // Question 4 : Others ideas
    /*
    // idea 1 : max temperature in each hour, log message each 15 minutes (slide) and watemarking of one hour to handle late data
    df = df.withWatermark("time", "60 minutes")
    .groupBy($"UUID_iot", window($"time", "60 minutes", "15 minutes")
      .as("window")).max("temp")

    // idea 2 : average temperature of each sensor every 10 minutes, message each 5 minutes
    df = df.groupBy($"sensors_id", window($"time", "10 minutes", "5 minutes")
      .as("window")).avg("temp")

    // idea 3, get number of times where the temperature was over 24.5 °C in a quarter (15) min
    df = df.filter($"temp" > 26.5)
    df = df.groupBy($"UUID_iot", window($"time", "15 minutes")
      .as("window")).agg(count($"time").as("count"))

    */

    val query = new_df.writeStream
      .outputMode("append")
      .format("json")

      //.option("path", "s3:eu-west-3:973044993483:accesspoint/projetspark4iabd2ana")
      //.trigger(Trigger.ProcessingTime("10 seconds")) ==> used before windows
      .start()
      .awaitTermination()
  }

}
