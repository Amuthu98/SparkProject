package fr.esgi.training.spark.streaming

import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming
import org.apache.spark.{SparkConf, SparkContext, streaming}
import org.apache.spark.streaming.Seconds

object StreamingReadS3DroneData {
  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder()
    // val spark = SparkUtils.spark()
    val spark = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val spark_context = new SparkContext(spark)
    val hadoopConf = spark_context.hadoopConfiguration

    //val sc = spark.sparkContext.hadoopConfiguration
    //import spark.implicits._

    //hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    // AWS CREDENTIALS
    hadoopConf.set("fs.s3a.access.key", "")
    hadoopConf.set("fs.s3a.secret.key", "")
    // S3 Endpoint
    hadoopConf.set("fs.s3a.endpoint", "s3-eu-west-3.amazonaws.com")


    // Streaming context
    val spark_streaming_context = new org.apache.spark.streaming.StreamingContext(spark_context,Seconds(60))

    val lines = spark_streaming_context.textFileStream("s3a://projetspark4iabd2ana/raw_data")
    lines.print()

    spark_streaming_context.start()
    spark_streaming_context.awaitTermination()


  }

}
