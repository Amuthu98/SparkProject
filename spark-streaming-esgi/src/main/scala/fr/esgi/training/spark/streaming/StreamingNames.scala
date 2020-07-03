package fr.esgi.training.spark.streaming
import org.apache.spark.sql.functions._
import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingNames {
  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder()
    val spark = SparkUtils.spark()
    import spark.implicits._

    var df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val stock_df = df
    df.printSchema

    df = df.withColumn("value_splited",  split($"value", ";"))
    df = df.withColumn("tmp", col("value_splited")).select(
      $"tmp".getItem(0).as("sexe"),
      $"tmp".getItem(1).as("preusuel"),
      $"tmp".getItem(2).as("annais"),
      $"tmp".getItem(3).as("nombre").cast(DataTypes.IntegerType)
    )

    //Q1 Nombre de naissance par sexe
    val aggregatedDf_sexe = df.select($"sexe", col("nombre"))
      .groupBy($"sexe")
      .sum("nombre")

    //Q2 TOP 20 des prénoms les plus donnés
    val aggregatedDf_prenom = df.select($"preusuel", col("nombre"))
      .groupBy($"preusuel")
      .sum("nombre")
      .orderBy(col("sum(nombre)").desc)

    //Q3 TOP 20 années avec le plus de naissances
    val aggregatedDf = df.select($"annais", col("nombre"))
      .groupBy($"annais")
      .sum("nombre")
      .orderBy(col("sum(nombre)").desc)


    //Q4 Autres données que l'on peut sortir du stream ?
    print("Q4 : Il est possible de sortir d'autres données de ce stream, comme par exemple" +
      "les prénoms féminins les plus donnés pendant les années 90, ou encore le nombre de" +
      "prénoms avec moins de 5 lettres, etc")

    aggregatedDf_sexe.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()

  }

}
