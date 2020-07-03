package fr.esgi.training.spark.ml

import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object MachineLearning_P2 {
  def main(args: Array[String]): Unit = {
    println("Programm launching ...")
    println("ALL THE ALL CODES BUT NOT APPLY THE PIPELINE")

    // créer une session
    val spark = SparkUtils.spark()

    val rawTraining = spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv("train.csv")

    val rawTest = spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv("test.csv")

    val expected = spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv("gender_submission.csv")

    println("TRAIN DATASET")
    rawTraining.show(1, false)
    println("TEST DATASET")
    rawTest.show(1, false)
    println("EXPECTED")
    expected.show(1, false)

    println("schema of train : ")
    rawTraining.printSchema()

    println("Exercise 1 : Fill the missing values")

    // fix function calcMeanAge
    def calcMeanAge(df: DataFrame, inputCol: String): Double = df
      .agg(avg(col(inputCol)))
      .head
      .getDouble(0)

    // calling calcMeanAge function to get mean age for
    val mean_age_train = calcMeanAge(rawTraining, "Age").round
    val mean_age_test = calcMeanAge(rawTest, "Age").round

    println("mean_age_train = " + mean_age_train)
    println("mean_age_test = " + mean_age_test)

    // fix function fillMissingAge
    def fillMissingAge(df: DataFrame, inputCol: String, outputCol: String, replacementValue:
    Double): DataFrame = {
      val ageOrMeanAge: (Any) => Double = age => age match {
        case age: Double => age
        case _ => replacementValue
      }
      val udfAgeOrMeanAge = udf(ageOrMeanAge)
      df.withColumn(outputCol, udfAgeOrMeanAge(col(inputCol)))
    }

    // calling fillMissingAge to replace empty values by mean age for train and test datasets
    val rawTraining_with_mean_age = fillMissingAge(rawTraining, "Age", "Age_cleaned", mean_age_train)
    val rawTest_with_mean_age = fillMissingAge(rawTest, "Age", "Age_cleaned", mean_age_test)
    println("NEW TRAIN DATASET FILLING MISSING AGE")
    rawTraining_with_mean_age.show(20, false)
    println("NEW TEST DATASET FILLING MISSING AGE")
    rawTest_with_mean_age.show(20, false)

    // convert Survived command into numeric in a column named label
    val surviveLabelIndexor = new StringIndexer()
      .setInputCol("Survived")
      .setOutputCol("label")
      .fit(rawTraining_with_mean_age)
    val rawTraining_with_survive_label = surviveLabelIndexor
      .transform(rawTraining_with_mean_age)

    print("NEW TRAIN DATASET WITH SURVIVE LABEL CLEANED")
    rawTraining_with_survive_label.show(10, false)

    // prepare a StringIndexer to convert sex column, not apply yet
    val sexLabelIndexor = new StringIndexer()
      .setInputCol("Sex")
      .setOutputCol("Sex_indexed")

    // prepare pipeline
    println("Exercise 2, prepare the pipeline\n")

    //
    val vectorAssembler = new VectorAssembler()
      .setInputCols(
        Array("Pclass", "Sex_indexed", "Age_cleaned")
      )
      .setOutputCol("features")
      //.transform(rawTraining_with_survive_label)

    val randomForest = new RandomForestClassifier()

    val pipeline = new Pipeline()
      .setStages(
        Array(
          sexLabelIndexor,
          vectorAssembler,
          randomForest
        )
      )

    println("Exercise : Prepare the parameter grid for the grid search")
    // create paramGrid
    val my_grid = new ParamGridBuilder()
      .addGrid(randomForest.maxDepth,
        Array(2, 5, 10))
      .addGrid(randomForest.numTrees,
        Array(15, 30, 50))
      .addGrid(randomForest.maxBins,
        Array(16, 32, 64))
      .build()

    val evaluator = new MulticlassClassificationEvaluator()

    val crossValidator = new CrossValidator()
      .setNumFolds(3)
      .setEstimator(pipeline)
      .setEstimatorParamMaps(my_grid)
      .setEvaluator(evaluator)

    // Appliquer le cross validator
    val model = crossValidator.fit(rawTraining_with_survive_label)

    println(model.toString())

    val predicted_dataframe = model.transform(rawTest_with_mean_age)

    predicted_dataframe.show()

    // Join predictions and real results
    val join_final_predictions = predicted_dataframe.join(expected, predicted_dataframe("PassengerId") === expected("PassengerId"))
      .drop(predicted_dataframe("PassengerId"))
      .select("PassengerId","Prediction", "Survived")
      .groupBy("Survived", "Prediction")
      .count()
    //print("ARTICLES LES MOINS VENDUS: ")

    // comparatif prédiction finales (predictions) et vrai résultats (Survived)
    join_final_predictions.show()
    println("\n BEST SCORE WITH 21 BAD PREDICTIONS AND 397 GOOD PREDCITIONS")

    println("Best result with cross validation, because I have tried several models with different parameters ==> it increased the chance to have a good prediction")







  }




}
