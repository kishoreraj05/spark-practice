package com.spark.practice.instructor.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.spark.practice.instructor.sparkbatch.quesans.AssigmentSolutions

object SparkStreamingIndiaPopulation {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val solutions = new AssigmentSolutions(spark)

    val df = spark
      .readStream
      .format("json")
      .schema("area STRING, area_code STRING, age STRING, total_persons STRING, total_males STRING, total_females STRING, rural_persons STRING, rural_males STRING, rural_females STRING, urban_persons STRING, urban_males STRING, urban_females STRING")
      .load("D:\\learning\\spark\\streaming\\india-population-streaming-input-data\\") // input data location

    df.createOrReplaceTempView("indiaPopulation")

    val resultDf = solutions.solutionOfQuesTwo("indiaPopulation")

val query = ouputJson(resultDf)
    query.awaitTermination()
    
  }
  private def ouputJson(resultDf: DataFrame) = {
    resultDf.repartition(1)
      .writeStream
      .outputMode("append")
      .format("json")
      .option("checkpointLocation", "D:\\learning\\spark\\streaming\\checkpoint\\india-population-checkpoint\\") // to store checkpoint
      .option("path", "./resources/outputs/instructor/streaming-output/india-population-age_limit/") // output data location
      .start()
  }
  private def ouputCompleteConsole(resultDf: DataFrame) = {
    resultDf.repartition(1)
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", "D:\\learning\\spark\\streaming\\checkpoint\\india-population-checkpoint\\") // to store checkpoint
      .option("path", "./resources/outputs/instructor/streaming-output/india-population/") // output data location
      .start()
  }
}
