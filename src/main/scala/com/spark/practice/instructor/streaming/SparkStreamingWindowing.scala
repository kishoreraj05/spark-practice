package com.spark.practice.instructor.streaming

import com.spark.practice.instructor.sparkbatch.quesans.AssigmentSolutions
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object SparkStreamingWindowing {

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
      .withColumn("processingTime", functions.current_timestamp())

    import spark.implicits._
    val rd = df
      .withWatermark("processingTime", "20 seconds")
      .groupBy(functions.window($"processingTime", "15 seconds"),
        $"area").count()
    val query = ouputJson(rd)
    query.awaitTermination()

  }

  private def ouputJson(resultDf: DataFrame) = {
    resultDf.repartition(1)
      .writeStream
      .outputMode("append")
      .format("json")
      .option("checkpointLocation", "D:\\learning\\spark\\streaming\\checkpoint\\SparkStreamingWindowing\\") // to store checkpoint
      .option("path", "./resources/outputs/instructor/streaming-output/aggregate_result/") // output data location
      .start()
  }

}
