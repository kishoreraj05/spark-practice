package com.spark.practice.instructor.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkStreaming {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val df = spark
      .readStream
      .format("json")
      .schema("city STRING, tweets INT")
      .load("D:\\learning\\spark\\streaming\\data-lander")

    val cityWiseCount = df.groupBy("city").count()

val query = df.writeStream//.outputMode("complete")
  .outputMode("append")
  .format("json")
    .option("checkpointLocation", "D:\\learning\\spark\\streaming\\checkpoint\\WordCount\\")
  .option("path", "D:\\learning\\spark\\streaming\\output\\")
  .start()
    query.awaitTermination()
    
  }
}
