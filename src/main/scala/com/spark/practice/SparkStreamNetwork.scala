package com.spark.practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamWriter

object SparkStreamNetwork {
  def main(args: Array[String]): Unit = {
    val sparkConfg = new SparkConf()
    sparkConfg.setMaster("local[2]")
    val spark = SparkSession
      .builder
      .config(sparkConfg)
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream.foreach(new DataStreamWriter[Row]()e)
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
