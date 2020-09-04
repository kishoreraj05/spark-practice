package com.spark.practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkStreamExp {

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

    //val resultDf = df.groupBy("city")
    df.writeStream.outputMode("complete")
      .format("console")
      .start()


    // df.createOrReplaceTempView("dataTable")

   // val resultDF = spark.sql("select city,sum(tweets) from dataTable group by city")
   // resultDF.show()
  }
}
