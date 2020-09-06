package com.spark.practice.instructor.sparkbatch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkBatch {

  def main(args: Array[String]): Unit = {
    // create spark config object
    val sparkConf:SparkConf = new SparkConf()
    sparkConf.setMaster("local[1]")

    // create spark session object
    val spark = SparkSession.builder()
      .appName("SparkBatchApplication")
      .config(sparkConf)
      .getOrCreate()

    // read local file
    val df = spark.read.json("./resources/datasets/countries_population.json")

    // print 10 sample records in console
    df.show(10)

    // create view in memory over data frame
    df.createOrReplaceTempView("sampleData")

    // run sql query on the view
    val resultDf = spark.sql(
      """
        |SELECT country, count(country) as records_count
        |FROM sampleData
        |GROUP BY country
        |""".stripMargin)

    // show top 20 grouped by results in console
    resultDf.show()

    resultDf.write.json("./resources/outputs/countryWiseRecordsCount")
  }
}
