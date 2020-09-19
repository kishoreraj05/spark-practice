package com.spark.practice.instructor.sparkbatch

import com.spark.practice.instructor.sparkbatch.quesans.Solutions
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkBatchCsvReader {

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
    val df = spark.read
      .option("header", true)
      .option("enforceSchema", false)
      .csv("./resources/datasets/india_population_by_area_age.csv")

    // print 10 sample records in console
    df.show(10)

    // create view in memory over data frame
    df.createOrReplaceTempView("sampleData")
    // run sql query on the view
    val resultDf = Solutions.solutionsOfQues1(spark, "sampleData")
    resultDf.createOrReplaceTempView("resultDF")

    val resultdf1 = spark.sql("select * from resultDF where area = 'MEGHALAYA' ")

    // write operations will write output in multiple parts file.
    // no of files depends on the no of partitions in resultDF
    // resultDf.write.json("./resources/outputs/countryWiseRecordsCount")

    // how to write output into one files, use repartition function
    writeOutputInJson(resultdf1, "male_female_ratio_for_specific_area")
  }

  def writeOutputInJson(df: DataFrame, outputName: String) : Unit = {
    df
      .repartition(1)
      .write
      .mode("overwrite")  // to overwrite the output
      .json(s"./resources/outputs/instructor/$outputName")
  }

  def writeOutputInCsv(df: DataFrame) : Unit = {
    df
      .repartition(1)
      .write
      .mode("overwrite")  // to overwrite the output
      .csv("./resources/outputs/instructor/unique_area_list_csv")
  }

  /**
   * Assignment to do
   * Q1 - Select any state and write below sql query
   * Query - Find the ratio of percentage of male and percentage of female in that state with in 25 to 45 years of age.
    */


}
