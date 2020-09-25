package com.spark.practice.students.harsha.sparkBatch

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkBatchCsvRajasthan {
  /**
   * Assignment to do
   * Q1 - Select any state and write below sql query
   * Query - Find the ratio of percentage of male and percentage of female in that state with in 25 to 45 years of age.
   */

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
    //area,area_code,age,total_persons,total_males,total_females
    df.createOrReplaceTempView("sampleData")
    val resultdf1 = RatioOfGender.solutionsOfQues1(spark, tableName = "SampleData")
    resultdf1.createOrReplaceTempView(viewName = "resultDF")
    val resultDF = spark.sql(sqlText = "Select * from resultdF where area = 'MEGHALAYA' ")
    // run sql query on the view
    val resultDf = spark.sql(
      """
        |SELECT area, (Males/total)*100 as MalePercentage
        |FROM (
        |SELECT SUM(total_persons) as total , SUM(total_males) as Males, SUM(total_females) as females
        |FROM sampleData
        |where area = 'RAJASTHAN'
        |AND age > 24 AND age < 46)
        |""".stripMargin)

    // show top 20 grouped by results in console
    resultDf.show()

    // write operations will write output in multiple parts file.
    // no of files depends on the no of partitions in resultDF
    // resultDf.write.json("./resources/outputs/harsha/countryWiseRecordsCount")

    // how to write output into one files, use repartition function
   // writeOutputInJson(resultDf)
    writeOutputInCsv(resultDf, outputName= "male_female")
  }

  def writeOutputInJson(df: DataFrame, outputName : String) : Unit = {
    df
      .repartition(1)
      .write
      .mode("overwrite")  // to overwrite the output
      .json("./resources/outputs/harsha/unique_area_list")
  }

  def writeOutputInCsv(df: DataFrame) : Unit = {
    df
      .repartition(1)
      .write
      .mode("overwrite")  // to overwrite the output
      .csv("./resources/outputs/harsha/unique_area_list_csv")
  }



}
