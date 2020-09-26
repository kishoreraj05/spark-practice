package com.spark.practice.students.harsha.sparkBatch

import com.spark.practice.students.harsha.sparkBatch.quescan.AssignmentSolution1
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkCsvReader {
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
    df.show(7)

    // create view in memory over data frame
    //area,area_code,age,total_persons,total_males,total_females
    df.createOrReplaceTempView("sampleData")
    val solutions = new AssignmentSolution1(spark)
    // run sql query on the view
    val resultDx = solutions.solution1(tableName = "sampleData")
    resultDx.show(numRows = 3)
   //  filter MEGHALAYA result
    resultDx.createOrReplaceTempView(viewName = "result")
    val resultdx1 = spark.sql("select * from result where area = 'MEGHALAYA' ")
    resultdx1.show(numRows = 1)
    //writeOutputInJson(resultdx1, "output1ForRatio")      // not working properly
    //writeOutputInCsv(resultdx1, outputName = "output1ForRatioIncsv")      // not working prperly

    //Q2
    val resultSol2 = solutions.solution2(tableName = "sampleData")
    resultSol2.show(numRows = 2)
    //Q3 Find the list of states  where urban area  female population greater than rural area
    val dfForSolution3 = solutions.solution3n4(tableName = "sampleData")
    dfForSolution3.createOrReplaceTempView(viewName = "result3")
    //dfForSolution3.show(numRows = 3)
    val resultSol3 = spark.sql(sqlText = "Select *from result3 where UrbanFemales > RuralFemales")
    resultSol3.show(numRows = 3)
    //Q4 Find the list of states  where rural area female population greater urban area female population
    val resultSol4 = spark.sql(sqlText = "Select * from result3 where RuralFemales > UrbanFemales")
    resultSol4.show(numRows = 4)
  }

  def writeOutputInJson(df: DataFrame, outputName: String) : Unit = {
    df
      .repartition(1)
      .write
      .mode("overwrite")  // to overwrite the output
      .json(s"./resources/outputs/harsha/$outputName")
  }

  def writeOutputInCsv(df: DataFrame, outputName: String) : Unit = {
    df
      .repartition(1)
      .write
      .mode("overwrite")  // to overwrite the output
      .csv(s"./resources/outputs/harsha/$outputName")
  }



}
