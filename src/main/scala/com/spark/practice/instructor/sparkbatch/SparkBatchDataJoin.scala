package com.spark.practice.instructor.sparkbatch

import com.spark.practice.instructor.sparkbatch.quesans.AssigmentSolutions
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkBatchDataJoin {

  def main(args: Array[String]): Unit = {
    // create spark config object
    val sparkConf:SparkConf = new SparkConf()
    sparkConf.setMaster("local[1]")

    // create spark session object
    val spark = SparkSession.builder()
      .appName("SparkBatchApplication")
      .config(sparkConf)
      .getOrCreate()

    val solutions = new AssigmentSolutions(spark)

    // read local file
    val df = loadData(spark, "./resources/datasets/india_population_by_area_age.csv")
    val lookupDf = loadData(spark, "./resources/datasets/india_lat_lon.csv")
   /* lookupDf.show(10)
    val joinedDf = df.join(lookupDf,
      df("area") === lookupDf("area1"), "left_outer")

    */
    df.createOrReplaceTempView("sourceDf")
    lookupDf.createOrReplaceTempView("lookupDf")
    val joinedDf = spark.sql(
      """
        |select * from sourceDf JOIN lookupDf ON sourceDf.area == lookupDf.area1
        |""".stripMargin)
    writeOutputInJson(joinedDf, "india_states_population_with_lat_lon")

  }

  def loadData(sparkSession: SparkSession, dataLoc: String): DataFrame = {
    sparkSession.read
      .option("header", true)
      .option("enforceSchema", false)
      .csv(dataLoc)
  }

  def writeOutputInJsonWithPartitionCol(df: DataFrame, outputName: String, partitionColName: String) : Unit = {
    df
      .repartition(1)
      .write
      .mode("overwrite")  // to overwrite the output
      .partitionBy(partitionColName)
      .json(s"./resources/outputs/instructor/$outputName")
  }

  def writeOutputInJson(df: DataFrame, outputName: String) : Unit = {
    df
      .repartition(1)
      .write
      .mode("overwrite")  // to overwrite the output
      .json(s"./resources/outputs/instructor/$outputName")
  }

  def writeOutputInCsv(df: DataFrame, outputName: String) : Unit = {
    df
      .repartition(1)
      .write
      .mode("overwrite")  // to overwrite the output
      .csv(s"./resources/outputs/instructor/$outputName")
  }

  /**
   * Assignment to do
   * Q1 - Select any state and write below sql query
   * Query - Find the ratio of percentage of male and percentage of female in that state with in 25 to 45 years of age.
    */


}
