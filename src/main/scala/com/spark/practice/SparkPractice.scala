package com.spark.practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkPractice {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")

    val spark = SparkSession.builder()
      .appName("sparkPractice")
      .config(sparkConf)
      .getOrCreate()

    val df = spark
      .read
      .option("enforceSchema",false)
      .option("header", true)
      .csv("C:\\Users\\kisho\\Desktop\\up_unnao_kisan_callcenter.csv")
    df.show(2)

    df.createOrReplaceTempView("callCenterData")

    val sectorWiseCountDF = spark.sql(
      """
        |Select Category, count(Category)
        |FROM callCenterData
        |GROUP BY Category
        |""".stripMargin)

    sectorWiseCountDF.show()
  }
}
