package com.spark.practice.students.harsha.sparkBatch.quescan

import org.apache.spark.sql.{DataFrame, SparkSession}

class AssignmentSolution1(spark: SparkSession) {

  /**
   * Q1 - Select any state and write below sql query
   * Query - Find the ratio of percentage of male and percentage of female
   * in that state within 25 to 45 years of age.
   * @param df
   * @return
   */
  def solution1(tableName: String): DataFrame = {

    spark.sql(
      s"""
         |SELECT area, (sum_females/sum_total)*100 as female_perc,
         |(sum_males/sum_total)*100 as male_perc
         |FROM ( SELECT area, sum(total_persons) as sum_total,
         |sum(total_females) as sum_females,
         |sum(total_males) as sum_males
         |FROM $tableName
         |WHERE age >= 25 AND age <= 45
         |GROUP BY area )
         |""".stripMargin)
  }
  // Q2 Find the list of states  where female population greater than or equal to male population
  def solution2(tableName: String): DataFrame = {
    spark.sql(
      s"""
         |SELECT area, Females, Males
         |FROM ( SELECT area,
         |sum(total_females) as Females,
         |sum(total_males) as Males
         |FROM $tableName
         |GROUP BY area )
         |where Females >= Males
         |""".stripMargin)
  }

  /*
  Q3 Find the list of states  where urban area  female population greater than rural area
  Q4 Find the list of states  where rural area female population greater urban area female population
   */
  def solution3n4(tableName: String): DataFrame = {

    spark.sql(
      s"""
         |SELECT area, UrbanFemales, RuralFemales
         |FROM ( SELECT area,
         |sum(urban_females) as UrbanFemales,
         |sum(rural_females) as RuralFemales
         |FROM $tableName
         |GROUP BY area )
         |""".stripMargin)
  }
}
