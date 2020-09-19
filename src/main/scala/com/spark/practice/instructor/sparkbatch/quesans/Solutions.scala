package com.spark.practice.instructor.sparkbatch.quesans

import org.apache.spark.sql.{DataFrame, SparkSession}

object Solutions {

  /**
   * Q1 - Select any state and write below sql query
   * Query - Find the ratio of percentage of male and percentage of female
   * in that state with in 25 to 45 years of age.
   *
   * @param df
   * @return
   */
  def solutionsOfQues1(spark: SparkSession,
                       tableName: String): DataFrame = {

    spark.sql(
      """
        | SELECT area, (sum_females/sum_total)*100 as female_perc,
        | (sum_males/sum_total)*100 as male_perc
        | FROM ( SELECT area, sum(total_persons) as sum_total,
        |   sum(total_females) as sum_females,
        |   sum(total_males) as sum_males
        | FROM sampleData
        | WHERE age >= 25 AND age <= 45
        | GROUP BY area )
        |""".stripMargin)
  }
}
