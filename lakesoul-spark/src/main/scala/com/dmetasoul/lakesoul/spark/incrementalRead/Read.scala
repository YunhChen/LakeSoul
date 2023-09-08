package com.dmetasoul.lakesoul.spark.incrementalRead

import com.dmetasoul.lakesoul.spark.ParametersTool
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.SparkSession

object Read {

  val TABLE_PATH = "lakesoul.tablePath"
  val START_TIME = "statTime"
  val END_TIME = "endTime"
  def main(args: Array[String]): Unit = {

    val parameter = ParametersTool.fromArgs(args)

    val tablePath = parameter.get(TABLE_PATH,"")

    val statTime = parameter.get(START_TIME,"2023-09-06 12:00:00")

    val endTime = parameter.get(END_TIME,"2023-12-31 12:00:00")

    val spark = SparkSession.builder().master("local")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtensio")
      .getOrCreate()

    val lake1 = LakeSoulTable.forPathIncremental(tablePath, "", statTime, endTime)

    lake1.toDF.createTempView("table")

    spark.sql(
      """
        |select col4,sum(col5),sum(col6),sum(col7) from test_table group by col4;
        |""".stripMargin).show(100)

    spark.sql(
      """
        |select col4,max(col5),max(col6),min(col7),count(16) from test_table
        |group by col4;
        |""".stripMargin).show(100)

    spark.sql(
      """
        |select * from test_table order by col3,id;
        |""".stripMargin).show(100)

    spark.sql(
      """
        |select * from test_table where id<100000000;
        |""".stripMargin).show(100)

    spark.sql(
      """
        |select distinct col3 from test_table order by col3;
        |""".stripMargin).show(100)

    spark.close()

  }

}
