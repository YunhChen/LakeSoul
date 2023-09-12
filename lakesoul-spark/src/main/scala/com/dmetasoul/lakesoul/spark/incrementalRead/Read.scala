package com.dmetasoul.lakesoul.spark.incrementalRead

import com.dmetasoul.lakesoul.spark.ParametersTool
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.lakesoul.LakeSoulOptions
import org.apache.spark.sql.streaming.Trigger

object Read {

  val TABLE_PATH = "lakesoul.tablePath"
  val START_TIME = "statTime"
  val END_TIME = "endTime"
  val CHECK_POINT_LOCATION = "checkpointLocation"
  val OUTPUT_DIR = "output.dir"
  def main(args: Array[String]): Unit = {

    val parameter = ParametersTool.fromArgs(args)

    val tablePath = parameter.get(TABLE_PATH)

    val statTime = parameter.get(START_TIME,"2023-09-06 12:00:00")

    val outputDir = parameter.get(OUTPUT_DIR)

    val checkpointLocation = parameter.get(CHECK_POINT_LOCATION)

    val spark = SparkSession.builder().master("local")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtensio")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val readStream = spark.readStream.format("lakesoul")
      .option(LakeSoulOptions.PARTITION_DESC, "")
      .option(LakeSoulOptions.READ_START_TIME, statTime)
      .option(LakeSoulOptions.TIME_ZONE, "Asia/Shanghai")
      .option(LakeSoulOptions.READ_TYPE, "incremental")
      .load(tablePath)

    readStream.createTempView("table")

    spark.sql(
      """
        |select col4,sum(col5),sum(col6),sum(col7) from table group by col4;
        |""".stripMargin)
      .writeStream
      .format("lakesoul")
      .outputMode("append")
      .format("lakesoul")
      .option("checkpointLocation", checkpointLocation+"test1")
      .trigger(Trigger.ProcessingTime(1000))
      .start(outputDir+"test1")
      .awaitTermination()

    spark.sql(
      """
        |select col4,max(col5),max(col6),min(col7),count(16) from table
        |group by col4;
        |""".stripMargin)
      .writeStream
      .format("lakesoul")
      .outputMode("append")
      .format("lakesoul")
      .option("checkpointLocation", checkpointLocation+"test2")
      .trigger(Trigger.ProcessingTime(1000))
      .start(outputDir + "test2")
      .awaitTermination()

    spark.sql(
      """
        |select * from table order by col3,id;
        |""".stripMargin).writeStream
      .format("lakesoul")
      .outputMode("append")
      .format("lakesoul")
      .option("checkpointLocation", checkpointLocation+"test3")
      .trigger(Trigger.ProcessingTime(1000))
      .start(outputDir + "test3")
      .awaitTermination()


    spark.close()

  }

}
