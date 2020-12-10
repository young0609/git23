package com.structstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
 * create by young
 * date:20/12/6
 * desc:
 */
object Demo02TriggerAppend {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("TriggerAppend")
      .getOrCreate()


    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9998")
      .load()


    val query = lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(3000)) // 每3秒输出一次
      .start()

    query.awaitTermination()
  }
}
