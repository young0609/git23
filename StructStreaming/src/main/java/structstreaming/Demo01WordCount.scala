package com.structstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Demo01WordCount {

  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("WordCount")
      .getOrCreate()

    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "jinghang01")
      .option("port", 9999)
      .load()

    // 隐式转换
    import spark.implicits._

    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

    wordCounts.show()

    val query = wordCounts
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }
}
