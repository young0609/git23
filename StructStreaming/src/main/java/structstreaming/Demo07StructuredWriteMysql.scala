package com.structstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.sql.{Connection, DriverManager}
import org.apache.spark.sql.{ForeachWriter, Row}


object Demo07StructuredWriteMysql {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .master("local")
      .appName("Test")
      .getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9998)
      .load()

    lines.createOrReplaceTempView("tmp1")

    /*
    数据格式
    laowang,10,nan,101
     */
    val lines2 = spark.sql("select split(value,',') as a from tmp1")

    lines2.createOrReplaceTempView("tmp2")

    val result = spark.sql("select a[0] as name, a[1] as age, a[2] as sex,a[3] as uuid from tmp2")

    val mysqlSink = new MysqlSink("jdbc:mysql://localhost:3306/test", "root", "root")

    val query = result.writeStream
      .outputMode("append")
      .foreach(mysqlSink)
      .start()

    query.awaitTermination()
  }
}

/*
use test;

create table people(
name varchar(20),
age varchar(20),
sex varchar(20),
uuid varchar(20)
)

 */

class MysqlSink(url: String, user: String, pwd: String) extends ForeachWriter[Row] {
  var conn: Connection = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(url, user, pwd)
    true
  }

  override def process(value: Row): Unit = {
    val p = conn.prepareStatement("replace into people(name,age,sex,uuid) values(?,?,?,?)")
    p.setString(1, value(0).toString)
    p.setString(2, value(1).toString)
    p.setString(3, value(2).toString)
    p.setString(4, value(3).toString)
    p.execute()
  }

  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}