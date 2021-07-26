package com.bigdata.spark.sparksql

import org.apache.spark.sql._

object testing {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = "C:\\bigdata\\datasets\\world_bank.json"
    val df = spark.read.format("json").load(data)
    df.show(9)
    df.printSchema()
    spark.stop()
  }
}
