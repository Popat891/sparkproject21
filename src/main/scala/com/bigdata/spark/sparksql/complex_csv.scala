package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object complex_csv {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("csv_file").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

        val  data="C:\\bigdata\\datasets\\10000Records.csv"
        val df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
        val reg = "[^a-zA-Z]"
        val cols = df.columns.map(x=>x.replaceAll(reg,""))
        val ndf = df.toDF(cols:_*) // toDF used to rename all columns
        ndf.printSchema()
        //ndf.show(5)
        ndf.createOrReplaceTempView("tab")
        val res = spark.sql("select * from tab where email like '%gmail.com' OR email like '%yahoo.com'")
        res.show(9,false)


    spark.stop()
  }
}
