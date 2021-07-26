package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object complexJson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexJson").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val data = "C:\\bigdata\\datasets\\world_bank.json"
    val df = spark.read.format("json").load(data)
    //df.show(9)
    //df.printSchema()
    //withColumn used to create new column, if column exists update existing column.
    //struct datatype ... use parentCol.ChildColumn
    /*
    if you have  Array(struct ... at that time use explode ... it remove array
    theme_namecode: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- code: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     */
    val res = df.withColumn("theme1name", $"theme1.Name")
      .withColumn("theme1percent", $"theme1.Percent").drop($"theme1")
      .withColumn("theme_namecode", explode($"theme_namecode"))
      .withColumn("sector_namecode", explode($"sector_namecode"))
      .withColumn("projectdocs", explode($"projectdocs"))
      .withColumn("sector", explode($"sector"))
      .withColumn("mjtheme_namecode", explode($"mjtheme_namecode"))
      .withColumn("mjtheme", explode($"mjtheme"))
      .withColumn("mjsector_namecode", explode($"mjsector_namecode"))
      .withColumn("majorsector_percent", explode($"majorsector_percent"))
      .withColumn("theme_namecodecode", $"theme_namecode.code")
      .withColumn("theme_namecodename", $"theme_namecode.name")
      .withColumn("sector_namecodecode", $"sector_namecode.code")
      .withColumn("sector_namecodename", $"sector_namecode.name")
      .withColumn("sector4Name", $"sector4.Name")
      .withColumn("sector3Name", $"sector3.Name")
      .withColumn("sector2Name", $"sector2.Name")
      .withColumn("sector1Name", $"sector1.Name")
      .withColumn("sector4Percent", $"sector4.Percent")
      .withColumn("sector3Percent", $"sector3.Percent")
      .withColumn("sector2Percent", $"sector2.Percent")
      .withColumn("sector1Percent", $"sector1.Percent")
      .withColumn("sectorname", $"sector.Name")
      .withColumn("theme_namecodename", $"theme_namecode.Name")
      .withColumn("theme_namecodecode", $"theme_namecode.code")
      .withColumn("project_abstract", $"project_abstract.cdata")
      .withColumn("docdate", $"projectdocs.DocDate")
      .withColumn("docurl", $"projectdocs.DocURL")
      .withColumn("idoid", $"_id.$$oid")
      .withColumn("majorsector_percentname", $"majorsector_percent.Name")
      .withColumn("majorsector_percentpercent", $"majorsector_percent.Percent")
      .withColumn("mjsector_namecodecode", $"mjsector_namecode.code")
      .withColumn("mjsector_namecodepercent", $"mjsector_namecode.name")
      .withColumn("mjtheme_namecodename", $"mjtheme_namecode.name")
      .withColumn("mjtheme_namecodecode", $"mjtheme_namecode.code")
      .drop("theme_namecode", "_id", "majorsector_percent", "mjsector_namecode", "mjtheme_namecode", "sector_namecode", "sector4", "sector3", "sector3", "sector2", "sector1", "sector", "projectdocs", "project_abstract")
    //res.createOrReplaceTempView("table")

    //val host = "jdbc:mysql://mysqldb.cto8cz9itkcp.ap-south-1.rds.amazonaws.com:3306/mydb"
    //res.write.format("jdbc").option("url",host).option("user","mysqluser").option("password","mysqlpass").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","sparkjsom").save()
      val localhostname = "jdbc:mysql://localhost:3306/avd_emp"
      res.write.format("jdbc").option("url",localhostname).option("user","root").option("password","root").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","sparkjsom").save()
    //.withColumn("newcol", lit(1))
    //  .withColumn("rownum", monotonically_increasing_id())
    //  .withColumn("supplementprojectflg", when($"supplementprojectflg"==="N","No").otherwise("Yes"))
    res.printSchema()
    res.show(9, false)
    spark.stop()
  }
}
