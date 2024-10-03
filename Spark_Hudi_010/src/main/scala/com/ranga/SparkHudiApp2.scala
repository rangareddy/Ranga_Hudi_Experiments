package com.ranga

import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkHudiApp2 extends App {

  val name = this.getClass.getSimpleName.replace("$", "")
  val sparkConf = new SparkConf().setAppName(name).setIfMissing("spark.master", "local[2]")

  val spark = SparkSession.builder.appName(name).config(sparkConf)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.hadoop.spark.sql.legacy.parquet.nanosAsLong", "false")
    .config("spark.hadoop.spark.sql.parquet.binaryAsString", "false")
    .config("spark.hadoop.spark.sql.parquet.int96AsTimestamp", "true")
    .config("spark.hadoop.spark.sql.caseSensitive", "false")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    .config("spark.sql.storeAssignmentPolicy", "legacy")
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .getOrCreate()

  val dataGenerator = new DataGenerator()
  val inserts = convertToStringList(dataGenerator.generateInserts(10))
  val input_df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

  val tableName = name
  val basePath = f"file:///tmp/$tableName"

  val hoodieConf = scala.collection.mutable.Map[String, String]()
  hoodieConf.put("hoodie.datasource.write.recordkey.field", "uuid")
  hoodieConf.put("hoodie.table.precombine.field", "ts")
  hoodieConf.put("hoodie.datasource.write.partitionpath.field", "partitionpath")
  hoodieConf.put("hoodie.datasource.write.operation", "upsert")
  hoodieConf.put("hoodie.table.name", tableName)

  input_df.write.format("hudi").
    options(hoodieConf).
    mode("overwrite").
    save(basePath)

  val df = spark.read.format("hudi").load(basePath)
  df.show()

  spark.close()
}