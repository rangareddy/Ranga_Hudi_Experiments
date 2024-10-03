package com.ranga

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object SparkHudiApp extends App {

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

  val input_data = Seq(
    Row(1L, "hello", 42, BigDecimal(123331.15), 1695159649087L),
    Row(2L, "world", 13, BigDecimal(223331.72), 1695091554788L),
    Row(3L, "spark", 7, BigDecimal(323331.60), 1695115999911L)
  )

  val input_schema = StructType(Seq(
    StructField("id", LongType),
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("salary", DecimalType(13, 4)),
    StructField("ts", LongType),
  ))

  val input_df = spark.createDataFrame(spark.sparkContext.parallelize(input_data), input_schema)
  input_df.show(truncate = false)

  val tableName = name
  val basePath = f"file:///tmp/$tableName"

  val hoodieConf = scala.collection.mutable.Map[String, String]()
  hoodieConf.put("hoodie.datasource.write.recordkey.field", "id")
  hoodieConf.put("hoodie.table.precombine.field", "name")
  hoodieConf.put("hoodie.datasource.write.operation", "upsert")
  hoodieConf.put("hoodie.table.name", tableName)

  input_df.write.format("hudi").
    options(hoodieConf).
    mode("overwrite").
    save(basePath)

  spark.read.format("hudi").load(basePath).show(false)

  spark.close()
}