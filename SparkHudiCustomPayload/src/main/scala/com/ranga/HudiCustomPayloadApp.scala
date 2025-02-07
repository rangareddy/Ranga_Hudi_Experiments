package com.ranga

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession

import scala.util.Random

object HudiCustomPayloadApp extends App {

  val name = this.getClass.getSimpleName.replace("$", "")
  val sparkConf = new SparkConf().setAppName(name).setIfMissing("spark.master", "local[*]")

  val spark = SparkSession.builder.appName(name).config(sparkConf)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .getOrCreate()

  val rangeData = 10000000L
  import spark.implicits._
  val randomData = spark
    .range(1, 10 * rangeData)
    .map(f => RandomData(id = f, partition = Random.shuffle(List("One", "Two", "Three", "Four")).head, fruits = "apple"))

  val tableName = "randomDataWithFruits"

  val insertOptions: Map[String, String] = Map(
    DataSourceWriteOptions.OPERATION.key() -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
    DataSourceWriteOptions.TABLE_TYPE.key() -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
    HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key() -> "snappy",
    HoodieTableConfig.POPULATE_META_FIELDS.key() -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key() -> "true",
    HoodieIndexConfig.INDEX_TYPE.key() -> "RECORD_INDEX",
    DataSourceWriteOptions.META_SYNC_ENABLED.key() -> "false",
    "hoodie.metadata.record.index.enable" -> "true",
    "hoodie.metadata.enable" -> "true",
    "hoodie.datasource.write.hive_style_partitioning" -> "true",
    "hoodie.datasource.write.partitionpath.field" -> "partition",
    "hoodie.datasource.write.recordkey.field" -> "id",
    "hoodie.datasource.write.precombine.field" -> "ts",
    "hoodie.table.name" -> tableName,
    DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key() -> classOf[SimpleKeyGenerator].getName,
    "hoodie.write.markers.type" -> "DIRECT",
    "hoodie.embed.timeline.server" -> "false"
  )

  val basePath = f"/tmp/$tableName"
  println(s"Inserting data to the path $basePath")
  randomData.repartition(100).write.format("hudi").options(insertOptions).mode(Overwrite).save(basePath)
  println("Data Inserted successfully")

  val updateParcel = randomData.map(f => f.copy(ts = f.ts + 100, fruits = "banana")).limit(50000)

  val randomDataUpsertOptions: Map[String, String] = Map(
    "hoodie.datasource.write.precombine.field" -> "ts",
    "hoodie.datasource.write.recordkey.field" -> "id",
    "hoodie.table.name" -> "randomDataWithFruits",
    "hoodie.datasource.write.partitionpath.field" -> "partition",
    "hoodie.datasource.write.payload.class" -> classOf[RandomDataPayload].getName,
    "hoodie.upsert.shuffle.parallelism" -> "2000"
  )

  updateParcel.write.format("hudi").mode("append").options(randomDataUpsertOptions).save(basePath)
  println(f"Data Updated successfully... Input Range: ${rangeData}")
  spark.stop()
}
