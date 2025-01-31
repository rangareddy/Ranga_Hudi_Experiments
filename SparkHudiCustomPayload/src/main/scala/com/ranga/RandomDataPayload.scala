package com.ranga

import java.io.IOException
import java.util.Properties
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}
import org.apache.avro.reflect.ReflectData
import org.apache.hudi.common.model.{BaseAvroPayload, HoodieRecordPayload}
import org.apache.hudi.common.util.{Option => HudiOption}

class RandomDataPayload(record: GenericRecord, orderingVal: Comparable[_])
  extends BaseAvroPayload(record, orderingVal)
    with HoodieRecordPayload[RandomDataPayload] {

  def this(record: HudiOption[GenericRecord]) = this(record = record.orElse(null), orderingVal = 0)

  def toAvro: RandomData => GenericRecord = RandomDataPayload.toAvro

  def fromAvro: GenericRecord => RandomData = RandomDataPayload.fromAvro

  override def preCombine(oldValue: RandomDataPayload): RandomDataPayload = {
    (oldValue.recordBytes.isEmpty, oldValue.orderingVal.asInstanceOf[Comparable[Any]].compareTo(orderingVal)) match {
      case (true, _)            => this
      case (false, c) if c > 0  => oldValue
      case (false, c) if c <= 0 => this
    }
  }

  override def preCombine(oldValue: RandomDataPayload, schema: Schema, properties: Properties): RandomDataPayload = preCombine(oldValue = oldValue)

  @throws[IOException]
  override def combineAndGetUpdateValue(valueInStorage: IndexedRecord, schema: Schema): HudiOption[IndexedRecord] = {
    val currentValueObject = fromAvro(valueInStorage.asInstanceOf[GenericRecord])
    val otherObject = fromAvro(this.record)

    HudiOption.of(toAvro(currentValueObject.combine(otherObject)))
  }

  @throws[IOException]
  override def combineAndGetUpdateValue(valueInStorage: IndexedRecord, schema: Schema, properties: Properties): HudiOption[IndexedRecord] =
    combineAndGetUpdateValue(valueInStorage = valueInStorage, schema = schema)

  @throws[IOException]
  override def getInsertValue(schema: Schema): HudiOption[IndexedRecord] = {
    if (recordBytes.isEmpty || isDeletedRecord) {
      HudiOption.empty[IndexedRecord]
    } else {
      HudiOption.of(record)
    }
  }

  @throws[IOException]
  override def getInsertValue(schema: Schema, properties: Properties): HudiOption[IndexedRecord] = getInsertValue(schema)
}


object RandomDataPayload {
  val RandomDataSchema: Schema = {
    ReflectData.get.getSchema(classOf[RandomData])
  }

  def toAvro(payload: RandomData): GenericRecord = {
    val record = new GenericData.Record(RandomDataSchema)
    record.put("id", payload.id)
    record.put("field1", payload.field1)
    record.put("field2", payload.field2)
    record.put("field3", payload.field3)
    record.put("field4", payload.field4)
    record.put("field5", payload.field5)
    record.put("ts", payload.ts)
    record.put("partition", payload.partition)
    record.put("fruits", payload.fruits)
    record
  }

  def fromAvro(record: GenericRecord): RandomData = {
    RandomData(
      id = record.get("id").toString,
      field1 = record.get("field1").toString,
      field2 = record.get("field2").toString,
      field3 = record.get("field3").toString,
      field4 = record.get("field4").toString.toBoolean,
      field5 = record.get("field5").toString.toLong,
      partition = record.get("partition").toString,
      ts = record.get("ts").toString.toLong,
      fruits = record.get("fruits").toString
    )
  }

}

