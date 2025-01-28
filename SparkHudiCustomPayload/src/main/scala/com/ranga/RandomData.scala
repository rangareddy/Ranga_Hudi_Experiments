package com.ranga

import java.util.UUID

final case class RandomData(
                             id: String,
                             field1: String,
                             field2: String,
                             field3: String,
                             field4: Boolean,
                             field5: Long,
                             ts: Long,
                             partition: String,
                             fruits: String
                           ) {
  def combine(that: RandomData): RandomData = {
    val updatedFruit: String = this.fruits + s",${that.fruits}"
    RandomData(
      id = this.id,
      field1 = this.field1,
      field2 = this.field2,
      field3 = this.field3,
      field4 = this.field4,
      field5 = this.field5,
      ts = Math.max(this.ts, that.ts),
      partition = that.partition,
      fruits = updatedFruit
    )
  }
}

object RandomData {
  def apply(id: Long, partition: String, fruits: String): RandomData = {
    RandomData(
      id = id.toString,
      field1 = UUID.randomUUID().toString,
      field2 = UUID.randomUUID().toString,
      field3 = UUID.randomUUID().toString,
      field4 = true,
      field5 = 1000L,
      ts = 2880000L,
      partition = partition,
      fruits = fruits
    )
  }
}
