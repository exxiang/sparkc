package org.apache.sparkc.rdd

import org.apache.sparkc.{Aggregator, Partitioner}
import org.apache.sparkc.serializer.Serializer

import scala.reflect.ClassTag

class PairRDDFunctions[K, V](self: RDD[(K, V)])
                            (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) {
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
  }

  def combineByKeyWithClassTag[C](
                                   createCombiner: V => C,
                                   mergeValue: (C, V) => C,
                                   mergeCombiners: (C, C) => C,
                                   partitioner: Partitioner,
                                   mapSideCombine: Boolean = true,
                                   serializer: Serializer = null)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0

    val aggregator = new Aggregator[K, V, C](
      createCombiner,
      mergeValue,
      mergeCombiners)
    new ShuffledRDD[K, V, C](self, partitioner)
      .setSerializer(serializer)
      .setAggregator(aggregator)
      .setMapSideCombine(mapSideCombine)
  }
}
