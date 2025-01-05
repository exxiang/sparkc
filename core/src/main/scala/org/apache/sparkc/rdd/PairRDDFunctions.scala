package org.apache.sparkc.rdd

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

  }
}
