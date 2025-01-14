package org.apache.sparkc.rdd

import org.apache.sparkc.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 *
 * @param prev the parent RDD.
 * @param f The function used to map a tuple of (TaskContext, partition index, input iterator) to
 *          an output iterator.
 * @param preservesPartitioning Whether the input function preserves the partitioner, which should
 *                              be `false` unless `prev` is a pair RDD and the input function
 *                              doesn't modify the keys.
 * @param isFromBarrier Indicates whether this RDD is transformed from an RDDBarrier, a stage
 *                      containing at least one RDDBarrier shall be turned into a barrier stage.
 * @param isOrderSensitive whether or not the function is order-sensitive. If it's order
 *                         sensitive, it may return totally different result when the input order
 *                         is changed. Mostly stateful functions are order-sensitive.
 */
private[sparkc] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
                                                                  var prev: RDD[T],
                                                                  f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
                                                                  preservesPartitioning: Boolean = false,
                                                                  isFromBarrier: Boolean = false,
                                                                  isOrderSensitive: Boolean = false)
  extends RDD[U](prev) {

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
}