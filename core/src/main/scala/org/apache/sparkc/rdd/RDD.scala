package org.apache.sparkc.rdd

import org.apache.sparkc.{Dependency, OneToOneDependency, SparkContext, TaskContext}

import scala.reflect.ClassTag

abstract class RDD[T: ClassTag](
     @transient private var _sc: SparkContext,
     @transient private var deps: Seq[Dependency[_]]
   ) {
  def context: SparkContext = sc

  private[sparkc] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](sc)(body)

  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context, List(new OneToOneDependency(oneParent)))

  private def sc: SparkContext = {
    if (_sc == null) {
//      throw NullPointerException
    }
    _sc
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.flatMap(cleanF))
  }

  def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.map(cleanF))
  }

  @transient var name: String = _
  def setName(_name: String): this.type = {
    name = _name
    this
  }

  def foreach(f: T => Unit): Unit = withScope {
    runJob(this)
  }

  def runJob[T, U: ClassTag](
                              rdd: RDD[T]): Array[U] = {
    val results = new Array[U](1)
    results
  }
}

object RDD {

  private[sparkc] val CHECKPOINT_ALL_MARKED_ANCESTORS =
    "spark.checkpoint.checkpointAllMarkedAncestors"

  // The following implicit functions were in SparkContext before 1.3 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, we still keep the old functions in SparkContext for backward
  // compatibility and forward to the following functions directly.

  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
                                          (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }
}
