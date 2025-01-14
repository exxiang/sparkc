package org.apache.sparkc.rdd

import org.apache.sparkc.{Dependency, OneToOneDependency, Partition, SparkContext, TaskContext}

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

  protected[sparkc] def firstParent[U: ClassTag]: RDD[U] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  /** Returns the jth parent RDD: e.g. rdd.parent[T](0) is equivalent to rdd.firstParent[T] */
  protected[sparkc] def parent[U: ClassTag](j: Int): RDD[U] = {
    dependencies(j).rdd.asInstanceOf[RDD[U]]
  }

  protected def getDependencies: Seq[Dependency[_]] = deps

  @volatile private var dependencies_ : Seq[Dependency[_]] = _

  final def dependencies: Seq[Dependency[_]] = {
      dependencies_ = getDependencies
      dependencies_
  }

  def foreach(f: T => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }

  protected def clearDependencies(): Unit = {
    dependencies_ = null
  }

  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    val split = new Partition {
      /**
       * Get the partition's index within its parent RDD
       */
      override def index: Int = 0
    }
    val firstRdd = dependencies.head.rdd.asInstanceOf[RDD[U]]
    val taskContext = TaskContext.get()
    firstRdd.compute(split, TaskContext.get())
//    while (firstRdd.deps != null) {
//      val rdd = firstRdd.deps
//    }
    val results = new Array[U](1)
    results
  }

  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    computeOrReadCheckpoint(split, context)
  }

  private[sparkc] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] = {
    compute(split, context)
  }

  def compute(split: Partition, context: TaskContext): Iterator[T]
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
