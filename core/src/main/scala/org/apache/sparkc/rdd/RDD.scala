package org.apache.sparkc.rdd

import org.apache.sparkc.{Dependency, SparkContext, TaskContext}

import scala.reflect.ClassTag

abstract class RDD[T: ClassTag](
     @transient private var _sc: SparkContext,
     @transient private var deps: Seq[Dependency[_]]
   ) {
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](sc)(body)

  private def sc: SparkContext = {
    if (_sc == null) {
      throw NullPointerException
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

  }
}
