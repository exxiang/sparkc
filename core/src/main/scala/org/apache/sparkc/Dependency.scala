package org.apache.sparkc

import org.apache.sparkc.rdd.RDD

abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}
