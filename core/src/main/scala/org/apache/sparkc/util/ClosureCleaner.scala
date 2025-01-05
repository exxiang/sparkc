package org.apache.sparkc.util

private[spark] object ClosureCleaner {
  def clean(
             closure: AnyRef,
             checkSerializable: Boolean = true,
             cleanTransitively: Boolean = true): Unit = {
    clean(closure, checkSerializable, cleanTransitively, Map.empty)
  }

  private def clean(
                     func: AnyRef,
                     checkSerializable: Boolean,
                     cleanTransitively: Boolean,
                     accessedFields: Map[Class[_], Set[String]]): Unit = {

  }
}
