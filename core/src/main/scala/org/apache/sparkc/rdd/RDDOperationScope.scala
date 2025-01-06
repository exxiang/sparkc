package org.apache.sparkc.rdd

import org.apache.sparkc.SparkContext

private[sparkc] object RDDOperationScope {
  private[sparkc] def withScope[T](
                                   sc: SparkContext,
                                   allowNesting: Boolean = false)(body: => T): T = {
    val ourMethodName = "withScope"
    val callerMethodName = Thread.currentThread.getStackTrace()
      .dropWhile(_.getMethodName != ourMethodName)
      .find(_.getMethodName != ourMethodName)
      .map(_.getMethodName)
      .getOrElse {
        // Log a warning just in case, but this should almost certainly never happen
//        logWarning("No valid method name for this RDD operation scope!")
        "N/A"
      }
    withScope[T](sc, callerMethodName, allowNesting, ignoreParent = false)(body)
  }

  private[sparkc] def withScope[T](
                                   sc: SparkContext,
                                   name: String,
                                   allowNesting: Boolean,
                                   ignoreParent: Boolean)(body: => T): T = {
    // Save the old scope to restore it later
    val scopeKey = SparkContext.RDD_SCOPE_KEY
    val noOverrideKey = SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY
    val oldScopeJson = sc.getLocalProperty(scopeKey)
//    val oldScope = Option(oldScopeJson).map(RDDOperationScope.fromJson)
    val oldNoOverride = sc.getLocalProperty(noOverrideKey)
    try {
//      if (ignoreParent) {
//        // Ignore all parent settings and scopes and start afresh with our own root scope
//        sc.setLocalProperty(scopeKey, new RDDOperationScope(name).toJson)
//      } else if (sc.getLocalProperty(noOverrideKey) == null) {
//        // Otherwise, set the scope only if the higher level caller allows us to do so
//        sc.setLocalProperty(scopeKey, new RDDOperationScope(name, oldScope).toJson)
//      }
      // Optionally disallow the child body to override our scope
      if (!allowNesting) {
        sc.setLocalProperty(noOverrideKey, "true")
      }
      body
    } finally {
      // Remember to restore any state that was modified before exiting
      sc.setLocalProperty(scopeKey, oldScopeJson)
      sc.setLocalProperty(noOverrideKey, oldNoOverride)
    }
  }
}
