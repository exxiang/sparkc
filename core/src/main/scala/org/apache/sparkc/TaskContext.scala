package org.apache.sparkc

import java.util.Properties

object TaskContext {
  def get(): TaskContext = taskContext.get

  /**
   * Returns the partition id of currently active TaskContext. It will return 0
   * if there is no active TaskContext for cases like local execution.
   */
  def getPartitionId(): Int = {
    val tc = taskContext.get()
    if (tc eq null) {
      0
    } else {
      tc.partitionId()
    }
  }

  private[this] val taskContext: ThreadLocal[TaskContext] = new ThreadLocal[TaskContext]

  // Note: protected[spark] instead of private[spark] to prevent the following two from
  // showing up in JavaDoc.

  /**
   * Set the thread local TaskContext. Internal to Spark.
   */
  protected[spark] def setTaskContext(tc: TaskContext): Unit = taskContext.set(tc)

  /**
   * Unset the thread local TaskContext. Internal to Spark.
   */
  protected[spark] def unset(): Unit = taskContext.remove()
}


/**
 * Contextual information about a task which can be read or mutated during
 * execution. To access the TaskContext for a running task, use:
 * {{{
 *   org.apache.spark.TaskContext.get()
 * }}}
 */
abstract class TaskContext extends Serializable {
  // Note: TaskContext must NOT define a get method. Otherwise it will prevent the Scala compiler
  // from generating a static get method (based on the companion object's get method).

  // Note: Update JavaTaskContextCompileCheck when new methods are added to this class.

  // Note: getters in this class are defined with parentheses to maintain backward compatibility.

  /**
   * Returns true if the task has completed.
   */
  def isCompleted(): Boolean

  /**
   * Returns true if the task has been killed.
   */
  def isInterrupted(): Boolean

  /**
   * The ID of the stage that this task belong to.
   */
  def stageId(): Int

  /**
   * How many times the stage that this task belongs to has been attempted. The first stage attempt
   * will be assigned stageAttemptNumber = 0, and subsequent attempts will have increasing attempt
   * numbers.
   */
  def stageAttemptNumber(): Int

  /**
   * The ID of the RDD partition that is computed by this task.
   */
  def partitionId(): Int

  /**
   * Total number of partitions in the stage that this task belongs to.
   */
  def numPartitions(): Int

  /**
   * How many times this task has been attempted.  The first task attempt will be assigned
   * attemptNumber = 0, and subsequent attempts will have increasing attempt numbers.
   */
  def attemptNumber(): Int

  /**
   * An ID that is unique to this task attempt (within the same SparkContext, no two task attempts
   * will share the same attempt ID).  This is roughly equivalent to Hadoop's TaskAttemptID.
   */
  def taskAttemptId(): Long

  /**
   * Get a local property set upstream in the driver, or null if it is missing. See also
   * `org.apache.spark.SparkContext.setLocalProperty`.
   */
  def getLocalProperty(key: String): String

  /**
   * If the task is interrupted, throws TaskKilledException with the reason for the interrupt.
   */
  private[spark] def killTaskIfInterrupted(): Unit

  /**
   * If the task is interrupted, the reason this task was killed, otherwise None.
   */
  private[spark] def getKillReason(): Option[String]

  /** Marks the task for interruption, i.e. cancellation. */
  private[spark] def markInterrupted(reason: String): Unit

  /** Marks the task as failed and triggers the failure listeners. */
  private[spark] def markTaskFailed(error: Throwable): Unit

  /** Marks the task as completed and triggers the completion listeners. */
  private[spark] def markTaskCompleted(error: Option[Throwable]): Unit

  /** Gets local properties set upstream in the driver. */
  private[spark] def getLocalProperties: Properties
}
