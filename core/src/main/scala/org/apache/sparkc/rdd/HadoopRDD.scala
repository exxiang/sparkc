/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sparkc.rdd

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hadoop.mapreduce.TaskType
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.util.ReflectionUtils
import org.apache.sparkc.SparkContext

import java.io.{FileNotFoundException, IOException}
import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import scala.reflect.ClassTag

/**
 * A Spark split class that wraps around a Hadoop InputSplit.
 */
private[spark] class HadoopPartition(rddId: Int,  val index: Int, s: InputSplit) {

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

/**
 * :: DeveloperApi ::
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the older MapReduce API (`org.apache.hadoop.mapred`).
 *
 * @param sc The SparkContext to associate the RDD with.
 * @param broadcastedConf A general Hadoop Configuration, or a subclass of it. If the enclosed
 *   variable references an instance of JobConf, then that JobConf will be used for the Hadoop job.
 *   Otherwise, a new JobConf will be created on each executor using the enclosed Configuration.
 * @param initLocalJobConfFuncOpt Optional closure used to initialize any JobConf that HadoopRDD
 *     creates.
 * @param inputFormatClass Storage format of the data to be read.
 * @param keyClass Class of the key associated with the inputFormatClass.
 * @param valueClass Class of the value associated with the inputFormatClass.
 * @param minPartitions Minimum number of HadoopRDD partitions (Hadoop Splits) to generate.
 *
 * @note Instantiating this class directly is not recommended, please use
 * `org.apache.spark.SparkContext.hadoopRDD()`
 */
class HadoopRDD[K, V](
    sc: SparkContext,
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int)
  extends RDD[(K, V)](sc, Nil) {

  if (initLocalJobConfFuncOpt.isDefined) {
    sparkContext.clean(initLocalJobConfFuncOpt.get)
  }

  def sparkContext: SparkContext = sc

  def this(
      sc: SparkContext,
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int) = {
    this(
      sc,
      initLocalJobConfFuncOpt = None,
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions)
  }

  // used to build JobTracker ID
  private val createTime = new Date()

  protected def getInputFormat(conf: JobConf): InputFormat[K, V] = {
    val newInputFormat = ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[K, V]]
    newInputFormat match {
      case c: Configurable => c.setConf(conf)
      case _ =>
    }
    newInputFormat
  }
}

private[spark] object HadoopRDD {
  /**
   * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
   * Therefore, we synchronize on this lock before calling new JobConf() or new Configuration().
   */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  /** Update the input bytes read metric each time this number of records has been read */
  val RECORDS_BETWEEN_BYTES_READ_METRIC_UPDATES = 256

  /** Add Hadoop configuration specific to a single partition and attempt. */
  def addLocalConfiguration(jobTrackerId: String, jobId: Int, splitId: Int, attemptId: Int,
                            conf: JobConf): Unit = {
    val jobID = new JobID(jobTrackerId, jobId)
    val taId = new TaskAttemptID(new TaskID(jobID, TaskType.MAP, splitId), attemptId)

    conf.set("mapreduce.task.id", taId.getTaskID.toString)
    conf.set("mapreduce.task.attempt.id", taId.toString)
    conf.setBoolean("mapreduce.task.ismap", true)
    conf.setInt("mapreduce.task.partition", splitId)
    conf.set("mapreduce.job.id", jobID.toString)
  }
}
