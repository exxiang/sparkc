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

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.ReflectionUtils
import org.apache.sparkc.util.NextIterator
import org.apache.sparkc.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import java.io.{FileNotFoundException, IOException}
import java.util.Date

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

  protected var finished = false

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

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new NextIterator[(K, V)] {

      private val split = theSplit.asInstanceOf[HadoopPartition]
      private val jobConf = new JobConf()
      private var reader: RecordReader[K, V] = null
      private val inputFormat = getInputFormat(jobConf)
      reader =
        try {
          inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)
        } catch {
          case e: FileNotFoundException =>
            finished = true
            null
        }

      private val key: K = if (reader == null) null.asInstanceOf[K] else reader.createKey()
      private val value: V = if (reader == null) null.asInstanceOf[V] else reader.createValue()

      override def getNext(): (K, V) = {
        try {
          finished = !reader.next(key, value)
        } catch {
          case e: FileNotFoundException =>
            finished = true
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException => throw e
          case e: IOException =>
            finished = true
        }
        (key, value)
      }

      override def close(): Unit = {
        if (reader != null) {
          try {
            reader.close()
          } catch {
            case e: Exception =>

          } finally {
            reader = null
          }
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }
}
