package org.apache.sparkc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, TextInputFormat}
import org.apache.sparkc.rdd.{HadoopRDD, RDD, RDDOperationScope}
import org.apache.sparkc.util.{ClosureCleaner, Utils}

import java.util.Properties

class SparkContext(config: SparkConf) {
  private var _hadoopConfiguration: Configuration = _

  def defaultMinPartitions: Int = 2
  def hadoopConfiguration: Configuration = _hadoopConfiguration

  protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
    override def childValue(parent: Properties): Properties = {
      // Note: make a clone such that changes in the parent properties aren't reflected in
      // the those of the children threads, which has confusing semantics (SPARK-10563).
      Utils.cloneProperties(parent)
    }

    override protected def initialValue(): Properties = new Properties()
  }

  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](this)(body)

  private[spark] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    ClosureCleaner.clean(f, checkSerializable)
    f
  }

  def textFile(
                path: String,
                minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
//    assertNotStopped()
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }

  def hadoopFile[K, V](
                        path: String,
                        inputFormatClass: Class[_ <: InputFormat[K, V]],
                        keyClass: Class[K],
                        valueClass: Class[V],
                        minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
//    assertNotStopped()

    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    FileSystem.getLocal(hadoopConfiguration)

    // A Hadoop configuration can be about 10 KiB, which is pretty big, so broadcast it.
//    val confBroadcast = broadcast(new SerializableConfiguration(hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    new HadoopRDD(
      this,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
  }

  def setLocalProperty(key: String, value: String): Unit = {
    if (value == null) {
      localProperties.get.remove(key)
    } else {
      localProperties.get.setProperty(key, value)
    }
  }

  def getLocalProperty(key: String): String =
    Option(localProperties.get).map(_.getProperty(key)).orNull
}

object SparkContext {
  private[spark] val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private[spark] val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  private[spark] val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"
  private[spark] val SPARK_SCHEDULER_POOL = "spark.scheduler.pool"
  private[spark] val RDD_SCOPE_KEY = "spark.rdd.scope"
  private[spark] val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"
}
