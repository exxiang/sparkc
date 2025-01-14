package org.apache.sparkc.util

import java.io.IOException
import java.util.Properties
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.Map
import scala.util.control.NonFatal

private[sparkc] object Utils {
  def getCurrentUserName(): String = {
    Option(System.getenv("SPARK_USER"))
      .getOrElse("exx")
  }

  def cloneProperties(props: Properties): Properties = {
    if (props == null) {
      return props
    }
    val resultProps = new Properties()
    props.forEach((k, v) => resultProps.put(k, v))
    resultProps
  }

  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        throw e
      case NonFatal(e) =>
        throw new IOException(e)
    }
  }
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def getSystemProperties: Map[String, String] = {
    System.getProperties.stringPropertyNames().asScala
      .map(key => (key, System.getProperty(key))).toMap
  }
}
