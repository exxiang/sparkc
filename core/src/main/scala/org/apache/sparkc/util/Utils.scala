package org.apache.sparkc.util

import java.util.Properties
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.Map

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

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def getSystemProperties: Map[String, String] = {
    System.getProperties.stringPropertyNames().asScala
      .map(key => (key, System.getProperty(key))).toMap
  }
}
