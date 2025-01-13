package org.apache.sparkc

import org.apache.sparkc.util.Utils

import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.asScalaSetConverter

class SparkConf(loadDefaults: Boolean) {

  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  if (loadDefaults) {
    loadFromSystemProperties(false)
  }

  private[sparkc] def loadFromSystemProperties(silent: Boolean): SparkConf = {
    // Load any spark.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
      set(key, value, silent)
    }
    this
  }

  def setMaster(master: String): SparkConf = {
    set("spark.master", master)
  }

  def setAppName(name: String): SparkConf = {
    set("spark.app.name", name)
  }

  def set(key: String, value: String): SparkConf = {
    set(key, value, false)
  }

  private[sparkc] def set(key: String, value: String, silent: Boolean): SparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(key, value)
    this
  }

  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  def getOption(key: String): Option[String] = {
    Option(settings.get(key)).orElse(getDeprecatedConfig(key, settings))
  }

  private val configsWithAlternatives = Map[String, Seq[AlternateConfig]]()

  private case class AlternateConfig(
                                      key: String,
                                      version: String,
                                      translation: String => String = null)

  def getDeprecatedConfig(key: String, conf: JMap[String, String]): Option[String] = {
    configsWithAlternatives.get(key).flatMap { alts =>
      alts.collectFirst { case alt if conf.containsKey(alt.key) =>
        val value = conf.get(alt.key)
        if (alt.translation != null) alt.translation(value) else value
      }
    }
  }
}
