package org.apache.sparkc

import java.util.concurrent.ConcurrentHashMap

class SparkConf {
  private val settings = new ConcurrentHashMap[String, String]()

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
}
