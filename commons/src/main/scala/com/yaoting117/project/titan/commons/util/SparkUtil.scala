package com.yaoting117.project.titan.commons.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtil {

  def getSparkSession(
    appName: String = "app",
    master: String = "local[*]",
    confMap: Map[String, String] = Map.empty
  ): SparkSession = {
    val conf: SparkConf = new SparkConf()
    conf.setAll(confMap)
    SparkSession.builder().appName(appName).master(master).config(conf).getOrCreate()
  }

}
