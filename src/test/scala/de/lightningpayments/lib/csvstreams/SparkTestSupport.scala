package de.lightningpayments.lib.csvstreams

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.UUID

trait SparkTestSupport extends Serializable {

  private val appName: String = s"app_${UUID.randomUUID().toString}"
  private val master: String = "local[*]"

  private val spark: SparkSession = {
    val config = new SparkConf()
    val builder = SparkSession.builder().appName(appName).master(master).config(config)
    builder.getOrCreate()
  }

  def withSparkSession[A, T](f: SparkSession => T): T = f(spark)

}
