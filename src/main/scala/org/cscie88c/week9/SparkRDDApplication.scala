package org.cscie88c.week9

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.{LazyLogging}
import org.cscie88c.config.{ConfigUtils}
import org.cscie88c.utils.{SparkUtils}
import org.apache.spark.rdd.{RDD}
import pureconfig.generic.auto._

// write case class below
// case class SparkRDDConfig()

// run with: sbt "runMain org.cscie88c.week9.SparkRDDApplication"
object SparkRDDApplication {

  // application entry point
  def main(args: Array[String]): Unit = {
    implicit val conf:SparkRDDConfig = readConfig()                         // 1. read configuration
    val spark = SparkUtils.sparkSession(conf.name, conf.masterUrl)          // 2. initialize spark session
    val rddLines = loadData(spark)                                          // 3.load data
    val rddTransactions = lineToTransactions(rddLines)                      // 4. convert lines to transaction objects
    val yearlyTransactionsRDD = transactionsAmountsByYear(rddTransactions)  // 5. transform data
    printTransactionsAmountsByYear(yearlyTransactionsRDD)                   // 6. print results
    spark.stop()                                                            // 7. stop spark cluster
  }

  def readConfig(): SparkRDDConfig = ???

  def loadData(spark: SparkSession)(implicit conf: SparkRDDConfig): RDD[String] = ???

  def lineToTransactions(lines: RDD[String]): RDD[CustomerTransaction] = ???

  def transactionsAmountsByYear(transactions: RDD[CustomerTransaction]): RDD[(String, Double)] = ???

  def printTransactionsAmountsByYear(transactions: RDD[(String, Double)]): Unit = ???
}