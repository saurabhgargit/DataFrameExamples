package com.dsm.practice

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, collect_set, count, max, min, sum}

object DSLFinancialDataAnakysis {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[*]").appName("DataFrame").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    //import sparkSession.implicits._

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val finFilePath = s"s3n://${s3Config.getString("s3_bucket")}/finances-small"
    val financeDf = sparkSession.read.parquet(finFilePath)

    financeDf.printSchema()
    financeDf.show()

    val aggFinanceDf = financeDf.groupBy("AccountNumber").agg(avg("Amount").as("AverageTransaction"),
          sum("Amount").as("TotalTransaction"),
          count("Amount").as("NumberOfTransaction"),
          max("Amount").as("MaxTransaction"),
          min("Amount").as("MinTransaction"),
          collect_set("Description").as("UniqueTransactionDescriptions")
        )
    aggFinanceDf.show(truncate = false)




  }

}
