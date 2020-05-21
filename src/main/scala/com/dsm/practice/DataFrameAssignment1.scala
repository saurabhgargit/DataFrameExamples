package com.dsm.practice

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_date, _}


object DataFrameAssignment1 {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    import sparkSession.implicits._

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))
    val txnFilePath = s"s3n://${s3Config.getString("s3_bucket")}/cred_txn.csv"
    val custTxnDf = sparkSession.read
                    .option("header", "true")
                    .option("delimiter", "~")
                    .format("csv").load(txnFilePath)

    custTxnDf.printSchema()
    custTxnDf.show(100,truncate = false)

    val tierDf = custTxnDf
      .select($"*",
         when(to_date($"Date","MM/dd/yyyy").isNotNull,
          to_date($"Date","MM/dd/yyyy"))
         .otherwise("Unknown Format").as("FormatedDate"),

        when($"Amount" <=100, "Tier-1" )
          .when($"Amount" >100 && $"Amount"<1000, "Tier-2")
         .otherwise("Tier-3") as("Tier")
      )

      .withColumn("YEAR", year($"FormatedDate"))
      .withColumn("Quarter", quarter($"FormatedDate"))
        .withColumn("WEEK",weekofyear($"FormatedDate"))

    tierDf.show(50)
  }

}