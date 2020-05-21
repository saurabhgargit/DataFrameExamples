package com.dsm.practice

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StructType, DateType,StringType}

object MailformedTest {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[*]").appName("Mailformed").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIA2477WNKNZ6ZYPAOZ")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")



     val custTxnSchema = new StructType()
      .add("AccNum", StringType,true)
      .add("Amount", DoubleType,true)
      .add("Date", DateType,true)
      .add("Category", StringType,true)
      .add("_corrupt_record", StringType, true)

    val custDf = sparkSession.read


      //  .option("mode", "DROPMALFORMED")

        .option("header", "true")
        .option("delimiter", "~")

      .option("dateFormat", "MM/DD/YYYY")
        .format("csv")
        .schema(custTxnSchema)
        .load("/Users/saurabh.garg/Downloads/BigDataLearning/dataframe-examples-master/src/main/resources/data/cred_txn_copy.csv")

    //  val bad = custDf.filter($"_corrupt_record".isNull)


    custDf.printSchema()


    println("Printing data")

    custDf.show(100)

    print(custDf.count())
    sparkSession.close()

    //print("bad records="+bad.count())


/*
    println("Creating dataframe from CSV file using 'SparkSession.read.csv()',")
    val financeDf = sparkSession.read
      .option("mode", "DROPMALFORMED")
      .option("header", "true")
      .option("delimiter", "~")
      .option("inferSchema", "true")
      .csv("s3n://" + Constants.S3_BUCKET + "/cred_txn_copy.csv")
      .toDF("AccNum", "Amount", "Date", "Category")

    financeDf.printSchema()
    financeDf.show(100)
    print(financeDf.count()) */
  }

}
