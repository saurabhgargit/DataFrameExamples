package com.dsm.practice

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession

object DemographicTest {

  def main(args: Array[String]): Unit = {
    var sparkSession = SparkSession.builder().master("local[*]").appName("Demographic Test").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Constants.ACCESS_KEY)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Constants.SECRET_ACCESS_KEY)

    println("\nConvert RDD to Dataframe using SparkSession.createDataframe(),")
    // Creating RDD of Row
    val txnFctRdd = sparkSession.sparkContext.textFile("s3n://" + Constants.S3_BUCKET + "/demographic.csv")
    //txnFctRdd.printschema


  }

}
