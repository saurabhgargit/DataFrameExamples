package com.dsm.practice

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DFJSONAssignment {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[2]").appName("Dataframe Example").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    import sparkSession.implicits._

    val employeeDf = sparkSession.read.option("multiline", "true")

        .json("/Users/saurabh.garg/Downloads/BigDataLearning/data/multiLineJSON.json")

    employeeDf.printSchema()
    employeeDf.show(false)

   // employeeDf.withColumn("EmailID",explode($"email.mobile")).alias("helloJi").show()

    //employeeDf.select(explode($"email").alias("flatt")).show(false)
  }

}


