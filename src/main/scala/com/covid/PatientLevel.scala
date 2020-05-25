package com.covid

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession

object PatientLevel {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .appName("Patient Level Data")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")


  }

}
