package com.edureka

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCountDriver {
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf();
    
    sparkConf.setAppName("Helloworld-Spark");
    sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf);
  }
}