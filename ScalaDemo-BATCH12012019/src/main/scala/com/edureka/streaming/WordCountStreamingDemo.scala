package com.edureka.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

object WordCountStreamingDemo {
  
  def main(args: Array[String]): Unit = {
    
     val sparkConf = new SparkConf()
    .setAppName("WordCountStreamingDemo")
    .set("spark.ui.enabled", "true")
    .setMaster("local[*]");
    
    val sc = new SparkContext(sparkConf);
    
    val ssc = new StreamingContext(sc,Seconds(5));
    
    val lineData = ssc.socketTextStream("localhost", 9999,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lineData.flatMap(iLine => iLine.split(" "));
    val wordCount  = words.map(w => (w,1)).reduceByKey(_+_);
    wordCount.print
    
    ssc.start();
    ssc.awaitTermination();
  }
  
}