package com.edureka.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object StreamingDemo {
  def main(args: Array[String]): Unit = {
    //StreamingContext
    val sparkConf = new SparkConf()
    .setAppName("StreamingDemo")
    .setMaster("local[*]")
    .set("spark.submit.deployMode","client")
    .set("spark.ui.enabled", "true");
    
    val sc = new SparkContext(sparkConf);
    
    val ssc = new StreamingContext(sc,Seconds(5));
    
    val inputData = ssc.socketTextStream("localhost", 8888)
//    inputData.foreachRDD(r => r.foreach(println))
    
    val rddFn = (rdd:RDD[String]) => rdd.foreach(println);
    
    inputData.foreachRDD(rddFn)
    inputData.persist(StorageLevel.MEMORY_AND_DISK_2)
    sc.broadcast(inputData);
    
    
    
    
    inputData.flatMap(iLine => iLine.split(" ")).filter(f => f.length() >4).map(r => (r,1))
    .reduceByKey(_+_).foreachRDD(r=>r.foreach(r => println(s"$r._1 --> $r._2")));
    ssc.start()
    ssc.awaitTermination()
    
    
  }
}