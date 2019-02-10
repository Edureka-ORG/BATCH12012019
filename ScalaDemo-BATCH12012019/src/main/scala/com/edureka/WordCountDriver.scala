package com.edureka

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCountDriver {
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf();
    
    sparkConf.setAppName("Helloworld-Spark");
    sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf);
    
    val input = List("DEER RIVER RIVER","CAT BEAR RIVER","RIVER BEAR RIVER")
    
    val inputRDD = sc.parallelize(input, 4);
    
//    val tokens = inputRDD.flatMap((e:String) => e.split(" "));RDD[Array("DEER,"RIVER","RIVER"), Array("CAT","BEAR","RIVER")
    
    val tokens = inputRDD.flatMap(iLine => MyUtil.split(iLine, " "));//RDD("DEER","RIVER","RIVER","CAT","CAT")
    
    val wordOne = tokens.map(word=>(word,1));
    
    val wc=wordOne.reduceByKey((acc:Int,c:Int)=>acc+c)
    
    val output = wc.collect
    
    output.foreach(t=>println(t._1 +"-->"+t._2))
    
  }
}