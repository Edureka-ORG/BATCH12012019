package com.edureka

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestTransformations {
  
  def main(args: Array[String]): Unit = {
    
    //SparkConf and SparkContext
    
    //Create SparkConf
    import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
    val sparkConf = new SparkConf();
    
    sparkConf.setAppName("TestTransformations.getClass.getName");
    sparkConf.setMaster("local[1]");
    
    //Create SparkContext
    
    val sc  = new SparkContext(sparkConf); //Environment Ready
    
    val data = List("dog","salman","salman","salman","rat","elephant");
    
    val inputRDD = sc.parallelize(data, 3);
    
    
    var fn =(e:String) => e.length();
    
    var lenRDD = inputRDD.map(fn);
    
    inputRDD.map((e:String) => e.length());
    inputRDD.map(e => e.length());
    inputRDD.map(_.length());
    
    
    lenRDD.collect().foreach(println);
//    data.map(
    
//    inputRDD.glom().collect
    
    
    val filterInput = 1 to 10;
    val fiRDD  = sc.parallelize(filterInput, 3);
    
    val filteredRDD = fiRDD.filter((i:Int) => i%2 == 0)
    filteredRDD.collect();
    
    val combineData = lenRDD.zip(inputRDD);
    
    
    val gkRDD = combineData.groupByKey().collect();
    
    val adata = List("dog","salman","salman","salman","elephant","AAAAAAA");
    
    val aRDD = sc.parallelize(adata, 3);
    
    val lRDD = aRDD.keyBy(_.length());
    
    val interRDD = combineData.intersection(lRDD);
    interRDD.collect()
    
    
    
  }
}