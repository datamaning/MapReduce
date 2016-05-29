package com.dataman.demo


import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable._
import scala.collection.mutable
/**
 * Created by mymac on 15/11/3.
 */
object Assignment1 {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("assignment1")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    // val words = sc.textFile("hdfs://192.168.70.141:8020/Assignment1").map(file=>file.split(",")).map(item=>{(item(0),item(1))}).flatMap(file=>{
  
  val words = sc.textFile("hdfs://192.168.70.141:8020/Assignment1").map(file=>file.split(",")).
      map(item =>{
      (item(0),item(1))
    }).flatMap(file => {
	  
// 	  var map1=mutable.Map[String,Int]()
      var map = mutable.Map[String,String]()
      val words = file._2.split(" ").iterator
      val doc = file._1
      var n:Int=0
      var map1 = mutable.Map[String,String]()
      while(words.hasNext){
        n=n+1
        val x=words.next()
        map1+=("{\""+x+"\":"-> (doc+":"+n))
       // map1+=(words.next()->n)
       
       if(map.contains("{\""+x+"\":"))
         map+=("{\""+x+"\":"-> (map("{\""+x+"\":")+","+n))
        else
         map+=("{\""+x+"\":"-> ("AAA{\""+doc+"\":[AAA"+n))
        
      }
      map
    })

     val final1= words.reduceByKey(_+" "+_).flatMap(x=>{
     var words = x._2.split("AAA").iterator
      val doc = x._1
      var n=0
     val maps=mutable.Map[String,String]()
    while(words.hasNext){
        words.next()
        n=n+1;
      }
  var words1 = x._2.split("AAA").iterator
  var x1=""
      if(n==21)
      {
        var aa:Int=0;
        while(words1.hasNext){
         x1=words1.next().trim()
         aa=aa+1
        if(maps.contains(doc))
        {
                
      if(aa%2==1&&aa!=21)
      {
      x1=maps(doc)+x1+"]},"
      maps+=(doc->(x1))
      }else if(aa%2==0)
     { x1=maps(doc)+x1
        maps+=(doc->(x1))
     }
    
    //  if(aa==1)
    //  {
    //      x1="[{\""+maps(doc)+x1+"]}"
    //   maps+=(doc->(x1))
    //  }
     if(aa==21)
     {
         x1=maps(doc)+x1+"]}"
      maps+=(doc->(x1))
     }
        }
        else{
             x1="["+x1
        maps+=(doc->(x1))
        }
        }  
      
      }
      maps
    })
    
    final1.reduceByKey(_+" "+_).map(x=>{
      x._1+x._2+"]}"
    }).collect().foreach(println)
      sc.stop() 
    
    //   words.reduceByKey(_+" "+_).map(x=>{
    //  x._1+x._2+""+"\"}" 
    // }).collect().foreach(println)
    //   sc.stop() 
  }
  
}