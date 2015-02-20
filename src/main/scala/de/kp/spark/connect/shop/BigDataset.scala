package de.kp.spark.connect.shop
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
 * 
 * This file is part of the Spark-Connect project
 * (https://github.com/skrusche63/spark-connect).
 * 
 * Spark-Connect is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * Spark-Connect is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with
 * Spark-Connect. 
 * 
 * If not, see <http://www.gnu.org/licenses/>.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Buffer
/**
 * The Bigcommerce REST API does not support counting for supported resources; 
 * this implies, that we have not enough data to do partitioning and retrieve 
 * resource data per partition.
 * 
 * In other words, we cannot user Spark's RDD mechanism directly, but have to
 * collect all the data first, and partition then.
 */
class BigDataset(
  /* Reference to SparkContext */
  @transient sc:SparkContext,
  /* resource */
  resource:String,
  /* Request parameters */
  params:Map[String,String],
  /* Total number of partitions */
  numPartitions:Int) {
  
  /*
   * Prepare request parameters, i.e. in case of an identifier provided, 
   * this value is used to determine the list of a dependent resource:
   * 
   * E.g. articles have to be retrieved by provided the identifier of
   * the associated blog
   */
  private val excludes = List("id")
  private val req_params = params.filter(kv => excludes.contains(kv._1) == false)
  
  private val rid = if (params.contains("id")) params("id").toInt else -1
  private val client = createClient
  
  private val dataset = getDataset
  
  def toRDD = sc.parallelize(dataset, numPartitions)
  
  private def createClient:BigClient = {
  
    val key = params("key")
    val token = params("token")
    
    val context = params("context")
    new BigClient(key,token,context)
    
  }
 
  private def getDataset:Seq[Map[String,Any]] = {
    
    val dataset = Buffer.empty[Map[String,Any]]
      
    var page = 1
    var finished = false
    
    while (finished == false) {
      
      val records = client.getResources(resource,rid,req_params ++ Map("limit" -> "250","page" -> page.toString))
      dataset ++= records

      page += 1
      /*
       * Check whether this request has been the last request; 
       * the respective condition is given, if less than 250 
       * records are retrieved
       */
      if (records.size < 250) finished = true       
              
    }
   
    dataset 
    
  }
  
}