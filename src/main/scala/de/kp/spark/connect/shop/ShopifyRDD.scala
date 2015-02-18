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

import org.apache.spark.{Partition,SparkContext,TaskContext}
import org.apache.spark.TaskKilledException

import org.apache.spark.rdd.RDD
import org.apache.spark.util.NextIterator

import scala.collection.mutable.Buffer

class ShopifyPartition(idx:Int,val start:Int,val end:Int) extends Partition {
  override def index = idx  
}

class ShopifyRDD(
    /* Reference to SparkContext */
    @transient sc:SparkContext,
    /* resource */
    resource:String,
    /* Request parameters */
    params:Map[String,String],
    /* Total number of partitions */
    numPartitions:Int) extends RDD[Map[String,Any]](sc,Nil) {
  
  /*
   * Prepare request parameters, i.e. in case of an identifier provided, 
   * this value is used to determine the list of a dependent resource:
   * 
   * E.g. articles have to be retrieved by provided the identifier of
   * the associated blog
   */
  private val excludes = List("id")
  private val req_params = params.filter(kv => excludes.contains(kv._1) == false)
  
  private val rid = if (params.contains("id")) params("id").toLong else -1
  
  private def createClient:ShopifyClient = {
  
    val key = params("key")
    val secret = params("secret")
    
    val url = params("url")
    new ShopifyClient(key,secret,url)
    
  }
  
  override def getPartitions:Array[Partition] = {

    val client = createClient
    
    /*
     * The ShopifyRDD collects all items of a certain resource from the
     * shop platform; in order to calculate the respective partitions,
     * we have to determine the total number of items first  
     */
    val count = client.getResourceCount(resource,rid,req_params)
    client.close
    
    val pages = Math.ceil(count / 250.0).toInt
    
    val pagesPerPartition = Math.floor(pages.toDouble / numPartitions).toInt
    val diff = pages - numPartitions * pagesPerPartition
    
    
    (0 until numPartitions).map(i => {
      
      val start = 1 + i * pagesPerPartition
      val end = (i+1) * pagesPerPartition
    
      if (i == numPartitions - 1)
        new ShopifyPartition(i,start,end + diff)
      
      else
        new ShopifyPartition(i,start,end)
    
    }).toArray
    
  }
  
  override def compute(thePart:Partition,context:TaskContext) = new Iterator[Map[String,Any]] {
    
    private var closed = false  
    private var finished = false
    
    context.addTaskCompletionListener{ context => closeIfNeeded() }

    /*
     * A partition is characterized by a begin & end page
     */ 
    private val partition = thePart.asInstanceOf[ShopifyPartition]    
    
    val start = partition.start
    val end   = partition.end

    val client = createClient
    
    val resources = Buffer.empty[Map[String,Any]]    
    (start to end).foreach(page => {
      resources ++= client.getResources(resource,rid,req_params ++ Map("page" -> page.toString,"limit" -> "250"))
    }) 
    
    val dataset = resources.toIterator
    
    def hasNext:Boolean = {
      
      if (context.isInterrupted())
        throw new TaskKilledException
      
      !finished && dataset.hasNext
      
    }
    
    def next:Map[String,Any] = {
      
      if (hasNext) {
        dataset.next
        
      } else {
        
        finished = true
        null.asInstanceOf[Map[String,Any]]
      
      }
      
    }
    
    def closeIfNeeded() {
      if (!closed) {
        close()
        closed = true
      }
    }  
    
    def close() {   
      client.close
    }
  
  } 

}