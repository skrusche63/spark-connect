package de.kp.spark.connect
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
import org.apache.spark.rdd.RDD

import org.apache.hadoop.io.{ArrayWritable,MapWritable,NullWritable,Text}

import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.common.logging.Loggers

import org.apache.hadoop.conf.{Configuration => HConfig}
import org.elasticsearch.hadoop.mr.EsInputFormat

import scala.collection.JavaConversions._

class ElasticReader(@transient sc:SparkContext) extends Serializable {

  val ES_QUERY:String = "es.query"
  val ES_RESOURCE:String = "es.resource"

  /*
   * Create an Elasticsearch node by interacting with
   * the Elasticsearch server on the local machine
   */
  private val node = nodeBuilder().node()
  private val client = node.client()
  
  private val logger = Loggers.getLogger(getClass())
  
  def read(config:HConfig):RDD[Map[String,String]] = {

    val source = sc.newAPIHadoopRDD(config, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    source.map(hit => toMap(hit._2))
    
  }
  
  def read(config:ConnectConfig,index:String,mapping:String,query:String):RDD[Map[String,String]] = {
          
    val conf = config.elastic
    
    /*
     * Append dynamic request specific data to Elasticsearch configuration;
     * this comprises the search query to be used and the index (and mapping)
     * to be accessed
     */
    conf.set(ES_QUERY,query)
    conf.set(ES_RESOURCE,(index + "/" + mapping))
 
    read(conf)
    
  }
  
  def close() {
    if (node != null) node.close()
  }
  
  private def toMap(mw:MapWritable):Map[String,String] = {
      
    val m = mw.map(e => {
        
      val k = e._1.toString        
      val v = (if (e._2.isInstanceOf[Text]) e._2.toString()
        else if (e._2.isInstanceOf[ArrayWritable]) {
        
          val array = e._2.asInstanceOf[ArrayWritable].get()
          array.map(item => {
            
            (if (item.isInstanceOf[NullWritable]) "" else item.asInstanceOf[Text].toString)}).mkString(",")
            
        }
        else "")
        
    
      k -> v
        
    })
      
    m.toMap
    
  }

}