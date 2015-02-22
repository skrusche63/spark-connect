package de.kp.spark.connect.elasticsearch
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

import org.apache.hadoop.io.{ArrayWritable,DoubleWritable,IntWritable,LongWritable,MapWritable,NullWritable,Text,Writable}

import org.apache.hadoop.conf.{Configuration => HConfig}

import org.elasticsearch.hadoop.mr.EsInputFormat
import de.kp.spark.connect.ConnectConfig

import scala.collection.JavaConversions._

class ElasticReader(@transient sc:SparkContext) extends Serializable {

  val ES_QUERY:String = "es.query"
  val ES_RESOURCE:String = "es.resource"
  
  def read(config:HConfig):RDD[Map[String,Any]] = {

    val source = sc.newAPIHadoopRDD(config, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    source.map(hit => toMap(hit._2))
    
  }
  
  def read(config:ConnectConfig,index:String,mapping:String,query:String):RDD[Map[String,Any]] = {
          
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
  
  private def toMap(mw:MapWritable):Map[String,Any] = {
    
    mw.entrySet().map(kv => {
        
      val k = kv.getKey().asInstanceOf[Text].toString
      val v = kv.getValue() match {
          
        case valu:ArrayWritable => {

          val array = valu.get
          array.map(record => {
              
            record.asInstanceOf[MapWritable].entrySet().map(entry => {
                
              val sub_k = entry.getKey().asInstanceOf[Text].toString()
              val sub_v = entry.getValue() match {
          
                case sub_valu:IntWritable => valu.get()
                case sub_valu:DoubleWritable => valu.get()
          
                case sub_valu:LongWritable => valu.get()
                case sub_valu:Text => valu.toString

                case _ => throw new Exception("Data type is not supported.")
                  
              }
                
              (sub_k,sub_v)
                
            }).toMap
              
          }).toList
            
        }
          
        case valu:IntWritable => valu.get()
        case valu:DoubleWritable => valu.get()
          
        case valu:LongWritable => valu.get()
        case valu:Text => valu.toString

        case _ => throw new Exception("Data type is not supported.")
          
      }
      
      (k,v)
        
    }).toMap

  }

}