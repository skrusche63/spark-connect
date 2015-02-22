package de.kp.spark.connect.mongodb
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

import com.mongodb.hadoop.MongoInputFormat
import org.bson.BSONObject

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import de.kp.spark.connect.ConnectConfig

class MongoReader(@transient sc:SparkContext) extends Serializable {
  
  def read(config:ConnectConfig,query:String):RDD[Map[String,Any]] = {
    
    val conf = config.mongo
    conf.set("mongo.input.query",query)    
    
    val source = sc.newAPIHadoopRDD(conf, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])
    source.map(x => toMap(x._2))
    
  }

  private def toMap(obj:BSONObject):Map[String,Any] = {
    
    val data = HashMap.empty[String,Any]
    
    val keys = obj.keySet()
    for (k <- keys) {
      
      val v = obj.get(k)
      data += k -> v
    
    }
    
    data.toMap
    
  }
}