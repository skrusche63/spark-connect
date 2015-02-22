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

import org.apache.hadoop.conf.{Configuration => HConfig}

import com.aerospike.hadoop._
import scala.collection.JavaConversions._

class AerospikeReader(@transient sc:SparkContext) extends Serializable {

  /*
   * Background to Aerospike:
   * 
   * At the highest level, data is collected in containers called namespaces;
   * namespaces are similar to databases. Within a namespace, data are divided
   * into sets (equivalent to tables), and finally records (rows).  
   */
  def read(config:ConnectConfig,params:Map[String,String]):RDD[Map[String,Any]] = {
    
    val settings = config.aerospike
    
    val conf = new HConfig()
    /* Add host & port to configuration */
    val host = if (settings.contains("aerospike.input.host")) 
      settings("aerospike.input.host") else "localhost"
   
    conf.set("aerospike.input.host", host)

    val port = if (settings.contains("aerospike.input.port")) 
      settings("aerospike.input.port") else "3000"
   
    conf.set("aerospike.input.port", port)
    
    /* Add namespace and set name to configuration */
    conf.set("aerospike.input.namespace",params("namespace"))
    conf.set("aerospike.input.setname",params("setnames"))
    
    /* Add bin names & operation */
    val binnames = if (params.contains("binnames"))
      params("binnames") else ""
    
    conf.set("aerospike.input.binnames",binnames)

    val operation = if (params.contains("operation"))
      params("operation") else "scan"
    
    conf.set("aerospike.input.operation",operation)

    if (operation == "numrange") {

      conf.set("aerospike.input.numrange.bin",params("numrange_bin"))
      
      conf.set("aerospike.input.numrange.begin",params("numrange_begin"))
      conf.set("aerospike.input.numrange.end",params("numrange_end"))

    }

    read(conf)
    
  }

  def read(config:HConfig):RDD[Map[String,Any]] = {

    val source = sc.newAPIHadoopRDD(config, classOf[AerospikeInputFormat], classOf[AerospikeKey], classOf[AerospikeRecord])
    source.map{case(key,record) => toMap(key,record)}
    
  }

  private def toMap(key:AerospikeKey,record:AerospikeRecord):Map[String,Any] = {
    
    val bins = record.bins
    bins.toMap
    
  }
}