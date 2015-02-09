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

class ElasticSource(@transient sc:SparkContext) extends Serializable {
 
  def connect(config:ConnectConfig,requestParams:Map[String,String]):RDD[Map[String,String]] = {

    val index = requestParams("index")
    val mapping = requestParams("mapping")
    
    val query = requestParams("query").asInstanceOf[String]
    new ElasticReader(sc).read(config,index,mapping,query)

  }

}