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

class GaReader(@transient sc:SparkContext) extends Serializable {

  def read(config:ConnectConfig,params:Map[String,String]):RDD[Map[String,Any]] = {
      
    val settings = config.ga

    val req_params = params ++ Map(
      
      "app_name" -> params("app_name"),
      
      "user_name" ->  params("user_name"),
      "password"  ->  params("password")
    )

    val numResults = params("num_results").toInt
    val numPartitions = params("num_partitions").toInt
    
    val source = new GaRDD(sc,req_params,numResults,numPartitions)
    source.map(toMap(_))
    
  }

  private def toMap(row:GaRow):Map[String,Any] = {

    val columns = row.columns
    columns.map(column => {
      
      val k = column.name
      val v = if (column.category == "dimension") {
        column.value
      
      } else {
        
        column.datatype match {
          /*
           * The datatype 'integer' describes a Long (see Metric
           * implementation); all other values describe Doubles
           */
          case "integer" => column.value.toLong
          /*
           * currency, us_currency, float, percent, time
           */
          case _ => column.value.toDouble
        }
      }
    
      (k,v)
      
    }).toMap
    
  }

}