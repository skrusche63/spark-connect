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
import org.apache.hadoop.conf.{Configuration => HConf}

trait ConnectConfig {

  /**
   * This method retrieves the settings to access
   * a Cassandra Cluster
   */
  def cassandra:Map[String,String]  
  /**
   * This method retrieves a Hadoop configuration
   * to access Elasticsearch
   */
  def elastic:HConf
  /**
   * This method retrieves the settings to access
   * HBase
   */
  def hbase:Map[String,String]  
  /**
   * This method retrieves a Hadoop configuration
   * to access MongoDB
   */
  def mongo:HConf
   /**
    * This method retrieves the access parameter for a MySQL
    * data source, comprising url, db, user, password
    */
  def mysql:Map[String,String]
  /**
   * This method retrieves Apache Spark configuration
   */
  def spark:Map[String,String]
  
}