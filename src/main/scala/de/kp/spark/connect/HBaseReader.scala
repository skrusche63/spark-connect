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

import it.nerdammer.spark.hbase._
import it.nerdammer.spark.hbase.conversion._

class HBaseReader(@transient sc:SparkContext) extends Serializable {
  /**
   * This method reads the content of an HBase table of a specific
   * keyspace. Actually, all data records are retrieved from the table 
   */
  def read(config:ConnectConfig,keyspace:String,table:String,columns:List[String] = List.empty[String]):RDD[Map[String,Any]] = {
      
    val settings = config.hbase
    val host = settings("spark.hbase.host")
    /*
     * We add the configuration parameters to connect to HBase here;
     * note, that 'host' refers to 'hbase.zookeeper.quorum', and, 
     * 'hbase.rootdir' is set to '/hbase' as default value.
     * 
     * 'hbase.zookeeper.property.clientPort' is not considered by
     * the hbase connector and therefore requires '2181'
     * 
     * Below is a default configuration for low level access to HBase:
     * 
     * val config = HBaseConfiguration.create()
     * config.set("hbase.zookeeper.quorum", "localhost")
     * config.set("hbase.zookeeper.property.clientPort","2181")
     * config.set("hbase.mapreduce.inputtable", "hbaseTableName")
     * 
     * var source = sc.newAPIHadoopRDD(
     *                 config, 
     *                 classOf[TableInputFormat], 
     *                 classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], 
     *                 classOf[org.apache.hadoop.hbase.client.Result])
     * 
     * Relevant methods from Result:
     * 
     * - result.getRow
     * - result.getColumn
     * 
     */
    sc.getConf.set("spark.hbase.host",host)
    
    null
  }

}