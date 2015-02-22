package de.kp.spark.connect.hbase
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

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import de.kp.spark.connect.ConnectConfig

class HBaseReader(@transient sc:SparkContext) extends Serializable {
  
  private val HBASE_ROOTDIR = "/hbase"
    
  /**
   * This method reads the content of an HBase table of a specific
   * keyspace. Actually, all data records are retrieved from the table 
   */
  def read(config:ConnectConfig,columnfamily:String,table:String,names:List[String],types:List[String]):RDD[Map[String,Any]] = {
      
    val settings = config.hbase
    val host = settings("spark.hbase.host")
     
    val conf = HBaseConfiguration.create
    conf.setBoolean("hbase.cluster.distributed", true)
    conf.setInt("hbase.client.scanner.caching", 10000)
    
    conf.set("hbase.rootdir", HBASE_ROOTDIR)
    
    conf.set("hbase.zookeeper.quorum", host)   
    conf.set("hbase.zookeeper.property.clientPort","2181")
    
    val columns = names.map(name => columnfamily + ":" + name)
    conf.set(TableInputFormat.SCAN_COLUMNS, columns.mkString(" "))

    val typedNames = names.zip(types)
    
    def toMap(key:ImmutableBytesWritable,row:Result):Map[String,Any] = {
      
      typedNames.map{case(colname,coltype) => {
        /*
         * Convert column family and respective columns
         * into HBase readable Byte array
         */
        val cf = Bytes.toBytes(columnfamily)
        val cn = Bytes.toBytes(colname)
        
        if (row.containsColumn(cf,cn) == false) throw new Exception(
            String.format("""Combination of cf:%s and cn:%s does not exist""",columnfamily,colname))
        
        val byteValue = CellUtil.cloneValue(row.getColumnLatestCell(cf,cn)).array
        /*
         * We actually support the following data types:
         * 
         * double, integer, long, string
         * 
         * as these are needed by Predictiveworks
         */
        val colvalu = coltype match {
          
          case "double" => Bytes.toDouble(byteValue)
          
          case "integer" => Bytes.toInt(byteValue)
            
          case "long" => Bytes.toLong(byteValue)
            
          case "string" => Bytes.toString(byteValue)
            
          case _ => throw new Exception(String.format("""The data type '%s' is not supported.""",coltype))
          
        }
        
        (colname,colvalu)
        
      }}.toMap
      
    }

    val source = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    source.map{case(key,row) => toMap(key,row)}

  }

}