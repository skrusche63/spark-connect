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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import de.kp.spark.connect.aerospike.AerospikeSource
import de.kp.spark.connect.cassandra.CassandraSource

import de.kp.spark.connect.elasticsearch.ElasticSource
import de.kp.spark.connect.hbase.HBaseSource

import de.kp.spark.connect.jdbc.JdbcSource
import de.kp.spark.connect.mongodb.MongoSource

import de.kp.spark.connect.parquet.ParquetSource

object Sources {
    
  val AEROSPIKE:String = "aerospike"
  val CASSANDRA:String = "cassandra"
    
  val ELASTICSEARCH:String = "elasticsearch"

  val HBASE:String = "hbase"
  val JDBC:String  = "jdbc"
    
  val MONGODB:String = "mongodb"    
  val PARQUET:String = "parquet"
    
}
// TODO schema & params should be harmonized
class SQLSource(
    @transient sqlContext:SQLContext,
    config:ConnectConfig,
    source:String,
    table:String,
    schema:StructType,
    params:Map[String,String]) extends Serializable {

  /*
   * Retrieve dataset from source and convert
   * result into Row
   */
  private val names = sqlContext.sparkContext.broadcast(schema.fieldNames)

  private val rowRDD = getRDD(source,params).map(rec => {
    val values = names.value.map(name => rec(name))
    Row.fromSeq(values)
  })
  
  /*
   * Apply schema to rows and register as table
   */
  private val tableRDD = sqlContext.applySchema(rowRDD, schema)
  tableRDD.registerTempTable(table)
  
  def executeQuery(query:String):SchemaRDD = sqlContext.sql(query)
  
  private def getRDD(source:String,params:Map[String,String]):RDD[Map[String,Any]] = {
    
    val sc = sqlContext.sparkContext
    
    source match {
      
      case Sources.AEROSPIKE => new AerospikeSource(sc).read(config,params)
      case Sources.CASSANDRA => new CassandraSource(sc).read(config,params)

      case Sources.ELASTICSEARCH => new ElasticSource(sc).read(config,params)
      case Sources.HBASE => new HBaseSource(sc).read(config,params)

      case Sources.JDBC => new JdbcSource(sc).read(config,params)
      case Sources.MONGODB => new MongoSource(sc).read(config,params)

      case Sources.PARQUET => new ParquetSource(sc).read(config,params)
      
      case _ => throw new Exception(String.format("""Data source %s is not supported.""",source))
      
    }
    
  }

}