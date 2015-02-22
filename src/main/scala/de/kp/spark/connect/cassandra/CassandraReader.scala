package de.kp.spark.connect.cassandra
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

import com.datastax.spark.connector._
import de.kp.spark.connect.ConnectConfig

class CassandraReader(@transient sc:SparkContext) extends Serializable {
  /**
   * This method reads the content of a Cassandra table of a specific
   * keyspace. Actually, all data records are retrieved from the table 
   */
  def read(config:ConnectConfig,keyspace:String,table:String,columns:List[String] = List.empty[String]):RDD[Map[String,Any]] = {
      
    val settings = config.cassandra
    val host = settings("spark.cassandra.connection.host")

    /*
     * We add the configuration parameters 
     * to connect to a Cassandra cluster here
     */
    sc.getConf.set("spark.cassandra.connection.host",host)
    /*
     * Read from specified keyspace and table; note, that the number
     * of entries to be returned must be specified 
     */  
    val source = if (columns.isEmpty) 
      sc.cassandraTable(keyspace, table) else sc.cassandraTable(keyspace, table).select(columns.map(ColumnName(_)):_*)
    
    source.map(toMap(_))
    
  }

  /**
   * For the primitive data types required by the different
   * engines of Predictiveworks, the conversion of the column
   * names and values using the toMap method is sufficient.
   * 
   * In case of more complex data types, this method must be
   * adapted to these additional requirements
   */
  private def toMap(row:CassandraRow):Map[String,Any] = {
    row.toMap
  }
  
}