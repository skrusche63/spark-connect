package de.kp.spark.connect.parquet
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
import org.apache.spark.SparkContext._

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashMap

class ParquetReader(@transient sc:SparkContext) extends Serializable {

  def read(store:String,fields:List[String] = List.empty[String]):RDD[Map[String,Any]] = {

    val sqlCtx = new SQLContext(sc)
    import sqlCtx.createSchemaRDD
    
    /* 
     * Read in the parquet file created above.  Parquet files are self-describing 
     * so the schema is preserved. The result of loading a Parquet file is also a 
     * SchemaRDD. 
     */
    val parquetFile = sqlCtx.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    parquetFile.map(row => toMap(row,metadata,fields))

  }

  private def toMap(row:Row,metadata:Seq[(StructField,Int)],fields:List[String]):Map[String,Any] = {

    val data = HashMap.empty[String,Any]
    val values = row.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
    
    metadata.foreach(entry => {
      
      val field = entry._1
      val col   = entry._2
      
      val colname = field.name
      val colvalu = values(col)
      
      if (fields.isEmpty) {
        data += colname -> colvalu
        
      } else {        
        if (fields.contains(colname)) data += colname -> colvalu
        
      }
     
    })
    
    data.toMap
    
  }
  
}
