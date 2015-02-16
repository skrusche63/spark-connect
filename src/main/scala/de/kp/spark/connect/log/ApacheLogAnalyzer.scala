package de.kp.spark.connect.log
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
import org.apache.spark.sql.SQLContext

case class ApacheLogStats(
  contentSizeStats:(Long,Long,Long,Long),
  responseCodeCount:Seq[(Int,Long)],
  ipAddresses:Seq[String],
  topEndpoints:Seq[(String,Long)]
)

class ApacheLogAnalyzer(@transient sc:SparkContext) extends Serializable {

  private val sqlContext = new SQLContext(sc)
  import sqlContext.createSchemaRDD

  def stats(store:String):ApacheLogStats = {
    
    /*
     * Data structure
     * 
     * ip_address
     * client_identd
     * user_id
     * datetime
     * method
     * endpoint
     * protocol
     * response_code
     * content_size    
     * 
     */
    val logs = sc.textFile(store).map(ApacheLogParser.parse(_))
    logs.registerTempTable("logs")
    
    /* Calculate statistics based on the content size */
    val CONTENT_SIZE_SQL = "SELECT SUM(content_size), COUNT(*), MIN(content_size), MAX(content_size) FROM logs"
    val contentSizeStats = sqlContext.sql(CONTENT_SIZE_SQL).map(row => 
      (row.getLong(0), row.getLong(1), row.getLong(2), row.getLong(3))
    
    ).first
    
    /* Compute Response Code to Count */
    val RESPONSE_CODE_SQL = "SELECT response_code, COUNT(*) FROM logs GROUP BY response_code"
    val responseCodeCount = sqlContext.sql(RESPONSE_CODE_SQL).map(row => 
      (row.getInt(0), row.getLong(1))
    
    ).take(1000).toList
          
    /* Any IPAddress that has accessed the server more than 10 times */
    val IP_ADDRESS_SQL = "SELECT ip_address, COUNT(*) AS total FROM logs GROUP BY ip_address HAVING total > 10"
    val ipAddresses = sqlContext.sql(IP_ADDRESS_SQL).map(row => 
      row.getString(0)
    ).take(100)  // Take only 100 in case this is a super large data set.

    /* Top Endpoints */
    val ENDPOINT_SQL = "SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10"
    val topEndpoints = sqlContext.sql(ENDPOINT_SQL).map(row => 
      (row.getString(0), row.getLong(1))
    ).collect()
    
    ApacheLogStats(
      contentSizeStats,responseCodeCount,ipAddresses,topEndpoints
    )
    
  }

}