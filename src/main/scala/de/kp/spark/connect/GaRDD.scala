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

import java.net.URL

import org.apache.spark.{Partition,SparkContext,TaskContext}
import org.apache.spark.TaskKilledException

import org.apache.spark.rdd.RDD

import org.apache.spark.util.NextIterator
import scala.reflect.ClassTag

import com.google.gdata.client.analytics.{AnalyticsService,DataQuery}
import com.google.gdata.data.analytics.{DataEntry,DataFeed}

import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

case class GaRow(columns:Seq[GaColumn])
case class GaColumn(name:String,category:String,datatype:String,value:String)

class GaPartition(idx:Int,val startIndex:Int,val maxResult:Int) extends Partition {
  override def index = idx  
}


class GaRDD(
    /* Reference to SparkContext */
    @transient sc:SparkContext,
    /* Request parameters */
    params:Map[String,String],
    /* Total number of results */
    numResults:Int,
    /* Total number of partitions */
    numPartitions:Int) extends RDD[GaRow](sc,Nil) {

  override def getPartitions:Array[Partition] = {
    
    /*
     * The maximum number of results returned with a request;
     * note, that the Analytics Core Reporting API returns a
     * maximum of 10,000 rows per request, no matter how many
     * one asks for
     */
    val maxResult = Math.round(numResults.toDouble / numPartitions).toInt
    
    (0 until numPartitions).map(i => {
      
      val startIx = 1 + (i-1) * maxResult
      new GaPartition(i,startIx,maxResult)
    
    }).toArray

  }
  
  override def compute(thePart:Partition,context:TaskContext) = new Iterator[GaRow] {
    
    private var closed = false  
    private var finished = false
    
    context.addTaskCompletionListener{ context => closeIfNeeded() }

    private val partition = thePart.asInstanceOf[GaPartition]    
    private val query = buildQuery(partition)
    
    val service = buildService    
    val datafeed = service.getFeed(query.getUrl,classOf[DataFeed])

    val dataset = datafeed.getEntries.map(mapEntry(_)).toIterator
      
    /*
     * Build query and determine maximum number of results
     * from the request parameters (or default = 10.000)
     */
    def hasNext:Boolean = {
      
      if (context.isInterrupted())
        throw new TaskKilledException
      
      !finished && dataset.hasNext
      
    }
    
    def next:GaRow = {
      
      if (hasNext) {
        dataset.next
        
      } else {
        
        finished = true
        null.asInstanceOf[GaRow]
      
      }
      
    }
    
    def closeIfNeeded() {
      if (!closed) {
        close()
        closed = true
      }
    }  
    
    def close() {   
      /* 
       * The connection to a GData service is properly closed
       * after the request has been performed; this implies
       * that we do nothing here
       */
    }
  
    private def mapEntry(entry:DataEntry):GaRow = {

      val columns = Buffer.empty[GaColumn]

      /* DIMENSIONS */
      val dimensions = entry.getDimensions
      if (!dimensions.isEmpty) { 
        dimensions.map(dimension => GaColumn(dimension.getName,"dimension","string",dimension.getValue)) 
      }
        
      /* METRICS */
      val metrics = entry.getMetrics
      metrics.map(metric => GaColumn(metric.getName,"metric",metric.getType,metric.getValue))
      
      GaRow(columns.toSeq)
      
    }
    
    private def buildQuery(partition:GaPartition):DataQuery = {
    
      /* REQURED */
      val query = new DataQuery(new URL(params("url")))
    
      /* REQUIRED */
      val start_date = params("start_date")
      query.setStartDate(start_date)

      val end_date = params("end_date")
      query.setEndDate(end_date)
    
      /*
       * REQUIRED
       * 
       * The aggregated statistics for user activity in a view (profile), 
       * such as clicks or pageviews. When queried by alone, metrics provide 
       * the total values for the requested date range, such as overall pageviews 
       * or total bounces. 
       * 
       * However, when requested with dimensions, values are segmented by the dimension. 
       * For example, ga:pageviews requested with ga:country returns the total pageviews 
       * per country. 
       * 
       * When requesting metrics, keep in mind: All requests require at least one metric.
       * 
       * You can supply a maximum of 10 metrics for any query.Not all dimensions and metrics 
       * can be used together. Consult the Valid Combinations tool to see which combinations 
       * work together.
       * 
       */
      val metrics = params("metrics")
      query.setMetrics(metrics)
      /*
       * REQUIRED
       * 
       * The unique table ID used to retrieve the Analytics Report data.
       */
      val table_id = params("table_id")
      query.setIds(table_id)
    
      /* OPTIONAL */    
      if (params.contains("dimensions")) {
        query.setDimensions(params("dimensions"))
      }

      /* OPTIONAL */    
      if (params.contains("filters")) {
        query.setFilters(params("filters"))
      }

      /* OPTIONAL */    
      if (params.contains("sort")) {
        query.setSort(params("sort"))
      }
    
      query.setStartIndex(partition.startIndex)
      query.setMaxResults(partition.maxResult)
      
      query
     
    }
  
    private def buildService:AnalyticsService = {
  
      val app_name = params("app_name")
      val analytics = new AnalyticsService(app_name)
   
      val user_name = params("user_name")
      val password = params("password")
    
      analytics.setUserCredentials(user_name,password)   
      analytics
   
    }
  
  }
  
}
