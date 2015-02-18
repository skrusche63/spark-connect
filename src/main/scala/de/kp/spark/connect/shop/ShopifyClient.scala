package de.kp.spark.connect.shop
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

import java.io.IOException

import javax.ws.rs.HttpMethod
import javax.ws.rs.client.{Client,ClientBuilder,Entity,WebTarget}
import javax.ws.rs.core.MediaType

import com.fasterxml.jackson.databind.{Module, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.slf4j.{Logger,LoggerFactory}

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

class ShopifyClient(key:String,secret:String,url:String) extends Serializable {

  private val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)
  
  private val client = ClientBuilder.newClient()
  private val endpoint = url.replaceFirst("://", "://" + key + ":" + secret + "@")
      
  private val webTarget = client.target(endpoint).path("admin")   
 
  def close = client.close
 
  def getResourceCount(name:String,id:Long,params:Map[String,String]):Long = {
    
    name match {
      
      case "article" => if (id == -1) -1 else getArticlesCount(id,params)
      case "blog" => getBlogsCount(params)
      
      case "customer" => getCustomersCount(params)
      
      case "order" => getOrdersCount(params)            
      case "product" => getProductsCount(params)

      case _ => -1
    
    }    
    
  }
  def getResources(name:String,id:Long,params:Map[String,String]):List[Map[String,Any]] = {
    
    name match {
      
      case "article" => if (id == -1) List.empty[Map[String,Any]] else getArticles(id,params)
      case "blog" => getBlogs(params)
      
      case "customer" => getCustomers(params)
      
      case "order" => getOrders(params)            
      case "product" => getProducts(params)

      case _ => List.empty[Map[String,Any]]
    
    }    
    
  }
  
  /**************************************************************************
   * 
   *                        ARTICLE SUPPORT
   * 
   *************************************************************************/
  
  def getArticles(bid:Long,params:Map[String,String]):List[Map[String,Any]] = {
    
    val result = getResponse("blogs/" + bid + "/articles.json", params, HttpMethod.GET)
    /*
     * { "articles": [ ... ] }
     */
    val response = JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
    if (response.contains("articles")) 
      response("articles").asInstanceOf[List[Map[String,Any]]] 
    
    else List.empty[Map[String,Any]]
  
  }
  
  def getArticlesCount(bid:Long,params:Map[String,String]):Long = {
    
    val result = getResponse("blogs/" + bid + "/articles/count.json", params, HttpMethod.GET)
    /*
     * { "count": 1 }
     */
    val response = JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
    if (response.contains("count")) response("count").asInstanceOf[Long] else -1
    
  }

  /**************************************************************************
   * 
   *                        BLOG SUPPORT
   * 
   *************************************************************************/
  
  def getBlogs(params:Map[String,String]):List[Map[String,Any]] = {
    
    val result = getResponse("blogs.json", params, HttpMethod.GET)
    /*
     * { "blogs": [ ... ] }
     */
    val response = JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
    if (response.contains("blogs")) 
      response("blogs").asInstanceOf[List[Map[String,Any]]] 
    
    else List.empty[Map[String,Any]]
  
  }
  
  def getBlogsCount(params:Map[String,String]):Long = {
    
    val result = getResponse("blogs/count.json", params, HttpMethod.GET)
    /*
     * { "count": 1 }
     */
    val response = JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
    if (response.contains("count")) response("count").asInstanceOf[Long] else -1
    
  }

  /**************************************************************************
   * 
   *                        CUSTOMER SUPPORT
   * 
   *************************************************************************/
  
  def getCustomers(params:Map[String,String]):List[Map[String,Any]] = {
    
    val result = getResponse("customers.json", params, HttpMethod.GET)
    /*
     * { "customers": [ ... ] }
     */
    val response = JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
    if (response.contains("customers")) 
      response("customers").asInstanceOf[List[Map[String,Any]]] 
    
    else List.empty[Map[String,Any]]
  
  }
  
  def getCustomersCount(params:Map[String,String]):Long = {
    
    val result = getResponse("customers/count.json", params, HttpMethod.GET)
    /*
     * { "count": 1 }
     */
    val response = JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
    if (response.contains("count")) response("count").asInstanceOf[Long] else -1
    
  }

  /**************************************************************************
   * 
   *                        PRODUCT SUPPORT
   * 
   *************************************************************************/
  
  def getProducts(params:Map[String,String]):List[Map[String,Any]] = {
   
    val result = getResponse("products.json", params, HttpMethod.GET)
    /*
     * { "products": [ ... ] }
     */
    val response = JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
    if (response.contains("products")) 
      response("customers").asInstanceOf[List[Map[String,Any]]] 
    
    else List.empty[Map[String,Any]]

  }
  
  def getProductsCount(params:Map[String,String]):Long = {
    
    val result = getResponse("products/count.json", params, HttpMethod.GET)
    /*
     * { "count": 1 }
     */
    val response = JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
    if (response.contains("count")) response("count").asInstanceOf[Long] else -1
    
  }

  /**************************************************************************
   * 
   *                        ORDER SUPPORT
   * 
   *************************************************************************/
  
  def getOrders(params:Map[String,String]):List[Map[String,Any]] = {
    
    val result = getResponse("orders.json", params, HttpMethod.GET)
    /*
     * { "orders": [ ... ] }
     */
    val response = JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
    if (response.contains("orders")) 
      response("orders").asInstanceOf[List[Map[String,Any]]] 
    
    else List.empty[Map[String,Any]]

  }
  
  def getOrdersCount(params:Map[String,String]):Long = {
    
    val result = getResponse("orders/count.json", params, HttpMethod.GET)
    /*
     * { "count": 1 }
     */
    val response = JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
    if (response.contains("count")) response("count").asInstanceOf[Long] else -1
    
  }

  private def getResponse(resource:String,params:Map[String,String],method:String):String = {
       
    try {
      
      var qt = webTarget.path(resource)
      for (entry <- params) {
        val (k,v) = entry
        qt = qt.queryParam(k,v)
      }

      qt.request(MediaType.APPLICATION_JSON_TYPE).method(method, null, classOf[String])
    
    } catch {
      case e:Exception => throw new Exception("Could not process query",e)
    }

  }

}