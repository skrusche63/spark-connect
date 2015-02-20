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

import org.scribe.model._
import org.slf4j.LoggerFactory

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.mutable.Buffer

class BigClient(val key:String,val token:String,val context:String) {

  private val LOG = LoggerFactory.getLogger(classOf[BigClient])
  
  private val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)

  val ENDPOINT = String.format("""https://api.bigcommerce.com/%s/v2/""",context)

  def getResources(resource:String,id:Int,params:Map[String,String]):List[Map[String,Any]] = {
    
    resource match {
      
      case "brand" => getBrands(params)
      case "customer" => getCustomers(params)
      
      case "image" => if (id == -1) List.empty[Map[String,Any]] else getImages(id,params)
      case "lineitem" => if (id == -1) List.empty[Map[String,Any]] else getLineItems(id,params)
      
      case "order" => getOrders(params)
      case "product" => getProducts(params)
      
      case _ => List.empty[Map[String,Any]]
      
    }
    
  }
  
  def getBrands(requestParams:Map[String,String]):List[Map[String,Any]] = {
 
    val endpoint = ENDPOINT + "brands" + getSimpleUrlParams(requestParams)
    getResponseAsList(endpoint)
    
  }

  def getCustomers(requestParams:Map[String,String]):List[Map[String,Any]] = {
 
    val endpoint = ENDPOINT + "customers" + getSimpleUrlParams(requestParams)
    getResponseAsList(endpoint)
    
  }
  
  def getOrders(requestParams:Map[String,String]):List[Map[String,Any]] = {
 
    val endpoint = ENDPOINT + "orders"
    getResponseAsList(endpoint)
    
  }
  
  def getBrand(brand:Int):Map[String,Any] = {
 
    val endpoint = ENDPOINT + "brands/" + brand 
    getResponseAsObject(endpoint)
    
  }
  def getLineItems(order:Int,requestParams:Map[String,String]):List[Map[String,Any]] = {
 
    val endpoint = ENDPOINT + "orders/" + order + "/products" + getSimpleUrlParams(requestParams)
    getResponseAsList(endpoint)
    
  }
  
  def getImages(product:Int,requestParams:Map[String,String]):List[Map[String,Any]] = {
 
    val endpoint = ENDPOINT + "products/" + product + "/images" + getSimpleUrlParams(requestParams)
    getResponseAsList(endpoint)
    
  }
  
  def getProducts(requestParams:Map[String,String]):List[Map[String,Any]] = {
 
    val endpoint = ENDPOINT + "products" + getSimpleUrlParams(requestParams)
    getResponseAsList(endpoint)
    
  }
  
  private def getOrderUrlParams(params:Map[String,String]):String = {
    
    
    val accepted = List("page","limit","min_date_created","status_id","max_date_created")
    
    val sb = Buffer.empty[String]
    for (kv <- params) {
       
      if (accepted.contains(kv._1)) {
        
        val value = String.format("""?%s=%s""",kv._1,kv._2)
        sb += value
      
      }
      
    }
    
    val s = "?" + sb.mkString("&")    
    java.net.URLEncoder.encode(s, "UTF-8")
    
  }

  private def getSimpleUrlParams(params:Map[String,String]):String = {
    
    val accepted = List("page","limit")
    
    val sb = Buffer.empty[String]
    for (kv <- params) {
       
      if (accepted.contains(kv._1)) {
        
        val value = String.format("""?%s=%s""",kv._1,kv._2)
        sb += value
      
      }
      
    }
    
    val s = "?" + sb.mkString("&")    
    java.net.URLEncoder.encode(s, "UTF-8")
    
  }
  
  def getResponseAsList(endpoint:String):List[Map[String,Any]] = {

    val request = new OAuthRequest(Verb.GET, endpoint)
    request.addHeader("accept", "application/json")

    request.addHeader("X-Auth-Client", key)
    request.addHeader("X-Auth-Token", token)
		
    val response = request.send()
    if (response.getCode == 200) {
      
      val body = response.getBody
      JSON_MAPPER.readValue(body, classOf[List[Map[String,Any]]])
      
    } else {
      throw new Exception("Bad request: " + response.getCode)
    }
    
  }
  
  def getResponseAsObject(endpoint:String):Map[String,Any] = {

    val request = new OAuthRequest(Verb.GET, endpoint)
    request.addHeader("accept", "application/json")

    request.addHeader("X-Auth-Client", key)
    request.addHeader("X-Auth-Token", token)
		
    val response = request.send()
    if (response.getCode == 200) {
      
      val body = response.getBody
      JSON_MAPPER.readValue(body, classOf[Map[String,Any]])
      
    } else {
      throw new Exception("Bad request: " + response.getCode)
    }
    
  }

}