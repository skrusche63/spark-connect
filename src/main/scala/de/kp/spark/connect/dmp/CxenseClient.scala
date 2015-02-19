package de.kp.spark.connect.dmp
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

import com.fasterxml.jackson.databind.{Module, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import javax.ws.rs.HttpMethod

import javax.ws.rs.client.{ClientBuilder,Entity}
import javax.ws.rs.core.MediaType

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.ISODateTimeFormat

import org.apache.commons.codec.binary.Base64
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

class CxenseClient(username:String,secret:String) {
 
  private val CXENSE_URI = "https://api.cxense.com"

  private val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)
  /**
   * Retrieve a single content profile
   */
  def getContentProfile(params:Map[String,String]):Map[String,Any] = {
    
    val endpoint = "profile/content/fetch"
    /*
     * The following parameters can be set with this request:
     * 
     * url:String (required)	
     * 
     * The URL to retrieve a content profile for
     * 
     * groups:Array of Strings (optional)
     * 
     * A list of strings that specify profile item groups to keep 
     * in the returned profile (see below for descriptions of the 
     * profile field in the response object). 
     * 
     * If not specified, all groups available for the content will 
     * be returned.
     * 
     */
    val builder = XContentFactory.jsonBuilder()
    builder.startObject()
    
    builder.field("url",params("url"))    
    if (params.contains("groups")) {
      
      val values = params("groups").split(",")
      
      builder.startArray("groups")
      values.foreach(v => builder.value(v))
      builder.endArray()
      
    }
    
    builder.endObject()
    val body = builder.string()
    
    val result = getResponse(endpoint,body)
    JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
    
  } 

  def getExternalUserProfile(params:Map[String,String]):Map[String,Any] = {
    
    val endpoint = "profile/user/external/read"
    /*
     * id:String (optional)
     * 
     * The externally defined identifier for the user.
     * 
     * type:String (required)	
     * 
     * The customer identifier type as registered with Cxense 
     * (see Customer Prefix)
     */
    val builder = XContentFactory.jsonBuilder()
    builder.startObject()
      
    if (params.contains("id")) builder.field("id",params("id"))
    builder.field("type",params("type"))    
    
    builder.endObject()
    val body = builder.string()
    
    val result = getResponse(endpoint,body)
    JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
 
  }
  
  def getUserProfile(params:Map[String,String]):Map[String,Any] = {
    
    val endpoint = "profile/user"
    /*
     * id:String (required)	
     * 
     * Identifies the user whose interest profile should be returned.
     * 
     * type:String (required)
     * 
     * The type of user identifier, i.e., tells us how to interpret id. 
     * The value cx indicates that the identifier is the site-specific 
     * cXense identifier value, as read using the cX.getUserId() function 
     * call from cx.js. 
     * 
     * Customer-specific identifiers via a customer-assigned prefix are 
     * also possible.
     * 
     * groups:Array of Strings (optional)	
     * 
     * A list of strings that specify profile item groups to keep in the 
     * returned profile (see below for descriptions of the profile field 
     * in the response object). If not specified, all groups available for 
     * the user will be returned. Group specifications may enable server-side 
     * optimizations on Cxense's side, which can lead to a quicker response.
     * 
     * recent:Boolean (optional)
     * 
     * Returns quickly if this user has not been seen recently. Cxense stores 
     * user profile information in many storage layers, where the most recently 
     * seen profiles are the quickest profiles to retrieve. In an interactive 
     * session where events are generated (and as a consequence the user profile 
     * is updated and considered a fresh profile), it may be more appropriate to 
     * return quickly than wait for a complete response on the first page view.
     * 
     * identityTypes:Array of Strings (optional)
     * 
     * A list of external customer identifier types. If an external customer 
     * identifier exists for the user, it will be included in the response (the 
     * identifier would first have been added through the addExternalId() Javascript 
     * API, or through /profile/user/external/link/update).
     */
    val builder = XContentFactory.jsonBuilder()
    builder.startObject()

    builder.field("id",params("id"))    
    builder.field("type",params("type"))    

    if (params.contains("groups")) {
      
      val values = params("groups").split(",")
      
      builder.startArray("groups")
      values.foreach(v => builder.value(v))
      builder.endArray()
      
    }

    if (params.contains("recent")) builder.field("recent",params("recent").toBoolean)

    if (params.contains("identityTypes")) {
      
      val values = params("identityTypes").split(",")
      
      builder.startArray("identityTypes")
      values.foreach(v => builder.value(v))
      builder.endArray()
      
    }
    
    builder.endObject()
    val body = builder.string()
    
    val result = getResponse(endpoint,body)
    JSON_MAPPER.readValue(result, classOf[Map[String,Any]])
  
  }
  def getUserSegment(params:Map[String,String]):Map[String,Any] = {

    val endpoint = "profile/user/segment"
    /*
     * siteGroupIds:Array of Strings (required)
     * 
     * The list of site groups to retrieve segments for.
     * 
     * id:String (optional)
     * 
     * The externally defined identifier for the user.
     * 
     * type:String	(optional)
     * 	
     * Either the built-in user identifier type "cx" (first party cookie),
     * or the customer identifier type as registered with Cxense 
     * (see Customer Prefix)
     * 
     * identities:Array of Objects	(optional)	
     * 
     * A list of user identifiers to consult if multiple identities are available.
     * This parameter is NOT SUPPORTED by this client
     * 
     */
    val builder = XContentFactory.jsonBuilder()
    builder.startObject()
      
    val siteGroupIds = params("siteGroupIds").split(",")
      
    builder.startArray("siteGroupIds")
    siteGroupIds.foreach(v => builder.value(v))
    builder.endArray()
    
    if (params.contains("id")) builder.field("id",params("id"))
    if (params.contains("type")) builder.field("type",params("type"))
    
    builder.endObject()
    val body = builder.string()
    
    val result = getResponse(endpoint,body)
    JSON_MAPPER.readValue(result, classOf[Map[String,Any]])

  }
  
  def getSegment(params:Map[String,String]):Map[String,Any] = {

    val endpoint = "segment/read"
    /*
     * siteGroupIds:Array of Strings (optional)
     * 
     * Return all audience segmentation rules for the given site group 
     * identifier.
     * 
     * id:String (optional)
     * 
     * Return the audience segmentation rule for the given id.
     * 
     * annotations:Array of Objects	(optional)	
     * 
     * Return the audience segmentation rules that matches the given annotations.
     * This parameter is NOT SUPPORTED by this client
     * 
     */
    val builder = XContentFactory.jsonBuilder()
    builder.startObject()
      
    if (params.contains("siteGroupIds")) {
      
      val values = params("siteGroupIds").split(",")
      
      builder.startArray("siteGroupIds")
      values.foreach(v => builder.value(v))
      builder.endArray()
      
    }
    
    if (params.contains("id")) builder.field("id",params("id"))
    
    builder.endObject()
    val body = builder.string()
    
    val result = getResponse(endpoint,body)
    JSON_MAPPER.readValue(result, classOf[Map[String,Any]])

  }
  
  def getSite(params:Map[String,String]):Map[String,Any] = {

    val endpoint = "site"
    /*
     * siteId:String (optional)
     * 
     * The identifier of the site to retrieve. Can not be combined 
     * with siteIds or siteGroupIds.
     * 
     * siteIds:Array of String (optional)	
     * 
     * An array of identifiers for sites to retrieve. Can not be combined 
     * with siteId.
     * 
     * siteGroupIds:Array of String	(optional)
     * 	
     * An array of identifiers for site groups whose sites will be retrieved. 
     * Can not be combined with siteId.
     */
    val builder = XContentFactory.jsonBuilder()
    builder.startObject()

    if (params.contains("siteId")) {

      builder.field("siteId",params("siteId"))
      
    } else {
      
       if (params.contains("siteIds")) {
      
        val values = params("siteIds").split(",")
      
        builder.startArray("siteIds")
        values.foreach(v => builder.value(v))
        builder.endArray()
      
      }
     
      if (params.contains("siteGroupIds")) {
      
        val values = params("siteGroupIds").split(",")
      
        builder.startArray("siteGroupIds")
        values.foreach(v => builder.value(v))
        builder.endArray()
      
      }
    
    }
    
    builder.endObject()
    val body = builder.string()
    
    val result = getResponse(endpoint,body)
    JSON_MAPPER.readValue(result, classOf[Map[String,Any]])

  }
  
  def getSiteGroup(params:Map[String,String]):Map[String,Any] = {

    val endpoint = "site/group"
    /*
     * siteGroupId:String (optional)	
     * 
     * The identifier of the site group to retrieve. Can not be combined 
     * with siteGroupIds.
     * 
     * siteGroupIds:Array of String	(optional)
     * 	
     * An array of identifiers for site groups to retrieve. Can not be combined 
     * with siteGroupId.
     */
    val builder = XContentFactory.jsonBuilder()
    builder.startObject()

    if (params.contains("siteGroupId")) {

      builder.field("siteGroupId",params("siteGroupId"))
      
    } else {
     
      if (params.contains("siteGroupIds")) {
      
        val values = params("siteGroupIds").split(",")
      
        builder.startArray("siteGroupIds")
        values.foreach(v => builder.value(v))
        builder.endArray()
      
      }
    
    }
    
    builder.endObject()
    val body = builder.string()
    
    val result = getResponse(endpoint,body)
    JSON_MAPPER.readValue(result, classOf[Map[String,Any]])

  }
  
  def getTraffic(params:Map[String,String]):Map[String,Any] = {

    val endpoint = "traffic"
    /*
     * siteId:String (optional)	
     * 
     * The site to aggregate over. A more convenient alternative to siteIds, 
     * in the case of just one site.
     * 
     * siteIds:Array of String (optional)	
     * 
     * The set of sites to aggregate over.
     * 
     * siteGroupId:String (optional)
     * 
     * The site group to aggregate over. A more convenient alternative to 
     * siteGroupIds, in the case of just one site group.
     * 
     * siteGroupIds:Array of String	(optional)	
     * 
     * The set of site groups to aggregate over.
     * 
     * start:Integer or String (optional)	
     * 
     * The start time of the aggregation period specified according to Traffic 
     * time specification. The default value is one hour ago.
     * 
     * stop:Integer or String (optional)
     * 
     * The stop time of the aggregation period specified according to Traffic 
     * time specification. The default value is right now.
     * 
     * filters:Array of Object (optional)
     * 
     * Traffic filters to be applied. Conjunctive semantics (and) are assumed 
     * for the top-level filters. Actually NOT SUPPORTED
     * 
     * fields:Array of String (optional)
     * 
     * The metrics to aggregate. The available field names are listed below.
     * 
     * historyFields:Array of String (optional)
     * 
     * The fields to aggregate a history for. Must be a subset of fields. Empty by 
     * default, i.e., no history data will be returned. While fields will only return 
     * a single value for the full period, history will return an array of values for
     * intervals within the period. Useful when creating tables or graphs.
     * 
     * historyBuckets:Integer (optional)
     * 
     * The number of intervals for history aggregation. The time between start and stop 
     * will be divided into this many buckets or intervals. The default value is 10, the
     * maximum value is 200.
     * 
     * historyResolution:String (optional)
     * 
     * If defined, overrides historyBuckets and divides the time between start and stop into 
     * intervals of the given length. The acceptable values are "month", "week", "day", "hour" 
     * and "minute". The resulting number of buckets has to be between 2 and 200. The resolution 
     * "minute" is not supported for data older than 31 days. 
     */  
    val builder = XContentFactory.jsonBuilder()
    builder.startObject()
   
    if (params.contains("siteId")) {

      builder.field("siteId",params("siteId"))
      
    } else {
     
      if (params.contains("siteIds")) {
      
        val values = params("siteIds").split(",")
      
        builder.startArray("siteIds")
        values.foreach(v => builder.value(v))
        builder.endArray()
      
      }
    
    }
      
    if (params.contains("siteGroupIds")) {
      
      val values = params("siteGroupIds").split(",")
      
      builder.startArray("siteGroupIds")
      values.foreach(v => builder.value(v))
      builder.endArray()
      
    }
    
    if (params.contains("start")) builder.field("start",params("start"))
    if (params.contains("stop")) builder.field("stop",params("stop"))
    
    if (params.contains("fields")) {
      
      val values = params("fields").split(",")
      
      builder.startArray("fields")
      values.foreach(v => builder.value(v))
      builder.endArray()
      
    }
     
    if (params.contains("historyFields")) {
      
      val values = params("historyFields").split(",")
      
      builder.startArray("historyFields")
      values.foreach(v => builder.value(v))
      builder.endArray()
      
    }
     
    if (params.contains("historyBuckets")) builder.field("historyBuckets",params("historyBuckets").toInt)
    if (params.contains("historyResolution:String")) builder.field("historyResolution:String",params("historyResolution:String"))
    
    builder.endObject()
    val body = builder.string()
    
    val result = getResponse(endpoint,body)
    JSON_MAPPER.readValue(result, classOf[Map[String,Any]])

  }
  
  private def getAuthenticationHeader:String = {
        
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA256"))

    val date = ISODateTimeFormat.dateTime().print(new DateTime(DateTimeZone.UTC))
    val signature = new String(Base64.encodeBase64(mac.doFinal(date.getBytes("UTF-8"))))

    "username=" + username + " date=" + date + " hmac-sha256-base64=" + signature
    
  }
  
  private def getResponse(endpoint:String,body:String):String = {
    
    val client = ClientBuilder.newClient()
    val request = client.target(CXENSE_URI).path("/").path(endpoint).request(MediaType.APPLICATION_JSON_TYPE)

    val response = request
                     .header("X-cXense-Authentication", getAuthenticationHeader)
                     .method(HttpMethod.POST, if (body == null) null else Entity.json(body), classOf[String])
    
    client.close()    
    response   
    
  }
 
}