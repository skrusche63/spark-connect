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
     * url:String	(required)	
     * 
     * The URL to retrieve a content profile for
     * 
     * groups:Array of Strings	(optional)
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
    if (params.contains("groups")) builder.field("groups",params("groups"))
    
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