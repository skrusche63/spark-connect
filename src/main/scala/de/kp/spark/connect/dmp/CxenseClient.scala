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

class CxenseClient(username:String,secret:String) {
 
  private val CXENSE_URI = "https://api.cxense.com"

  private val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)

  def getProfileContentFetch(params:Map[String,Any]):Map[String,Any] = {
    
    val endpoint = "profile/content/fetch"
    getResponse(endpoint,params)
    
  } 

  def getProfileUserExternalRead(params:Map[String,Any]):Map[String,Any] = {
    
    val endpoint = "profile/user/external/read"
    getResponse(endpoint,params)
 
  }
  /*
   * Collect interest profile for a certain user; the relevant 
   * part of the response (profile) is equivalent to the 'content
   * fetch' request 
   */
  def getProfileUser(params:Map[String,Any]):Map[String,Any] = {
    
    val endpoint = "profile/user"
    getResponse(endpoint,params)
  
  }
  
  def getProfileUserSegment(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "profile/user/segment"
    getResponse(endpoint,params)

  }
  
  def getSegmentRead(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "segment/read"
    getResponse(endpoint,params)

  }
  
  def getSite(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "site"
    getResponse(endpoint,params)

  }
  
  def getSiteGroup(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "site/group"
    getResponse(endpoint,params)

  }
  
  def getTraffic(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic"
    getResponse(endpoint,params)

  }
  
  def getTrafficCompare(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/compare"
    getResponse(endpoint,params)

  }

  def getTrafficCustom(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/custom"
    getResponse(endpoint,params)

  }

  def getTrafficCustomDescribe(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/custom/describe"
    getResponse(endpoint,params)

  }

  def getTrafficEvent(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/event"
    getResponse(endpoint,params)

  }

  def getTrafficEventDescribe(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/event/describe"
    getResponse(endpoint,params)

  }

  def getTrafficIntent(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/intent"
    getResponse(endpoint,params)

  }

  def getTrafficKeyword(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/keyword"
    getResponse(endpoint,params)

  }

  def getTrafficKeywordDescribe(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/keyword/describe"
    getResponse(endpoint,params)

  }

  def getTrafficRelated(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/related"
    getResponse(endpoint,params)

  }

  def getTrafficUser(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/user"
    getResponse(endpoint,params)

  }

  def getTrafficUserExternal(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/user/external"
    getResponse(endpoint,params)

  }

  def getTrafficUserHistogram(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/user/histogram"
    getResponse(endpoint,params)

  }

  def getTrafficUserHistogramEvent(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/user/histogram/event"
    getResponse(endpoint,params)

  }

  def getTrafficUserInterest(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/user/interest"
    getResponse(endpoint,params)

  }

  def getTrafficUserKeyword(params:Map[String,Any]):Map[String,Any] = {

    val endpoint = "traffic/user/keyword"
    getResponse(endpoint,params)

  }
  
  private def getAuthenticationHeader:String = {
        
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA256"))

    val date = ISODateTimeFormat.dateTime().print(new DateTime(DateTimeZone.UTC))
    val signature = new String(Base64.encodeBase64(mac.doFinal(date.getBytes("UTF-8"))))

    "username=" + username + " date=" + date + " hmac-sha256-base64=" + signature
    
  }
  
  private def getResponse(endpoint:String,req_params:Map[String,Any]):Map[String,Any] = {

    val body = JSON_MAPPER.writeValueAsString(req_params)
    
    val client = ClientBuilder.newClient()
    val request = client.target(CXENSE_URI).path("/").path(endpoint).request(MediaType.APPLICATION_JSON_TYPE)

    val response = request
                     .header("X-cXense-Authentication", getAuthenticationHeader)
                     .method(HttpMethod.POST, if (body == null) null else Entity.json(body), classOf[String])
    
    client.close()    
    
    JSON_MAPPER.readValue(response, classOf[Map[String,Any]])
    
  }
 
}