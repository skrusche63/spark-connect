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

import java.util.regex.Matcher
import java.util.regex.Pattern

case class ApacheLogInfo(
  ip_address:String,
  client_identd:String,
  user_id:String,
  datetime:String,
  method:String,
  endpoint:String,
  protocol:String,
  response_code:Int,
  content_size:Long    
)

object ApacheLogParser extends Serializable{
  /*
   * Example Apache log line:
   * 
   * 127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
   * 
   */
  private val LOG_ENTRY_PATTERN =
      // 1:IP  2:client 3:user 4:date time  5:method 6:req 7:proto   8:respcode 9:size
      "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)"
  
  private val PATTERN = Pattern.compile(LOG_ENTRY_PATTERN)

  def parse(logline:String):ApacheLogInfo = {
    
    val m = PATTERN.matcher(logline)
    if (!m.find()) {
      throw new RuntimeException("Error parsing logline");
    }

    ApacheLogInfo(
      m.group(1), 
      m.group(2), 
      m.group(3), 
      m.group(4),
      m.group(5), 
      m.group(6), 
      m.group(7), 
      m.group(8).toInt, 
      m.group(9).toLong)
      
  }

}