package org.myorg.quickstart
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode


class Event(input: JsonNode){
  // Event object attributes
  var eventType: String = input.get("eventType").asText()
  var uid: String  = input.get("uid").asText()
  var timestamp: Long  = input.get("timestamp").asLong()
  var ip: String  = input.get("ip").asText()
  var impressionId: String = input.get("impressionId").asText()

  // Override toString to get something printable by Flink
  override def toString : String = {
    s"Event({'eventType': $eventType, 'uid': $uid, 'timestamp': $timestamp, 'ip': $ip, 'impressionId': $impressionId)}"
  }
}