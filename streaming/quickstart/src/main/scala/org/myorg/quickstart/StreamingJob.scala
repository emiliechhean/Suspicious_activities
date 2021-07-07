package org.myorg.quickstart

import java.util.Properties
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE

object StreamingJob {

  def main(args: Array[String]) {

    // Set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // Set Event Time as the time characteristic for all streams from this environment
    // By default, we have a watermark update interval of 200 ms.
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    // Set properties
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")


    // Variables set up
    // One can modify those values
    val minutes: Int = 5
    // The chosen window size
    val windowSize: Time = Time.minutes(minutes)
    // The minimum CTR per uid to be considered as suspicious
    val minCTR: Double = 0.3
    // given that the number of displays has to be >= to minDisplaysCTR
    val minDisplaysCTR: Int = 3 * minutes
    // For a given IP, the minimum number of uid associated to be considered as suspicious
    val minUIDPerIP: Int = 2 * minutes
    // For a given UID, the minimum number of IP associated to be considered as suspicious
    // We consider that a uid is assumed to be associated with a single IP
    val minIPPerUID: Int = 1
    // For a given UID, the minimum number of displays seen (clicked or not clicked) to be considered as suspicious
    val minDisplaysPerUID: Int = 4 * minutes

    // In order to get something understandable by Flink, we need to deserialize the input
    // source: https://riptutorial.com/apache-flink/example/27995/built-in-deserialization-schemas
    val deserializedClicks: DataStream[ObjectNode] = env.addSource(new FlinkKafkaConsumer[ObjectNode]("clicks", new JSONKeyValueDeserializationSchema(false), properties))
    val deserializedDisplays: DataStream[ObjectNode]= env.addSource(new FlinkKafkaConsumer[ObjectNode]("displays", new JSONKeyValueDeserializationSchema(false), properties))

    // Get watermarks
    // source : https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/event_timestamps_watermarks.html
    class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[Event] {
      // Since we override functions, we left the parameters unused
      override def extractTimestamp(element: Event, previousElementTimestamp: Long): Long = {
      // Convert to correct format
        element.timestamp * 1000
      }
      override def checkAndGetNextWatermark(lastElement: Event, extractedTimestamp: Long): Watermark = {
        // Generate watermark
        new Watermark(extractedTimestamp)
      }
    }

    // DataStream of click Events with watermarks
    val clicks: DataStream[Event] = deserializedClicks
      .map(rowClick => new Event(rowClick.get("value")))
      .assignTimestampsAndWatermarks(new PunctuatedAssigner)

    // DataStream of displays Events with watermarks
    val displays: DataStream[Event] = deserializedDisplays
      .map(rowDisplay => new Event(rowDisplay.get("value")))
      .assignTimestampsAndWatermarks(new PunctuatedAssigner)


    // Creation of a FraudDetector object which will detect all suspicious activities
    val fraudDetector = new FraudDetector

    // We have created methods for all patterns that seemed visible and explicit to us
    // We can distinguish suspicious `uids` and suspicious `ip`

    // #### Fraudulent UIDs ####
    // Pattern 1 :
    // Detect all UIDs that have more than minCTR CTR and having more than minDisplaysCTR in a given window.
    val suspiciousUID_CTR: DataStream[String] = fraudDetector.getFraudulentUID_CTR(dataStreamClicks = clicks, dataStreamDisplays=displays, minCTR = minCTR, minDisplaysCTR = minDisplaysCTR, windowSize = windowSize)


    // Pattern 2 :
    // Detect all UIDs that have more than minIPPerUID IPs in a given window.
    val suspiciousUID_IP: DataStream[String] = fraudDetector.getFraudulentUID_IP(dataStream = clicks, minIPPerUID = minIPPerUID, windowSize = windowSize)

    // Pattern 3 :
    // Detect all UIDs that have more than minDisplaysPerUID impressionIds in a given window.
    val suspiciousUID_nbDisplays: DataStream[String] = fraudDetector.getFraudulentUID_nbDisplays(dataStream_displays = displays, minDisplaysPerUID = minDisplaysPerUID, windowSize = windowSize)

    // #### Fraudulent IPs ####

    // Pattern 4 :
    // Detect all IPs that have more than minUIDPerIP UIDs in a given window.
    val suspiciousIP: DataStream[String] = fraudDetector.getFraudulentIP(dataStream = clicks, minUIDPerIP = minUIDPerIP, windowSize = windowSize)

    // Keep unique uid : concatenate the different suspicious_uid to keep the unique
    val suspiciousUIDs: DataStream[String] = suspiciousUID_CTR
      .union(suspiciousUID_IP)
      .union(suspiciousUID_nbDisplays)
      .map(row => (row, 1))
      .keyBy(_._1)
      .reduce((x, y) => (x._1, y._2 + y._2))
      // return UIDs
      .map(x => (x._1)).setParallelism(1)


    suspiciousUIDs.print()
    suspiciousIP.print()


    // Write to file
    clicks.writeAsText("./Events/clicks.txt", OVERWRITE).setParallelism(1)
    displays.writeAsText("./Events/displays.txt", OVERWRITE).setParallelism(1)
    suspiciousUIDs.writeAsText("./suspiciousInputs/suspiciousUID.txt", OVERWRITE).setParallelism(1)
    suspiciousIP.writeAsText("./suspiciousInputs/suspiciousIP.txt", OVERWRITE).setParallelism(1)
    
    // execute program
    env.execute("Suspicious behavior detector")

  }
}
