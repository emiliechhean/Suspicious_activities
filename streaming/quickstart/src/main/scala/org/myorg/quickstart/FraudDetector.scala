package org.myorg.quickstart

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

class FraudDetector {

  // Return all UIDs that have more than minCTR CTR in a given window.
  def getFraudulentUID_CTR(dataStreamClicks: DataStream[Event], dataStreamDisplays: DataStream[Event], minCTR: Double, minDisplaysCTR: Int, windowSize: Time): DataStream[String] = {
    // DataStream of clicks
    val uidClicks = dataStreamClicks
      // Keep only the uid column and a column of 1
      .map(row => (row.uid, 1))
      // KeyBy uid
      .keyBy(_._1)
      // on a TumblingEventTimeWindows of windowSize
      .window(TumblingEventTimeWindows.of(windowSize))
      // Sum number of clicks for same uid
      .reduce((x, y) => (x._1, x._2 + y._2))

    // DataStream of displays
    val uidDisplays = dataStreamDisplays
      // Keep only the uid column and a column of 1
      .map(row => (row.uid, 1))
      // KeyBy uid
      .keyBy(_._1)
      // on a TumblingEventTimeWindows of windowSize
      .window(TumblingEventTimeWindows.of(windowSize))
      // Sum number of clicks for same uid
      .reduce((x, y) => (x._1, x._2 + y._2))

    // Join DataStream on uid since we want to calculate CTR per uid
    val clicksDisplays = uidDisplays.join(uidClicks)
      .where(row => row._1).equalTo(row => row._1)
      // on a TumblingEventTimeWindows of windowSize
      .window(TumblingEventTimeWindows.of(windowSize))
      // Compute the CTR per uid
      .apply((e1, e2) => (e1._1, e2._2.toFloat/e1._2, e1._2))//.toDouble))


    val suspiciousCTR = clicksDisplays
      // Keep only rows satisfying conditions
      .filter(row => row._2 > minCTR && row._3 >= minDisplaysCTR)
      // Return UIDs
      .map(row => row._1)

    suspiciousCTR
  }

  // Return all IPs that have more than minUIDPerIP UIDs in a given window.
  def getFraudulentIP(dataStream: DataStream[Event], minUIDPerIP: Int, windowSize: Time): DataStream[String] = {

    // DataStream of clicks OR displays
    val uidPerIP = dataStream
      // Keep ip and create a list of uid to have at the end a list of all uids
      .map(row => (row.ip, List(row.uid)))
      // KeyBy on IP
      .keyBy(_._1)
      // on a TumblingEventTimeWindows of windowSize
      .window(TumblingEventTimeWindows.of(windowSize))
      //  .reduce((x, y) => (x._1++y._1, x._2++y._2, x._3++y._3))
      // Compute distinct uid for each IP
      .reduce((x, y) => (x._1, (x._2++y._2).distinct))
      // Compute uid list length
      .map(x => (x._1, x._2, x._2.length))

    val suspiciousIP = uidPerIP
      // Keep only rows satisfying conditions
      .filter(row => row._3 >= minUIDPerIP)
      // Return IPs
      .map(row => row._1)

    suspiciousIP
  }

  // Return all UIDs that have more than minIPPerUID IPs in a given window.
  def getFraudulentUID_IP(dataStream: DataStream[Event], minIPPerUID: Int, windowSize: Time): DataStream[String] = {

    val ipPerUID = dataStream
      // Keep uid and create a list of ips to have at the end a list of all ips
      .map(row => (row.uid, List(row.ip)))
      // KeyBy on uid
      .keyBy(_._1)
      // on a TumblingEventTimeWindows of windowSize
      .window(TumblingEventTimeWindows.of(windowSize))
      // Compute distinct uid for each IP
      .reduce((x, y) => (x._1, (x._2++y._2).distinct))
      // Compute ip list length
      .map(x => (x._1, x._2, x._2.length))

    val suspiciousUID = ipPerUID
      // Keep only rows satisfying conditions
      .filter(row => row._3 >= minIPPerUID)
      // Return UIDs
      .map(row => row._1)

    suspiciousUID
  }

  // Return all UIDs that have more than minDisplaysPerUID impressionIds in a given window.
  def getFraudulentUID_nbDisplays(dataStream_displays: DataStream[Event], minDisplaysPerUID: Int, windowSize: Time): DataStream[String] = {

    val displaysPerUID= dataStream_displays
      // Keep uid and create a list of impressionId to have at the end a list of all impressionIds
      .map(row => (row.uid, List(row.impressionId)))
      // KeyBy uid
      .keyBy(_._1)
      // on a TumblingEventTimeWindows of windowSize
      .window(TumblingEventTimeWindows.of(windowSize))
      // Compute distinct impressionIds
      .reduce((x, y) => (x._1, (x._2++y._2).distinct))
      // Compute impressionIds list length
      .map(x => (x._1, x._2, x._2.length))

    val suspiciousUID = displaysPerUID
      // Keep only rows satisfying conditions
      .filter(row => row._3 >= minDisplaysPerUID)
      // Return UIDs
      .map(row => row._1)

    suspiciousUID

  }
}
