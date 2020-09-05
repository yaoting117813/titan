package com.yaoting117.project.titan.dataware.pre

import java.lang

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import com.yaoting117.project.titan.commons.util.SparkUtil
import com.yaoting117.project.titan.dataware.beans.AppLogBean
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * app 埋点数据预处理
 *  提交数据
 */
object AppLogDataPreprocess {

    def main(args: Array[String]): Unit = {

        val sparkSession: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._

        // 地理位置字典,使用广播变量方式广播
        val geoMap: collection.Map[String, (String, String, String)] = sparkSession.read.parquet("data/dict/geo_dict/output").rdd
          .map({
              case Row(geo: String, province: String, city: String, district: String) => (geo, (province, city, district))
          }).collectAsMap()
        val geoMapBC: Broadcast[collection.Map[String, (String, String, String)]] = sparkSession.sparkContext.broadcast(geoMap)

        // IdMapping
        val idMappingMap: collection.Map[VertexId, VertexId] = sparkSession.read.parquet("data/idmp/2020-09-05").rdd.map({
            case Row(tags: VertexId, guid: VertexId) => (tags, guid)
        }).collectAsMap()
        val idMappingBC: Broadcast[collection.Map[VertexId, VertexId]] = sparkSession.sparkContext.broadcast(idMappingMap)

        // App Log Data
        val ds: Dataset[String] = sparkSession.read.textFile("G:\\titan-logs\\2020-09-05\\app")

        // 数据解析 数据清洗 数据集成 保存结果
        ds
          .map((line: String) => parseAppLogBean(line))
          .filter(_ != null)
          .map(logDataBean => integrateAppLogBean(logDataBean, geoMapBC, idMappingBC))
          .filter(bean => bean.guid != Long.MinValue)
          .toDF().write.parquet("data/applog_processed/2020-09-05")

        sparkSession.close()

    }

    def integrateAppLogBean(logDataBean: AppLogBean, geoMapBC: Broadcast[collection.Map[String, (String, String, String)]], idMappingBC: Broadcast[collection.Map[VertexId, VertexId]]): AppLogBean = {
        val geoDict: collection.Map[String, (String, String, String)] = geoMapBC.value
        val idMappingDict: collection.Map[VertexId, VertexId] = idMappingBC.value
        val lng: Double = logDataBean.longitude
        val lat: Double = logDataBean.latitude

        val geo: String = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5)
        val maybeTuple: Option[(String, String, String)] = geoDict.get(geo)
        if (maybeTuple.isDefined) {
            val area: (String, String, String) = maybeTuple.get
            logDataBean.province = area._1
            logDataBean.city = area._2
            logDataBean.district = area._3
        }
        val ids: Array[String] = Array(logDataBean.imei, logDataBean.imsi, logDataBean.mac, logDataBean.uid, logDataBean.androidId, logDataBean.uuid)
        var find = false
        for (elem <- ids if !find) {
            val maybeLong: Option[VertexId] = idMappingDict.get(elem.hashCode.toLong)
            if (maybeLong.isDefined) {
                logDataBean.guid = maybeLong.get
                find = true
            }
        }
        logDataBean
    }

    def parseAppLogBean(line: String): AppLogBean = {
        try {
            // 数据解析
            val jsonObject: JSONObject = JSON.parseObject(line)
            val eventid: String = jsonObject.getString("eventid")
            import scala.collection.JavaConversions._
            val event: Map[String, String] = jsonObject.getJSONObject("event").getInnerMap.asInstanceOf[java.util.Map[String, String]].toMap

            /*

              {
                  eventid:
                  event: {
                  }
                  user:　{
                  }
                  phone: {
                  }
                  app: {
                  }
                  loc: {
                  }
                  timestamp
              }


             */

            val userObject: JSONObject = jsonObject.getJSONObject("user")
            val uid: String = userObject.getString("uid")

            val phoneObject: JSONObject = userObject.getJSONObject("phone")
            val imei: String = phoneObject.getString("imei")
            val mac: String = phoneObject.getString("mac")
            val imsi: String = phoneObject.getString("imsi")
            val osName: String = phoneObject.getString("osName")
            val osVer: String = phoneObject.getString("osVer")
            val androidId: String = phoneObject.getString("androidId")
            val resolution: String = phoneObject.getString("resolution")
            val deviceType: String = phoneObject.getString("deviceType")
            val deviceId: String = phoneObject.getString("deviceId")
            val uuid: String = phoneObject.getString("uuid")

            val appObject: JSONObject = userObject.getJSONObject("app")
            val appid: String = appObject.getString("appid")
            val appVer: String = appObject.getString("appVer")
            // 应用市场渠道 channel
            val release_ch: String = appObject.getString("release_ch")
            // 推广渠道 channel
            val promotion_ch: String = appObject.getString("promotion_ch")

            val locObject: JSONObject = userObject.getJSONObject("loc")
            val longitude: lang.Double = locObject.getDouble("longitude")
            val latitude: lang.Double = locObject.getDouble("latitude")
            val carrier: String = locObject.getString("carrier")
            val netType: String = locObject.getString("netType")
            val cid_sn: String = locObject.getString("cid_sn")
            val ip: String = locObject.getString("ip")

            val sessionId: String = userObject.getString("sessionId")

            val timestamp: Long = jsonObject.getString("timestamp").toLong

            // 数据清洗
            /*
              1) IdTag 标识字段不能全部为空
              2) 必须含有的字段 event eventid sessionId
             */

            val sb = new StringBuilder
            val idTags: String = sb.append(uid).append(imei).append(uuid).append(mac).append(androidId).append(imsi).toString().replaceAll("null", "")
            var appLogBean: AppLogBean = null
            if (StringUtils.isNotBlank(idTags) && event != null && StringUtils.isNotBlank(eventid) && StringUtils.isNotBlank(sessionId)) {
                appLogBean = AppLogBean(
                    Long.MinValue, eventid, event, uid, imei, mac,
                    imsi, osName, osVer, androidId, resolution, deviceType, deviceId,
                    uuid, appid, appVer, release_ch, promotion_ch,
                    longitude: Double, latitude: Double, carrier, netType, cid_sn,
                    ip, sessionId, timestamp
                )
            }
            appLogBean
        } catch {
            case e: Exception => null
        }
    }

}
