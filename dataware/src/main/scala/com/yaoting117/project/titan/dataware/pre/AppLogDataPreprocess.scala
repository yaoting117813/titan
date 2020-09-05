package com.yaoting117.project.titan.dataware.pre

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yaoting117.project.titan.commons.util.SparkUtil
import com.yaoting117.project.titan.dataware.beans.AppLogBean
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * app 埋点数据预处理
 *  提交数据
 */
object AppLogDataPreprocess {

    def main(args: Array[String]): Unit = {

        val sparkSession: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._

        // 地理位置字典,使用广播变量方式广播
        val geoDF: DataFrame = sparkSession.read.parquet("data/dict/geo_dict/output")

        val ds: Dataset[String] = sparkSession.read.textFile("G:\\titan-logs\\2020-09-05\\app")


        // 数据解析 数据清洗
        ds
          // 数据解析,清洗
          .map(line => {
              try {
                  // 数据解析
                  val jsonObject: JSONObject = JSON.parseObject(line)
                  val eventid: String = jsonObject.getString("eventid")
                  import scala.collection.JavaConversions._
                  val event: Map[String, String] = jsonObject.getJSONObject("event").getInnerMap.asInstanceOf[java.util.Map[String, String]].toMap

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
                    2) 必须含有的字段
                   */

                  val sb = new StringBuilder
                  val idTags: String = sb.append(uid).append(imei).append(uuid).append(mac).append(androidId).append(imsi).toString().replaceAll("null", "")
                  var appLogBean: AppLogBean = null
                  if (StringUtils.isNotBlank(idTags) && event != null && StringUtils.isNotBlank(eventid) && StringUtils.isNotBlank(sessionId)) {
                    appLogBean = AppLogBean(
                        Long.MaxValue,
                        eventid,
                        event,
                        uid,
                        imei,
                        mac,
                        imsi,
                        osName,
                        osVer,
                        androidId,
                        resolution,
                        deviceType,
                        deviceId,
                        uuid,
                        appid,
                        appVer,
                        release_ch,
                        promotion_ch,
                        longitude: Double,
                        latitude: Double,
                        carrier,
                        netType,
                        cid_sn,
                        ip,
                        sessionId,
                        timestamp)
                  }
                  appLogBean
              } catch {
                  case e: Exception => null
              }

        })
          // 数据过滤
          .filter(_ != null)
          // 数据集成
          /*.map(logDataBean => {

          })*/
          .toDF().write.parquet("")


        sparkSession.close()

    }

}
