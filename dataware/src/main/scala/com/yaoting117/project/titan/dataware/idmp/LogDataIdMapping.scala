package com.yaoting117.project.titan.dataware.idmp

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yaoting117.project.titan.commons.util.SparkUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 不考虑前一天的日志数据
 */
object LogDataIdMapping {

    def main(args: Array[String]): Unit = {

        val sparkSession: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)

        val appLog: Dataset[String] = sparkSession.read.textFile("")
        val webLog: Dataset[String] = sparkSession.read.textFile("")
        val wechatLog: Dataset[String] = sparkSession.read.textFile("")

        import sparkSession.implicits._

        // 获取所有的ID Tag 标签   uid imei mac imsi
        val appIds: RDD[Array[String]] = extractIds(appLog)
        val webIds: RDD[Array[String]] = extractIds(appLog)
        val wechatIds: RDD[Array[String]] = extractIds(appLog)

        val ids: RDD[Array[String]] = appIds.union(webIds).union(wechatIds)

        // vertices 点集合
        val vertices: RDD[(Long, String)] = ids.flatMap(arr => {
            for (tag <- arr) yield (tag.hashCode.toLong, tag)
        })

        // edges 边集合
        /*
            对IdTag数组中的Tag 两两组合成为边
            eg [tag1, tag2, tag3, tag4]
                --> (tag1,tag2) (tag1,tag3) (tag1,tag4) (tag2,tag3) (tag2,tag4) (tag3,tag4)
                --> 对数组双重for循环
                --> 可以使用 filter 对边进行过滤 (wordcount) 防止小概率事件,使用其它设备登录...
         */
        val edges: RDD[Edge[String]] = ids.flatMap(arr => {
            for (i <- 0 to arr.length - 2; j <- i + 1 until arr.length) yield Edge(arr(i).hashCode.toLong, arr(j).hashCode.toLong, "")
        }).map(edge => (edge, 1)).reduceByKey(_ + _).filter(tp => tp._2 > 2).map(tp => tp._1)

        // 构建图 Graph
        val graph: Graph[String, String] = Graph(vertices, edges)

        // 默认为 RDD[(点ID-Long, 组中最小的点ID-Long)] 可以不使用默认 groupBy 分组 每一组手动生成一个UUID RDD[(点ID-Long,UUID)]
        val resultVertices: VertexRDD[VertexId] = graph.connectedComponents(5).vertices

        // 使用图计算结果中一组的最小值的HashCode 作为此组的UUID
        resultVertices.map(vertex => (vertex._1, vertex._2)).toDF("TAGS", "GUID").write.parquet("")



    }


    def extractIds(logDS: Dataset[String]): RDD[Array[String]] = {
        logDS.rdd.map(line => {
            val jsonObject: JSONObject = JSON.parseObject(line)
            val userObject: JSONObject = jsonObject.getJSONObject("user")
            val uid: String = userObject.getString("uid")

            val phoneObject: JSONObject = userObject.getJSONObject("phone")
            val imei: String = phoneObject.getString("imei")
            val mac: String = phoneObject.getString("mac")
            val imsi: String = phoneObject.getString("imsi")
            val androidId: String = phoneObject.getString("androidId")
            val deviceId: String = phoneObject.getString("deviceId")
            val uuid: String = phoneObject.getString("uuid")

            Array(uid, imei, mac, imsi, androidId, deviceId, uuid).filter(StringUtils.isNotBlank(_))
        })
    }

}
