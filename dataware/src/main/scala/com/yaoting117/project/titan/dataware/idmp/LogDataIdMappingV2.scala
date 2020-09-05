package com.yaoting117.project.titan.dataware.idmp

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yaoting117.project.titan.commons.util.SparkUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object LogDataIdMappingV2 {

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
                --> 可以使用 filter 对边进行过滤 (wordcount)
         */
        val edges: RDD[Edge[String]] = ids.flatMap(arr => {
            for (i <- 0 to arr.length - 2; j <- i + 1 until arr.length) yield Edge(arr(i).hashCode.toLong, arr(j).hashCode.toLong, "")
        }).map(edge => (edge, 1)).reduceByKey(_ + _).filter(tp => tp._2 > 2).map(tp => tp._1)

        // 上一日的点 边 集合 (上一日的结果已经保存在文件系统中了)
        val preDayIdMapping: DataFrame = sparkSession.read.parquet("")

        val preDayVertices: RDD[(VertexId, String)] = preDayIdMapping.rdd.map({
            case Row(idFlag: VertexId, guid: VertexId) => (idFlag, "")
        })

        val preDayEdges: RDD[Edge[String]] = preDayIdMapping.rdd.map(row => {
            val idTag: VertexId = row.getAs[VertexId]("TAGS")
            val guid: VertexId = row.getAs[VertexId]("GUID")
            Edge(idTag, guid, "")
        })

        // 构建图 Graph 当天的和上一天的数据进行UNION操作  然后构建
        val graph: Graph[String, String] = Graph(vertices.union(preDayVertices), edges.union(preDayEdges))

        // 默认为 RDD[(点ID-Long, 组中最小的点ID-Long)] 可以不使用默认 groupBy 分组 每一组手动生成一个UUID RDD[(点ID-Long,UUID)]
        val resultVertices: VertexRDD[VertexId] = graph.connectedComponents(5).vertices

        val idMap: collection.Map[VertexId, VertexId] = preDayIdMapping.rdd.map(row => {
            val idTag: VertexId = row.getAs[VertexId]("TAGS")
            val guid: VertexId = row.getAs[VertexId]("GUID")
            (idTag, guid)
        }).collectAsMap()

        val bc: Broadcast[collection.Map[VertexId, VertexId]] = sparkSession.sparkContext.broadcast(idMap)

        // RDD 算子是在 Executor 上计算的
        val todayIdMappingResult: RDD[(VertexId, VertexId)] = resultVertices.map(tp => (tp._2, tp._1)).groupByKey().mapPartitions(iter => {
            // 使用 mapPartitions 取广播变量时 每个分区取一次,而不是每个RDD取一次
            val idmappingMap: collection.Map[VertexId, VertexId] = bc.value
            iter.map(tp => {
                //                val idmappingMap: collection.Map[VertexId, VertexId] = bc.value
                var todayGuid: VertexId = tp._1
                val ids: Iterable[VertexId] = tp._2

                var find = false

                for (elem <- ids if !find) {
                    val maybeGuid: Option[VertexId] = idmappingMap.get(elem)
                    if (maybeGuid.isDefined) {
                        todayGuid = maybeGuid.get
                        find = true
                    }

                }

                (todayGuid, ids)
            })
        }).flatMap(tp => {
            val ids: Iterable[VertexId] = tp._2
            val guid = tp._1
            for (elem <- ids) yield (elem, guid)
        })

        // 合并ID
        /*
            1)将当日的数据按照GUID分组    guid01 -> [id1, id2, id2]
                                       guid04 -> [id4, id6]
                                       guid10 -> [id10, id11]
            2)使用分组后的数据和上一日的结果求交集运算
                若有交集    合并上日的结果
                没有交集    使用当日的结果
         */

        // 使用图计算结果中一组的最小值的HashCode 作为此组的UUID
        resultVertices.map(vertex => (vertex._1, vertex._2)).toDF("TAGS", "GUID").write.parquet("")


        sparkSession.close()


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
