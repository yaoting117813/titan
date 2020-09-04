package com.yaoting117.project.titan.dataware.idmp

import com.yaoting117.project.titan.commons.util.SparkUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * IdMapping 一个简单Spark GraphX Api 使用的 Demo
 * 使用Spark GraphX 框架 找出下列数据哪些是同一个人的信息
 * 测试数据为:
 *      手机号       姓名            微信号           薪水
 *      13986688416,  夏耀庭,      wx_yaoting,       2000
 *      13986688416,  yaoting117, wx_yaoting,       2000
 *      13886624679,  夏耀庭,      wx_helloworld,    500
 *      13247163315,  胡倩倩,      wx_huqianqian,    2000
 *      13247163315,  青青,       wx_qingqingqing,  1000
 *      13247163315,  倩倩,       wx_qianqian,      1000
 *  Spark GraphX
 *    点vertices需要表示为一个Tuple (点的唯一Long类型的ID,点的数据)
 *    边edge
 *    (-634317196,-1941208943)
 *    (1613534750,-1941208943)
 *    (32362945,-1941208943)
 *    (-307779908,-1941208943)
 *    (1507423,-1941208943)
 *    (1239616,-1941208943)
 *    (1858853816,-1941208943)
 *    (656672,-1941208943)
 *    (-1941208943,-1941208943)
 *    (52469,-1941208943)
 *    (-188428747,-1941208943)
 *    (1198644855,-1941208943)
 *    (-155349592,-1941208943)
 *    (22949884,-1941208943)
 *    (1537214,-1941208943)
 *    (1627674658,-1941208943)
 */
object Demo {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)

    import sparkSession.implicits._

    // 读取数据
    val ds: Dataset[String] = sparkSession.read.textFile("D:\\code\\0901\\project\\titan\\data\\graphx\\input")

    // Vertices RDD
    val verticesRDD: RDD[(VertexId, String)] = ds.rdd.flatMap(line => {
      val fields: Array[String] = line.split(",")
      for (ele <- fields if StringUtils.isNotBlank(ele)) yield (ele.hashCode.toLong, ele)
    })

//    verticesRDD.take(30).foreach(println)

    // Edge RDD   Edge(起始点ID, 目标点ID, 边数据)
    val edgesRDD: RDD[Edge[String]] = ds.rdd.flatMap(line => {
      val fields: Array[String] = line.split(",")

      /*val listBuffer: ListBuffer[Edge[String]] = new ListBuffer[Edge[String]]()
      for (i <- 0 to fields.length - 2) {
        val edge: Edge[String] = Edge(fields(i).hashCode.toLong, fields(i + 1).hashCode.toLong, "")
        listBuffer += edge
      }*/

      // Scala的for是有返回值的, yield将每一次循环产生的结果存储到一个集合中,集合类型与for中的集合类型一致, 即返回一个 Array[Edge[String]]
      for (i <- 0 to fields.length - 3 if StringUtils.isNotBlank(fields(i))) yield Edge(fields(i).hashCode.toLong, fields(i + 1).hashCode.toLong, "")
    })

    // 调用 API 使用点集合和边集合 构建 图(Graph)
    val graph: Graph[String, String] = Graph(verticesRDD, edgesRDD)

    val connectedGraph: Graph[VertexId, String] = graph.connectedComponents(5)

    // 关联的点集合 集合中的元素为 (verticesValue, minVerticesValueHashCode)
    val connectedVertices: VertexRDD[VertexId] = connectedGraph.vertices

//    connectedVertices.take(30).foreach(println)


    // 使用广播变量或JOIN操作
    /*

      使用广播变量
        将connectedVertices收集到Driver端,然后在创建广播变量,然后在将此广播变量发送到各个Executor
          connectedVertices.collectAsMap()
          sparkSession.sparkContext.broadcast(idMappingMap)

     */

    val idMappingMap: collection.Map[VertexId, VertexId] = connectedVertices.collectAsMap()
    val bc: Broadcast[collection.Map[VertexId, VertexId]] = sparkSession.sparkContext.broadcast(idMappingMap)

    val dataWithGidDS: Dataset[String] = ds.map(line => {
      val value: collection.Map[VertexId, VertexId] = bc.value
      val firstIdTag: String = line.split(",").filter(StringUtils.isNotBlank(_))(0)
      val maybeId: Option[VertexId] = value.get(firstIdTag.hashCode.toLong)
      val gid: VertexId = maybeId.get
      gid + "," + line
    })

    dataWithGidDS.show()

    sparkSession.close()

  }

}
