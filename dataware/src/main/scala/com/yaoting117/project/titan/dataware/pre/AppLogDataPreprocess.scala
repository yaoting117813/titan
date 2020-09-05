package com.yaoting117.project.titan.dataware.pre

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yaoting117.project.titan.commons.util.SparkUtil
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * app 埋点数据预处理
 *  提交数据
 */
object AppLogDataPreprocess {

    def main(args: Array[String]): Unit = {

        val sparkSession: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._

        val ds: Dataset[String] = sparkSession.read.textFile("G:\\titan-logs\\2020-09-05\\app")

        // 数据解析 数据清洗
        ds
          // 数据解析,清洗
          .map(line => {
            val jsonObject: JSONObject = JSON.parseObject(line)
            null
        })
          // 数据过滤
          .filter(_ != null)
          // 数据集成
          .map(logDataBean => {

          })
          .toDF().write.parquet("")


        sparkSession.close()

    }

}
