package com.yaoting117.project.titan.dataware.dict

import java.util.Properties

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 将经纬度转换为GeoHash编码
 */
class GeoHashDict {

    def main(args: Array[String]): Unit = {

        // 构建Spark上下文
        val sparkSession: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
        import sparkSession.implicits._

        // 读取MySQL中的GPS坐标地理位置表
        val url: String = "jdbc:mysql://localhost:3306/dicts"
        val table: String = "tmp"
        val properties: Properties = new Properties()
        properties.setProperty("user", "user")
        properties.setProperty("password", "password")


        val df: DataFrame = sparkSession.read.jdbc(url, table, properties)

        // 将GPS坐标通过GeoHash算法转换为GeoHash编码
        val result: DataFrame = df.map(row => {
            val lng: Double = row.getAs[Double]("BD09_LNG")
            val lat: Double = row.getAs[Double]("BD09_LAT")
            val province: String = row.getAs[String]("province")
            val city: String = row.getAs[String]("city")
            val district: String = row.getAs[String]("district")
            val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5)
            (geoHash, province, city, district)
        }).toDF("geo", "province", "city", "district")

        // 保存结果
        result.write.parquet("data/dict/geo_dict/output")

        // 关闭Spark
        sparkSession.close()

    }

}
