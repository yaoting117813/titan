package com.yaoting117.project.titan.dataware.test

import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadParquet {

    def main(args: Array[String]): Unit = {

        val sparkSession: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
        import sparkSession.implicits._

        val df: DataFrame = sparkSession.read.parquet("data/dict/geo_dict/output")

        df.show(10, truncate = false)

        sparkSession.close()

    }

}
