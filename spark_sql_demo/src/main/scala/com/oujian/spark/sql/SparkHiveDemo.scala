package com.oujian.spark.sql


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkHiveDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql")
    conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.compression.lzo.LzopCodec")
    conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec")
    val session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val frame = session.sql("select count(*) from gmall.ods_start_log")
    frame.show(100)

  }
}
