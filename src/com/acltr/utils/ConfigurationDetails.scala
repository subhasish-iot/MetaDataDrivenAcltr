package com.acltr.utils

import java.text.SimpleDateFormat
import java.sql.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration

trait ConfigurationDetails {
  val MASTER = "local"
  val APPLICATION_NAME = "kafka_spark_streaming"
  val SPARK_EXECUTOR_CORES = "20"
  val SPARK_EXECUTOR_MEMORY = "10g"
  val SPARK_NETWORK_TIMOUT_TS = "800"
  val DB_NAME = ""
  val JDBC_URL = ""
  val LOG_FILE_LOC = "/home/application_name/log/"
  val SPARK_SQL_BROADCAST_TIMEOUT = "600"
  val FORMAT_TS = new SimpleDateFormat("MMddyyyyhhmmssSSS")
  val FORMAT_JOB_TS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val FILESYSTEM = FileSystem.get(new Configuration)
}