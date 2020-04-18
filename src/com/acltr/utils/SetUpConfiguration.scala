package com.acltr.utils

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import java.math.BigInteger
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.Subscribe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.catalyst.plans.logical.With

object SetUpConfiguration extends ConfigurationDetails {

  /**
   * Function to set the Spark Context objects
   *
   * @return Configuration Object
   */
  def getSparkContext(): ConfigObject = {
    try {
      val spark = SparkSession.builder().enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").config("spark.sql.hive.convertMetastoreOrc", "false").getOrCreate()
      // val spark:SparkSession=SparkSession.builder().appName(APPLICATION_NAME).enableHiveSupport().getOrCreate()

      val configObject = new ConfigObject(spark)
      return configObject
    } catch {
      case textError: Throwable =>
        textError.printStackTrace() // TODO: handle error
        //logger.error("Could Not create the sparkConf,sparkContext,sqlContext,hiveContext")
        //logger.debug(textError)
        sys.exit(1)
    }
  }

  /**
   * Function to set the Spark related properties
   *
   * @param configObject: Configuration Object
   */
  def Setproperties(configObject: ConfigObject) {
    // logger.info("Setting up the properties for SparkContext")
    /*configObject.spark.conf.set("spark.executor.cores", SPARK_EXECUTOR_CORES)
configObject.spark.conf.set("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
configObject.spark.conf.set("spark.network.timeout", SPARK_NETWORK_TIMOUT_TS)
configObject.spark.conf.set("spark.sql.broadcastTimeout", SPARK_SQL_BROADCAST_TIMEOUT)
configObject.spark.conf.set("spark.sql.tungsten.enabled", "true")
configObject.spark.conf.set("spark.eventLog.enabled", "true")*/
    configObject.spark.conf.set("spark.io.compression.codec", "snappy")
    configObject.spark.conf.set("spark.rdd.compress", "true")
    configObject.spark.conf.set("spark.dynamicAllocation.enabled", "true")
    configObject.spark.conf.set("spark.shuffle.service.enabled", "true")
    configObject.spark.sqlContext.setConf("hive.exec.dynamic.partition", "true");
    configObject.spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
    configObject.spark.sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "false");
    configObject.spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "10000")
    configObject.spark.conf.set("spark.streaming.concurrentJobs", "6")
    configObject.spark.conf.set("spark.streaming.unpersist", "true")
    configObject.spark.conf.set("spark.streaming.backpressure.enabled", "true")
    configObject.spark.conf.set("spark.cores.max", "50")
    configObject.spark.conf.set("spark.executor.instances", "4")
    configObject.spark.conf.set("spark.executor.cores", "10")
    configObject.spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    configObject.spark.conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    configObject.spark.conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")

  }

  /**
   * setup method invokes other methods for setup before job processing starts
   * @return : Configuration Object
   */
  def setup(): ConfigObject = {
    val config = SetUpConfiguration.getSparkContext()
    SetUpConfiguration.Setproperties(config)
    return config
  }

}
