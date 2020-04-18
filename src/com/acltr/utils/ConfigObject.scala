package com.acltr.utils

import scala.beans.BeanProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.collection.Map
import org.datanucleus.store.types.backed.HashMap
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.LinkedHashMap

class ConfigObject(

  @BeanProperty var spark: SparkSession) extends Serializable

class PropertiesObject(

  @BeanProperty var objName: String,

  @BeanProperty var dbName: String,

  @BeanProperty var NulchkCol: String,

  @BeanProperty var rcdDelimiter: String,

  @BeanProperty var thresholdLimit: String,

  @BeanProperty var srcDataError: String,

  @BeanProperty var tgtTblRw: String,

  @BeanProperty var tgtTblErr: String,

  @BeanProperty var tgtTblRef: String,

  @BeanProperty var tgtTblEnr: String,

  @BeanProperty var tgt_ctrl_tbl: String,

  @BeanProperty var unqKeyCols: String,

  @BeanProperty var srcRDBMSPropertyPath: String,

  @BeanProperty var tgtPropertyPath: String,

  @BeanProperty var sqlPropertyPath: String,

  @BeanProperty var masterDataFields: String,

  @BeanProperty var dqRules: String) extends Serializable

class OptimizedPropertiesObject(

  @BeanProperty var objName: String,

  @BeanProperty var dbName: String,

  @BeanProperty var srcTblnm: String,

  @BeanProperty var tgtTblnm: String,

  @BeanProperty var tgtTblSaveMode: String,

  @BeanProperty var partitionColList: String,

  @BeanProperty var sqlPropertyPath: String) extends Serializable

class SourceRDBMSPropertiesObject(

  @BeanProperty var srcSQLHostName: String,

  @BeanProperty var srcSQLDBName: String,

  @BeanProperty var srcSQLUserName: String,

  @BeanProperty var srcSQLPassword: String,

  @BeanProperty var srcSQLPort: String,

  @BeanProperty var srcSQLAuditTbl: String) extends Serializable

class TgtRDBMSPropertiesObject(

  @BeanProperty var tgtSqlHostName: String,

  @BeanProperty var tgtSqlDBName: String,

  @BeanProperty var tgtSqlUserName: String,

  @BeanProperty var tgtSqlPassword: String,

  @BeanProperty var tgtSqlPort: String,

  @BeanProperty var tgtSqlAuditTbl: String) extends Serializable

class AuditSQLPropertiesObject(

  @BeanProperty var mySqlHostName: String,

  @BeanProperty var mySqlDBName: String,

  @BeanProperty var mySqlUserName: String,

  @BeanProperty var mySqlPassword: String,

  @BeanProperty var mySqlPort: String,

  @BeanProperty var mySqlAuditTbl: String) extends Serializable

class AuditLoadObject(

  @BeanProperty var audBatchId: String,

  @BeanProperty var audApplicationName: String,

  @BeanProperty var audObjectName: String,

  @BeanProperty var audDataLayerName: String,

  @BeanProperty var audJobStatusCode: String,

  @BeanProperty var audJobStartTimeStamp: String,

  @BeanProperty var audJobEndTimestamp: String,

  @BeanProperty var audLoadTimeStamp: String,

  @BeanProperty var audSrcRowCount: Long,

  @BeanProperty var audTgtRowCount: Long,

  @BeanProperty var audErrorRecords: Long,

  @BeanProperty var audCreatedBy: String,

  @BeanProperty var audJobDuration: String) extends Serializable {

}

class ControllerObject(

  @BeanProperty var batchId: String,

  @BeanProperty var applicationName: String,

  @BeanProperty var srcFileDirectory: String,

  @BeanProperty var fileProcessingtype: String,

  @BeanProperty var propertieObject: PropertiesObject,

  @BeanProperty var configObject: ConfigObject,

  @BeanProperty var loadType: String) extends Serializable {

}

class RawLoadObjects(

  @BeanProperty var propertiesObject: PropertiesObject,

  @BeanProperty var rawvalidDF: DataFrame,

  @BeanProperty var rawInvalidDF: DataFrame,

  @BeanProperty var rawFullDF: DataFrame,

  @BeanProperty var rawValidCount: Long,

  @BeanProperty var rawInvalidCount: Long,

  @BeanProperty var rawFullCount: Long,

  @BeanProperty var fileName: String,

  @BeanProperty var thresholdstatus: Boolean) extends Serializable {

  def this(thresholdstatus: Boolean) {
    this(null, null, null, null, 0, 0, 0, null, thresholdstatus)

  }

  def this(propertiesObject: PropertiesObject, fileName: String, thresholdstatus: Boolean) {

    this(propertiesObject, null, null, null, 0, 0, 0, fileName, thresholdstatus)
  }

}

class RefinedLoadObjects(

  @BeanProperty var propertiesObject: PropertiesObject,

  @BeanProperty var refinedvalidDF: DataFrame,

  @BeanProperty var refinedInvalidDF: DataFrame,

  @BeanProperty var refinedFullDF: DataFrame,

  @BeanProperty var refinedValidCount: Long,

  @BeanProperty var refinedInvalidCount: Long,

  @BeanProperty var refinedFullCount: Long,

  @BeanProperty var refinedChecks: LinkedHashMap[String, String],

  @BeanProperty var refineInitStatus: Boolean) extends Serializable {

  def this(refineInitStatus: Boolean) {
    this(null, null, null, null, 0, 0, 0, null, refineInitStatus)
  }

}

class EnRLoadObjects(

  @BeanProperty var propertiesObject: PropertiesObject,

  @BeanProperty var enrPreparedDF: DataFrame,

  @BeanProperty var enrbeginCount: Long,

  @BeanProperty var enrtransformedCount: Long,

  @BeanProperty var enrInitStatus: Boolean) extends Serializable {

  def this(enrInitStatus: Boolean) {

    this(null, null, 0, 0, enrInitStatus)
  }

}