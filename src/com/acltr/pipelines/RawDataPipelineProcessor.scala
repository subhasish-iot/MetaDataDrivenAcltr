package com.acltr.pipelines


import org.apache.log4j.Logger
import java.sql.Connection
import scala.collection.Map
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ lit, col, concat_ws, split, struct }
import org.apache.spark.sql.catalyst.expressions.Explode
import org.apache.spark.sql.types.{ StructType, StructField, StringType };
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions.{ lower, upper }
import scala.collection.Map
import org.apache.spark.sql.functions.col
import java.util.regex.Pattern
import org.apache.spark.sql.functions.{ input_file_name, regexp_extract }
import com.acltr.utils.ConfigurationDetails
import com.acltr.utils.RawLoadObjects
import com.acltr.utils.ControllerObject
import com.acltr.utils.Utilities
import com.acltr.controllers.PipeLineController
import com.acltr.utils.AuditSQLPropertiesObject
import com.acltr.utils.AuditLoadObject


object RawDataPipelineProcessor extends ConfigurationDetails {

  val log = Logger.getLogger(getClass.getName)
  var rawDataLoadObjects = new RawLoadObjects(false)

  def rawDataPipelineInit(ctrlObject: ControllerObject, srcName: String): RawLoadObjects = {
    val piplinController = new PipeLineController(ctrlObject)
    var rawDataLoadObjects = new RawLoadObjects(false)
    var path = "";
    val sqlPropertiesObject: AuditSQLPropertiesObject = Utilities.getSQLPropertiesObject(ctrlObject.getPropertieObject().getSqlPropertyPath())
    val sqlCon: Connection = Utilities.getConnection(sqlPropertiesObject)
    try {
      if ("SEQ".equalsIgnoreCase(ctrlObject.getLoadType())) {
        path = ctrlObject.getSrcFileDirectory() + "/" + srcName
      } else {
        path = srcName
      }

      val delimiter = ctrlObject.getPropertieObject().getRcdDelimiter()
      val orcRawTable = ctrlObject.getPropertieObject().getTgtTblRw()
      val thresholdLimit = ctrlObject.getPropertieObject().getThresholdLimit()
      val mappingData = Utilities.getSourceColumnMapping(sqlCon, "metadatadrivenacltr_srcfiles_header_json_tags", ctrlObject.getPropertieObject().getObjName())
      var schema = StructType(mappingData.split("\\,").map(fieldName => StructField(fieldName.split("\\|")(1), StringType, true)))
      log.info("======================" + schema)
      //var collection_df = ctrlObject.getConfigObject().getSpark().read.option("header", "true").option("quote", "\"").schema(schema).csv("/user/headwise_collection/")
      //log.info("====================RawData Count========================"+collection_df.show(false))
      var rawDF = ctrlObject.getConfigObject().getSpark().read.option("header", true).text(path)
      log.info("Raw File Records " + rawDF.show(20))
      val rawDataCount = rawDF.count()
      val rawTblSchema = schema
      val sparkSession = ctrlObject.getConfigObject().getSpark()
      import org.apache.spark.sql.functions.udf
      import org.apache.spark.sql.functions.{ lit, max, row_number }
      import sparkSession.implicits._
      import org.apache.spark.sql.Row
      val recdValidator = udf(Utilities.dataValidator _)
      val rawCheckedDF = rawDF.withColumn("flag", recdValidator(rawDF("value"), lit(delimiter), lit(rawTblSchema.length)))
      val rawValidRecordDF = rawCheckedDF.filter(rawCheckedDF("flag") === "V").drop("flag")
      log.info("Raw valid Records " + rawValidRecordDF.show(20))
      val rawInvalidDF = rawCheckedDF.filter(rawCheckedDF { "flag" } === "I" || rawCheckedDF { "flag" } === "F").drop("flag").withColumn("fl_nm", input_file_name()).withColumn("batch_id", lit(ctrlObject.getBatchId()))
      val rawValidRecordCount = rawValidRecordDF.count()
      val rawInvalidRecordCount = rawInvalidDF.count()
      log.info("Count of Valid Records : " + rawValidRecordCount + " and Invalid Records : " + rawInvalidRecordCount)
      val rawValidRecordRDD = rawValidRecordDF.rdd.map { row: Row => (row.getString(0)) }.map(line => line.split(delimiter + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)).map(line => Row.fromSeq(line))
      val rawvalidDF = ctrlObject.configObject.getSpark().createDataFrame(rawValidRecordRDD, rawTblSchema).withColumn("fl_nm", input_file_name()).withColumn("batch_id", lit(ctrlObject.getBatchId()))
      log.info("======================================" + rawvalidDF.show())
      log.info("======================================" + rawInvalidDF.show())
      rawDF = rawDF.withColumn("fl_nm", input_file_name()).withColumn("batch_id", lit(ctrlObject.getBatchId()))
      val thresholdStatus = Utilities.thresholdLimitStatus(thresholdLimit.toInt, rawDataCount, rawValidRecordCount)
      rawDataLoadObjects = new RawLoadObjects(ctrlObject.getPropertieObject(), rawvalidDF, rawInvalidDF, rawDF, rawValidRecordCount, rawInvalidRecordCount, rawDataCount, path, thresholdStatus)
      sqlCon.close()
      return rawDataLoadObjects
    } catch {
      case t: Throwable =>
        t.printStackTrace(); sqlCon.close();
        return rawDataLoadObjects
      // TODO: handle error
    }

  }

  def rawDataLoad(ctrlObject: ControllerObject, rawLoadObject: RawLoadObjects): Boolean = {
    log.info("##############################################Inside Raw Data Pipeline LOAD##############################################")
    if (rawLoadObject.thresholdstatus) {
      val loadStatus = Utilities.storeAsJson(rawLoadObject.getRawvalidDF(), "overwrite", "orc", ctrlObject.getPropertieObject().getTgtTblRw())
      val writeStatus = Utilities.saveRejectsToFile(rawLoadObject.getRawInvalidDF(), "append", ctrlObject.getPropertieObject().getRcdDelimiter(), ctrlObject.getPropertieObject().getSrcDataError())
      return loadStatus
    } else {
      val writeStatus = Utilities.saveRejectsToFile(rawLoadObject.getRawFullDF(), "append", ctrlObject.getPropertieObject().getRcdDelimiter(), ctrlObject.getPropertieObject().getSrcDataError())
      return writeStatus
    }
  }

  def rawDataPipelineClose(ctrlObject: ControllerObject, auditObject: AuditLoadObject) {
    val sqlPropertiesObject: AuditSQLPropertiesObject = Utilities.getSQLPropertiesObject(ctrlObject.getPropertieObject().getSqlPropertyPath())
    val sqlCon: Connection = Utilities.getConnection(sqlPropertiesObject)
    Utilities.updatetoAudit(sqlCon, auditObject, sqlPropertiesObject.getMySqlAuditTbl())
    sqlCon.close()
  }

}