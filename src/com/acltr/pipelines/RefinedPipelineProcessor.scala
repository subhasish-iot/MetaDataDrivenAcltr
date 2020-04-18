package com.acltr.pipelines

import org.apache.log4j.Logger
import java.util.ArrayList
import java.util.LinkedHashSet
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import java.text.SimpleDateFormat
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{ StructType, StructField, StringType };
import scala.collection.mutable.ListBuffer
import java.sql.Connection
import org.apache.spark.sql.Row
import com.acltr.utils.ControllerObject
import com.acltr.utils.RawLoadObjects
import com.acltr.utils.RefinedLoadObjects
import com.acltr.utils.AuditSQLPropertiesObject
import com.acltr.utils.Utilities
import com.acltr.utils.DataQuality
import com.acltr.utils.AuditLoadObject

object RefineDataPipelineProcessor {
  val log = Logger.getLogger(getClass.getName)
  def refDataPipelineInit(ctrlObject: ControllerObject, rawLoadObject: RawLoadObjects): RefinedLoadObjects = {

    var refinedLoadObjects = new RefinedLoadObjects(false)
    try {
      var refinedTblName = ctrlObject.getPropertieObject().getDbName() + "." + ctrlObject.getPropertieObject().getTgtTblRef().trim()
      var refinedDataFrame = ctrlObject.configObject.getSpark().sql(f"""select * from $refinedTblName limit 0""")
      var refinedSchema = refinedDataFrame.schema
      var colList = refinedDataFrame.columns
      val sparkSession = ctrlObject.getConfigObject().getSpark()
      import org.apache.spark.sql.functions.udf
      import org.apache.spark.sql.functions.{ lit, max, row_number }
      import sparkSession.implicits._
      import org.apache.spark.sql.Row

      log.info("##############################################Inside Refined Data Pipeline Init##############################################")
      if (rawLoadObject.getThresholdstatus() == false) {
        refinedDataFrame = ctrlObject.configObject.getSpark().read.json(ctrlObject.getPropertieObject().getTgtTblRw())
        refinedDataFrame = refinedDataFrame.select(colList.head, colList.tail: _*)
      } else {
        refinedDataFrame = rawLoadObject.getRawvalidDF()
        refinedDataFrame = refinedDataFrame.select(colList.head, colList.tail: _*)
      }
      import scala.collection.mutable.LinkedHashMap
      var linkedHashMap: LinkedHashMap[String, String] = new LinkedHashMap()

      val sqlPropertiesObject: AuditSQLPropertiesObject = Utilities.getSQLPropertiesObject(ctrlObject.getPropertieObject().getSqlPropertyPath())
      val sqlCon: Connection = Utilities.getConnection(sqlPropertiesObject)
      val fullCount = refinedDataFrame.count()
      var dqRules = Utilities.getDQMapping(sqlCon, ctrlObject.getPropertieObject().getObjName())
      sqlCon.close()
      if (dqRules.trim().length > 0) {
        var dq = dqRules.split("\\,", -1)
        dq.foreach(x =>
          linkedHashMap.put(x.split("\\|", -1)(0), x.split("\\|", -1)(1)))
        var schema = StructType(colList.map(fieldName => StructField(fieldName, StringType, true)))
        var refinedRDD = refinedDataFrame.rdd.map(row => DataQuality.DQValidchck(row, linkedHashMap))
        refinedSchema = schema.add("ERROR_COL", StringType, true).add("ERROR_DESCRIPTION", StringType, true)
        var newrefinedDataFrame = ctrlObject.getConfigObject().getSpark().createDataFrame(refinedRDD, refinedSchema)

        var validrefinedDataFrame = newrefinedDataFrame.filter(newrefinedDataFrame("ERROR_DESCRIPTION") === "VALID")
        var invalidrefinedDataFrame = newrefinedDataFrame.filter(newrefinedDataFrame("ERROR_DESCRIPTION") =!= "VALID")
        validrefinedDataFrame = validrefinedDataFrame.drop("ERROR_COL", "ERROR_DESCRIPTION")

        val validCount = validrefinedDataFrame.count()
        val invalidCount = invalidrefinedDataFrame.count()
        refinedLoadObjects = new RefinedLoadObjects(ctrlObject.getPropertieObject(), validrefinedDataFrame, invalidrefinedDataFrame, refinedDataFrame, validCount, invalidCount, fullCount, linkedHashMap, true)
      } else {
        var validrefinedDataFrame = refinedDataFrame
        refinedLoadObjects = new RefinedLoadObjects(ctrlObject.getPropertieObject(), refinedDataFrame, null, validrefinedDataFrame, fullCount, 0, fullCount, linkedHashMap, true)
      }
      return refinedLoadObjects
    } catch {
      case t: Throwable => t.printStackTrace(); return refinedLoadObjects
    }
  }

  /**
   * refinedDataLoad method is responsible for data load to refined layer table.
   * @param refinedLoadObject : Refined Load Object
   */
  def refinedDataLoad(refinedLoadObject: RefinedLoadObjects, ctrlObject: ControllerObject): Boolean = {
    log.info("##############################################Inside Refined Data Pipeline LOAD##############################################")
    log.info("Valid count in refined : " + refinedLoadObject.getRefinedInvalidCount())
    log.info("Invalid count in refined : " + refinedLoadObject.getRefinedInvalidCount())
    val sparkSession = ctrlObject.getConfigObject().getSpark()
    import sparkSession.implicits._
    val dateValidator = udf(Utilities.convertDateTime _)
    var finalValidDF = ctrlObject.getConfigObject().getSpark().createDataFrame(ctrlObject.getConfigObject().getSpark().sparkContext.emptyRDD[Row], refinedLoadObject.getRefinedvalidDF().schema)
    val sqlPropertiesObject: AuditSQLPropertiesObject = Utilities.getSQLPropertiesObject(ctrlObject.getPropertieObject().getSqlPropertyPath())
    val sqlCon: Connection = Utilities.getConnection(sqlPropertiesObject)
    var lm: String = Utilities.getDateFormatColumns(sqlCon, ctrlObject.getPropertieObject().getObjName())
    sqlCon.close()
    if (lm.trim.length() > 0) {
      var list = lm.split("\\,", -1).toList
      finalValidDF = list.foldLeft(refinedLoadObject.getRefinedvalidDF()) { (tempDF, listValue) =>
        val key = listValue.split("\\|")(0)
        val value = listValue.split("\\|")(1)
        tempDF.withColumn(key, dateValidator(tempDF(key), lit(value)))
      }
    } else {
      finalValidDF = refinedLoadObject.getRefinedvalidDF()
    }

    val refTable = ctrlObject.getPropertieObject().getDbName() + "." + ctrlObject.getPropertieObject().getTgtTblRef()
    val referrTable = ctrlObject.getPropertieObject().getDbName() + "." + ctrlObject.getPropertieObject().getTgtTblErr()
    log.info("===================valid df=====================" + finalValidDF.show(false))
    val refLoadStatus = Utilities.storeDataFrame(finalValidDF, "overwrite", "ORC", refTable)
    val errorLoadStatus = Utilities.storeDataFrame(refinedLoadObject.getRefinedInvalidDF(), "overwrite", "ORC", referrTable)
    if (refLoadStatus && errorLoadStatus)
      return true
    else
      return false
  }

  /**
   * refinedDataClose method is for closing the connections (if needed) and Auditing
   * @param refinedLoadObject : Refined Load Object
   * @param ctrlObjects : Global Controller Object
   */
  def refinedDataClose(ctrlObject: ControllerObject, auditObject: AuditLoadObject) {
    log.info("##############################################Inside Refined Data Pipeline CLOSE##############################################")
    val sqlPropertiesObject: AuditSQLPropertiesObject = Utilities.getSQLPropertiesObject(ctrlObject.getPropertieObject().getSqlPropertyPath())
    val sqlCon: Connection = Utilities.getConnection(sqlPropertiesObject)
    Utilities.updatetoAudit(sqlCon, auditObject, sqlPropertiesObject.getMySqlAuditTbl())
    sqlCon.close()
  }

}