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
import com.acltr.utils.RefinedLoadObjects
import com.acltr.utils.EnRLoadObjects
import com.acltr.utils.Utilities
import com.acltr.utils.AuditSQLPropertiesObject
import com.acltr.utils.AuditLoadObject

object EnRDataPipeLineProcessor {
  val log = Logger.getLogger(getClass.getName)
  def enrDataPipelineInit(ctrlObject: ControllerObject, refLoadObject: RefinedLoadObjects): EnRLoadObjects = {

    var enrLoadObjects = new EnRLoadObjects(false)
    try {
      var enrTblName = ctrlObject.getPropertieObject().getDbName() + "." + ctrlObject.getPropertieObject().getTgtTblEnr().trim()
      var enrDataFrame = ctrlObject.configObject.getSpark().sql(f"""select * from $enrTblName limit 0""")
      var enrSchema = enrDataFrame.schema
      var colList = enrDataFrame.columns
      val sparkSession = ctrlObject.getConfigObject().getSpark()
      import org.apache.spark.sql.functions.udf
      import org.apache.spark.sql.functions.{ lit, max, row_number }
      import sparkSession.implicits._
      import org.apache.spark.sql.Row

      log.info("##############################################Inside EnR Data Pipeline Init##############################################")
      if (refLoadObject.getRefineInitStatus() == false) {
        var refTblName = ctrlObject.getPropertieObject().getDbName() + "." + ctrlObject.getPropertieObject().getTgtTblRef().trim()
        enrDataFrame = ctrlObject.configObject.getSpark().sql(f"""select * from $refTblName""")
      } else {
        var refTblName = ctrlObject.getPropertieObject().getDbName() + "." + ctrlObject.getPropertieObject().getTgtTblRef().trim()
        enrDataFrame = ctrlObject.configObject.getSpark().sql(f"""select * from $refTblName""")
      }
      val srcCount = enrDataFrame.count()
      import scala.collection.mutable.LinkedHashMap
      var linkedHashMap: LinkedHashMap[String, String] = new LinkedHashMap()
      val sqlPropertiesObject: AuditSQLPropertiesObject = Utilities.getSQLPropertiesObject(ctrlObject.getPropertieObject().getSqlPropertyPath())
      val sqlCon: Connection = Utilities.getConnection(sqlPropertiesObject)
      val fullCount = enrDataFrame.count()
      var dtRules = Utilities.getTransformation(sqlCon, ctrlObject.getPropertieObject().getObjName())
      sqlCon.close()
      if (dtRules.trim().length > 0) {
        var dt = dtRules.split("\\#", -1).toList
        var transformedDF = Utilities.prepareEnr(enrDataFrame, dt, ctrlObject)
        transformedDF = transformedDF.select(colList.head, colList.tail: _*)
        val enrtransformedCount = transformedDF.count()
        enrLoadObjects = new EnRLoadObjects(ctrlObject.getPropertieObject(), transformedDF, srcCount, enrtransformedCount, true)
      } else {
        var transformedDF = enrDataFrame
        var enrtransformedCount = srcCount
        enrLoadObjects = new EnRLoadObjects(ctrlObject.getPropertieObject(), transformedDF, srcCount, enrtransformedCount, true)
      }
      return enrLoadObjects
    } catch {
      case t: Throwable => t.printStackTrace(); return enrLoadObjects
    }
  }

  def enrDataLoad(enrLoadObjects: EnRLoadObjects, ctrlObject: ControllerObject): Boolean =
    {
      log.info("##############################################Inside EnR Data Pipeline Load##############################################")
      val enrTable = ctrlObject.getPropertieObject().getDbName() + "." + ctrlObject.getPropertieObject().getTgtTblEnr()
      val enrLoadStatus = Utilities.storeDataFrame(enrLoadObjects.getEnrPreparedDF(), "overwrite", "ORC", enrTable)
      //val enrLoadStatus=Utilities.storePartitonedDataFrame(enrLoadObjects.getEnrPreparedDF(), "overwrite", ctrlObject.getPropertieObject().unqKeyCols.split("\\|").toList, "ORC", enrTable, ctrlObject.getConfigObject())
      return enrLoadStatus
    }

  def enrDataClose(ctrlObject: ControllerObject, auditObject: AuditLoadObject) {
    log.info("##############################################Inside EnR Data Pipeline CLOSE##############################################")
    val sqlPropertiesObject: AuditSQLPropertiesObject = Utilities.getSQLPropertiesObject(ctrlObject.getPropertieObject().getSqlPropertyPath())
    val sqlCon: Connection = Utilities.getConnection(sqlPropertiesObject)
    Utilities.updatetoAudit(sqlCon, auditObject, sqlPropertiesObject.getMySqlAuditTbl())
    sqlCon.close()
  }
}