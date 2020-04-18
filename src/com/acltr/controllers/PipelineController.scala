package com.acltr.controllers

import org.apache.log4j.Logger
import java.util.Calendar
import java.sql.Connection
import com.acltr.utils.ControllerObject
import com.acltr.utils.ConfigurationDetails
import com.acltr.utils.RawLoadObjects
import com.acltr.utils.RefinedLoadObjects
import com.acltr.utils.AuditLoadObject
import com.acltr.utils.Utilities
import com.acltr.utils.AuditSQLPropertiesObject
import com.acltr.pipelines.RawDataPipelineProcessor
import com.acltr.pipelines.RefineDataPipelineProcessor
import com.acltr.pipelines.EnRDataPipeLineProcessor
import com.acltr.utils.EnRLoadObjects



class PipeLineController(ctrlObject: ControllerObject) extends ConfigurationDetails {
  val log = Logger.getLogger(getClass.getName)
  val user = ctrlObject.getConfigObject().spark.sparkContext.sparkUser
  var rawLoadObject = new RawLoadObjects(false)
  var refLoadObject = new RefinedLoadObjects(false)
  var auditObj = new AuditLoadObject("", "", "", "", "", "", "", "", 0, 0, 0, "", "")
  def pipelineControl() {
    log.info("Checking for the load type argument based on which pipeline will be triggered.")
    if ("RAW".equalsIgnoreCase(ctrlObject.getLoadType()) || "RAW_REF".equalsIgnoreCase(ctrlObject.getLoadType()) || "RAW_REF_ENR".equalsIgnoreCase(ctrlObject.getLoadType())) { // When Raw to Refined will Run Together
      log.info("Invoking rawLoadPipelineTrigger")
      rawLoadPipelineTrigger()
    } else if ("REF_ENR".equalsIgnoreCase(ctrlObject.getLoadType())) {
      log.info("Invoking refLoadPipelineTrigger")
      refLoadPipeLineTrigger(ctrlObject, rawLoadObject) // This argument should be passed from CLI
    } else if ("ENR".equalsIgnoreCase(ctrlObject.getLoadType())) {
      log.info("Invoking enrLoadPipelineTrigger")
      enrLoadTrigger(ctrlObject, refLoadObject)
    }

  }

  def rawLoadPipelineTrigger() {
    val JOB_START_TS = Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS")
    val sqlPropertiesObject: AuditSQLPropertiesObject = Utilities.getSQLPropertiesObject(ctrlObject.getPropertieObject().getSqlPropertyPath())
    val sqlCon: Connection = Utilities.getConnection(sqlPropertiesObject)
    auditObj.setAudBatchId(ctrlObject.getBatchId())
    auditObj.setAudApplicationName(ctrlObject.getConfigObject().getSpark().sparkContext.appName)
    auditObj.setAudObjectName(ctrlObject.getPropertieObject().getObjName())
    auditObj.setAudDataLayerName("raw_load")
    auditObj.setAudJobStartTimeStamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
    auditObj.setAudLoadTimeStamp("9999-12-31 00:00:00")
    auditObj.setAudJobStatusCode("Running")
    auditObj.setAudSrcRowCount(0)
    auditObj.setAudTgtRowCount(0)
    auditObj.setAudErrorRecords(0)
    auditObj.setAudCreatedBy(ctrlObject.getConfigObject().getSpark().sparkContext.sparkUser)
    try {
      if ("SEQ".equalsIgnoreCase(ctrlObject.getFileProcessingtype())) {

        val fileList = Utilities.sortedFiles(ctrlObject.getConfigObject().getSpark().sparkContext, ctrlObject.getSrcFileDirectory())
        Utilities.insertIntoAudit(sqlCon, auditObj, sqlPropertiesObject.getMySqlAuditTbl())
        sqlCon.close()
        if (fileList.size != 0 || fileList.isEmpty || fileList == null) {
          log.info("Found " + fileList.size + " Files(s) in the incoming folder :" + ctrlObject.getSrcFileDirectory())
          fileList.toList.foreach { fileName =>
            log.info("Starting Raw Load for File : " + fileName)
            var rawLoadObject = RawDataPipelineProcessor.rawDataPipelineInit(ctrlObject, fileName)
            rawLoadPipeline(rawLoadObject, auditObj)
          }

        } else {
          log.info("No Files Found in the incoming folder")
          System.exit(1)
        }
      } else {
        val fileList = Utilities.sortedFiles(ctrlObject.getConfigObject().getSpark().sparkContext, ctrlObject.getSrcFileDirectory())
        if (fileList.size != 0 || fileList.isEmpty || fileList == null) {
          Utilities.insertIntoAudit(sqlCon, auditObj, sqlPropertiesObject.getMySqlAuditTbl())
          sqlCon.close()
          var rawLoadObject = RawDataPipelineProcessor.rawDataPipelineInit(ctrlObject, ctrlObject.getSrcFileDirectory())
          rawLoadPipeline(rawLoadObject, auditObj)
        } else {
          log.info("No Files Found in the incoming folder")
          System.exit(1)
        }

      }
    } catch {
      case t: Throwable => t.printStackTrace(); sqlCon.close();
    }
  }

  def rawLoadPipeline(rawLoadObject: RawLoadObjects, auditObject: AuditLoadObject) {
    var tgtCount: Long = 0
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    if (rawLoadObject.thresholdstatus == false && rawLoadObject.rawValidCount == 0 && rawLoadObject.rawInvalidCount == 0) {

      auditObj.setAudJobStatusCode("failed-Error Ocuured while processing")
      auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
      auditObj.setAudJobDuration((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString())
      RawDataPipelineProcessor.rawDataPipelineClose(ctrlObject, auditObject)
    } else if (rawLoadObject.thresholdstatus == false && (rawLoadObject.rawValidCount > 0 || rawLoadObject.rawInvalidCount > 0)) {
      log.info("THRESHOLD NOT MET")
      val fileStatus = "NOT LOADED"
      val errorDesc = "THRESHOLD NOT MET"

      auditObj.setAudJobStatusCode("failed-Threshold No Met")
      auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
      auditObj.setAudJobDuration((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString())
      RawDataPipelineProcessor.rawDataPipelineClose(ctrlObject, auditObject)
    } else {
      var fileStatus = "Failed"
      log.info("STARTING TO LOAD FILE TO RAW LAYER TABLE")

      val loadStatus = RawDataPipelineProcessor.rawDataLoad(ctrlObject, rawLoadObject)
      if (loadStatus) {
        fileStatus = "LOADED"
        auditObject.setAudSrcRowCount(rawLoadObject.getRawFullCount())
        auditObject.setAudTgtRowCount(rawLoadObject.rawValidCount)
        auditObject.setAudErrorRecords(rawLoadObject.getRawInvalidCount())
      } else {
        fileStatus = "NOT LOADED-ERROR WRITING TO HIVE TABLE"
        auditObject.setAudSrcRowCount(rawLoadObject.getRawFullCount())
        auditObject.setAudTgtRowCount(0)
        auditObject.setAudErrorRecords(rawLoadObject.getRawInvalidCount())
      }
      auditObj.setAudJobStatusCode(fileStatus)
      auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
      auditObject.setAudLoadTimeStamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
      auditObj.setAudJobDuration((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString())
      RawDataPipelineProcessor.rawDataPipelineClose(ctrlObject, auditObject)

      if (loadStatus) {
        if (("RAW_REF".equalsIgnoreCase(ctrlObject.getLoadType())) || ("RAW_REF_ENR".equalsIgnoreCase(ctrlObject.getLoadType())))
          refLoadPipeLineTrigger(ctrlObject, rawLoadObject)
      }
    }
  }

  def refLoadPipeLineTrigger(ctrlObject: ControllerObject, rawLoadObject: RawLoadObjects) {

    log.info("The Node name for refined trigger")
    val sqlPropertiesObject: AuditSQLPropertiesObject = Utilities.getSQLPropertiesObject(ctrlObject.getPropertieObject().getSqlPropertyPath())
    val sqlCon: Connection = Utilities.getConnection(sqlPropertiesObject)
    auditObj.setAudApplicationName(ctrlObject.getConfigObject().getSpark().sparkContext.appName)
    auditObj.setAudObjectName(ctrlObject.getPropertieObject().getObjName())
    auditObj.setAudDataLayerName("ref_load")
    auditObj.setAudJobStartTimeStamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
    auditObj.setAudLoadTimeStamp("9999-12-31 00:00:00")
    auditObj.setAudJobStatusCode("Running")
    auditObj.setAudSrcRowCount(0)
    auditObj.setAudTgtRowCount(0)
    auditObj.setAudErrorRecords(0)
    auditObj.setAudJobDuration("0")
    auditObj.setAudCreatedBy(ctrlObject.getConfigObject().getSpark().sparkContext.sparkUser)
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    try {
      if (("RAW_REF".equalsIgnoreCase(ctrlObject.getLoadType())) || ("RAW_REF_ENR".equalsIgnoreCase(ctrlObject.getLoadType()))) {
        log.info("STARTING TO LOAD TABLES DATA FROM RAW LAYER TO REFINED LAYER")
        Utilities.insertIntoAudit(sqlCon, auditObj, sqlPropertiesObject.getMySqlAuditTbl())
        sqlCon.close()
        val refLoadObject = RefineDataPipelineProcessor.refDataPipelineInit(ctrlObject, rawLoadObject)
        if (refLoadObject.getRefineInitStatus()) {
          refLoadPipeline(refLoadObject, ctrlObject, auditObj)
        } else {
          auditObj.setAudJobStatusCode("failed-Error Ocuured while processing")
          auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
          auditObj.setAudJobDuration((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString())
          RefineDataPipelineProcessor.refinedDataClose(ctrlObject, auditObj)
        }
      } else if ("REF".equalsIgnoreCase(ctrlObject.getLoadType()) || "REF_ENR".equalsIgnoreCase(ctrlObject.getLoadType())) // All Raw table to ref
      {

        log.info("STARTING TO LOAD ALL TABLES' DATA FROM RAW LAYER TO REFINED LAYER")
        Utilities.insertIntoAudit(sqlCon, auditObj, sqlPropertiesObject.getMySqlAuditTbl())
        sqlCon.close()
        val refLoadObject = RefineDataPipelineProcessor.refDataPipelineInit(ctrlObject, rawLoadObject)
        if (refLoadObject.getRefineInitStatus()) {
          refLoadPipeline(refLoadObject, ctrlObject, auditObj)
        } else {
          auditObj.setAudJobStatusCode("failed-Error Ocuured while processing")
          auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
          auditObj.setAudJobDuration((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString())
          RefineDataPipelineProcessor.refinedDataClose(ctrlObject, auditObj)
        }

      }
    } catch {
      case t: Throwable => t.printStackTrace(); sqlCon.close(); // TODO: handle error
    }
  }
  def refLoadPipeline(refLoadObject: RefinedLoadObjects, ctrlObject: ControllerObject, auditObj: AuditLoadObject) {
    val loadStatus = RefineDataPipelineProcessor.refinedDataLoad(refLoadObject, ctrlObject)
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var fileStatus = "Failed"
    if (loadStatus) {
      fileStatus = "LOADED"
      auditObj.setAudSrcRowCount(refLoadObject.getRefinedFullCount())
      auditObj.setAudTgtRowCount(refLoadObject.getRefinedValidCount())
      auditObj.setAudErrorRecords(refLoadObject.getRefinedInvalidCount())
    } else {
      fileStatus = "NOT LOADED-ERROR WRITING TO HIVE TABLE"
      auditObj.setAudSrcRowCount(refLoadObject.getRefinedFullCount())
      auditObj.setAudTgtRowCount(0)
      auditObj.setAudErrorRecords(refLoadObject.getRefinedInvalidCount())
    }
    auditObj.setAudJobStatusCode(fileStatus)
    auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
    auditObj.setAudLoadTimeStamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
    auditObj.setAudJobDuration((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString())
    RefineDataPipelineProcessor.refinedDataClose(ctrlObject, auditObj)
    if (loadStatus) {
      enrLoadTrigger(ctrlObject, refLoadObject)
    }
  }

  def enrLoadTrigger(ctrlObject: ControllerObject, refLoadObject: RefinedLoadObjects) {
    log.info("The Node name for EnR trigger")
    val sqlPropertiesObject: AuditSQLPropertiesObject = Utilities.getSQLPropertiesObject(ctrlObject.getPropertieObject().getSqlPropertyPath())
    val sqlCon: Connection = Utilities.getConnection(sqlPropertiesObject)
    auditObj.setAudApplicationName(ctrlObject.getConfigObject().getSpark().sparkContext.appName)
    auditObj.setAudObjectName(ctrlObject.getPropertieObject().getObjName())
    auditObj.setAudDataLayerName("enr_load")
    auditObj.setAudJobStartTimeStamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
    auditObj.setAudLoadTimeStamp("9999-12-31 00:00:00")
    auditObj.setAudJobStatusCode("Running")
    auditObj.setAudSrcRowCount(0)
    auditObj.setAudTgtRowCount(0)
    auditObj.setAudErrorRecords(0)
    auditObj.setAudJobDuration("0")
    auditObj.setAudCreatedBy(ctrlObject.getConfigObject().getSpark().sparkContext.sparkUser)
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    try {
      if (("RAW_REF_ENR".equalsIgnoreCase(ctrlObject.getLoadType())) || ("REF_ENR".equalsIgnoreCase(ctrlObject.getLoadType()) || "ENR".equalsIgnoreCase(ctrlObject.getLoadType()))) {
        log.info("STARTING TO LOAD TABLES DATA FROM REF LAYER TO ENR LAYER")
        Utilities.insertIntoAudit(sqlCon, auditObj, sqlPropertiesObject.getMySqlAuditTbl())
        sqlCon.close()
        val enrLoadObject = EnRDataPipeLineProcessor.enrDataPipelineInit(ctrlObject, refLoadObject)
        if (enrLoadObject.getEnrInitStatus()) {
          enrLoadPipeline(enrLoadObject, ctrlObject, auditObj)
        } else {
          auditObj.setAudJobStatusCode("failed-Error Ocuured while processing")
          auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
          auditObj.setAudJobDuration((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString())
          EnRDataPipeLineProcessor.enrDataClose(ctrlObject, auditObj)
        }
      }

    } catch {
      case t: Throwable => t.printStackTrace(); sqlCon.close(); // TODO: handle error
    }
  }

  def enrLoadPipeline(enrLoadObject: EnRLoadObjects, ctrlObject: ControllerObject, auditObj: AuditLoadObject) {
    val loadStatus = EnRDataPipeLineProcessor.enrDataLoad(enrLoadObject, ctrlObject)
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var fileStatus = "Failed"
    if (loadStatus) {
      fileStatus = "LOADED"
      auditObj.setAudSrcRowCount(enrLoadObject.getEnrbeginCount())
      auditObj.setAudTgtRowCount(enrLoadObject.getEnrtransformedCount())
      auditObj.setAudErrorRecords(0)
    } else {
      fileStatus = "NOT LOADED-ERROR WRITING TO HIVE TABLE"
      auditObj.setAudSrcRowCount(enrLoadObject.getEnrbeginCount())
      auditObj.setAudTgtRowCount(0)
      auditObj.setAudErrorRecords(0)
    }
    auditObj.setAudJobStatusCode(fileStatus)
    auditObj.setAudJobEndTimestamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
    auditObj.setAudLoadTimeStamp(Utilities.getCurrentTimestamp("yyyy-MM-dd HH:mm:ss.SSS"))
    auditObj.setAudJobDuration((format.parse(auditObj.getAudJobEndTimestamp()).getTime - format.parse(auditObj.getAudJobStartTimeStamp()).getTime).toString())
    EnRDataPipeLineProcessor.enrDataClose(ctrlObject, auditObj)
  }
}