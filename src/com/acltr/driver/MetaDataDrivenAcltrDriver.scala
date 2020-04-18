package com.acltr.driver

import java.io.FileInputStream
import java.util._;
import java.io._;
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import com.acltr.utils.ConfigObject
import com.acltr.utils.SetUpConfiguration
import com.acltr.utils.PropertiesObject
import com.acltr.utils.Utilities
import com.acltr.utils.ControllerObject
import com.acltr.controllers.PipeLineController


object MetaDataDrivenAcltrDriver {

  val log = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    if (args == null || args.isEmpty || args.length != 4) {
      log.error("Invalid number of arguments passed.")
      log.error("Arguments Usage: <Properties File path> <Src File Directory> <File Processing> <Load Type> <Batch ID(Optional)>")
      log.error("Stopping the flow")
      System.exit(1)
    }

    val propertiesFilePath: String = String.valueOf(args(0).trim())
    val srcFileDirectory: String = String.valueOf(args(1).trim())
    val fileProcessingtype: String = String.valueOf(args(2).trim())
    val loadType: String = String.valueOf(args(3).trim())
    var loadBatchID: String = null
    if (args.length == 5) {
      loadBatchID = String.valueOf(args(4).trim())
    }
    val configObject: ConfigObject = SetUpConfiguration.setup()
    val propertiesObject: PropertiesObject = Utilities.getPropertiesobject(propertiesFilePath)
    val batchID: String = propertiesObject.getObjName() + "_" + Utilities.getBatchCurrentTimestamp()
    val audApplicationName: String = propertiesObject.getObjName() + "_" + loadType + "_Load"
    val controllerObject = new ControllerObject(batchID, audApplicationName, srcFileDirectory, fileProcessingtype, propertiesObject, configObject, loadType)
    val pipleController = new PipeLineController(controllerObject)
    pipleController.pipelineControl()

  }
}