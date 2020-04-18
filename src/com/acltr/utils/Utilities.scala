package com.acltr.utils

import org.apache.log4j.Logger

import util.control.Breaks._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ udf, lit, col, broadcast }
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.DateTime
import java.util.HashMap
import scala.collection.mutable.Map
import org.apache.spark.streaming.kafka010.Subscribe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import scala.util.control.Breaks.break
import scala.sys.process._
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InvalidInputException
import scala.collection.JavaConverters._
import scala.collection.mutable._
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import java.time
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.DataFrame
import javax.xml.crypto.Data
import org.apache.spark.rdd.RDD
import java.util.Calendar
import org.apache.commons.lang3.StringUtils
import java.sql.Connection
import java.sql.DriverManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DateType
import java.util.Properties
import java.io.FileReader
import org.apache.hadoop.fs.FSDataInputStream
import java.io.FileNotFoundException
import java.util.ArrayList
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameWriter
import java.nio.ByteBuffer
import scala.Byte
import java.math.BigInteger
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lit
import scala.io.Source
import scala.util.control.Exception.Catch
import java.io.IOException
import java.io.FileInputStream
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import com.mysql.jdbc._
import java.sql.Statement
import java.sql.ResultSet
import java.util.regex.Pattern
import org.apache.spark.sql.SparkSession


object Utilities {
  val log = Logger.getLogger(getClass.getName)
  val ISOFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  /**
   * removeDuplicates function removes the duplicate rows from a DataFrame.
   *
   *
   * @param DataFrame: Input DataFrame
   * @return DataFrame : DataFrame with unique rows
   */
  def removeDuplicates(inputDF: DataFrame): DataFrame = {
    log.info("Removing duplicate rows")
    val columnList = inputDF.drop("intgtn_fbrc_msg_id").drop("ins_gmt_ts").columns
    val de_dup_DF = inputDF.dropDuplicates(columnList)
    log.info("Count After Dropping the Duplicate Rows :" + de_dup_DF.count())
    return de_dup_DF
  }

  /**
   * @param str
   * @return
   */

  /* def getLocalPropeties(str:String):PropertiesObject =
{
var properties:Properties= new Properties
var reader:FileReader=new FileReader("C:\\Personalized\\metadata.properties");
properties.load(reader);
reader.close();
return null
}*/

  def getPropertiesobject(str: String): PropertiesObject =
    {

      import scala.collection.JavaConversions._
      val propfileSystem: FileSystem = FileSystem.get(new Configuration)
      val propFileInputStream = propfileSystem.open(new Path(str))
      var properties: Properties = new Properties();
      properties.load(propFileInputStream);
      // var properties: Properties = null
      var pObj: PropertiesObject = null
      log.info("++++++++++++++++++++++++++" + str)
      try {
        // properties.load(new FileInputStream(str))
        //val path = getClass.getResource(str)
        log.info("########################################Utilities::::::::::::::" + str.toString())
        if (str != null) {
          /*val source = Source.fromURL(path)
properties = new Properties()
properties.load(source.bufferedReader())*/
          pObj = new PropertiesObject(
            properties.getProperty("objName"),
            properties.getProperty("dbName"),
            properties.getProperty("NulchkCol"),
            properties.getProperty("rcdDelimiter"),
            properties.getProperty("thresholdLimit"),
            properties.getProperty("srcDataError"),
            properties.getProperty("tgtTblRw"),
            properties.getProperty("tgtTblErr"),
            properties.getProperty("tgtTblRef"),
            properties.getProperty("tgtTblEnr"),
            properties.getProperty("tgt_ctrl_tbl"),
            properties.getProperty("unqKeyCols"),
            properties.getProperty("srcRDBMSPropertyPath"),
            properties.getProperty("tgtPropertyPath"),
            properties.getProperty("sqlPropertyPath"),
            properties.getProperty("masterDataFields"),
            properties.getProperty("dqRules"))
        }
        return pObj
      } catch {
        case ex: FileNotFoundException => return null
        case ex: IOException           => return null

      }

    }

  def getOptimizedPropertiesobject(str: String): OptimizedPropertiesObject =
    {

      import scala.collection.JavaConversions._
      val propfileSystem: FileSystem = FileSystem.get(new Configuration)
      val propFileInputStream = propfileSystem.open(new Path(str))
      var properties: Properties = new Properties();
      properties.load(propFileInputStream);
      // var properties: Properties = null
      var pObj: OptimizedPropertiesObject = null
      log.info("++++++++++++++++++++++++++" + str)
      try {
        log.info("########################################Utilities::::::::::::::" + str.toString())
        if (str != null) {

          pObj = new OptimizedPropertiesObject(
            properties.getProperty("objName"),
            properties.getProperty("dbName"),
            properties.getProperty("srcTblnm"),
            properties.getProperty("tgtTblnm"),
            properties.getProperty("tgtTblSaveMode"),
            properties.getProperty("partitionColList"),
            properties.getProperty("sqlPropertyPath"))
        }
        return pObj
      } catch {
        case ex: FileNotFoundException => return null
        case ex: IOException           => return null

      }

    }

  def convertDateTime(dt: String, fmt: String): String = {
    try {
      val dateFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern(fmt)
      val jodatime: DateTime = dateFormatGeneration.parseDateTime(dt);
      val str: String = ISOFormatGeneration.print(jodatime);
      str
    } catch {
      case xe: IllegalArgumentException => return null
      case xe: NullPointerException     => return null
    }
  }

  def sequenceIDGenerator(str: String): Long = {
    import java.nio.ByteBuffer
    import java.math.BigInteger
    val buffer: ByteBuffer = ByteBuffer.wrap(Array.ofDim[Byte](18))
    buffer.put(str.getBytes, 0, str.getBytes.length)
    val idbuffer: BigInteger = new BigInteger(buffer.array())
    return idbuffer.longValue()
  }

  /**
   * getCurrentTimestamp function returns the current timestamp in ISO format
   * @return : current timestamp in ISO format
   */
  def getCurrentTimestamp(): String = {
    val ISOFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    val now: DateTime = new org.joda.time.DateTime()
    ISOFormatGeneration.print(now)
  }

  def getCurrentTimestamp(format: String): String = {
    val ISOFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern(format)
    //("yyyy-MM-dd HH:mm:ss.SSS");
    val now: DateTime = new org.joda.time.DateTime()
    ISOFormatGeneration.print(now)
  }

  def getBatchCurrentTimestamp(): String = {
    val ISOFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmssSSS");
    val now: DateTime = new org.joda.time.DateTime()
    ISOFormatGeneration.print(now)
  }

  /**
   * insertIntoAuditTable method writes the incoming data to Audit table
   * @param hiveContext: Hive Context
   * @param auditObj: Audit Object
   */
  /* def insertIntoAuditTable(hiveContext: HiveContext,auditObj:AuditLoadObject,fullTablename:String) = {

try {
log.info("WRITING INTO AUDIT LOG --- ")
val audBatchId = auditObj.getAudBatchId()
val audjobId = auditObj.getAudjobId()
val audBusinessAreaId = auditObj.getAudBusinessAreaId()
val audJobstartTimestamp = auditObj.getAudJobstartTimestamp()
val audJobEndTimestamp = auditObj.getAudJobEndTimestamp()
val audJobrunStatus = auditObj.getAudJobrunStatus()
val audSrcRowCount = auditObj.getAudSrcRowCount()
val audTgtRowCount = auditObj.getAudTgtRowCount()
val audValidRecords = auditObj.getAudValidRecords()
val audErrorRecords = auditObj.getAudErrorRecords()
val audFileName = auditObj.getAudFileName()
val audRemarks = auditObj.getAudRemarks()
val audCreationDate = auditObj.getAudCreationDate()
val audCreatedBy = auditObj.getAudCreatedBy()


/* log.info("HQL ::"+f"""select "$audBatchId" as aud_batch_id, "$audjobId" as aud_jobid,
"$audBusinessAreaId" as aud_businessarea_id,"$audJobstartTimestamp" as aud_jobstart_timestamp,
"$audJobEndTimestamp" as audJobEndTimestamp,"$audJobrunStatus" as aud_jobrun_status ,
"$audSrcRowCount" as aud_srcrowcount,"$audTgtRowCount" as aud_tgtrowcount,
"$audValidRecords" as aud_valid_records,"$audErrorRecords" as aud_error_records,
"$audFileName" as aud_filename,"$audRemarks" as aud_remarks,
"$audCreationDate" as aud_creation_date,"$audCreatedBy" as aud_created_by""")
*/
val auditDF = hiveContext.sql(f"""select "$audBatchId" as aud_batch_id, "$audjobId" as aud_jobid,
"$audBusinessAreaId" as aud_businessarea_id,"$audJobstartTimestamp" as aud_jobstart_timestamp,
"$audJobEndTimestamp" as audJobEndTimestamp,"$audJobrunStatus" as aud_jobrun_status ,
"$audSrcRowCount" as aud_srcrowcount,"$audTgtRowCount" as aud_tgtrowcount,
"$audValidRecords" as aud_valid_records,"$audErrorRecords" as aud_error_records,
"$audFileName" as aud_filename,"$audRemarks" as aud_remarks,
"$audCreationDate" as aud_creation_date,"$audCreatedBy" as aud_created_by""")
storeDataFrame(auditDF, "Append", "orc", fullTablename.trim())
log.info("INSERTED INTO AUDIT LOG")
} catch {
case e: Exception =>e.printStackTrace()
}
}
*/
  /*
def generateSequenceId (df: DataFrame, list:List[String], hiveContext:HiveContext): DataFrame ={
var natural_key_columns=""
list.foreach { x =>
natural_key_columns=natural_key_columns+","+x
}
natural_key_columns=natural_key_columns.drop(1)
import hiveContext.implicits._
hiveContext.udf.register("udf_sequenceIDGenerator", sequenceIDGenerator _)
df.registerTempTable("raw_table")
var sequenceIDGenerator_sql = f"""select udf_sequenceIDGenerator($natural_key_columns) as sequence_ID,* from raw_table"""
println("Date Check Query:"+sequenceIDGenerator_sql)
val seqId_df=hiveContext.sql(sequenceIDGenerator_sql)
hiveContext.dropTempTable("raw_table")
return seqId_df
}*/
  def getTableCount(hiveContext: HiveContext, dbName: String, tgtTableName: String): Long = {
    val tableName = dbName + "." + tgtTableName
    val df = hiveContext.table(tableName)
    return df.count()
  }

  case class DataList(filePath: String, mod_ts: Long)
  def sortedFiles(sparkContext: SparkContext, path: String): List[String] = {
    var finalList: List[DataList] = List[DataList]()
    var returnList: List[DataList] = List[DataList]()
    val listOfDirectories = FileSystem.get(sparkContext.hadoopConfiguration).listStatus(new Path(path)).filter(x => x.isDirectory() == true).map { x => x.getPath.toString() }.toList
    if (listOfDirectories.length > 0) {
      listOfDirectories.foreach { directories =>
        var newList = getSortedFileObjects(sparkContext, directories)
        finalList = finalList ::: newList
      }
      returnList = finalList.sortBy(files => files.mod_ts)
    } else {
      returnList = getSortedFileObjects(sparkContext, path)
    }
    returnList.foreach { println }
    val r = returnList.map { x => x.filePath }: List[String]
    r.foreach { println }
    return r
  }

  /**
   * getSortedFileObjects method lists the file name in a HDFS folder
   * @param sparkContext: Spark Context
   * @param path: Input path of the folder
   * @return: List of files
   */
  def getSortedFileObjects(sparkContext: SparkContext, path: String): List[DataList] = {
    FileSystem.get(sparkContext.hadoopConfiguration).listStatus(new Path(path)).map { x => DataList(x.getPath.toString(), x.getModificationTime) }.toList.sortBy { files => files.mod_ts }

  }

  /**
   * storeDataFrame function stores the data frame to input HIVE table
   *
   * @param transformedDF: DataFrame to be stored
   * @param saveMode: Mode of saving (OverWrite,Append,Ignore,ErrofIfExists)
   * @param storageFormat: Storage format for the target table (ORC,Parquet etc)
   * @param targetTable:Target HIVE table name with Database name
   * @return Boolean: flag to indicate if the action was successful or not
   */
  def storeDataFrame(transformedDF: DataFrame, saveMode: String, storageFormat: String, targetTable: String): Boolean = {
    var loadStatus = false
    try {
      //val tblCount=transformedDF.count
      val tblCount = 1
      log.info("Writing to HIVE TABLE : " + targetTable + " :: DataFrame Count :" + tblCount)
      if (tblCount > 0 && transformedDF != null && saveMode.trim().length() != 0 && storageFormat.trim().length() != 0 && targetTable.trim().length() != 0) {
        log.info("SAVE MODE::" + saveMode + " Table Name :: " + targetTable + " Format :: " + storageFormat)
        transformedDF.write.mode(saveMode).format(storageFormat.trim()).insertInto(targetTable.trim())
        return true
      } else {
        return false
      }

    } catch {
      case e: Exception => e.printStackTrace(); log.info("ERROR Writing to HIVE TABLE : " + targetTable); return false;
    }
    return loadStatus
  }

  def storeAsJson(transformedDF: DataFrame, saveMode: String, storageFormat: String, tgtpath: String): Boolean =
    {
      var loadStatus = false
      try {

        transformedDF.write.mode(saveMode).format(storageFormat.trim()).json(tgtpath);
        return true

      } catch {
        case e: Exception => e.printStackTrace(); log.info("ERROR Writing to Raw Directory : " + tgtpath); return false;
      }
      return loadStatus

    }

  def saveRejectsToFile(transformedDF: DataFrame, saveMode: String, delimiter: String, tgtpath: String): Boolean =
    {

      var loadStatus = false
      try {
        if (transformedDF.count() > 0 && transformedDF != null && saveMode.trim().length() != 0 && tgtpath.trim().length() != 0) {
          //transformedDF.write.mode(saveMode).option("header", "false").option("delimiter", delimiter).csv(tgtpath)
          transformedDF.write.mode(saveMode).option("header", "false").csv(tgtpath)
          return true
        } else {
          return false
        }

      } catch {
        case e: Exception => e.printStackTrace(); log.info("ERROR Writing to Reject File Directory : " + tgtpath); return false;
      }
      return loadStatus
    }

  def getSQLPropertiesObject(path: String): AuditSQLPropertiesObject =
    {
      import scala.collection.JavaConversions._
      val propfileSystem: FileSystem = FileSystem.get(new Configuration)
      val propFileInputStream = propfileSystem.open(new Path(path))
      var properties: Properties = new Properties();
      properties.load(propFileInputStream);
      // var properties: Properties = null
      var sqlObj: AuditSQLPropertiesObject = null
      try {
        if (path != null) {
          sqlObj = new AuditSQLPropertiesObject(
            properties.getProperty("mySqlHostName"),
            properties.getProperty("mySqlDBName"),
            properties.getProperty("mySqlUserName"),
            properties.getProperty("mySqlPassword"),
            properties.getProperty("mySqlPort"),
            properties.getProperty("mySqlAuditTbl"))
        }
        return sqlObj
      } catch {
        case ex: FileNotFoundException => return null
        case ex: IOException           => return null

      }

    }

  def getTgtRDBMSPropertiesObject(path: String): TgtRDBMSPropertiesObject =
    {
      import scala.collection.JavaConversions._
      val propfileSystem: FileSystem = FileSystem.get(new Configuration)
      val propFileInputStream = propfileSystem.open(new Path(path))
      var properties: Properties = new Properties();
      properties.load(propFileInputStream);
      // var properties: Properties = null
      var sqlObj: TgtRDBMSPropertiesObject = null
      try {
        if (path != null) {
          sqlObj = new TgtRDBMSPropertiesObject(
            properties.getProperty("tgtSqlHostName"),
            properties.getProperty("tgtSqlDBName"),
            properties.getProperty("tgtSqlUserName"),
            properties.getProperty("tgtSqlPassword"),
            properties.getProperty("tgtSqlPort"),
            properties.getProperty("tgtSqlAuditTbl"))
        }
        return sqlObj
      } catch {
        case ex: FileNotFoundException => return null
        case ex: IOException           => return null

      }

    }

  def getConnection(sqlPropertiesObject: AuditSQLPropertiesObject): Connection = {
    val host = sqlPropertiesObject.getMySqlHostName()
    val port = sqlPropertiesObject.getMySqlPort()
    val username = sqlPropertiesObject.getMySqlUserName()
    val password = sqlPropertiesObject.getMySqlPassword()
    val dbName = sqlPropertiesObject.getMySqlDBName()
    try {
      Class.forName("com.mysql.jdbc.Driver");
      var con: Connection = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + dbName, username, password);
      return con
    } catch {
      case e: Exception => return null
    }
  }

  def insertIntoAudit(sqlcon: Connection, auditObj: AuditLoadObject, fulltblName: String) = {

    try {
      log.info("WRITING INTO AUDIT LOG -- ")
      val audBatchId = auditObj.getAudBatchId()
      val audApplicationName = auditObj.getAudApplicationName
      val audObjectName = auditObj.getAudObjectName
      val audLayerName = auditObj.getAudDataLayerName
      val audStatusCode = auditObj.getAudJobStatusCode
      val audJobEndTimestamp = auditObj.getAudJobEndTimestamp()
      val audLoadTimeStamp = auditObj.getAudLoadTimeStamp()
      val audSrcRowCount = auditObj.getAudSrcRowCount()
      val audTgtRowCount = auditObj.getAudTgtRowCount()
      val audErrorRecords = auditObj.getAudErrorRecords()
      val audCreatedBy = auditObj.getAudCreatedBy()
      val audJobStartTimeStamp = auditObj.getAudJobStartTimeStamp()
      val audJobDuration = auditObj.getAudJobDuration()

      val auditSQL = "INSERT INTO " + fulltblName + " SELECT \"" + audBatchId + "\" as audBatchId, \"" + audApplicationName + "\" as audApplicationName,\"" + audObjectName + "\" as audObjectName , \"" + audLayerName + "\" as audDataLayerName, \"" + audStatusCode + "\" as audJobStatusCode, \"" + audJobStartTimeStamp + "\" as audJobStartTimeStamp, \"" + audJobEndTimestamp + "\" as audJobEndTimestamp, \"" + audLoadTimeStamp + "\" as audLoadTimeStamp, \"" + audSrcRowCount + "\" as audSrcRowCount, \"" + audTgtRowCount + "\" as audTgtRowCount, \"" + audErrorRecords + "\" as audErrorRecords ,\"" + audCreatedBy + "\" as audCreatedBy,\"" + audJobDuration + "\" as audJobDuration"
      log.info("===================Print SQL :: " + auditSQL)
      val preparedStmt = sqlcon.prepareStatement(auditSQL);
      preparedStmt.execute();

      log.info("INSERTED INTO AUDIT LOG")
    } catch {
      case e: Exception => e.printStackTrace()

    }

  }

  def updatetoAudit(sqlcon: Connection, auditObj: AuditLoadObject, fulltblName: String) = {

    try {
      log.info("WRITING INTO AUDIT LOG -- ")
      val audBatchId = auditObj.getAudBatchId()
      val audLayerName = auditObj.getAudDataLayerName
      val audStatusCode = auditObj.getAudJobStatusCode
      val audJobEndTimestamp = auditObj.getAudJobEndTimestamp()
      val audLoadTimeStamp = auditObj.getAudLoadTimeStamp()
      val audSrcRowCount = auditObj.getAudSrcRowCount()
      val audTgtRowCount = auditObj.getAudTgtRowCount()
      val audErrorRecords = auditObj.getAudErrorRecords()
      val jobDuration = auditObj.getAudJobDuration()

      val auditSQL = "UPDATE " + fulltblName + " SET audJobStatusCode=\"" + audStatusCode + "\", audJobEndTimestamp=\"" + audJobEndTimestamp + "\", audLoadTimeStamp=\"" + audLoadTimeStamp + "\", audSrcRowCount=\"" + audSrcRowCount + "\", audTgtRowCount=\"" + audTgtRowCount + "\", audErrorRecords=\"" + audErrorRecords + "\",audJobDuration=\"" + jobDuration + "\" where audBatchId=\"" + audBatchId + "\" and audDataLayerName=\"" + audLayerName + "\""
      log.info("===================Print SQL :: " + auditSQL)
      val preparedStmt = sqlcon.prepareStatement(auditSQL);
      preparedStmt.execute();
      log.info("INSERTED INTO AUDIT LOG")
    } catch {
      case e: Exception => e.printStackTrace()

    }

  }

  def getSourceColumnMapping(sqlcon: Connection, entrytblName: String, objName: String): String =
    {
      try {
        val stmt: Statement = sqlcon.createStatement()
        val sql = "select tag_mapping from " + entrytblName + " where object_name=\"" + objName + "\""
        log.info("======================" + sql + "=================")
        val rs: ResultSet = stmt.executeQuery(sql);
        var result = ""
        while (rs.next()) {
          result = rs.getString(1)
        }
        return result
      } catch {
        case e: Exception =>
          e.printStackTrace()
          return null;
      }

    }

  def getDQMapping(sqlcon: Connection, objName: String): String =
    {
      var result = ""
      try {
        val stmt: Statement = sqlcon.createStatement()
        val sql = "select dq_checks from metadatadrivenacltr_dataquality where obj_nm=\"" + objName + "\""
        log.info("======================" + sql + "=================")
        val rs: ResultSet = stmt.executeQuery(sql);

        while (rs.next()) {
          result = rs.getString(1)
        }
        return result
      } catch {
        case e: Exception =>
          e.printStackTrace()
          return result;
      }

    }

  def getDateFormatColumns(sqlcon: Connection, objName: String): String =
    {
      var result = ""
      try {
        val stmt: Statement = sqlcon.createStatement()
        val sql = "select date_fmt_columns from metadatadrivenacltr_dataquality where obj_nm=\"" + objName + "\""
        log.info("======================" + sql + "=================")
        val rs: ResultSet = stmt.executeQuery(sql);

        while (rs.next()) {
          result = rs.getString(1)
        }
        return result
      } catch {
        case e: Exception =>
          e.printStackTrace()
          return result;
      }

    }

  def getTransformation(sqlcon: Connection, objName: String): String =
    {
      var result = ""
      try {
        val stmt: Statement = sqlcon.createStatement()
        val sql = "select dt_list from metadatadrivenacltr_datatransformation where obj_nm=\"" + objName + "\""
        log.info("======================" + sql + "=================")
        val rs: ResultSet = stmt.executeQuery(sql);

        while (rs.next()) {
          result = rs.getString(1)
        }
        return result
      } catch {
        case e: Exception =>
          e.printStackTrace()
          return result;
      }

    }

  def prepareEnr(enrdf: DataFrame, list: List[String], ctrlObject: ControllerObject): DataFrame =
    {
      var prepareDF = enrdf
      val spark = ctrlObject.getConfigObject().getSpark()
      import spark.implicits._
      prepareDF.createOrReplaceTempView("transform_df")
      var sql: String = "SELECT *,"
      list.foreach { x =>
        var exp = x.split("\\=", -1)(1)
        var columnName = x.split("\\=", -1)(0)
        sql = sql + exp + " AS " + columnName + ","
      }
      sql = sql.dropRight(1) + " FROM transform_df"
      log.info("=============================SQl====================" + sql)
      prepareDF = spark.sql(sql)
      log.info("=============================transformed_DF====================" + prepareDF.show())
      spark.catalog.dropTempView("transform_df")
      return prepareDF
    }

  def dataValidator(fullRow: String, delimiter: String, schemaLength: Int): String = {

    try {
      val count: Int = fullRow.split(delimiter + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1).length
      if (count != schemaLength)
        return "I"
      else if (!fullRow.matches(".*\\d+.*")) {
        return "I"
      } else
        return "V"
    } catch {
      case t: Throwable =>
        t.printStackTrace().toString() // TODO: handle error
        return "F"
    }
  }

  def thresholdLimitStatus(thresholdLimit: Int, actualFileRecordsCount: Long, validRecordsCount: Long): Boolean = {
    log.info("Checking the % of valid records count against threshold Limit ")
    val validRecordsPercentage = (validRecordsCount * 100) / actualFileRecordsCount
    if (validRecordsPercentage >= thresholdLimit) {
      log.info("Valid records count more than threshold Limit.")
      return true
    } else {
      log.info("Valid records count less than threshold Limit.")
      return false
    }

  }

  def customExpression(rowStr: String, exp: String): String = {
    import scala.reflect.runtime.universe._
    import scala.reflect.runtime.currentMirror
    import scala.tools.reflect.ToolBox
    val toolbox = currentMirror.mkToolBox()

    var rowVal = s""""$rowStr""""
    val str: String = s"""var r="";
if($rowVal.toDouble>100.0)
{
r="correct"
}
else
{
r="incorrect"
}
r.toString"""
    // write your code starting with q and put it inside double quotes.
    // NOTE : you will have to use triple quotes if you have any double quotes usage in your code.
    //val code1 = q"""rowVal.substring(0,4)"""
    val result: String = toolbox.eval(toolbox.parse(str)).toString
    return result

  }

  def storePartitonedDataFrame(transformedDF: DataFrame, saveMode: String, partitionColList: List[String], storageFormat: String, targetTable: String, configObject: ConfigObject): Boolean = {
    var loadStatus = false
    try {
      log.info("Writing to HIVE TABLE : " + targetTable)
      import scala.collection.JavaConversions._

      var partition_column = ""
      for (column <- partitionColList) { partition_column += f"""$column,""" }
      partition_column = partition_column.dropRight(1)

      var table_select_columns = ""
      transformedDF.columns.toList.map { x => table_select_columns = table_select_columns + x + "," }
      table_select_columns = table_select_columns.dropRight(1)

      val spark = configObject.getSpark()
      import spark.implicits._
      transformedDF.createOrReplaceTempView("raw_source_table")
      var saveType = ""
      if ("overwrite".equalsIgnoreCase(saveMode)) {
        saveType = saveMode
      } else {
        saveType = "INTO"
      }
      val data_ingest_sql = f"""INSERT $saveType TABLE $targetTable PARTITION($partition_column) SELECT $table_select_columns FROM raw_source_table"""

      if (transformedDF != null && saveMode.trim().length() != 0 && storageFormat.trim().length() != 0
        && targetTable.trim().length() != 0 && partition_column.trim().length() != 0
        && table_select_columns.trim().length() != 0) {
        log.info("SAVE MODE::" + saveMode + " Table Name :: " + targetTable + " Format :: " + storageFormat)
        spark.sql(data_ingest_sql)
        spark.catalog.dropTempView("raw_source_table")
        return true
      } else { return false }
    } catch {
      case e: Exception => e.printStackTrace(); log.info("ERROR Writing to HIVE TABLE : " + targetTable); return false;
    }
    return loadStatus
  }

}