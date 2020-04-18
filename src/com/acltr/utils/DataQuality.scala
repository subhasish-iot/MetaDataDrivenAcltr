package com.acltr.utils

import java.util.Arrays
import java.text.SimpleDateFormat
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import scala.collection.mutable.LinkedHashMap
import org.apache.log4j.Logger

object DataQuality {
  val log = Logger.getLogger(getClass.getName)

  def isThisDateValid(dateToValidate: String, dateFromat: String): Boolean = {
    if (dateToValidate == null) {
      false
    }
    val sdf: SimpleDateFormat = new SimpleDateFormat(dateFromat)
    sdf.setLenient(false)
    try {
      //if not valid, it will throw ParseException
      val date: Date = sdf.parse(dateToValidate)
      true
    } catch {
      case e: ParseException => {
        //e.printStackTrace()
        false
      }

    }
  }
  def DQValidchck(row: Row, lm: LinkedHashMap[String, String]): Row =
    {

      import scala.collection.mutable.ListBuffer
      var err_col: String = ""
      var err_desc: String = "VALID"
      var errorCollist = new ListBuffer[String]()
      var errorDesclist = new ListBuffer[String]()
      var methodReturn = ""
      var listRow = Row()

      lm.foreach { x =>
        val key = x._1
        val checks = x._2
        val str: String = row.getAs[String](key)
        log.info("====================I am here======================================" + str)
        val listOfCheck = checks.split("\\;", -1).toList
        listOfCheck.foreach { f =>
          var chknm = ""
          var chkval = ""
          if (f.lastIndexOf('=') > 0) {
            chknm = f.substring(0, f.lastIndexOf('='))
            chkval = f.substring(f.lastIndexOf('=') + 1)
          } else {
            chknm = f
          }
          if (chknm.equalsIgnoreCase("NULL")) {
            methodReturn = nullCheck(str, key)
            if (!methodReturn.equalsIgnoreCase("VALID")) {
              errorCollist += key
              errorDesclist += methodReturn
            }
          } else if (chknm.equalsIgnoreCase("LENGTH")) {
            methodReturn = lengthCheck(str, chkval.toInt, key)
            if (!methodReturn.equalsIgnoreCase("VALID")) {
              errorCollist += key
              errorDesclist += methodReturn
            }
          } else if (chknm.equalsIgnoreCase("DATE")) {
            methodReturn = dateCheck(str, chkval, key)
            if (!methodReturn.equalsIgnoreCase("VALID")) {
              errorCollist += key
              errorDesclist += methodReturn
            }
          } else if (chknm.equalsIgnoreCase("INT") || chknm.equalsIgnoreCase("BIGINT")) {
            methodReturn = intCheck(str, key)
            if (!methodReturn.equalsIgnoreCase("VALID")) {
              errorCollist += key
              errorDesclist += methodReturn
            }
          } else if (chknm.equalsIgnoreCase("DECIMAL")) {

            methodReturn = doubleCheck(str, key)
            if (!methodReturn.equalsIgnoreCase("VALID")) {
              errorCollist += key
              errorDesclist += methodReturn
            }
          }

        }
      }
      if (errorCollist.size > 0) {
        err_col = errorCollist.mkString("|")
        err_desc = errorDesclist.mkString("|")
      }

      Row.merge(row, Row(err_col), Row(err_desc))
    }

  def nullCheck(str: String, keyName: String): String = {
    var dataqualitychkmsg: String = "VALID"
    try {
      if (str == null || str == "" || ("null").equalsIgnoreCase(str) || ("nul").equalsIgnoreCase(str) || str.trim().length == 0) {
        dataqualitychkmsg = keyName + " column is null"
      }
      return dataqualitychkmsg
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        dataqualitychkmsg = " Exception during null check" // TODO: handle error
    }

    return dataqualitychkmsg
  }

  def lengthCheck(str: String, value: Int, keyName: String): String = {
    var dataqualitychkmsg: String = "VALID"
    try {
      if (str != null) {
        if (str.trim().length() == 0 || str.trim().length() != value)
          dataqualitychkmsg = keyName + " column length is not matching"
      } else {
        dataqualitychkmsg = keyName + " column length is not matching"
      }
      return dataqualitychkmsg
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        dataqualitychkmsg = " Exception during Length check" // TODO: handle error
    }
    return dataqualitychkmsg
  }

  def dateCheck(str: String, value: String, keyName: String): String = {
    var dataqualitychkmsg: String = "VALID"
    try {
      if (str != null && str.trim().length() > 0 && !str.equalsIgnoreCase("null")) {
        if (str != ("00000000") && !(isThisDateValid(str, value)))
          dataqualitychkmsg = keyName + " column has Date format mismatch"
      } else {
        dataqualitychkmsg = keyName + " column has Date format mismatch"
      }
      return dataqualitychkmsg
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        dataqualitychkmsg = " Exception during Date check" // TODO: handle error
    }
    return dataqualitychkmsg
  }

  def intCheck(str: String, keyName: String): String = {
    var dataqualitychkmsg: String = "VALID"
    val intStringPattern: String = "^\\d+$"
    try {
      if ((str != null && str.!=("") && !"null".equalsIgnoreCase(str)) && !str.trim().matches(intStringPattern)) {
        dataqualitychkmsg = keyName + " column is Not an Integer"
      }
      return dataqualitychkmsg
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        dataqualitychkmsg = keyName + " column is Not an Integer" // TODO: handle error
    }
    return dataqualitychkmsg
  }

  def doubleCheck(str: String, keyName: String): String = {
    var dataqualitychkmsg: String = "VALID"
    val dblStringPattern: String = "[-+]?[0-9]+(\\.){0,1}[0-9]*"
    log.info("====================I am here======================================" + str)
    try {
      var d = str.toDouble
      return dataqualitychkmsg
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        dataqualitychkmsg = keyName + " column is Not a Double" // TODO: handle error
        return dataqualitychkmsg
    }
  }
}