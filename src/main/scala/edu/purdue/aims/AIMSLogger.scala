package edu.purdue.aims

import java.io.PrintWriter
import java.io.File
import spray.json.JsObject



object AIMSLogger {
  val loggers = scala.collection.mutable.Map[String,PrintWriter]()
  
  val RESP_TIME = "rt"
  val WORKLOAD = "wl"
  val MTX = "mtx"
  val WREPLAY = "wlreplay"
  
  loggers += (RESP_TIME -> new PrintWriter(new File("rtperf.csv")))
  loggers += (WORKLOAD -> new PrintWriter(new File("wl.csv")))
  loggers += (MTX -> new PrintWriter(new File("mtx.csv")))
  loggers += (WREPLAY -> new PrintWriter(new File("wltrace.json")))
  
  def logResponseTime(txid:Long,startTime:Long, commitTime:Long) = {
    val logger = loggers.get(RESP_TIME)
    
    logger match {
      case Some(log) => {
        log.println(List(txid,startTime,commitTime).mkString(","))
      } 
      case None =>{
        println("No logger <- this should never happen")
      }
    }
  }
  
  def logTxAccessEntry(e:List[Long]) = {
    val logger = loggers.get(WORKLOAD)
    logger match {
      case Some(log) => {
        log.println(e.mkString(","))
        log.flush()
      } 
      case None =>{
        println("No logger <- this should never happen")
      }
    }
    
  }
  
  def logMTxEntry(e:List[Long]) = {
    val logger = loggers.get(MTX)
    logger match {
      case Some(log) => {
        log.println(e.mkString(","))
        log.flush()
      } 
      case None =>{
        println("No logger <- this should never happen")
      }
    }
  }
  
  def logTransaction(txnspec:JsObject) = {
    val logger = loggers.get(WREPLAY)
    logger match {
      case Some(log) => {
        log.println(txnspec.toString())
        log.flush()
      } 
      case None =>{
        println("No logger <- this should never happen")
      }
    }
  }
  
  def closeLoggers() = {
    loggers.values.foreach { _.close()}
  }
  
}