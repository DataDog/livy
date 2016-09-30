package com.cloudera.livy.utils

import com.cloudera.livy.{LivyConf, Logging}
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

import scala.collection.mutable

class NewSparkProcessBuilder(livyConf: LivyConf) extends Logging {
  private[this] val _conf                       = mutable.HashMap[String, String]()
  private[this] var _deployMode: Option[String] = None
  private[this] var _className : Option[String] = None
  private[this] var _name      : Option[String] = Some("Livy")
  private[this] var _proxyUser : Option[String] = None
  private[this] var _queue     : Option[String] = None

  def conf(key: String): Option[String] = {
    _conf.get(key)
  }

  def conf(key: String, value: String, admin: Boolean = false) = {
    _conf(key) = value
  }

  def conf(conf: Traversable[(String, String)]): Unit = {
    conf.foreach { case (key, value) => this.conf(key, value) }
  }

  def className(className: String) = {
    _className = Some(className)
  }

  def deployMode(deployMode: String) = {
    _deployMode = Some(deployMode)
  }

  def driverMemory(driverMemory: String) = {
    conf("spark.driver.memory", driverMemory)
  }

  def driverCores(driverCores: Int): Unit = {
    this.driverCores(driverCores.toString)
  }

  def driverCores(driverCores: String) = {
    conf("spark.driver.cores", driverCores)
  }

  def executorCores(executorCores: Int): Unit = {
    this.executorCores(executorCores.toString)
  }

  def executorCores(executorCores: String) = {
    conf("spark.executor.cores", executorCores)
  }

  def executorMemory(executorMemory: String) = {
    conf("spark.executor.memory", executorMemory)
  }

  def numExecutors(numExecutors: Int): Unit = {
    this.numExecutors(numExecutors.toString)
  }

  def numExecutors(numExecutors: String) = {
    this.conf("spark.executor.instances", numExecutors)
  }

  def proxyUser(proxyUser: String) = {
    _proxyUser = Some(proxyUser)
  }

  def queue(queue: String) = {
    _queue = Some(queue)
  }

  def name(name: String) = {
    _name = Some(name)
  }

  def start(file: Option[String], args: Traversable[String]): SparkAppHandle = {
    val launcher = new SparkLauncher()

    _deployMode.foreach(launcher.setDeployMode)
    _name.foreach(launcher.setAppName)
    _className.foreach(launcher.setMainClass)

    _conf.remove(LivyConf.SPARK_JARS).foreach(_.split(",").foreach(jar => launcher.addJar(jar)))
    _conf.remove(LivyConf.SPARK_FILES).foreach(_.split(",").foreach(file => launcher.addFile(file)))

    _conf.foreach({ case (k, v) => launcher.setConf(k, v) })

    file.foreach(launcher.setAppResource)

    val argsString = args
      .map("'" + _.replace("'", "\\'") + "'")
      .mkString(" ")

    info(s"Running $argsString")

    launcher.addAppArgs(args.toSeq : _*)
    launcher.startApplication()
  }
}
