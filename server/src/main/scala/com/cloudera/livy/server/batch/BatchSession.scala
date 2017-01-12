/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy.server.batch

import com.cloudera.livy.LivyConf
import com.cloudera.livy.sessions.{Session, SessionState}
import com.cloudera.livy.utils.NewSparkProcessBuilder
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.launcher.SparkAppHandle
import org.apache.spark.launcher.SparkAppHandle.{Listener, State}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class BatchSession(
    id: String,
    owner: String,
    override val proxyUser: Option[String],
    livyConf: LivyConf,
    request: CreateBatchRequest)
    extends Session(id, owner, livyConf) {

  val yarnConf = new YarnConfiguration()
  val yarnClient: YarnClient = YarnClient.createYarnClient()
  yarnClient.init(yarnConf)
  yarnClient.start()

  private val handle = {
    val conf = prepareConf(request.conf, request.jars, request.files, request.archives,
      request.pyFiles)
    require(request.file != null, "File is required.")

    val builder = new NewSparkProcessBuilder(livyConf)
    builder.conf(conf)

    proxyUser.foreach(builder.proxyUser)
    request.className.foreach(builder.className)
    request.driverMemory.foreach(builder.driverMemory)
    request.driverCores.foreach(builder.driverCores)
    request.executorMemory.foreach(builder.executorMemory)
    request.executorCores.foreach(builder.executorCores)
    request.numExecutors.foreach(builder.numExecutors)
    request.queue.foreach(builder.queue)
    request.name.foreach(builder.name)

    // Spark 1.x does not support specifying deploy mode in conf and needs special handling.
    livyConf.sparkDeployMode().foreach(builder.deployMode)

//    builder.redirectOutput(Redirect.PIPE)
//    builder.redirectErrorStream(true)

    val file = resolveURIs(Seq(request.file)).head
    builder.start(Some(file), request.args)
  }
  handle.addListener(new Listener {
    override def infoChanged(handle: SparkAppHandle): Unit = {}

    override def stateChanged(handle: SparkAppHandle): Unit = {
      handle.getState match {
        case State.FINISHED =>
          val yarnAppId = handle.getAppId
          if (yarnAppId == null) {
            _state = SessionState.Error()
          } else {
            val applicationReport = yarnClient.getApplicationReport(new YarnApp(yarnAppId))
            val applicationStatus = applicationReport.getFinalApplicationStatus

            _state = applicationStatus match {
              case FinalApplicationStatus.SUCCEEDED => SessionState.Success()

              case FinalApplicationStatus.KILLED => SessionState.Dead()

              case _ => SessionState.Error()
            }
          }

        case State.KILLED =>
          _state = SessionState.Dead()

        case State.FAILED =>
          _state = SessionState.Error()

        case State.UNKNOWN | State.CONNECTED | State.SUBMITTED | State.RUNNING =>
          _state = SessionState.Running()
      }
    }
  })

  protected implicit def executor: ExecutionContextExecutor = ExecutionContext.global

  private[this] var _state: SessionState = SessionState.Running()

  override def state: SessionState = _state

  override def logLines(): IndexedSeq[String] = IndexedSeq()

  override def stopSession(): Unit = {
    val yarnAppId = handle.getAppId
    if (yarnAppId != null) {
      yarnClient.killApplication(new YarnApp(yarnAppId))
    }
    handle.stop()
  }
}

class YarnApp(appId: String) extends ApplicationIdPBImpl {
  appId.split("_").toList match {
    case List(_, timestamp, id) =>
      setClusterTimestamp(timestamp.toLong)
      setId(id.toInt)
      build()
  }
}
