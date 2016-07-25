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

import java.lang.ProcessBuilder.Redirect

import com.cloudera.livy.LivyConf
import com.cloudera.livy.sessions.{Session, SessionState}
import com.cloudera.livy.utils.NewSparkProcessBuilder
import org.apache.spark.launcher.SparkAppHandle
import org.apache.spark.launcher.SparkAppHandle.Listener
import org.apache.spark.launcher.SparkAppHandle.State

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class BatchSession(
    id: String,
    owner: String,
    override val proxyUser: Option[String],
    livyConf: LivyConf,
    request: CreateBatchRequest)
    extends Session(id, owner, livyConf) {

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
          _state = SessionState.Success()

        case State.KILLED =>
          _state = SessionState.Dead()

        case State.FAILED =>
          _state = SessionState.Failed()

        case State.UNKNOWN | State.CONNECTED | State.SUBMITTED | State.RUNNING => _state = SessionState.Running()
      }
    }
  })

  protected implicit def executor: ExecutionContextExecutor = ExecutionContext.global

  private[this] var _state: SessionState = SessionState.Running()

  override def state: SessionState = _state

  override def logLines(): IndexedSeq[String] = IndexedSeq()

  override def stopSession(): Unit = {
    handle.stop()
  }
}
