/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.refactor.state

import java.io._
import java.time.Instant
import java.util
import java.util.Map

import com.google.common.collect.Table
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.refactor.coder._
import org.apache.gearpump.streaming.refactor.state.api.StateInternals
import org.apache.gearpump.streaming.refactor.state.heap.HeapStateInternalsFactory
import org.apache.gearpump.streaming.state.impl.{CheckpointManager, PersistentStateConfig}
import org.apache.gearpump.streaming.task.{Task, TaskContext, UpdateCheckpointClock}
import org.apache.gearpump.streaming.transaction.api.CheckpointStoreFactory
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.{Message, TimeStamp}

abstract class StatefulTask(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  import taskContext._

  val checkpointStoreFactory = conf.getValue[CheckpointStoreFactory](
    PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY).get
  val checkpointStore = checkpointStoreFactory.getCheckpointStore(
    s"app$appId-task${taskId.processorId}_${taskId.index}")
  val checkpointInterval = conf.getLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS).get
  val checkpointManager = new CheckpointManager(checkpointInterval, checkpointStore)

  var stateContext: RuntimeContext = null

  var inited = false

  // core state data
  var encodedKeyStateMap: Map[String, Table[String, String, Array[Byte]]] = null

  def open(runtimeContext: RuntimeContext): Unit = {}

  def invoke(message: Message): Unit

  def close(runtimeContext: RuntimeContext): Unit = {}

  final override def onStart(startTime: Instant): Unit = {
    // recover state from snapshot
    LOG.info("[onStart] - recover from snapshot")
    val timestamp = startTime.toEpochMilli
    checkpointManager
      .recover(timestamp)
      .foreach(recoverState(timestamp, _))
    reportCheckpointClock(timestamp)

    inited = true
    stateContext = new StreamingRuntimeContext(startTime)
    open(stateContext)
  }

  final override def onNext(message: Message): Unit = {
    checkpointManager.update(message.timestamp.toEpochMilli)
    invoke(message)
  }

  override def onWatermarkProgress(watermark: Instant): Unit = {
    if (checkpointManager.shouldCheckpoint(watermark.toEpochMilli)) {
      checkpointManager.getCheckpointTime.foreach { checkpointTime =>
        val serialized = snapshot
        checkpointManager.checkpoint(checkpointTime, serialized)
        reportCheckpointClock(checkpointTime)
      }
    }
  }

  final override def onStop(): Unit = {
    LOG.info("[onStop] closing checkpoint manager")
    close(stateContext)
    checkpointManager.close()
  }

  private def reportCheckpointClock(timestamp: TimeStamp): Unit = {
    LOG.debug("reportCheckpointClock at : {}", timestamp)
    appMaster ! UpdateCheckpointClock(taskContext.taskId, timestamp)
  }

  private def recoverState(timestamp: TimeStamp, snapshot: Array[Byte]): Unit = {
    LOG.info("call recoverState snapshot with timestamp : {}", timestamp)
    var bis: Option[ByteArrayInputStream] = None
    var oin: Option[ObjectInputStream] = None
    try {
      bis = Some(new ByteArrayInputStream(snapshot))
      oin = Some(new ObjectInputStream(bis.get))
      encodedKeyStateMap = oin.get.readObject().asInstanceOf[
        Map[String, Table[String, String, Array[Byte]]]]
    } catch {
      case ex: IOException => throw new RuntimeException(ex)
      case e: Exception => throw new RuntimeException(e)
    } finally {
      try {
        if (oin.nonEmpty) oin.foreach(_.close())
      } catch {
        case ex: Exception => LOG.error("occurs exception when closing ObjectInputStream : {}", ex)
      }
      try {
        if (bis.nonEmpty) bis.foreach(_.close())
      } catch {
        case ex: Exception =>
          LOG.error("occurs exception when closing ByteArrayInputStream : {}", ex)
      }
    }
  }

  private def snapshot: Array[Byte] = {
    LOG.info("do snapshot ")
    var buffer: Option[ByteArrayOutputStream] = None
    var oout: Option[ObjectOutputStream] = None
    try {
      buffer = Some(new ByteArrayOutputStream)
      oout = Some(new ObjectOutputStream(buffer.get))
      oout.get.writeObject(encodedKeyStateMap)
      oout.get.flush()
      buffer.get.toByteArray
    } catch {
      case ex: IOException => throw new RuntimeException(ex)
      case e: Exception => throw new RuntimeException(e)
    } finally {
      try {
        if (oout.nonEmpty) oout.foreach(_.close())
      } catch {
        case ex: Exception => LOG.error("occurs exception when closing ObjectOutputStream : {}", ex)
      }
      try {
        if (buffer.nonEmpty) buffer.foreach(_.close())
      } catch {
        case ex: Exception =>
          LOG.error("occurs exception when closing ByteArrayOutputStream : {}", ex)
      }
    }
  }

  private class StreamingRuntimeContext(startTime: Instant) extends RuntimeContext {

    override def getStateInternals[KT](keyCoder: Coder[KT], key: KT): StateInternals = {
      if (!inited) {
        throw new RuntimeException(" please init state access object in `open` method! ")
      }
      if (encodedKeyStateMap == null) {
        encodedKeyStateMap = new util.HashMap[String, Table[String, String, Array[Byte]]]()
      }

      val factory = new HeapStateInternalsFactory[KT](keyCoder, encodedKeyStateMap)
      factory.stateInternalsForKey(key)
    }

    override def getStartTime: Instant = startTime
  }

}