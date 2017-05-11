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

package org.apache.gearpump.streaming.examples.window.dsl

import java.time.{Duration, Instant}

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.streaming.dsl.scalaapi.{LoggerSink, StreamApp}
import org.apache.gearpump.streaming.dsl.window.api.{EventTimeTrigger, SessionWindows}
import org.apache.gearpump.streaming.source.DataSource
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.AkkaApp

object SessionWindow extends AkkaApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array.empty

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val context = ClientContext(akkaConf)
    val app = StreamApp("sessionWindowDsl", context)

    app.source[String](new SessionTimedDataSource)
      .flatMap(line => line.split("[\\s]+"))
      .map((_, 1))
      .window(SessionWindows.apply(Duration.ofMillis(4L))
        .triggering(EventTimeTrigger))
      .groupBy(_._1)
      .sum
      .sink(new LoggerSink)

    context.submit(app)
    context.close()
  }

  private class SessionTimedDataSource extends DataSource {

    private var data = List(
      Message("foo", Instant.ofEpochMilli(1L)),
      Message("bar", Instant.ofEpochMilli(8L)),
      Message("foo", Instant.ofEpochMilli(15L)),
      Message("bar", Instant.ofEpochMilli(17L)),
      Message("foo", Instant.ofEpochMilli(25L)),
      Message("foo", Instant.ofEpochMilli(26L)),
      Message("bar", Instant.ofEpochMilli(30L)),
      Message("bar", Instant.ofEpochMilli(31L))
    )

    private var watermark: Instant = Instant.ofEpochMilli(0)

    override def open(context: TaskContext, startTime: Instant): Unit = {}

    override def read(): Message = {
      if (data.nonEmpty) {
        val msg = data.head
        data = data.tail
        watermark = msg.timestamp
        msg
      } else {
        null
      }
    }

    override def close(): Unit = {}

    override def getWatermark: Instant = {
      if (data.isEmpty) {
        watermark = watermark.plusMillis(1)
      }
      watermark
    }
  }

}
