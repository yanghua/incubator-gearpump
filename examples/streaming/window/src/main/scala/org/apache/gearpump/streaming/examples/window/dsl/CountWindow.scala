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

import java.time.Instant

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.streaming.dsl.scalaapi.{LoggerSink, StreamApp}
import org.apache.gearpump.streaming.dsl.window.api.{CountTrigger, CountWindows}
import org.apache.gearpump.streaming.source.DataSource
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.AkkaApp

object CountWindow extends AkkaApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array.empty

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val context = ClientContext(akkaConf)
    val app = StreamApp("countWindowDsl", context)
    app.source[String](new TestDataSource)
      .map((_, 1))
      .window(CountWindows.apply(3).triggering(CountTrigger))
      .groupBy(_._1)
      .sum
      .sink(new LoggerSink)
  }

  private class TestDataSource extends DataSource {

    private var data = List(
      Message("foo", 1),
      Message("bar", 2),
      Message("foo", 3),
      Message("bar", 4),
      Message("bar", 5),
      Message("foo", 6),
      Message("foo", 7),
      Message("bar", 8)
    )

    override def open(context: TaskContext, startTime: Instant): Unit = {}

    override def read(): Message = {
      if (data.nonEmpty) {
        val msg = data.head
        data = data.tail
        msg
      } else {
        null
      }
    }

    override def close(): Unit = {}

    override def getWatermark: Instant = {
      null
    }
  }


}
