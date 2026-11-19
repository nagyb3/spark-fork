/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.status.api.v1

import com.fasterxml.jackson.databind.JsonNode

import org.apache.spark.SparkFunSuite
import org.apache.spark.shuffle.RecordProcessTimeHistogram
import org.apache.spark.status.TaskDataWrapper

class ShuffleReadMetricsSuite extends SparkFunSuite {

  private val mapper = new JacksonMessageWriter().mapper

  private def taskShuffleReadMetrics(
      recordsProcessTime: Long,
      recordProcessTimeHistogram: Map[Long, Long]): ShuffleReadMetrics = {
    // The status store keeps the summary of a task's histogram, computed once.
    val (samples, percentiles) = RecordProcessTimeHistogram.summarize(recordProcessTimeHistogram)
    new TaskDataWrapper(
      1L, 0, 0, 0, 0L, 0L, 0L,
      "1", "host", "SUCCESS", "ANY", false, Nil, None, true,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, recordsProcessTime, Map.empty[Long, Long], Map.empty[Long, Long], Map.empty[Long, Long],
      Map.empty[Long, Long], samples, percentiles, Map.empty[String, Long], 0, 0, 0,
      0, 0
    ).toApi.taskMetrics.get.shuffleReadMetrics
  }

  test("task list reports the total records process time and record time percentiles") {
    val histogram = (1 to 100)
      .map(i => RecordProcessTimeHistogram.bucket(i * 1000L))
      .groupMapReduce(identity)(_ => 1L)(_ + _)
    val json = mapper.valueToTree[JsonNode](taskShuffleReadMetrics(33L, histogram))

    assert(json.get("recordsProcessTime").asLong === 33L)
    assert(json.get("recordProcessTimeSamples").asLong === 100L)
    val percentiles = json.get("recordProcessTimeNanosPercentiles")
    Seq("p50" -> 50000.0, "p90" -> 90000.0, "p99" -> 99000.0).foreach { case (name, expected) =>
      assert(math.abs(percentiles.get(name).asLong - expected) / expected < 0.04, name)
    }
    assert(!percentiles.has("max"))
    assert(!json.has("recordProcessTimeHistogram"))
    assert(!json.has("recordProcessTimeSummary0"))
  }

  test("record time percentiles are omitted when no records were sampled") {
    val json = mapper.valueToTree[JsonNode](taskShuffleReadMetrics(0L, Map.empty))
    assert(!json.has("recordProcessTimeNanosPercentiles"))
    assert(json.get("recordProcessTimeSamples").asLong === 0L)
  }
}
