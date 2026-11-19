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

package org.apache.spark.shuffle

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.util.ManualClock

class RecordProcessTimeSamplerSuite extends SparkFunSuite {

  private def bucketOfMs(ms: Long): Long =
    RecordProcessTimeHistogram.bucket(TimeUnit.MILLISECONDS.toNanos(ms))

  /** Reads the records of one block the way the shuffle reader drives the sampler. */
  private def readBlock[T](sampler: RecordProcessTimeSampler, clock: ManualClock,
      records: Iterator[T])(process: T => Unit): Unit = {
    records.foreach { record =>
      sampler.recordDelivered()
      process(record)
    }
    sampler.blockFinished(clock.nanoTime())
  }

  test("one in every sampleInterval records is timed on average") {
    val clock = new ManualClock()
    val metrics = new TempShuffleReadMetrics
    val sampler = new RecordProcessTimeSampler(10, clock, metrics)
    readBlock(sampler, clock, (1 to 100000).iterator)(_ => clock.advance(5))
    val samples = metrics.recordProcessTimeHistogram(bucketOfMs(5))
    assert(samples > 9000L && samples < 11000L, s"samples: $samples")
  }

  test("every record is timed with a sampleInterval of 1") {
    val clock = new ManualClock()
    val metrics = new TempShuffleReadMetrics
    val sampler = new RecordProcessTimeSampler(1, clock, metrics)
    readBlock(sampler, clock, (1 to 6).iterator)(_ => clock.advance(5))
    assert(metrics.recordProcessTimeHistogram === Map(bucketOfMs(5) -> 6L))
  }

  test("the first record is timed so that small tasks get a sample") {
    val clock = new ManualClock()
    val metrics = new TempShuffleReadMetrics
    val sampler = new RecordProcessTimeSampler(100, clock, metrics)
    readBlock(sampler, clock, Iterator(1))(_ => clock.advance(5))
    assert(metrics.recordProcessTimeHistogram === Map(bucketOfMs(5) -> 1L))
  }

  test("time spent between blocks is not part of any record") {
    val clock = new ManualClock()
    val metrics = new TempShuffleReadMetrics
    val sampler = new RecordProcessTimeSampler(1, clock, metrics)
    Seq(Seq(1, 2), Seq(3)).foreach { block =>
      clock.advance(1000)
      readBlock(sampler, clock, block.iterator)(_ => clock.advance(5))
    }
    assert(metrics.recordProcessTimeHistogram === Map(bucketOfMs(5) -> 3L))
  }

  test("records that are slow at a regular rhythm are sampled at their real rate") {
    val clock = new ManualClock()
    val metrics = new TempShuffleReadMetrics
    val sampler = new RecordProcessTimeSampler(100, clock, metrics)
    readBlock(sampler, clock, (1 to 1000000).iterator) { record =>
      clock.advance(if (record % 100 == 50) 1000 else 1)
    }
    val slowSamples = metrics.recordProcessTimeHistogram.getOrElse(bucketOfMs(1000), 0L)
    assert(slowSamples > 50L && slowSamples < 200L, s"slow samples: $slowSamples")
  }

  test("the histogram can be read while the task thread updates it") {
    val numBuckets = 100000
    (1 to 5).foreach { _ =>
      val metrics = new TempShuffleReadMetrics
      val writer = new Thread(() => {
        (0 until numBuckets).foreach(i => metrics.incRecordProcessTimeHistogram(i.toLong, 1L))
      })
      writer.start()
      var lastTotal = 0L
      while (writer.isAlive) {
        val total = metrics.recordProcessTimeHistogram.values.sum
        assert(total >= lastTotal, "a snapshot lost records that an earlier one had")
        lastTotal = total
      }
      writer.join()
      assert(metrics.recordProcessTimeHistogram.values.sum === numBuckets.toLong)
    }
  }

  test("histogram buckets keep record times within about 3%") {
    val values = (0 until 40).flatMap(shift => Seq(1L << shift, (1L << shift) + (1L << shift) / 3))
    values.foreach { value =>
      val estimate = RecordProcessTimeHistogram.representativeNanos(
        RecordProcessTimeHistogram.bucket(value))
      assert(math.abs(estimate - value).toDouble <= math.max(1.0, value * 0.0325), s"$value")
    }
    assert(values.sorted.map(RecordProcessTimeHistogram.bucket) ===
      values.sorted.map(RecordProcessTimeHistogram.bucket).sorted)
  }

  test("percentiles use the nearest rank and ignore empty buckets") {
    val histogram = Map(1L -> 90L, 2L -> 9L, 3L -> 1L, 4L -> 0L)
    assert(RecordProcessTimeHistogram.percentiles(histogram, Seq(0.5, 0.9, 0.99, 1.0)) ===
      Some(Seq(1L, 1L, 2L, 3L)))
    assert(RecordProcessTimeHistogram.percentiles(Map(1L -> 0L), Seq(0.5)) === None)
  }
}
