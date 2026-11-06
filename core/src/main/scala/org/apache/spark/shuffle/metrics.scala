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

import java.util.concurrent.ThreadLocalRandom

import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockChunkId, ShuffleBlockId}
import org.apache.spark.util.Clock

/**
 * An interface for reporting shuffle read metrics, for each shuffle. This interface assumes
 * all the methods are called on a single-threaded, i.e. concrete implementations would not need
 * to synchronize.
 *
 * All methods have additional Spark visibility modifier to allow public, concrete implementations
 * that still have these methods marked as private[spark].
 */
private[spark] trait ShuffleReadMetricsReporter {
  private[spark] def incRemoteBlocksFetched(v: Long): Unit
  private[spark] def incLocalBlocksFetched(v: Long): Unit
  private[spark] def incRemoteBytesRead(v: Long): Unit
  private[spark] def incRemoteBytesReadToDisk(v: Long): Unit
  private[spark] def incLocalBytesRead(v: Long): Unit
  private[spark] def incShuffleSourceBytes(sourceTaskId: Long, bytes: Long): Unit
  private[spark] def incBlocksFetchedSource(sourceTaskId: Long, blocks: Long): Unit
  private[spark] def incRecordsReadSource(sourceTaskId: Long, records: Long): Unit
  private[spark] def incFetchWaitTime(v: Long): Unit
  private[spark] def incFetchWaitTimeSource(sourceTaskId: Long, v: Long): Unit
  private[spark] def incRecordProcessTimeHistogram(bucket: Long, count: Long): Unit
  private[spark] def incRecordsProcessTime(v: Long): Unit
  private[spark] def incRecordsRead(v: Long): Unit
  private[spark] def incCorruptMergedBlockChunks(v: Long): Unit
  private[spark] def incMergedFetchFallbackCount(v: Long): Unit
  private[spark] def incRemoteMergedBlocksFetched(v: Long): Unit
  private[spark] def incLocalMergedBlocksFetched(v: Long): Unit
  private[spark] def incRemoteMergedChunksFetched(v: Long): Unit
  private[spark] def incLocalMergedChunksFetched(v: Long): Unit
  private[spark] def incRemoteMergedBytesRead(v: Long): Unit
  private[spark] def incLocalMergedBytesRead(v: Long): Unit
  private[spark] def incRemoteReqsDuration(v: Long): Unit
  private[spark] def incRemoteReqsDurationByExecutor(executorId: String, v: Long): Unit
  private[spark] def incRemoteMergedReqsDuration(v: Long): Unit
}

private[spark] object ShuffleReadMetricsReporter {

  /**
   * The source task ID that a shuffle block is attributed to in the per-source shuffle read
   * metrics, or None for blocks that are not attributed to a source.
   */
  def sourceTaskId(blockId: BlockId): Option[Long] = blockId match {
    case s: ShuffleBlockId => Some(s.mapId)
    case b: ShuffleBlockBatchId => Some(b.mapId)
    case c: ShuffleBlockChunkId => Some(c.shuffleMergeId.toLong)
    case _ => None
  }
}

private[spark] object RecordProcessTimeHistogram {
  private val SUB_BUCKET_BITS = 4
  private val SUB_BUCKETS = 1L << SUB_BUCKET_BITS

  def bucket(nanos: Long): Long = {
    val value = math.max(nanos, 0L)
    if (value < SUB_BUCKETS) {
      value
    } else {
      val shift = 63 - java.lang.Long.numberOfLeadingZeros(value) - SUB_BUCKET_BITS
      shift * SUB_BUCKETS + (value >> shift)
    }
  }

  def representativeNanos(bucket: Long): Long = {
    if (bucket < SUB_BUCKETS * 2) {
      bucket
    } else {
      val shift = (bucket / SUB_BUCKETS - 1).toInt
      val lowerBound = (bucket % SUB_BUCKETS + SUB_BUCKETS) << shift
      lowerBound + (1L << shift) / 2
    }
  }

  def percentiles(histogram: Map[Long, Long], probabilities: Seq[Double]): Option[Seq[Long]] = {
    val buckets = histogram.toSeq.filter(_._2 > 0).sortBy(_._1)
    val total = buckets.map(_._2).sum
    if (total == 0) {
      None
    } else {
      Some(probabilities.map { probability =>
        val rank = math.ceil(probability * total).toLong
        var seen = 0L
        val (bucket, _) = buckets.find { case (_, count) =>
          seen += count
          seen >= rank
        }.get
        representativeNanos(bucket)
      })
    }
  }
}

private[spark] class RecordProcessTimeSampler(
    sampleInterval: Int,
    clock: Clock,
    readMetrics: ShuffleReadMetricsReporter) {
  private[this] var recordsUntilSample = 1L
  private[this] var sampleStartTime = -1L

  private def onRecordPull(): Unit = {
    recordsUntilSample -= 1
    if (sampleStartTime >= 0 || recordsUntilSample == 0) {
      val now = clock.nanoTime()
      if (sampleStartTime >= 0) {
        readMetrics.incRecordProcessTimeHistogram(
          RecordProcessTimeHistogram.bucket(now - sampleStartTime), 1L)
      }
      if (recordsUntilSample == 0) {
        sampleStartTime = now
        recordsUntilSample = ThreadLocalRandom.current().nextLong(1L, 2L * sampleInterval)
      } else {
        sampleStartTime = -1L
      }
    }
  }

  private def onBlockExhausted(): Unit = {
    if (sampleStartTime >= 0) {
      sampleStartTime = -1L
      recordsUntilSample = 1L
    }
  }

  def wrap[T](records: Iterator[T]): Iterator[T] = new Iterator[T] {
    private[this] var pullPending = true

    override def hasNext: Boolean = {
      if (pullPending) {
        pullPending = false
        onRecordPull()
      }
      val hasMore = records.hasNext
      if (!hasMore) {
        onBlockExhausted()
      }
      hasMore
    }

    override def next(): T = {
      pullPending = true
      records.next()
    }
  }
}


/**
 * An interface for reporting shuffle write metrics. This interface assumes all the methods are
 * called on a single-threaded, i.e. concrete implementations would not need to synchronize.
 *
 * All methods have additional Spark visibility modifier to allow public, concrete implementations
 * that still have these methods marked as private[spark].
 */
private[spark] trait ShuffleWriteMetricsReporter {
  private[spark] def incBytesWritten(v: Long): Unit
  private[spark] def incRecordsWritten(v: Long): Unit
  private[spark] def incWriteTime(v: Long): Unit
  private[spark] def decBytesWritten(v: Long): Unit
  private[spark] def decRecordsWritten(v: Long): Unit
}
