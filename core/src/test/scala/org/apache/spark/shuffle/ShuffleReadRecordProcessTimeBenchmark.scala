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

import java.io.{DataInputStream, DataOutputStream, File, FileOutputStream, InputStream,
  OutputStream, PrintWriter}
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Random

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.output.TeeOutputStream

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.scheduler.{AccumulableInfo, SparkListener, SparkListenerTaskEnd}
import org.apache.spark.serializer.{DeserializationStream, KryoSerializer, SerializationStream,
  Serializer, SerializerInstance}
import org.apache.spark.status.api.v1

/**
 * Measures what each record process time metric costs: `recordsProcessTime` (a per block timer)
 * and `recordProcessTimeSamples` / `recordProcessTimeNanosPercentiles` (both derived from the
 * same sampled histogram, which only differ when the REST API serializes them). The variants:
 *  - off: `spark.shuffle.recordProcessTime.enabled=false`
 *  - timing: enabled with `sampleInterval=0`, so only `recordsProcessTime` is measured
 *  - full: enabled with the default sample interval, so all three metrics
 *  - baseline: the parent commit of the metrics, which has no record process time code at all
 * so that timing vs off is the cost of `recordsProcessTime`, and full vs timing the cost of the
 * histogram.
 *
 * Each scenario writes its shuffle once; every timed job re-runs only the reduce stage (the map
 * stage is skipped as its outputs are still registered), so the measurement is
 * `BlockStoreShuffleReader.read()` plus counting the records.
 *
 * It has three parts:
 *  - executor (default): the executor CPU time of the reduce tasks, each variant in its own
 *    child JVM, rotating the order per round. Toggling the config within one JVM is not reliable
 *    for this: the JIT compiles the shared iterator code differently depending on what ran
 *    before, which moves the results by more than the overhead itself. Its default scenarios are
 *    the worst cases: 1 record per block for the per block timer, and 100k records per block
 *    with a serializer as cheap as SQL's UnsafeRowSerializer for the per record sampling.
 *  - driver (default): stages with 1k tasks on 4 cores, off and full in child JVMs, measuring
 *    the driver: the CPU time of the DAG scheduler event loop (which merges the accumulator
 *    updates of every task), of the status listener and of the task result getters, the GC time,
 *    the heap the job leaves behind per task (mostly the status store), the time to serialize
 *    the stage's tasks as the REST API does, and the time of each REST getter. The driver cost
 *    grows with the histogram buckets per task, so its scenarios spread the time of the first
 *    records of every task over 0, 32 or 128 buckets, sampling every record to fill them.
 *  - sweep (optional): the cost per sampled record, in one JVM, from variants that sample every
 *    1, 10, 100 and 1000 records, interleaved per round with "off". The effects are much larger
 *    than the JIT noise, and "off (A/A)" shows the noise floor.
 * The executor and driver parts report the median of the per-round ratios between variants,
 * with a bootstrap 95% confidence interval. The default run takes ~10 minutes.
 *
 * The configs are set by their string keys, so that this file also compiles and runs on the
 * baseline. dev/record-process-time-benchmark has the scripts that check out and build it, and
 * write its test classpath to ../spark-baseline/core-test-classpath.txt (next to this repo),
 * where this benchmark looks for it by default. Both builds must contain the same version of
 * this file: the JIT is sensitive enough to how the benchmark itself is compiled to show
 * differences of several percent otherwise.
 *
 * Every run stores its results in dev/record-process-time-benchmark/results/<yyyy-MM-dd_HHmm>/:
 *  - summary.txt: all the tables and the progress of the child JVMs
 *  - rounds.tsv: every metric of every child JVM (round, part, variant, scenario, metric, value)
 *  - logs/: the output of every child JVM
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "core/Test/runMain <this class> [options]"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ShuffleReadRecordProcessTimeBenchmark-results.txt".
 * }}}
 *
 * Options:
 *   --parts <a,b,...>             parts to run: sweep, executor, driver; executor,driver by default
 *   --rounds <n>                  child JVMs per variant in the executor part, 5 by default
 *   --driver-rounds <n>           child JVMs per variant in the driver part, 3 by default
 *   --scenarios <a,b,...>         scenarios of the executor and driver parts, by name
 *   --baseline-classpath <file>   test classpath of the baseline build, by default
 *                                 ../spark-baseline/core-test-classpath.txt next to this repo
 *   --no-baseline                 skip the baseline variant (of the executor part)
 *   --warmup-seconds <n>          minimum warm-up time per executor scenario, 2 by default
 *   --measure-seconds <n>         minimum measured time per executor scenario, 5 by default
 *   --master <master>             Spark master of the executor and sweep parts, `local[1]`
 *   --driver-master <master>      Spark master of the driver part, `local[4]` by default
 * Internal, used for the child JVMs:
 *   --variant off|timing|full     run the scenarios with a single variant
 *   --baseline                    running on a build without the metrics: expect no samples
 */
object ShuffleReadRecordProcessTimeBenchmark extends BenchmarkBase {

  private val ENABLED_KEY = "spark.shuffle.recordProcessTime.enabled"
  private val SAMPLE_INTERVAL_KEY = "spark.shuffle.recordProcessTime.sampleInterval"
  private val HISTOGRAM_ACCUM_NAME = "internal.metrics.shuffle.read.recordProcessTimeHistogram"

  private var warmupNanos = TimeUnit.SECONDS.toNanos(2)
  private val MIN_WARMUP_ROUNDS = 2
  private var measureNanos = TimeUnit.SECONDS.toNanos(5)
  private val MIN_MEASURE_ROUNDS = 10
  private val DRIVER_WARMUP_JOBS = 1
  private val DRIVER_MEASURED_JOBS = 2
  /**
   * The driver part samples every record, so that the spread scenarios fill their buckets with
   * a few hundred records per task, as a task with ~100x more records would by default. The
   * driver cost depends on the buckets per task, not on the samples in them.
   */
  private val DRIVER_SAMPLE_INTERVAL = 1
  private val BOOTSTRAP_RESAMPLES = 2000

  private case class Variant(label: String, enabled: Boolean, sampleInterval: Int = 100) {
    def histogram: Boolean = enabled && sampleInterval > 0
  }

  private val OFF = Variant("off", enabled = false)
  private val TIMING = Variant("timing", enabled = true, sampleInterval = 0)
  private val FULL = Variant("full", enabled = true)
  private val OFF_AA = OFF.copy(label = "off (A/A)")
  private val childVariants = Seq(OFF, TIMING, FULL).map(v => v.label -> v).toMap

  /**
   * Executor scenarios: records per block decide whether the per block or the per record cost
   * dominates, and the serializer how much a record costs without the metrics. Driver scenarios:
   * 1k tasks, each reading 400 records from 10 map tasks, where the first `spreadBuckets` records
   * of every task take from 700 ns up to 700 ns * 2^(spreadBuckets / 16), so that their samples
   * land in about `spreadBuckets` buckets (16 per power of two) besides the natural ones.
   */
  private case class Scenario(
      name: String,
      numMaps: Int,
      numReducers: Int,
      recordsPerBlock: Int,
      rawSerializer: Boolean = false,
      driver: Boolean = false,
      spreadBuckets: Int = 0) {
    def recordsPerMap: Int = numReducers * recordsPerBlock
    def totalRecords: Long = numMaps.toLong * recordsPerMap
  }

  private val scenarios = Seq(
    // Reading a block costs ~30 us here, so this one is kept small.
    Scenario("1 record/block", numMaps = 100, numReducers = 100, recordsPerBlock = 1),
    Scenario("1k records/block", numMaps = 50, numReducers = 50, recordsPerBlock = 1000),
    Scenario("100k records/block", numMaps = 10, numReducers = 10, recordsPerBlock = 100000),
    Scenario("100k records/block (raw)", numMaps = 10, numReducers = 10, recordsPerBlock = 100000,
      rawSerializer = true),
    Scenario("1k tasks (no spread)", numMaps = 10, numReducers = 1000, recordsPerBlock = 40,
      driver = true),
    Scenario("1k tasks (32 buckets)", numMaps = 10, numReducers = 1000, recordsPerBlock = 40,
      driver = true, spreadBuckets = 32),
    Scenario("1k tasks (128 buckets)", numMaps = 10, numReducers = 1000,
      recordsPerBlock = 40, driver = true, spreadBuckets = 128))
  private val scenariosByName = scenarios.map(s => s.name -> s).toMap

  private val defaultScenarios = Seq("1 record/block", "100k records/block (raw)",
    "1k tasks (no spread)", "1k tasks (32 buckets)", "1k tasks (128 buckets)")
    .map(scenariosByName)

  private val sweepScenarios = Seq("1k records/block", "100k records/block").map(scenariosByName)
  private val sampleIntervalVariants = Seq(OFF) ++
    Seq(1, 10, 100, 1000).map(i => Variant(s"full, interval $i", enabled = true, i)) ++
    Seq(OFF_AA)

  private case class IterationStats(
      wallNanos: Long,
      cpuNanos: Long,
      tasks: Long,
      samples: Long,
      histogramEntries: Long,
      collectionEntries: Long)

  /** Sums the executor metrics of the tasks that ended since the last reset. */
  private class TaskTimeListener extends SparkListener {
    private var cpuNanos = 0L
    private var tasks = 0L
    private var samples = 0L
    private var histogramEntries = 0L
    private var collectionEntries = 0L
    /** The stage of the last task that ended. */
    @volatile var lastStageId = -1

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
      lastStageId = taskEnd.stageId
      if (taskEnd.taskMetrics != null) {
        cpuNanos += taskEnd.taskMetrics.executorCpuTime
      }
      tasks += 1
      taskEnd.taskInfo.accumulables.foreach { acc =>
        val entries = acc.update match {
          case Some(list: java.util.List[_]) => list.size
          case Some(map: scala.collection.Map[_, _]) => map.size
          case _ => 0
        }
        collectionEntries += entries
        if (acc.name.contains(HISTOGRAM_ACCUM_NAME)) {
          histogramEntries += entries
        }
      }
      samples += histogramSamples(taskEnd.taskInfo.accumulables)
    }

    def reset(): Unit = synchronized {
      cpuNanos = 0L
      tasks = 0L
      samples = 0L
      histogramEntries = 0L
      collectionEntries = 0L
    }

    def snapshot(wallNanos: Long): IterationStats = synchronized {
      IterationStats(wallNanos, cpuNanos, tasks, samples, histogramEntries, collectionEntries)
    }
  }

  /** Number of record process time samples in a task's accumulator updates, by name only. */
  private def histogramSamples(accumulables: Seq[AccumulableInfo]): Long = {
    accumulables.filter(_.name.contains(HISTOGRAM_ACCUM_NAME)).flatMap(_.update).map {
      case buckets: scala.collection.Map[_, _] =>
        buckets.values.collect { case count: Long => count }.sum
      case _ => 0L
    }.sum
  }

  @volatile private var blackhole = 0L

  private var master = "local[1]"
  private var sc: SparkContext = _
  private var listener: TaskTimeListener = _
  private var baseline = false

  /** The directory this run stores its results in; not set in the child JVMs. */
  private var resultsDir: File = _
  /** summary.txt in the results directory. */
  private var summaryOut: Option[OutputStream] = None

  /** The root of the repo: sbt passes it as spark.test.home, while the JVM runs in core/. */
  private def repoRoot: File = {
    val root = sys.props.get("spark.test.home").orElse(sys.env.get("SPARK_HOME")).getOrElse(".")
    new File(root).getCanonicalFile
  }

  /** dev/record-process-time-benchmark/results/<yyyy-MM-dd_HHmm>, with a suffix if taken. */
  private def createResultsDir(): File = {
    val parent = new File(repoRoot, "dev/record-process-time-benchmark/results")
    val name = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmm"))
    val dir = Iterator.from(1)
      .map(i => new File(parent, if (i == 1) name else s"${name}_$i"))
      .find(!_.exists())
      .get
    Files.createDirectories(dir.toPath)
    dir
  }

  // ---------------------------------------------------------------------------------------------
  // Running scenarios within this JVM
  // ---------------------------------------------------------------------------------------------

  private def withSparkContext(f: => Unit): Unit = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(this.getClass.getSimpleName)
      .set("spark.ui.enabled", "false")
    sc = new SparkContext(conf)
    listener = new TaskTimeListener
    sc.addSparkListener(listener)
    try f finally {
      sc.stop()
      sc = null
    }
  }

  private def applyVariant(variant: Variant): Unit = {
    val conf = SparkEnv.get.conf
    conf.set(ENABLED_KEY, variant.enabled.toString)
    conf.set(SAMPLE_INTERVAL_KEY, variant.sampleInterval.toString)
  }

  private def buildShuffle(scenario: Scenario): ShuffledRDD[Int, Int, Int] = {
    val recordsPerMap = scenario.recordsPerMap
    val data = sc.parallelize(0 until scenario.numMaps, scenario.numMaps).flatMap { mapId =>
      // Keys 0 until recordsPerMap hash to every reducer recordsPerBlock times per map.
      Iterator.tabulate(recordsPerMap)(i => (i, mapId + i))
    }
    val serializer =
      if (scenario.rawSerializer) new RawIntPairSerializer else new KryoSerializer(sc.getConf)
    new ShuffledRDD[Int, Int, Int](data, new HashPartitioner(scenario.numReducers))
      .setSerializer(serializer)
  }

  /**
   * Runs the reduce stage of the shuffle, counting the records it produces. The first
   * `spreadBuckets` records of every task take increasingly long, see [[Scenario]].
   */
  private def readShuffle(shuffled: ShuffledRDD[Int, Int, Int], scenario: Scenario): Unit = {
    val spreadBuckets = scenario.spreadBuckets
    val counts = sc.runJob(shuffled, (records: Iterator[(Int, Int)]) => {
      var n = 0L
      while (records.hasNext) {
        records.next()
        if (n < spreadBuckets) {
          val until = System.nanoTime() + (700 * math.pow(2, n / 16.0)).toLong
          while (System.nanoTime() < until) {}
        }
        n += 1
      }
      n
    })
    val records = counts.sum
    assert(records == scenario.totalRecords,
      s"${scenario.name}: read $records records, expected ${scenario.totalRecords}")
    blackhole += records
  }

  private def verifySamples(scenario: Scenario, variant: Variant, stats: IterationStats): Unit = {
    if (!variant.histogram || baseline) {
      assert(stats.samples == 0,
        s"${scenario.name} / ${variant.label}: expected no samples, got ${stats.samples}")
    } else {
      assert(stats.samples > 0,
        s"${scenario.name} / ${variant.label}: the record process time was not sampled")
    }
  }

  private def runIteration(
      shuffled: ShuffledRDD[Int, Int, Int],
      scenario: Scenario,
      variant: Variant): IterationStats = {
    applyVariant(variant)
    // So that garbage from the previous iteration doesn't get collected during this one.
    System.gc()
    listener.reset()
    val start = System.nanoTime()
    readShuffle(shuffled, scenario)
    val wallNanos = System.nanoTime() - start
    sc.listenerBus.waitUntilEmpty()
    val stats = listener.snapshot(wallNanos)
    verifySamples(scenario, variant, stats)
    stats
  }

  /**
   * Runs the variants of an executor scenario interleaved: every round runs each variant once,
   * in a random order, so that the slow drift of the machine affects all of them alike.
   */
  private def runScenario(scenario: Scenario, variants: Seq[Variant]): Unit = {
    // scalastyle:off println
    println(s"Running scenario: ${scenario.name}")
    // scalastyle:on println
    val shuffled = buildShuffle(scenario)
    // Materializes the map outputs, which every iteration below reuses.
    readShuffle(shuffled, scenario)

    val warmupStart = System.nanoTime()
    var warmupRounds = 0
    while (warmupRounds < MIN_WARMUP_ROUNDS || System.nanoTime() - warmupStart < warmupNanos) {
      variants.foreach(runIteration(shuffled, scenario, _))
      warmupRounds += 1
    }

    val random = new Random(scenario.name.hashCode)
    val rounds = mutable.ArrayBuffer[Map[String, IterationStats]]()
    val measureStart = System.nanoTime()
    while (rounds.size < MIN_MEASURE_ROUNDS || System.nanoTime() - measureStart < measureNanos) {
      rounds += random.shuffle(variants)
        .map(variant => variant.label -> runIteration(shuffled, scenario, variant))
        .toMap
    }
    printScenarioSummary(scenario, variants, rounds.toSeq)
    shuffled.cleanShuffleDependencies(blocking = true)
  }

  /**
   * Prints the median time per iteration of each variant, the paired overhead of each variant
   * relative to "off" if there is more than one, and a machine readable RESULT line per variant.
   */
  private def printScenarioSummary(
      scenario: Scenario,
      variants: Seq[Variant],
      rounds: Seq[Map[String, IterationStats]]): Unit = {
    def medianOf(variant: Variant)(f: IterationStats => Double): Double =
      median(rounds.map(round => f(round(variant.label))))

    printLine(s"${scenario.name}: ${scenario.numMaps} maps x ${scenario.numReducers} reducers, " +
      s"${scenario.recordsPerBlock} records/block, ${scenario.totalRecords} records, " +
      s"${rounds.size} rounds")
    printLine(f"${"Median per iteration"}%-24s ${"Wall(ms)"}%10s ${"Exec CPU(ms)"}%13s " +
      f"${"CPU/rec(ns)"}%12s ${"Samples"}%9s")
    printLine("-" * 72)
    variants.foreach { variant =>
      val cpuMs = medianOf(variant)(_.cpuNanos / 1e6)
      printLine(f"${variant.label}%-24s ${medianOf(variant)(_.wallNanos / 1e6)}%10.1f " +
        f"$cpuMs%13.1f ${cpuMs * 1e6 / scenario.totalRecords}%12.2f " +
        f"${medianOf(variant)(_.samples.toDouble).toLong}%9d")
    }

    if (variants.contains(OFF) && variants.size > 1) {
      val random = new Random(42)
      def ratioWithCi(variant: Variant)(f: IterationStats => Double): String =
        formatRatio(rounds.map(round => f(round(variant.label)) / f(round(OFF.label))), random)
      printLine("")
      printLine(f"${"Paired vs off, median [95% CI]"}%-34s ${"Exec CPU"}%-28s ${"Wall"}%-28s")
      printLine("-" * 92)
      variants.filterNot(_ == OFF).foreach { variant =>
        printLine(f"${variant.label}%-34s ${ratioWithCi(variant)(_.cpuNanos.toDouble)}%-28s " +
          f"${ratioWithCi(variant)(_.wallNanos.toDouble)}%-28s")
      }
    }
    printLine("")

    variants.foreach { variant =>
      printResult(scenario, variant, Seq(
        "wall_ms" -> medianOf(variant)(_.wallNanos / 1e6),
        "cpu_ms" -> medianOf(variant)(_.cpuNanos / 1e6)))
    }
  }

  private val threadMXBean = ManagementFactory.getThreadMXBean

  /** CPU time of the live threads whose name starts with the prefix. */
  private def threadCpuNanos(prefix: String): Long = {
    Thread.getAllStackTraces.keySet.asScala.toSeq
      .filter(_.getName.startsWith(prefix))
      .map(thread => threadMXBean.getThreadCpuTime(thread.getId))
      .filter(_ > 0)
      .sum
  }

  private def gcMillis(): Long =
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).filter(_ > 0).sum

  private def oldGenPools = ManagementFactory.getMemoryPoolMXBeans.asScala
    .filter(pool => pool.getName.contains("Old Gen") || pool.getName.contains("Tenured"))

  private def usedHeapBytes(): Long = {
    System.gc()
    val runtime = Runtime.getRuntime
    runtime.totalMemory() - runtime.freeMemory()
  }

  /**
   * Runs the reduce stage of a driver scenario as a few jobs and measures the driver: the CPU
   * time of the DAG scheduler event loop (which merges the accumulator updates of every task and
   * copies them into the task and stage infos), of the status listener and of the task result
   * getters, the GC time, the peak size of the old generation during the job, and the heap left
   * behind by the job (mostly the status store). Then measures the REST API on the last stage.
   */
  private def runDriverScenario(scenario: Scenario, variant: Variant): Unit = {
    // scalastyle:off println
    println(s"Running scenario: ${scenario.name}")
    // scalastyle:on println
    applyVariant(
      if (variant.histogram) variant.copy(sampleInterval = DRIVER_SAMPLE_INTERVAL) else variant)
    val shuffled = buildShuffle(scenario)
    // Materializes the map outputs, which every job below reuses.
    readShuffle(shuffled, scenario)
    (1 to DRIVER_WARMUP_JOBS).foreach(_ => readShuffle(shuffled, scenario))

    val jobs = (1 to DRIVER_MEASURED_JOBS).map { _ =>
      val heapBefore = usedHeapBytes()
      listener.reset()
      val dagBefore = threadCpuNanos("dag-scheduler-event-loop")
      val statusBefore = threadCpuNanos("spark-listener-group-appStatus")
      val resultBefore = threadCpuNanos("task-result-getter")
      val gcBefore = gcMillis()
      oldGenPools.foreach(_.resetPeakUsage())
      val start = System.nanoTime()
      readShuffle(shuffled, scenario)
      val wallNanos = System.nanoTime() - start
      sc.listenerBus.waitUntilEmpty()
      val metrics = Seq(
        "wall_ms" -> wallNanos / 1e6,
        "dag_cpu_ms" -> (threadCpuNanos("dag-scheduler-event-loop") - dagBefore) / 1e6,
        "status_cpu_ms" -> (threadCpuNanos("spark-listener-group-appStatus") - statusBefore) / 1e6,
        "result_cpu_ms" -> (threadCpuNanos("task-result-getter") - resultBefore) / 1e6,
        "gc_ms" -> (gcMillis() - gcBefore).toDouble,
        // Includes the garbage of the job that the old generation holds until it is collected.
        "peak_old_gen_mb" -> oldGenPools.map(_.getPeakUsage.getUsed).sum / 1e6)
      val stats = listener.snapshot(wallNanos)
      verifySamples(scenario, variant, stats)
      metrics ++ Seq(
        "retained_mb" -> (usedHeapBytes() - heapBefore) / 1e6,
        "retained_kb_per_task" -> (usedHeapBytes() - heapBefore) / 1e3 / stats.tasks,
        "histogram_entries_per_task" -> stats.histogramEntries.toDouble / stats.tasks,
        "list_entries_per_task" -> stats.collectionEntries.toDouble / stats.tasks)
    }
    val names = jobs.head.map(_._1)
    val medians = names.map(name => name -> median(jobs.map(_.toMap.apply(name)))) ++
      measureRest(listener.lastStageId)
    printLine(s"${scenario.name}: ${scenario.numMaps} maps x ${scenario.numReducers} reducers, " +
      s"${variant.label}, median of ${jobs.size} jobs")
    medians.foreach { case (name, value) => printLine(f"  $name%-28s $value%12.2f") }
    printLine("")
    printResult(scenario, variant, medians)
    shuffled.cleanShuffleDependencies(blocking = true)
  }

  /** The same JSON settings as the REST API's JacksonMessageWriter, minus the indentation. */
  private lazy val restMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .setDefaultPropertyInclusion(JsonInclude.Include.NON_ABSENT)

  /** The average time of a call, after a few warm-up calls, over at least ~300 ms. */
  private def nanosPerCall(f: => Any): Double = {
    (1 to 3).foreach(_ => blackhole += f.hashCode)
    val start = System.nanoTime()
    var calls = 0
    while (calls < 5 || System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(300)) {
      blackhole += f.hashCode
      calls += 1
    }
    (System.nanoTime() - start).toDouble / calls
  }

  /**
   * The REST API cost of the tasks of a stage: serializing them all as JSON (as
   * /stages/<id>/<attempt>/taskList and stage details do, a page at a time), the JSON size, and
   * the time of the recordProcessTimeSamples and recordProcessTimeNanosPercentiles getters, which
   * Jackson calls per task.
   */
  private def measureRest(stageId: Int): Seq[(String, Double)] = {
    val tasks = sc.statusStore.taskList(stageId, 0, Int.MaxValue)
    val readMetrics = tasks.flatMap(_.taskMetrics).map(_.shuffleReadMetrics)
    def getterNanos(getter: v1.ShuffleReadMetrics => Long): Double = nanosPerCall {
      var sum = 0L
      readMetrics.foreach(metrics => sum += getter(metrics))
      sum
    } / readMetrics.size
    Seq(
      "rest_us_per_task" -> nanosPerCall(restMapper.writeValueAsBytes(tasks)) / 1e3 / tasks.size,
      "json_bytes_per_task" -> restMapper.writeValueAsBytes(tasks).length.toDouble / tasks.size,
      "samples_getter_ns" -> getterNanos(_.recordProcessTimeSamples),
      "percentiles_getter_ns" -> getterNanos(_.recordProcessTimeNanosPercentiles.size))
  }

  /** The machine readable result of a scenario, read by the parent of a child JVM. */
  private def printResult(
      scenario: Scenario,
      variant: Variant,
      metrics: Seq[(String, Double)]): Unit = {
    val values = metrics.map { case (name, value) =>
      s"$name=${"%.3f".formatLocal(Locale.ROOT, value)}"
    }
    // scalastyle:off println
    println((Seq("RESULT", scenario.name, variant.label) ++ values).mkString("\t"))
    // scalastyle:on println
  }

  private def nanoTimeBenchmark(): Unit = {
    val calls = 10000000
    val benchmark = new Benchmark("System.nanoTime()", calls, minNumIters = 5, output = outputs)
    benchmark.addCase("System.nanoTime()") { _ =>
      var acc = 0L
      var i = 0
      while (i < calls) {
        acc += System.nanoTime()
        i += 1
      }
      blackhole += acc
    }
    benchmark.run()
  }

  // ---------------------------------------------------------------------------------------------
  // On vs off vs baseline, one child JVM per variant
  // ---------------------------------------------------------------------------------------------

  /** Where dev/record-process-time-benchmark/build-baseline.sh writes the classpath. */
  private def defaultBaselineClasspath: String =
    new File(repoRoot.getParentFile, "spark-baseline/core-test-classpath.txt").getPath

  private case class ForkedVariant(label: String, classpath: String, args: Seq[String])

  /** The metrics of every scenario, variant and round. */
  private type ForkedResults = Map[(String, String, Int), Map[String, Double]]

  private def forkedVariants(
      variants: Seq[Variant],
      baselineClasspath: Option[String]): Seq[ForkedVariant] = {
    val currentClasspath = System.getProperty("java.class.path")
    variants.map { v => ForkedVariant(v.label, currentClasspath, Seq("--variant", v.label)) } ++
      baselineClasspath.map { file =>
        val classpath = new String(Files.readAllBytes(Paths.get(file)), StandardCharsets.UTF_8)
        ForkedVariant("baseline", classpath.trim, Seq("--variant", "off", "--baseline"))
      }
  }

  /**
   * Runs the scenarios in a child JVM per variant and round, rotating the order of the variants
   * per round, and returns the metrics the children report.
   */
  private def runForked(
      part: String,
      forkedScenarios: Seq[Scenario],
      variants: Seq[ForkedVariant],
      numRounds: Int,
      childMaster: String,
      roundsTsv: PrintWriter)(
      progress: (Scenario, Map[String, Double]) => String): ForkedResults = {
    val javaBin = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    // The children get the same JVM options as this JVM, apart from a debugger agent.
    val jvmArgs = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.toSeq
      .filterNot(_.startsWith("-agentlib:jdwp"))
    val childArgs = Seq(
      "--scenarios", forkedScenarios.map(_.name).mkString(","),
      "--warmup-seconds", TimeUnit.NANOSECONDS.toSeconds(warmupNanos).toString,
      "--measure-seconds", TimeUnit.NANOSECONDS.toSeconds(measureNanos).toString,
      "--master", childMaster)
    val className = this.getClass.getName.stripSuffix("$")
    val logDir = new File(resultsDir, "logs")
    Files.createDirectories(logDir.toPath)

    val results = mutable.Map[(String, String, Int), Map[String, Double]]()
    (1 to numRounds).foreach { round =>
      // Rotates the order per round, so that no variant always runs first or last.
      val order = variants.indices.map(i => variants((i + round - 1) % variants.size))
      order.foreach { variant =>
        val log = new File(logDir, s"$part-round$round-${variant.label}.log")
        val command = Seq(javaBin) ++ jvmArgs ++ Seq("-cp", variant.classpath, className) ++
          variant.args ++ childArgs
        val processBuilder = new ProcessBuilder(command.asJava)
          .redirectErrorStream(true)
          .redirectOutput(log)
        // Otherwise the children would overwrite the results file of this JVM.
        processBuilder.environment().remove("SPARK_GENERATE_BENCHMARK_FILES")
        val exitCode = processBuilder.start().waitFor()
        require(exitCode == 0, s"${variant.label} failed with exit code $exitCode, see $log")

        val lines = Files.readAllLines(log.toPath).asScala.filter(_.startsWith("RESULT\t"))
        val summaries = lines.map { line =>
          val fields = line.split("\t")
          val scenario = scenariosByName(fields(1))
          val metrics = fields.drop(3).map { field =>
            val Array(name, value) = field.split("=", 2)
            roundsTsv.write(Seq(round, part, variant.label, scenario.name, name, value)
              .mkString("", "\t", "\n"))
            name -> value.toDouble
          }.toMap
          results((scenario.name, variant.label, round)) = metrics
          progress(scenario, metrics)
        }
        roundsTsv.flush()
        printProgress(s"Round $round, ${variant.label}: ${summaries.mkString("; ")}")
      }
    }
    printProgress("")
    results.toMap
  }

  /**
   * The variant pairs to compare, as (variant, reference): the cost of recordsProcessTime, of
   * the histogram, of both, of both vs the original code, and whether off matches the original.
   */
  private def comparisons(variants: Seq[ForkedVariant]): Seq[(String, String)] = {
    val labels = variants.map(_.label).toSet
    Seq("timing" -> "off", "full" -> "timing", "full" -> "off", "full" -> "baseline",
      "off" -> "baseline")
      .filter { case (a, b) => labels.contains(a) && labels.contains(b) }
  }

  /** Prints the paired ratios of the given metrics between the compared variants. */
  private def printPaired(
      scenario: Scenario,
      variants: Seq[ForkedVariant],
      numRounds: Int,
      results: ForkedResults,
      metrics: Seq[(String, String)]): Unit = {
    val random = new Random(42)
    printLine(f"${"Paired, median [95% CI]"}%-24s " +
      metrics.map { case (_, title) => f"$title%-28s" }.mkString(" "))
    printLine("-" * (25 + 29 * metrics.size))
    comparisons(variants).foreach { case (variant, reference) =>
      val cells = metrics.map { case (name, _) =>
        val ratios = (1 to numRounds).map { round =>
          results((scenario.name, variant, round))(name) /
            results((scenario.name, reference, round))(name)
        }
        f"${formatRatio(ratios, random)}%-28s"
      }
      printLine(f"${s"$variant vs $reference"}%-24s " + cells.mkString(" "))
    }
    printLine("")
  }

  private def runExecutorPart(
      executorScenarios: Seq[Scenario],
      variants: Seq[ForkedVariant],
      numRounds: Int,
      roundsTsv: PrintWriter): Unit = {
    val results = runForked(
        "executor", executorScenarios, variants, numRounds, master, roundsTsv) {
      (scenario, metrics) =>
        val nanosPerRecord = metrics("cpu_ms") * 1e6 / scenario.totalRecords
        f"${scenario.name} $nanosPerRecord%.1f ns/record " +
          f"(wall/CPU ${metrics("wall_ms") / metrics("cpu_ms")}%.2f)"
    }
    executorScenarios.foreach { scenario =>
      printLine(s"${scenario.name}: ${scenario.numMaps} maps x ${scenario.numReducers} " +
        s"reducers, ${scenario.recordsPerBlock} records/block, ${scenario.totalRecords} records, " +
        s"$numRounds JVMs per variant")
      printLine(f"${"Variant"}%-10s ${"CPU/rec(ns)"}%12s ${"Min"}%8s ${"Max"}%8s ${"Wall/CPU"}%9s")
      printLine("-" * 51)
      variants.foreach { variant =>
        val runs = (1 to numRounds).map(round => results((scenario.name, variant.label, round)))
        val nanosPerRecord = runs.map(_("cpu_ms") * 1e6 / scenario.totalRecords)
        printLine(f"${variant.label}%-10s ${median(nanosPerRecord)}%12.2f " +
          f"${nanosPerRecord.min}%8.2f ${nanosPerRecord.max}%8.2f " +
          f"${median(runs.map(r => r("wall_ms") / r("cpu_ms")))}%9.2f")
      }
      printLine("")
      printPaired(scenario, variants, numRounds, results,
        Seq("cpu_ms" -> "Exec CPU", "wall_ms" -> "Wall"))
    }
  }

  private def runDriverPart(
      driverScenarios: Seq[Scenario],
      variants: Seq[ForkedVariant],
      numRounds: Int,
      driverMaster: String,
      roundsTsv: PrintWriter): Unit = {
    val results = runForked(
        "driver", driverScenarios, variants, numRounds, driverMaster, roundsTsv) {
      (scenario, metrics) =>
        f"${scenario.name} job ${metrics("wall_ms")}%.0f ms, " +
          f"DAG scheduler CPU ${metrics("dag_cpu_ms")}%.0f ms, " +
          f"${metrics("histogram_entries_per_task")}%.0f buckets/task"
    }
    val driverColumns = Seq(
      "wall_ms" -> "Job(ms)",
      "dag_cpu_ms" -> "DAG CPU(ms)",
      "status_cpu_ms" -> "Status CPU(ms)",
      "result_cpu_ms" -> "Result CPU(ms)",
      "gc_ms" -> "GC(ms)",
      "peak_old_gen_mb" -> "Peak old(MB)",
      "histogram_entries_per_task" -> "Hist/task",
      "list_entries_per_task" -> "Lists/task")
    val statusColumns = Seq(
      "retained_kb_per_task" -> "Retained(KB/task)",
      "rest_us_per_task" -> "REST JSON(us/task)",
      "json_bytes_per_task" -> "JSON(B/task)",
      "samples_getter_ns" -> "Samples(ns/task)",
      "percentiles_getter_ns" -> "Pctiles(ns/task)")
    def printTable(scenario: Scenario, columns: Seq[(String, String)]): Unit = {
      printLine(f"${"Variant"}%-10s " + columns.map { case (_, t) => f"$t%18s" }.mkString(" "))
      printLine("-" * (11 + 19 * columns.size))
      variants.foreach { variant =>
        val runs = (1 to numRounds).map(round => results((scenario.name, variant.label, round)))
        printLine(f"${variant.label}%-10s " + columns.map { case (name, _) =>
          f"${median(runs.map(_(name)))}%18.1f"
        }.mkString(" "))
      }
    }
    driverScenarios.foreach { scenario =>
      printLine(s"${scenario.name}: ${scenario.numMaps} maps x ${scenario.numReducers} " +
        s"reducers, ${scenario.numMaps * scenario.recordsPerBlock} records/task, " +
        s"$numRounds JVMs per variant, $driverMaster, medians")
      printTable(scenario, driverColumns)
      printLine("  Hist/task: record process time histogram entries (buckets) per task; " +
        "Lists/task: all list and histogram accumulator entries per task")
      printLine("")
      printTable(scenario, statusColumns)
      printLine("  Retained: heap left behind per task, mostly the status store; REST JSON: " +
        "serializing the stage's tasks as the REST API does; Samples, Pctiles: the getters alone")
      printLine("")
      printPaired(scenario, variants, numRounds, results, Seq(
        "wall_ms" -> "Job",
        "dag_cpu_ms" -> "DAG scheduler CPU",
        "status_cpu_ms" -> "Status listener CPU",
        "rest_us_per_task" -> "REST JSON"))
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Statistics and output
  // ---------------------------------------------------------------------------------------------

  private def median(values: Seq[Double]): Double = {
    val sorted = values.sorted
    val n = sorted.size
    if (n % 2 == 1) sorted(n / 2) else (sorted(n / 2 - 1) + sorted(n / 2)) / 2
  }

  /** The median of the ratios with a percentile bootstrap 95% confidence interval. */
  private def formatRatio(ratios: Seq[Double], random: Random): String = {
    val resampled = Array.fill(BOOTSTRAP_RESAMPLES) {
      median(Seq.fill(ratios.size)(ratios(random.nextInt(ratios.size))))
    }.sorted
    val lower = resampled((BOOTSTRAP_RESAMPLES * 0.025).toInt)
    val upper = resampled((BOOTSTRAP_RESAMPLES * 0.975).toInt - 1)
    def pct(ratio: Double): String = f"${(ratio - 1) * 100}%+.2f%%"
    s"${pct(median(ratios))} [${pct(lower)}, ${pct(upper)}]"
  }

  /** The results file of BenchmarkBase, if it generates one, and summary.txt. */
  private def outputs: Option[OutputStream] = output.toSeq ++ summaryOut.toSeq match {
    case Seq() => None
    case Seq(single) => Some(single)
    case Seq(first, second) => Some(new TeeOutputStream(first, second))
  }

  /** Prints a line of the results: to the console, the results file and summary.txt. */
  private def printLine(line: String): Unit = {
    // scalastyle:off println
    println(line)
    // scalastyle:on println
    outputs.foreach(_.write((line + "\n").getBytes(StandardCharsets.UTF_8)))
  }

  /** Prints a line of progress: to the console and summary.txt. */
  private def printProgress(line: String): Unit = {
    // scalastyle:off println
    println(line)
    // scalastyle:on println
    summaryOut.foreach(_.write((line + "\n").getBytes(StandardCharsets.UTF_8)))
  }

  /** A section of the results, with a header in the results file and summary.txt. */
  private def section(name: String)(f: => Unit): Unit = {
    val separator = "=" * 96
    val header = s"$separator\n$name\n$separator\n\n"
    summaryOut.foreach(_.write(header.getBytes(StandardCharsets.UTF_8)))
    runBenchmark(name)(f)
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val allParts = Seq("sweep", "executor", "driver")
    var parts = Seq("executor", "driver")
    var numRounds = 5
    var driverRounds = 3
    var driverMaster = "local[4]"
    var selectedScenarios = defaultScenarios
    var baselineClasspath: Option[String] = Some(defaultBaselineClasspath)
    var childVariant: Option[Variant] = None
    var args = mainArgs.toList
    while (args.nonEmpty) {
      args match {
        case "--parts" :: names :: rest =>
          parts = names.split(",").map(_.trim).toSeq
          val unknown = parts.filterNot(allParts.contains)
          require(unknown.isEmpty, s"Unknown parts: ${unknown.mkString(", ")}; " +
            s"available: ${allParts.mkString(", ")}")
          args = rest
        case "--rounds" :: n :: rest => numRounds = n.toInt; args = rest
        case "--driver-rounds" :: n :: rest => driverRounds = n.toInt; args = rest
        case "--scenarios" :: names :: rest =>
          selectedScenarios = names.split(",").map(_.trim).toSeq.map { name =>
            scenariosByName.getOrElse(name, throw new IllegalArgumentException(
              s"Unknown scenario '$name', available: ${scenarios.map(_.name).mkString(", ")}"))
          }
          args = rest
        case "--baseline-classpath" :: file :: rest => baselineClasspath = Some(file); args = rest
        case "--no-baseline" :: rest => baselineClasspath = None; args = rest
        case "--warmup-seconds" :: n :: rest =>
          warmupNanos = TimeUnit.SECONDS.toNanos(n.toLong); args = rest
        case "--measure-seconds" :: n :: rest =>
          measureNanos = TimeUnit.SECONDS.toNanos(n.toLong); args = rest
        case "--master" :: m :: rest => master = m; args = rest
        case "--driver-master" :: m :: rest => driverMaster = m; args = rest
        case "--variant" :: label :: rest if childVariants.contains(label) =>
          childVariant = Some(childVariants(label)); args = rest
        case "--baseline" :: rest => baseline = true; args = rest
        case other => throw new IllegalArgumentException(s"Unknown arguments: $other")
      }
    }
    val (driverScenarios, executorScenarios) = selectedScenarios.partition(_.driver)

    childVariant match {
      case Some(variant) =>
        withSparkContext {
          executorScenarios.foreach(runScenario(_, Seq(variant)))
          driverScenarios.foreach(runDriverScenario(_, variant))
        }
      case None =>
        val runExecutor = parts.contains("executor") && executorScenarios.nonEmpty
        val runDriver = parts.contains("driver") && driverScenarios.nonEmpty
        if (runExecutor) {
          baselineClasspath.foreach { file =>
            require(new File(file).isFile, s"No baseline classpath at $file. Build the " +
              "baseline with dev/record-process-time-benchmark/build-baseline.sh, pass its " +
              "location with --baseline-classpath, or skip it with --no-baseline.")
          }
        }
        resultsDir = createResultsDir()
        val summary = new FileOutputStream(new File(resultsDir, "summary.txt"))
        summaryOut = Some(summary)
        val roundsTsv = new PrintWriter(new File(resultsDir, "rounds.tsv"), StandardCharsets.UTF_8)
        roundsTsv.write(Seq("round", "part", "variant", "scenario", "metric", "value")
          .mkString("", "\t", "\n"))
        try {
          printProgress(s"Results: $resultsDir")
          printProgress(s"Arguments: ${mainArgs.mkString(" ")}")
          if (runExecutor) {
            printProgress(s"Baseline classpath: ${baselineClasspath.getOrElse("none")}")
          }
          printProgress(Benchmark.getJVMOSInfo())
          printProgress(Benchmark.getProcessorName())
          printProgress("")
          if (parts.contains("sweep")) {
            section("System.nanoTime() cost") {
              nanoTimeBenchmark()
            }
            section("Shuffle read, sample interval (one JVM)") {
              withSparkContext {
                sweepScenarios.foreach(runScenario(_, sampleIntervalVariants))
              }
            }
          }
          if (runExecutor) {
            section("Executor: shuffle read CPU time (one JVM per variant)") {
              runExecutorPart(executorScenarios,
                forkedVariants(Seq(OFF, TIMING, FULL), baselineClasspath), numRounds, roundsTsv)
            }
          }
          if (runDriver) {
            // The driver only gets one more number per task from recordsProcessTime, and off
            // matched the baseline in the earlier driver runs, so this compares off and full.
            section("Driver: histogram buckets per task (one JVM per variant)") {
              runDriverPart(driverScenarios, forkedVariants(Seq(OFF, FULL), None), driverRounds,
                driverMaster, roundsTsv)
            }
          }
          printProgress(s"Results: $resultsDir")
        } finally {
          roundsTsv.close()
          summaryOut = None
          summary.close()
        }
    }
  }
}

/**
 * Writes (Int, Int) records as two raw ints: about as cheap to read as SQL's
 * UnsafeRowSerializer, so that the per record cost of the metrics is a larger share than with
 * Kryo. Only supports the streams that a shuffle uses.
 */
private class RawIntPairSerializer extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new SerializerInstance {
    override def serialize[T: ClassTag](t: T): ByteBuffer =
      throw new UnsupportedOperationException()
    override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
      throw new UnsupportedOperationException()
    override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
      throw new UnsupportedOperationException()

    override def serializeStream(s: OutputStream): SerializationStream = new SerializationStream {
      private[this] val out = new DataOutputStream(s)
      override def writeObject[T: ClassTag](t: T): SerializationStream = {
        out.writeInt(t.asInstanceOf[Int])
        this
      }
      override def flush(): Unit = out.flush()
      override def close(): Unit = out.close()
    }

    override def deserializeStream(s: InputStream): DeserializationStream =
      new DeserializationStream {
        private[this] val in = new DataInputStream(s)
        // An EOFException at the end of the stream ends asKeyValueIterator.
        override def readObject[T: ClassTag](): T = in.readInt().asInstanceOf[T]
        override def close(): Unit = in.close()
      }
  }
}
