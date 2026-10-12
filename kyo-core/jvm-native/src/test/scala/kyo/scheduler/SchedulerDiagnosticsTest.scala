package kyo.scheduler

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kyo.discard
import kyo.internal.Diagnostics

class SchedulerDiagnosticsTest extends kyo.test.Test[Any]:

    // SchedulerDiagnostics registers a single process-global entry named "kyo-scheduler" that is never closed (the scheduler is
    // process-lifetime). init() is idempotent, so calling it here is a no-op if an earlier fiber already installed it.

    "registers a kyo-scheduler dumper that renders live worker state" in {
        SchedulerDiagnostics.init()
        val dump = Diagnostics.dumpAll()
        assert(dump.contains("=== kyo-scheduler ==="), s"dump missing the kyo-scheduler section: $dump")
        assert(dump.contains("scheduler: currentWorkers="), s"dump missing the scheduler summary line: $dump")
        assert(dump.contains("busyFiberTraces:"), s"dump missing the busy-fiber-traces marker: $dump")
    }

    "exposes a not-closed probe whose cycles sum the workers' executions" in {
        SchedulerDiagnostics.init()
        val found = Diagnostics.probeAll().toMap.get("kyo-scheduler")
        assert(found.isDefined, "no kyo-scheduler probe registered")
        val p = found.get
        // closed must be false so the runner's stranded-op classifier considers this component at all (a closed probe is never flagged).
        assert(!p.closed, "the scheduler probe must report not-closed")
        // cycles is a monotonic execution sum; pending is timing-dependent (any queued task), so it is not asserted here.
        assert(p.cycles >= 0L, s"cycles must be a non-negative execution sum, got ${p.cycles}")
    }

    "a task parked inside its own run, with nothing queued, is not pending" in {
        withScheduler(Executors.newCachedThreadPool(daemon)) { scheduler =>
            val started = new CountDownLatch(1)
            val release = new CountDownLatch(1)
            scheduler.schedule(Task {
                started.countDown()
                discard(release.await(30, TimeUnit.SECONDS))
            })
            try
                assert(started.await(10, TimeUnit.SECONDS), "the parking task never started")
                assert(scheduler.busyWorkers() == 1, "the parked task must hold a worker of the scheduler under probe")
                val before = SchedulerDiagnostics.probe(scheduler)
                val after  = SchedulerDiagnostics.probe(scheduler)
                assert(before.cycles == after.cycles, s"no task may run while the only one is parked: $before then $after")
                assert(!before.pending && !after.pending, s"a parked executing task is not queued work: $before then $after")
            finally release.countDown()
            end try
        }
    }

    "a task queued on a worker whose dispatch never runs is pending with frozen cycles" in {
        val dropped = new AtomicInteger
        withScheduler(_ => discard(dropped.incrementAndGet())) { scheduler =>
            scheduler.schedule(Task(()))
            assert(dropped.get() == 1, s"the enqueue must dispatch exactly once, got ${dropped.get()}")
            val before = SchedulerDiagnostics.probe(scheduler)
            val after  = SchedulerDiagnostics.probe(scheduler)
            assert(before.pending && after.pending, s"a queued task nothing will run is pending: $before then $after")
            assert(before.cycles == 0L && after.cycles == 0L, s"no task ran: $before then $after")
        }
    }

    private val daemon: ThreadFactory = r =>
        val t = new Thread(r)
        t.setDaemon(true)
        t

    /** A dedicated scheduler, so the probe reads only the work the leaf placed. The regulators' periodic probes would each enqueue a task
      * onto it and make a sample racy, so their fixed-delay timers are pushed past the leaf's lifetime; the one-shot submits (the worker
      * cycle and the blocking monitor) still run.
      */
    private def withScheduler(workerExecutor: Executor)(f: Scheduler => Unit): Unit =
        val timer = new ScheduledThreadPoolExecutor(2, daemon):
            override def scheduleWithFixedDelay(
                command: Runnable,
                initialDelay: Long,
                delay: Long,
                unit: TimeUnit
            ): ScheduledFuture[?] =
                super.scheduleWithFixedDelay(command, 1, 1, TimeUnit.DAYS)
        val scheduler = new Scheduler(workerExecutor, Executors.newSingleThreadExecutor(daemon), timer)
        try f(scheduler)
        finally
            scheduler.shutdown()
            timer.shutdownNow()
            workerExecutor match
                case e: ExecutorService => discard(e.shutdownNow())
                case _                  => ()
        end try
    end withScheduler
end SchedulerDiagnosticsTest
