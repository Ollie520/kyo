package kyo.scheduler

import kyo.discard
import kyo.internal.Diagnostics

/** Process-global [[Diagnostics]] dumper for the shared kyo scheduler.
  *
  * A rare hang in the fiber resume path (a lost wakeup or a dispatch strand, most consequential on Scala Native where thread dumps are
  * unavailable) leaves the kyo-test runner's `Diagnostics.dumpAll()`, which it fires on the first STUCK minute of a leaf, with nothing to
  * report about the scheduler: no component describes it, so the dump reads `(no dumpers registered)` and the hang stays blind. This supplies
  * that description. The dump renders the scheduler summary and, per worker, `running`/`mount`/`load`/`executions`/`blocked`/`stalled`/`frame`
  * plus the busy workers' fiber traces, so the next occurrence self-localizes which worker (if any) holds work and whether it is advancing.
  *
  * The paired [[Diagnostics.Probe]] reports the summed per-worker executions as `cycles` and any queued task as `pending`, so the runner's
  * stranded-op classifier reads a frozen `cycles` under `pending` (across its two-probe settle window, with the scheduler not closed) as a
  * lost wakeup, distinct from continuous progress (advancing `cycles`). A task a worker is executing is not `pending`: kyo-net's poll loops
  * park their carrier inside the task for as long as the process is idle, executions stay frozen while they do, and counting them would
  * report every quiet settle window as a lost wakeup with nothing queued anywhere.
  *
  * Registration lives here in kyo-core because [[Diagnostics]] is visible only within kyo-core and the scheduler is a dependency of it (the
  * scheduler module cannot see the registry). It runs once, off [[IOTask]]'s first touch of `Scheduler.get`. The dump and probe read the
  * scheduler best-effort on the consumer's thread at snapshot time (a stale or partial read is acceptable, a throw is contained by the
  * registry), never on a hot path, and change no scheduler behavior.
  */
private[kyo] object SchedulerDiagnostics:

    private var registered = false

    /** Register the scheduler dumper once. Idempotent, so the first fiber created (which initializes [[IOTask]]) installs it and later
      * calls are no-ops. A no-op return value; its only purpose is the side effect at first call.
      */
    def init(): Unit =
        synchronized {
            if !registered then
                registered = true
                discard(Diagnostics.register("kyo-scheduler")(() => render(), () => probe(Scheduler.get)))
        }

    private def render(): String =
        val s       = Scheduler.get.status()
        val workers =
            s.workers.iterator.zipWithIndex.map {
                case (w, i) =>
                    if w eq null then s"  worker[$i]: (unallocated)"
                    else
                        s"  worker[${w.id}] running=${w.running} mount='${w.mount}' load=${w.load} " +
                            s"executions=${w.executions} blocked=${w.isBlocked} stalled=${w.isStalled} frame=${w.frame}"
            }.mkString("\n")
        val traces   = Scheduler.get.busyFiberTraces()
        val traceStr = if traces.isEmpty then "  (no busy workers)" else traces.mkString("\n")
        s"scheduler: currentWorkers=${s.currentWorkers} allocatedWorkers=${s.allocatedWorkers} loadAvg=${s.loadAvg} " +
            s"activeThreads=${s.activeThreads} totalThreads=${s.totalThreads} flushes=${s.flushes} queued=${Scheduler.get.queuedTasks()}\n" +
            workers + "\nbusyFiberTraces:\n" + traceStr
    end render

    private[scheduler] def probe(scheduler: Scheduler): Diagnostics.Probe =
        var cycles = 0L
        scheduler.status().workers.foreach { w =>
            if w ne null then cycles += w.executions
        }
        Diagnostics.Probe(closed = false, cycles = cycles, pending = scheduler.queuedTasks() > 0)
    end probe

end SchedulerDiagnostics
