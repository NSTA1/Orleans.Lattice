using System.Text;
using Microsoft.Diagnostics.Runtime;

namespace VehicleFleetSimulator.AzureThroughput.Silo;

/// <summary>
/// In-process stall watchdog for the Azure-throughput bench silo.
/// <para>
/// The WAL write pipeline wedges intermittently at the saturation rung
/// (25,000 vehicles): throughput drops to zero, the bench's in-flight
/// gauge pins at <c>WalMaxPendingBatches</c>, and the run never emits a
/// FINAL line. Three successive "bound the Nth candidate await" fixes
/// (WAL flush deadline, activation-readiness seed deadline, digest-publish
/// deadline) each added a <see cref="TimeoutException"/> to a plausible
/// parked-RPC site, and each fired <b>zero</b> times on the wedge - which
/// means the actually-parked await is none of them. Rather than guess a
/// fourth site, this watchdog captures the real parked state the moment
/// the wedge trips.
/// </para>
/// <para>
/// ACI gives the run exactly one exfiltration channel that survives the
/// container-group teardown: the silo's stdout, which the deploy script
/// streams to <c>.run/silo-*.log</c>. A core dump would need a file mount
/// the bench does not have, so the watchdog instead performs the analysis
/// <i>in process</i>: it snapshots its own runtime with ClrMD and walks
/// the managed heap for suspended async state machines (the in-process
/// equivalent of <c>dumpasync</c>) plus every managed thread stack (the
/// equivalent of <c>dotnet-stack</c>), then prints the result to stdout.
/// The parked continuation chain therefore lands in the same log file the
/// operator already retrieves, with no extra retrieval step.
/// </para>
/// <para>
/// The watchdog is observation-only - it never cancels, faults, or
/// recycles anything - so it cannot perturb the very stall it is meant to
/// characterise. It fires at most once per process to avoid flooding the
/// log if the wedge persists for the whole run.
/// </para>
/// </summary>
internal sealed class StallWatchdog
{
    private readonly Func<long> _writtenTotalSnapshot;
    private readonly Func<long> _inFlightSnapshot;
    private readonly int _pinnedInFlightThreshold;
    private readonly TimeSpan _stallWindow;
    private readonly TimeSpan _pollInterval;
    private int _fired;

    /// <summary>
    /// Creates a stall watchdog.
    /// </summary>
    /// <param name="writtenTotalSnapshot">
    /// Reads the monotonically-increasing total entries written. The
    /// watchdog declares a stall when this value stops advancing while
    /// in-flight work remains pinned.
    /// </param>
    /// <param name="inFlightSnapshot">
    /// Reads the current in-flight dispatch count. A wedge pins this at
    /// the configured ceiling; a clean idle leaves it at zero.
    /// </param>
    /// <param name="pinnedInFlightThreshold">
    /// The in-flight count at or above which a non-advancing
    /// written-total is treated as a wedge rather than a drained idle.
    /// Pass <c>WalMaxPendingBatches</c> (the value the wedge pins at).
    /// </param>
    /// <param name="stallWindow">
    /// How long the written-total must stay frozen (with in-flight pinned)
    /// before the watchdog fires. Long enough to exclude a transient
    /// back-pressure dip, short enough to fire well inside the run.
    /// </param>
    /// <param name="pollInterval">How often to sample.</param>
    public StallWatchdog(
        Func<long> writtenTotalSnapshot,
        Func<long> inFlightSnapshot,
        int pinnedInFlightThreshold,
        TimeSpan stallWindow,
        TimeSpan pollInterval)
    {
        ArgumentNullException.ThrowIfNull(writtenTotalSnapshot);
        ArgumentNullException.ThrowIfNull(inFlightSnapshot);
        _writtenTotalSnapshot = writtenTotalSnapshot;
        _inFlightSnapshot = inFlightSnapshot;
        _pinnedInFlightThreshold = pinnedInFlightThreshold;
        _stallWindow = stallWindow;
        _pollInterval = pollInterval;
    }

    /// <summary>
    /// Runs the sampling loop until <paramref name="ct"/> is cancelled.
    /// On the first detected stall it emits the parked-state report to
    /// stdout and keeps running (so a second, later stall after a partial
    /// recovery is still caught, though the report itself fires once).
    /// </summary>
    public async Task RunAsync(CancellationToken ct)
    {
        long lastWritten = _writtenTotalSnapshot();
        var lastProgressAt = DateTime.UtcNow;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(_pollInterval, ct).ConfigureAwait(false);

                var written = _writtenTotalSnapshot();
                var inFlight = _inFlightSnapshot();

                if (written != lastWritten)
                {
                    // Pipeline is making progress; reset the stall clock.
                    lastWritten = written;
                    lastProgressAt = DateTime.UtcNow;
                    continue;
                }

                // Written total is frozen. Only a wedge (not a drained
                // idle) keeps in-flight work pinned while no progress is
                // made, so gate on the in-flight ceiling to avoid firing
                // on the clean post-drain tail at end of run.
                if (inFlight < _pinnedInFlightThreshold)
                {
                    lastProgressAt = DateTime.UtcNow;
                    continue;
                }

                if (DateTime.UtcNow - lastProgressAt < _stallWindow)
                {
                    continue;
                }

                if (Interlocked.Exchange(ref _fired, 1) == 0)
                {
                    EmitParkedStateReport(written, inFlight);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
    }

    /// <summary>
    /// Snapshots the current process with ClrMD and prints, to stdout,
    /// (1) every suspended async state machine on the managed heap grouped
    /// by type with its <c>&lt;&gt;1__state</c> resume point, and (2) every
    /// managed thread's call stack. All output is single-line-prefixed
    /// with <c>[stall-watchdog]</c> so it is trivially grep-able out of the
    /// interleaved silo log.
    /// </summary>
    private static void EmitParkedStateReport(long written, long inFlight)
    {
        var sb = new StringBuilder(64 * 1024);
        void Line(string text) => sb.Append("[stall-watchdog] ").Append(text).Append('\n');

        Line($"WEDGE DETECTED writtenTotal={written:N0} inFlight={inFlight} pid={Environment.ProcessId} - capturing in-process async/thread state");

        try
        {
            // Snapshot-and-attach forks a suspended copy of this very
            // process (via the runtime's createdump) and reads it, so the
            // heap and stacks are internally consistent and the live
            // process is paused only for the fork. Reading our own live
            // runtime without a snapshot would race the GC and the
            // scheduler and produce torn frames.
            using var target = DataTarget.CreateSnapshotAndAttach(Environment.ProcessId);
            using var runtime = target.ClrVersions[0].CreateRuntime();

            EmitSuspendedAsyncStateMachines(runtime, Line);
            EmitThreadStacks(runtime, Line);
        }
        catch (Exception ex)
        {
            // The watchdog must never bring down the run it is observing.
            // A snapshot failure (insufficient privilege, createdump
            // missing in the base image, etc.) is reported and swallowed.
            Line($"FAILED to capture in-process snapshot: {ex.GetType().Name}: {ex.Message}");
            Line("hint: the aspnet base image must expose createdump; if absent, add the diagnostics tooling to the runtime image.");
        }

        // One Console.Write of the whole buffer keeps the report
        // contiguous in the interleaved stdout stream rather than letting
        // per-second reporter lines split it.
        Console.Write(sb.ToString());
        Console.Out.Flush();
    }

    /// <summary>
    /// Walks the managed heap for boxed async state machines that are
    /// suspended at an await (<c>&lt;&gt;1__state</c> &gt;= 0) and prints
    /// each one's declaring type and resume-point state, grouped and
    /// counted. A wedged pipeline shows the parked continuation chain here
    /// as a cluster of identical state-machine types all pinned at the
    /// same await - the await that is actually stuck.
    /// </summary>
    private static void EmitSuspendedAsyncStateMachines(ClrRuntime runtime, Action<string> line)
    {
        line("==== suspended async state machines (in-process dumpasync) ====");
        var heap = runtime.Heap;
        if (!heap.CanWalkHeap)
        {
            line("heap not walkable in snapshot; skipping async state-machine scan.");
            return;
        }

        // Group suspended state machines by "TypeName @ state" so a stuck
        // fan-out shows up as a single high-count row rather than thousands
        // of individual lines.
        var counts = new Dictionary<string, int>(StringComparer.Ordinal);
        var scanned = 0L;
        foreach (var obj in heap.EnumerateObjects())
        {
            if (obj.Type is null)
            {
                continue;
            }

            // An async method's state machine carries an int field named
            // "<>1__state": -1 = not started / completed, -2 = running,
            // >= 0 = suspended at the await whose ordinal is the value.
            // We only care about the suspended ones (state >= 0).
            //
            // Two shapes appear on the heap and BOTH must be inspected:
            //
            //   1. Class state machine (Debug builds, or methods the
            //      compiler chose to emit as a reference type): the
            //      "<>1__state" field lives directly on the heap object's
            //      own type. Read it at the object address.
            //
            //   2. Struct state machine (the Release-build default): the
            //      struct is boxed inside an
            //      AsyncTaskMethodBuilder[...]+AsyncStateMachineBox<T>
            //      whose own type does NOT carry "<>1__state". The state
            //      lives on the nested "StateMachine" struct field, read
            //      at an interior offset. The earlier version of this
            //      scan only handled shape (1) and therefore reported
            //      "0 suspended" even when thousands of struct state
            //      machines were parked - a false negative that the box
            //      MoveNext frames in the thread dump directly contradict.
            var directState = obj.Type.Fields.FirstOrDefault(f => f.Name == "<>1__state");
            if (directState is not null)
            {
                scanned++;
                var state = directState.Read<int>(obj.Address, interior: false);
                if (state < 0)
                {
                    continue;
                }

                var key = $"{obj.Type.Name} @ await#{state}";
                counts[key] = counts.TryGetValue(key, out var c) ? c + 1 : 1;
                continue;
            }

            // Shape (2): an AsyncStateMachineBox<T> exposes the boxed
            // struct through its "StateMachine" field. Reach into that
            // struct and read its "<>1__state".
            var boxedSm = obj.Type.Fields.FirstOrDefault(f => f.Name == "StateMachine");
            if (boxedSm is null || !boxedSm.IsValueType)
            {
                continue;
            }

            var sm = boxedSm.ReadStruct(obj.Address, interior: false);
            var stateField = sm.Type?.Fields.FirstOrDefault(f => f.Name == "<>1__state");
            if (stateField is null)
            {
                continue;
            }

            scanned++;
            var boxedState = sm.ReadField<int>("<>1__state");
            if (boxedState < 0)
            {
                continue;
            }

            // Prefer the real state-machine type name (the user's async
            // method) over the AsyncStateMachineBox<T> wrapper name.
            var smTypeName = sm.Type?.Name ?? obj.Type.Name;
            var boxedKey = $"{smTypeName} @ await#{boxedState}";
            counts[boxedKey] = counts.TryGetValue(boxedKey, out var bc) ? bc + 1 : 1;
        }

        if (counts.Count == 0)
        {
            line($"no suspended async state machines found (scanned {scanned:N0} state-machine objects).");
            return;
        }

        foreach (var kvp in counts.OrderByDescending(k => k.Value))
        {
            line($"count={kvp.Value,6:N0}  {kvp.Key}");
        }
        line($"---- {counts.Count} distinct suspend points across {scanned:N0} state-machine objects ----");
    }

    /// <summary>
    /// Prints every managed thread's call stack (the in-process
    /// equivalent of <c>dotnet-stack report</c>). The thread that holds a
    /// non-reentrant gate or is blocked on a never-completing task shows
    /// here, complementing the async-state-machine view for stalls parked
    /// on a synchronous wait rather than an await.
    /// </summary>
    private static void EmitThreadStacks(ClrRuntime runtime, Action<string> line)
    {
        line("==== managed thread stacks (in-process dotnet-stack) ====");
        foreach (var thread in runtime.Threads)
        {
            if (!thread.IsAlive)
            {
                continue;
            }

            var frames = thread.EnumerateStackTrace()
                .Where(f => f.Kind == ClrStackFrameKind.ManagedMethod && f.Method is not null)
                .Take(40)
                .ToList();

            if (frames.Count == 0)
            {
                continue;
            }

            line($"-- thread osid={thread.OSThreadId} managedId={thread.ManagedThreadId} --");
            foreach (var frame in frames)
            {
                var method = frame.Method!;
                line($"   at {method.Type?.Name}.{method.Name}");
            }
        }
    }
}
