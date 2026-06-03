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
            EmitWalShardSlotLifecycle(runtime, Line);
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
    /// Enumerates every live <c>Orleans.Lattice.BPlusTree.Grains.WalShardGrain</c>
    /// activation on the heap, follows its <c>_inFlight</c> linked list,
    /// and prints each in-flight flush slot's lifecycle stage and
    /// stuck-at-stage duration. A wedge cohort whose <c>WalAppendInFlight</c>
    /// histogram pins at <c>WalMaxPendingBatches</c> for 120+ seconds with
    /// no shipped deadline tripping can be attributed here to the exact
    /// stage of <c>FlushAsync</c> the head slot is parked at, with the
    /// <c>(tree, shard)</c> the grain owns. Mirrors the production code's
    /// <c>WalFlushStage</c> enum value-by-name; an unrecognised stage byte
    /// is printed as <c>?N</c> so a future enum addition surfaces cleanly
    /// in the log without a watchdog-side rebuild.
    /// </summary>
    private static void EmitWalShardSlotLifecycle(ClrRuntime runtime, Action<string> line)
    {
        line("==== WalShardGrain in-flight flush slot lifecycle ====");
        var heap = runtime.Heap;
        if (!heap.CanWalkHeap)
        {
            line("heap not walkable in snapshot; skipping WAL slot lifecycle scan.");
            return;
        }

        var nowTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        var tickFreq = System.Diagnostics.Stopwatch.Frequency;
        var grainCount = 0;
        var slotCount = 0;

        foreach (var obj in heap.EnumerateObjects())
        {
            if (obj.Type?.Name != "Orleans.Lattice.BPlusTree.Grains.WalShardGrain")
            {
                continue;
            }

            grainCount++;
            var treeIdField = obj.Type.Fields.FirstOrDefault(f => f.Name == "_treeId");
            var shardIndexField = obj.Type.Fields.FirstOrDefault(f => f.Name == "_shardIndex");
            var inFlightField = obj.Type.Fields.FirstOrDefault(f => f.Name == "_inFlight");
            if (treeIdField is null || shardIndexField is null || inFlightField is null)
            {
                line($"WalShardGrain instance at 0x{obj.Address:x} missing one of _treeId/_shardIndex/_inFlight - schema drift?");
                continue;
            }

            var treeIdObj = treeIdField.ReadObject(obj.Address, interior: false);
            var treeId = treeIdObj.IsNull ? "<null>" : treeIdObj.AsString() ?? "<empty>";
            var shardIndex = shardIndexField.Read<int>(obj.Address, interior: false);
            var inFlightObj = inFlightField.ReadObject(obj.Address, interior: false);
            if (inFlightObj.IsNull || inFlightObj.Type is null)
            {
                // Emit a per-grain line even when _inFlight itself is null so
                // the cohort can distinguish "grain holds no LinkedList" from
                // "LinkedList exists but is empty" from "LinkedList exists and
                // is populated but slot detection missed".
                line($"[wal-slot-grain] tree={treeId} shard={shardIndex} inFlight=null");
                continue;
            }

            // LinkedList<T>.head is the head node; each LinkedListNode<T>
            // has 'next' and 'item' fields. Walk the chain (bounded by the
            // observed Count so a torn read can't infinite-loop).
            var headField = inFlightObj.Type.Fields.FirstOrDefault(f => f.Name == "head");
            var countField = inFlightObj.Type.Fields.FirstOrDefault(f => f.Name == "count");
            var observedCount = countField is not null ? countField.Read<int>(inFlightObj.Address, interior: false) : -1;
            var headObj = headField is not null ? headField.ReadObject(inFlightObj.Address, interior: false) : default;
            var headIsNull = headField is null || headObj.IsNull;

            // Unconditional per-grain summary: emits BEFORE any early-return
            // on head==null so the 2026-06-03 mystery ("0 slots across 9
            // activations" with no per-grain rows) cannot recur. Distinguishes
            // (a) head==null + count==0 (legitimately empty: no in-flight
            // flush at snapshot time - callers parked upstream of FlushAsync)
            // from (b) head==null + count>0 (torn read / structural skew) from
            // (c) head!=null + slot-detection-misses-by-field-shape (in which
            // case the [wal-slot-debug] line below names the actual type).
            line($"[wal-slot-grain] tree={treeId} shard={shardIndex} inFlight.count={observedCount} head.IsNull={headIsNull}");

            if (headIsNull)
            {
                continue;
            }
            var safetyCap = Math.Max(observedCount, 1) + 8; // small headroom against torn read

            var node = headObj;
            for (var i = 0; i < safetyCap; i++)
            {
                if (node.IsNull || node.Type is null) { break; }
                var itemField = node.Type.Fields.FirstOrDefault(f => f.Name == "item");
                if (itemField is null) { break; }
                var slotObj = itemField.ReadObject(node.Address, interior: false);
                // Detect InFlightFlush by the field signature (Stage:byte +
                // StageStartedTicks:long) rather than by the nested-type name
                // literal - ClrMD's nested-type Name format varies across
                // versions and .NET runtime updates, and the previous literal
                // match silently dropped every slot in the 2026-06-03 cohort.
                if (!slotObj.IsNull && slotObj.Type is not null)
                {
                    var stageField = slotObj.Type.Fields.FirstOrDefault(f => f.Name == "Stage");
                    var stageStartedField = slotObj.Type.Fields.FirstOrDefault(f => f.Name == "StageStartedTicks");
                    if (stageField is not null && stageStartedField is not null)
                    {
                        var startOffsetField = slotObj.Type.Fields.FirstOrDefault(f => f.Name == "<StartOffset>k__BackingField")
                                              ?? slotObj.Type.Fields.FirstOrDefault(f => f.Name == "StartOffset");
                        var endOffsetField = slotObj.Type.Fields.FirstOrDefault(f => f.Name == "<EndOffsetExclusive>k__BackingField")
                                            ?? slotObj.Type.Fields.FirstOrDefault(f => f.Name == "EndOffsetExclusive");
                        var stageByte = stageField.Read<byte>(slotObj.Address, interior: false);
                        var stageStarted = stageStartedField.Read<long>(slotObj.Address, interior: false);
                        var startOffset = startOffsetField is not null ? startOffsetField.Read<long>(slotObj.Address, interior: false) : -1L;
                        var endOffset = endOffsetField is not null ? endOffsetField.Read<long>(slotObj.Address, interior: false) : -1L;
                        var stageName = stageByte switch
                        {
                            0 => "Created",
                            1 => "Yielded",
                            2 => "ProviderCallIssued",
                            3 => "ProviderCallReturned",
                            4 => "AcksApplied",
                            5 => "FailureHandled",
                            _ => $"?{stageByte}",
                        };
                        var stuckMs = stageStarted > 0 ? (long)((nowTicks - stageStarted) * 1000.0 / tickFreq) : -1L;
                        line($"[wal-slot] tree={treeId} shard={shardIndex} slot=[{startOffset},{endOffset}) stage={stageName} stuck={stuckMs}ms");
                        slotCount++;
                    }
                    else if (i == 0)
                    {
                        // First-node sanity diagnostic: report what type the
                        // item field carried so the next cohort tells us if
                        // the slot is shaped differently than expected (e.g.
                        // boxed-generic name surprise).
                        line($"[wal-slot-debug] tree={treeId} shard={shardIndex} first item type='{slotObj.Type.Name}' hasStageField={stageField is not null} hasStageStartedField={stageStartedField is not null}");
                    }
                }
                var nextField = node.Type.Fields.FirstOrDefault(f => f.Name == "next");
                if (nextField is null) { break; }
                var nextObj = nextField.ReadObject(node.Address, interior: false);
                if (nextObj.IsNull || nextObj.Address == headObj.Address)
                {
                    // LinkedList<T> is a circular ring; head's prev points
                    // back to the last node and next traversal eventually
                    // returns to head. Break on that loop closure.
                    break;
                }
                node = nextObj;
            }
        }

        line($"---- {slotCount} in-flight slot(s) across {grainCount} WalShardGrain activation(s) ----");
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
