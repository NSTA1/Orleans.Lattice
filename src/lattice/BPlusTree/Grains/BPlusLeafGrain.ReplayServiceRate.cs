namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// The activation's physical release seam. The reactive catch has already
    /// accounted for a withheld permit; all other decisions happen here, before
    /// returning the acquired permit. Adaptation diagnostics cannot interrupt
    /// disposition; hold diagnostics run after the physical release.
    /// </summary>
    internal static void FinishReplayPermit(
        SemaphoreSlim gate, bool memoryWithheld, bool completedCleanly,
        long acquiredAt, string metricTreeId, string? treeId, long serviceHold = -1)
    {
        CompleteReplayServiceRateHold(serviceHold);
        var heap = ReadReplayHeapPressure();
        if (!memoryWithheld && ReplayHeapPressure.IsPressured(heap))
            memoryWithheld = TryWithholdReplayPermitOnPressure(LatticeMetrics.PermitAdaptationTriggerOccupancy);

        var healthy = completedCleanly && ReplayHeapPressure.IsRelieved(heap);
        var serviceWithheld = !memoryWithheld && AdaptReplayServiceRate(
            gate, healthy, System.Diagnostics.Stopwatch.GetTimestamp());
        var storageWithheld = !memoryWithheld && !serviceWithheld && AdaptReplayStoragePressure(
            gate, System.Diagnostics.Stopwatch.GetTimestamp());
        if (!memoryWithheld && !serviceWithheld && !storageWithheld)
        {
            gate.Release();
            if (healthy && TryRestoreWithheldReplayPermit())
                gate.Release();
        }

        RecordReplayPermitHold(acquiredAt, metricTreeId, treeId);
    }

    /// <summary>
    /// Grades a saturated window before returning this activation's permit.
    /// The shared lock makes memory/service ledger changes and their floor
    /// one atomic decision. True means this acquired permit must be withheld.
    /// </summary>
    internal static bool AdaptReplayServiceRate(SemaphoreSlim gate, bool healthy, long now)
    {
        if (Volatile.Read(ref _replayServiceRateController) is null)
            return false;

        int disposition;
        lock (_replayConcurrencyGateLock)
            disposition = AdaptReplayServiceRateCore(gate, healthy, now);
        if (disposition != 0)
            RecordReplayPermitAdaptation(
                disposition < 0 ? LatticeMetrics.PermitAdaptationWithheld : LatticeMetrics.PermitAdaptationRestored,
                ServiceTimeTrigger);
        return disposition < 0;
    }

    private static int AdaptReplayServiceRateCore(SemaphoreSlim gate, bool healthy, long now)
    {
        var controller = _replayServiceRateController;
        if (controller is null)
            return 0;

        if (!healthy)
        {
            controller.Reset();
            return 0;
        }

        var memory = _withheldReplayPermits - _serviceWithheldReplayPermits - _storageWithheldReplayPermits;
        var change = controller.Observe(
            now, Volatile.Read(ref _replayPermitsServed),
            healthy && gate.CurrentCount == 0 && QueuedReplayPermitWaiters > 0,
            memory, CirculatingReplayPermits(_replayConcurrencyCeiling, _withheldReplayPermits),
            _serviceWithheldReplayPermits);

        if (change < 0 && _withheldReplayPermits < _replayConcurrencyCeiling - 1)
        {
            _serviceWithheldReplayPermits++;
            Volatile.Write(ref _withheldReplayPermits, _withheldReplayPermits + 1);
            return -1;
        }

        if (change > 0 && _serviceWithheldReplayPermits > 0 && healthy && memory == 0)
        {
            _serviceWithheldReplayPermits--;
            Volatile.Write(ref _withheldReplayPermits, _withheldReplayPermits - 1);
            gate.Release();
            return 1;
        }
        else if (change != 0)
        {
            controller.Reset();
        }

        return 0;
    }

    internal static int ServiceWithheldReplayPermitsForTest => Volatile.Read(ref _serviceWithheldReplayPermits);
    internal static int StorageWithheldReplayPermitsForTest => Volatile.Read(ref _storageWithheldReplayPermits);
    internal static bool ReplayServiceRateControllerEnabledForTest => Volatile.Read(ref _replayServiceRateController) is not null;
    internal static bool ReplayStoragePressureGovernorEnabledForTest => Volatile.Read(ref _replayStoragePressureGovernor) is not null;

    /// <summary>
    /// Lets the storage-pressure governor (issue #5877) close a window and moves
    /// its ledger one permit toward the governor's target. True means this
    /// acquired permit must be withheld; a step up releases one extra permit
    /// here. One permit per release keeps every ledger change paired with a
    /// physical permit, so the gate's count never drifts from the ledgers.
    /// </summary>
    internal static bool AdaptReplayStoragePressure(SemaphoreSlim gate, long now)
    {
        if (Volatile.Read(ref _replayStoragePressureGovernor) is null)
            return false;

        ReplayStoragePressureStep? step;
        int disposition;
        lock (_replayConcurrencyGateLock)
        {
            var governor = _replayStoragePressureGovernor;
            if (governor is null)
                return false;

            step = governor.IsDue(now)
                ? ObserveReplayStoragePressure(governor, now)
                : null;
            disposition = ReconcileReplayStorageLedger(gate, governor.Target);

            // A restore returns every permit down to the new target at once, as
            // the idle step does: a silo with headroom but few releases would
            // otherwise take one release per permit to recover (issue #5971).
            if (step is { Direction: > 0 })
            {
                while (_storageWithheldReplayPermits > governor.Target)
                    ReconcileReplayStorageLedger(gate, governor.Target);
            }
        }

        if (step is { } taken)
            RecordReplayStorageStep(taken);
        return disposition < 0;
    }

    /// <summary>
    /// Gives the storage-pressure governor (issue #5877) its idle verdict before
    /// an activation queues for a permit. A quiet silo releases no permits, so
    /// the release seam alone could never observe that a storm has passed; this
    /// is where the next storm arrives. On an idle step every storage-withheld
    /// permit is returned to the gate at once, so the storm starts at the full
    /// ceiling. A window that closes here as pressured only sets the target;
    /// permits are withheld at the release seam, one per release, as before.
    /// </summary>
    internal static void RestoreReplayStorageWhenIdle(SemaphoreSlim gate, long now)
    {
        if (Volatile.Read(ref _storageWithheldReplayPermits) == 0
            || Volatile.Read(ref _replayStoragePressureGovernor) is null)
            return;

        ReplayStoragePressureStep? step = null;
        lock (_replayConcurrencyGateLock)
        {
            var governor = _replayStoragePressureGovernor;
            if (governor is null || _storageWithheldReplayPermits == 0 || !governor.IsDue(now))
                return;

            var observed = ObserveReplayStoragePressure(governor, now);
            if (observed is { Direction: > 0 })
            {
                step = observed;
                while (_storageWithheldReplayPermits > governor.Target)
                    ReconcileReplayStorageLedger(gate, governor.Target);
            }
            else if (observed is { } other)
            {
                step = other;
            }
        }

        if (step is { } taken)
            RecordReplayStorageStep(taken);
    }

    /// <summary>
    /// The CPU the process can obtain, in cores: the lesser of the processor
    /// count and the enforced container grant, read once.
    /// </summary>
    /// <summary>Test-only: the CPU denominator the governor's utilisation uses.</summary>
    internal static int ReplayCpuCoresForTest => ReplayCpuCores.Value;

    private static readonly Lazy<int> ReplayCpuCores = new(
        () => ResolveDefaultReplayCeiling(Environment.ProcessorCount, Orleans.Lattice.Internal.Cgroups.ContainerCpuGrant.Read()));

    /// <summary>
    /// Closes the governor's window with the storage tallies, the replay permit
    /// queue depth and the process CPU time, so the latency arm can tell a CPU
    /// knee from queueing it caused itself (issue #5971).
    /// </summary>
    private static ReplayStoragePressureStep? ObserveReplayStoragePressure(ReplayStoragePressureGovernor governor, long now) =>
        governor.Observe(
            now,
            ReplayStoragePressure.Read(),
            _replayConcurrencyCeiling,
            Volatile.Read(ref _queuedReplayPermitWaiters),
            _replayProcessCpu(),
            ReplayCpuCores.Value);

    private static readonly Func<TimeSpan?> RealProcessCpu = static () => Environment.CpuUsage.TotalTime;

    /// <summary>
    /// The process CPU time source; a test that drives the governor on a
    /// synthetic clock replaces it, since real CPU time over synthetic windows
    /// is meaningless.
    /// </summary>
    private static Func<TimeSpan?> _replayProcessCpu = RealProcessCpu;

    private static int ReconcileReplayStorageLedger(SemaphoreSlim gate, int target)
    {
        if (_storageWithheldReplayPermits < target && _withheldReplayPermits < _replayConcurrencyCeiling - 1)
        {
            _storageWithheldReplayPermits++;
            Volatile.Write(ref _withheldReplayPermits, _withheldReplayPermits + 1);
            return -1;
        }

        if (_storageWithheldReplayPermits > target)
        {
            _storageWithheldReplayPermits--;
            Volatile.Write(ref _withheldReplayPermits, _withheldReplayPermits - 1);
            gate.Release();
            return 1;
        }

        return 0;
    }

    private static void RecordReplayStorageStep(ReplayStoragePressureStep step) =>
        RecordReplayPermitAdaptation(
            step.Direction < 0 ? LatticeMetrics.PermitAdaptationWithheld : LatticeMetrics.PermitAdaptationRestored,
            step.Reason switch
            {
                ReplayStoragePressureReason.Throttle => StorageThrottleTrigger,
                ReplayStoragePressureReason.Latency => ReplayLatencyTrigger,
                ReplayStoragePressureReason.Idle => StorageIdleTrigger,
                _ => StorageRecoveryTrigger,
            });

    /// <summary>
    /// Test-only: replaces the governor with one on <paramref name="windowTicks"/>
    /// windows. Synthetic timestamps make real CPU time meaningless, so the CPU
    /// source reads as unknown unless <paramref name="processCpu"/> supplies one.
    /// </summary>
    internal static void SeedReplayStoragePressureForTest(long windowTicks, Func<TimeSpan?>? processCpu = null)
    {
        lock (_replayConcurrencyGateLock)
        {
            _replayStoragePressureGovernor = new ReplayStoragePressureGovernor(windowTicks);
            _replayProcessCpu = processCpu ?? (static () => null);
        }
    }

    private static void RecordReplayPermitAdaptation(
        KeyValuePair<string, object?> outcome, KeyValuePair<string, object?>? trigger = null)
    {
        try
        {
            if (trigger is { } tag)
                LatticeMetrics.WalReplayPermitAdaptations.Add(1, outcome, tag, LatticeTenantLabel.Platform);
            else
                LatticeMetrics.WalReplayPermitAdaptations.Add(1, outcome, LatticeTenantLabel.Platform);
        }
        catch
        {
            // Like RecordReplayPermitHold, diagnostics cannot interrupt the
            // finally's physical disposition after its ledger already changed.
        }
    }

    internal static long BeginReplayServiceRateHold()
    {
        if (Volatile.Read(ref _replayServiceRateController) is null)
            return -1;
        lock (_replayConcurrencyGateLock)
            return _replayServiceRateController?.Begin() ?? -1;
    }

    internal static void CompleteReplayServiceRateHold(long generation)
    {
        if (generation < 0 || Volatile.Read(ref _replayServiceRateController) is null)
            return;
        lock (_replayConcurrencyGateLock)
        {
            _replayServiceRateController?.Complete(generation);
            // Census removal and the completion sample share the probe boundary.
            _replayPermitsServed++;
        }
    }

    private static void InvalidateReplayServiceRateWindow()
    {
        if (Volatile.Read(ref _replayServiceRateController) is null)
            return;
        lock (_replayConcurrencyGateLock)
            _replayServiceRateController?.Reset();
    }

    internal static void SeedReplayServiceRateForTest(long served, long windowTicks)
    {
        lock (_replayConcurrencyGateLock)
        {
            _replayPermitsServed = served;
            _replayServiceRateController ??= new ReplayServiceRateController(windowTicks);
        }
    }
}
