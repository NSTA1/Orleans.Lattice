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
        if (!memoryWithheld && !serviceWithheld)
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

        var memory = _withheldReplayPermits - _serviceWithheldReplayPermits;
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
    internal static bool ReplayServiceRateControllerEnabledForTest => Volatile.Read(ref _replayServiceRateController) is not null;

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
