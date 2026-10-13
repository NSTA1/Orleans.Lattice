namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// The activation's physical release seam. The reactive catch has already
    /// accounted for a withheld permit; all other decisions happen here, before
    /// returning the acquired permit. Diagnostics run only after its disposition.
    /// </summary>
    internal static void FinishReplayPermit(
        SemaphoreSlim gate, bool memoryWithheld, bool completedCleanly,
        long acquiredAt, string metricTreeId, string? treeId)
    {
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

        Interlocked.Increment(ref _replayPermitsServed);
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

        lock (_replayConcurrencyGateLock)
        {
            var controller = _replayServiceRateController;
            if (controller is null)
                return false;

            if (!healthy)
            {
                controller.Reset();
                return false;
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
                RecordServiceReplayPermitAdaptation(LatticeMetrics.PermitAdaptationWithheld);
                return true;
            }

            if (change > 0 && _serviceWithheldReplayPermits > 0 && healthy && memory == 0)
            {
                _serviceWithheldReplayPermits--;
                Volatile.Write(ref _withheldReplayPermits, _withheldReplayPermits - 1);
                gate.Release();
                RecordServiceReplayPermitAdaptation(LatticeMetrics.PermitAdaptationRestored);
            }
            else if (change != 0)
            {
                controller.Reset();
            }

            return false;
        }
    }

    internal static int ServiceWithheldReplayPermitsForTest => Volatile.Read(ref _serviceWithheldReplayPermits);
    internal static bool ReplayServiceRateControllerEnabledForTest => Volatile.Read(ref _replayServiceRateController) is not null;

    private static void RecordServiceReplayPermitAdaptation(KeyValuePair<string, object?> outcome)
    {
        try
        {
            LatticeMetrics.WalReplayPermitAdaptations.Add(
                1, outcome, ServiceTimeTrigger, LatticeTenantLabel.Platform);
        }
        catch
        {
            // Like RecordReplayPermitHold, diagnostics cannot interrupt the
            // finally's physical disposition after its ledger already changed.
        }
    }

    private static void CompleteReplayServiceRateHold(long acquiredAt)
    {
        if (Volatile.Read(ref _replayServiceRateController) is null)
            return;
        lock (_replayConcurrencyGateLock)
            _replayServiceRateController?.Complete(acquiredAt);
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
