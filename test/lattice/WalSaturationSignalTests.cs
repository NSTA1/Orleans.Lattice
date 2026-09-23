using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="WalSaturationSignal"/> covering the
/// polling getters (<see cref="IWalSaturationSignal.GetCurrentState"/>
/// / <see cref="IWalSaturationSignal.GetAggregateState"/>), the
/// await-able gate (<see cref="IWalSaturationSignal.WaitForHealthyAsync"/>),
/// and the sampler-side state-update path.
/// </summary>
[TestFixture]
public class WalSaturationSignalTests
{
    [Test]
    public void GetCurrentState_throws_on_null_treeId()
    {
        var signal = new WalSaturationSignal();
        Assert.That(() => signal.GetCurrentState(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void GetCurrentState_returns_Healthy_for_unobserved_tree()
    {
        var signal = new WalSaturationSignal();
        Assert.That(signal.GetCurrentState("unseen-tree"), Is.EqualTo(WalSaturationState.Healthy));
    }

    [Test]
    public void GetCurrentState_reflects_most_recent_UpdateState()
    {
        var signal = new WalSaturationSignal();
        signal.UpdateState("tree-A", WalSaturationState.Throttled);
        Assert.That(signal.GetCurrentState("tree-A"), Is.EqualTo(WalSaturationState.Throttled));

        signal.UpdateState("tree-A", WalSaturationState.Saturated);
        Assert.That(signal.GetCurrentState("tree-A"), Is.EqualTo(WalSaturationState.Saturated));
    }

    [Test]
    public void GetAggregateState_returns_Healthy_when_no_trees_observed()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        Assert.That(signal.GetAggregateState(), Is.EqualTo(WalSaturationState.Healthy));
    }

    [Test]
    public void GetAggregateState_returns_worst_case_across_observed_trees()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState("a", WalSaturationState.Healthy);
        signal.UpdateState("b", WalSaturationState.Throttled);
        signal.UpdateState("c", WalSaturationState.Healthy);
        Assert.That(signal.GetAggregateState(), Is.EqualTo(WalSaturationState.Throttled));

        signal.UpdateState("d", WalSaturationState.Saturated);
        Assert.That(signal.GetAggregateState(), Is.EqualTo(WalSaturationState.Saturated));
    }

    [Test]
    public void UpdateState_returns_previous_state_for_attribution()
    {
        var signal = new WalSaturationSignal();
        var first = signal.UpdateState("tree-prev", WalSaturationState.Throttled);
        Assert.That(first, Is.EqualTo(WalSaturationState.Healthy),
            "first observation must report the implicit Healthy baseline as previous");

        var second = signal.UpdateState("tree-prev", WalSaturationState.Saturated);
        Assert.That(second, Is.EqualTo(WalSaturationState.Throttled),
            "subsequent UpdateState must report the prior state for transition attribution");

        var third = signal.UpdateState("tree-prev", WalSaturationState.Saturated);
        Assert.That(third, Is.EqualTo(WalSaturationState.Saturated),
            "a no-op update must report the same state as previous so callers can short-circuit");
    }

    [Test]
    public void WaitForHealthyAsync_throws_on_null_treeId()
    {
        var signal = new WalSaturationSignal();
        Assert.That(() => signal.WaitForHealthyAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void WaitForHealthyAsync_completes_synchronously_when_already_Healthy()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        var task = signal.WaitForHealthyAsync("never-touched");
        Assert.That(task.IsCompletedSuccessfully, Is.True,
            "an already-Healthy tree must short-circuit to a completed task with no allocation");
    }

    [Test]
    public async Task WaitForHealthyAsync_completes_when_state_transitions_back_to_Healthy()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState("tree-wait", WalSaturationState.Saturated);

        var wait = signal.WaitForHealthyAsync("tree-wait");
        Assert.That(wait.IsCompleted, Is.False, "must not complete while tree is Saturated");

        signal.UpdateState("tree-wait", WalSaturationState.Healthy);

        // The TCS is RunContinuationsAsynchronously so await it.
        await wait.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(wait.IsCompletedSuccessfully, Is.True);
    }

    [Test]
    public async Task WaitForHealthyAsync_throws_OperationCanceledException_when_cancelled()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState("tree-cancel", WalSaturationState.Saturated);

        using var cts = new CancellationTokenSource();
        var wait = signal.WaitForHealthyAsync("tree-cancel", cts.Token);

        cts.Cancel();
        Assert.That(async () => await wait, Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void WaitForHealthyAsync_with_pre_cancelled_token_throws_immediately()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState("tree-pre-cancel", WalSaturationState.Saturated);

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            () => signal.WaitForHealthyAsync("tree-pre-cancel", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task WaitForHealthyAsync_multiple_waiters_all_complete_on_single_recovery()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState("tree-many", WalSaturationState.Throttled);

        var waits = new[]
        {
            signal.WaitForHealthyAsync("tree-many"),
            signal.WaitForHealthyAsync("tree-many"),
            signal.WaitForHealthyAsync("tree-many"),
        };

        signal.UpdateState("tree-many", WalSaturationState.Healthy);

        await Task.WhenAll(waits).WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(waits.All(w => w.IsCompletedSuccessfully), Is.True);
    }

    // ---- (#3402) Paced recovery release -------------------------------
    // A recovered partition used to complete every parked waiter in one
    // pass. Once the parked population exceeds the admission pipeline's
    // capacity the released herd re-saturates the partition immediately,
    // so the signal never sustains Healthy and no caller makes progress.
    // These cover the pacing and, critically, that pacing does not strand
    // the residue.

    /// <summary>
    /// Parks more waiters than the release batch and asserts a single
    /// Healthy observation hands out exactly the batch size, leaving the
    /// remainder parked rather than releasing the whole herd at once.
    /// </summary>
    [Test]
    public void UpdatePartitionState_releases_at_most_the_configured_batch()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdatePartitionState("tree-herd", 0, WalSaturationState.Saturated);

        var waits = Enumerable.Range(0, 5)
            .Select(_ => signal.WaitForHealthyAsync("tree-herd", 0))
            .ToArray();
        Assert.That(waits.Count(w => w.IsCompleted), Is.EqualTo(0), "all five must park while Saturated");

        signal.UpdatePartitionState("tree-herd", 0, WalSaturationState.Healthy, releaseBatch: 2);

        Assert.That(waits.Count(w => w.IsCompleted), Is.EqualTo(2),
            "exactly the release batch may be admitted by one tick; releasing the whole herd is the #3402 defect");
    }

    /// <summary>
    /// The no-stranding guarantee. The residue left by a paced release is
    /// drained by later ticks that observe the partition still reading
    /// Healthy - there is no second Saturated to Healthy edge to wait for,
    /// so an edge-triggered release would park the remainder until the next
    /// saturation cycle.
    /// </summary>
    [Test]
    public async Task UpdatePartitionState_drains_the_residue_on_later_healthy_ticks()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdatePartitionState("tree-drain", 0, WalSaturationState.Saturated);

        var waits = Enumerable.Range(0, 5)
            .Select(_ => signal.WaitForHealthyAsync("tree-drain", 0))
            .ToArray();

        signal.UpdatePartitionState("tree-drain", 0, WalSaturationState.Healthy, releaseBatch: 2);
        Assert.That(waits.Count(w => w.IsCompleted), Is.EqualTo(2));

        // Already Healthy: no transition occurs, so this only releases
        // more waiters if the release is level-triggered.
        signal.UpdatePartitionState("tree-drain", 0, WalSaturationState.Healthy, releaseBatch: 2);
        Assert.That(waits.Count(w => w.IsCompleted), Is.EqualTo(4),
            "a steady-state Healthy tick must keep draining the residue, not wait for the next saturation cycle");

        signal.UpdatePartitionState("tree-drain", 0, WalSaturationState.Healthy, releaseBatch: 2);
        await Task.WhenAll(waits).WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(waits.All(w => w.IsCompletedSuccessfully), Is.True);
    }

    /// <summary>
    /// Oldest-first release, so pacing cannot starve the callers that have
    /// waited longest and are closest to exhausting their wait budget.
    /// </summary>
    [Test]
    public void UpdatePartitionState_releases_oldest_waiters_first()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdatePartitionState("tree-fifo", 0, WalSaturationState.Saturated);

        var first = signal.WaitForHealthyAsync("tree-fifo", 0);
        var second = signal.WaitForHealthyAsync("tree-fifo", 0);
        var third = signal.WaitForHealthyAsync("tree-fifo", 0);

        signal.UpdatePartitionState("tree-fifo", 0, WalSaturationState.Healthy, releaseBatch: 1);

        Assert.Multiple(() =>
        {
            Assert.That(first.IsCompleted, Is.True, "the longest-parked caller is released first");
            Assert.That(second.IsCompleted, Is.False);
            Assert.That(third.IsCompleted, Is.False);
        });
    }

    /// <summary>
    /// Zero is the documented escape hatch and must restore the pre-#3402
    /// release-everything behaviour.
    /// </summary>
    [Test]
    public async Task UpdatePartitionState_with_zero_batch_releases_every_waiter()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdatePartitionState("tree-all", 0, WalSaturationState.Saturated);

        var waits = Enumerable.Range(0, 6)
            .Select(_ => signal.WaitForHealthyAsync("tree-all", 0))
            .ToArray();

        signal.UpdatePartitionState("tree-all", 0, WalSaturationState.Healthy, releaseBatch: 0);

        await Task.WhenAll(waits).WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(waits.All(w => w.IsCompletedSuccessfully), Is.True);
    }

    /// <summary>
    /// Pacing is per partition: draining one partition's backlog must not
    /// consume another partition's release quota or complete its waiters.
    /// </summary>
    [Test]
    public void UpdatePartitionState_paces_each_partition_independently()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdatePartitionState("tree-iso", 0, WalSaturationState.Saturated);
        signal.UpdatePartitionState("tree-iso", 1, WalSaturationState.Saturated);

        var p0 = Enumerable.Range(0, 3)
            .Select(_ => signal.WaitForHealthyAsync("tree-iso", 0))
            .ToArray();
        var p1 = Enumerable.Range(0, 3)
            .Select(_ => signal.WaitForHealthyAsync("tree-iso", 1))
            .ToArray();

        signal.UpdatePartitionState("tree-iso", 0, WalSaturationState.Healthy, releaseBatch: 2);

        Assert.Multiple(() =>
        {
            Assert.That(p0.Count(w => w.IsCompleted), Is.EqualTo(2));
            Assert.That(p1.Any(w => w.IsCompleted), Is.False,
                "recovering partition 0 must not complete a wait registered against partition 1");
        });
    }

    [Test]
    public void WaitForHealthyAsync_for_different_trees_is_independent()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState("tree-A", WalSaturationState.Throttled);
        signal.UpdateState("tree-B", WalSaturationState.Throttled);

        var waitA = signal.WaitForHealthyAsync("tree-A");
        var waitB = signal.WaitForHealthyAsync("tree-B");

        signal.UpdateState("tree-A", WalSaturationState.Healthy);

        // A completes; B still parked.
        Assert.That(waitA.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(waitB.IsCompleted, Is.False,
            "recovering tree A must not complete a wait registered against tree B");
    }

    [Test]
    public void StateGauge_emits_one_series_per_tree_with_no_state_label()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        var tree = "gauge-tree-" + Guid.NewGuid().ToString("N");
        signal.UpdateState(tree, WalSaturationState.Saturated);

        using var capture = new GaugeCapture();
        var measurements = capture.RecordFor(tree);

        Assert.That(measurements, Has.Count.EqualTo(1),
            "the gauge must publish exactly one series per tree");
        var only = measurements[0];
        Assert.That(only.Value, Is.EqualTo((long)WalSaturationState.Saturated));
        Assert.That(only.Tags.Any(t => t.Key == LatticeMetrics.TagWalSaturationState), Is.False,
            "the gauge must not carry the redundant state label - the ordinal value already encodes the regime, and the label fragments the series across transitions");
        Assert.That(only.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == tree), Is.True,
            "the gauge series is identified by the tree tag alone");
    }

    [Test]
    public void StateGauge_reflects_latest_value_after_recovery_without_leaving_stale_series()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        var tree = "gauge-recover-" + Guid.NewGuid().ToString("N");

        signal.UpdateState(tree, WalSaturationState.Saturated);
        signal.UpdateState(tree, WalSaturationState.Healthy);

        using var capture = new GaugeCapture();
        var measurements = capture.RecordFor(tree);

        // Because the series identity never changes across transitions
        // (no state label), a recovered tree collapses to a single
        // Healthy series rather than leaving an orphaned Saturated one
        // lingering at its last value.
        Assert.That(measurements, Has.Count.EqualTo(1),
            "a recovered tree must not leave a second, stale elevated series behind");
        Assert.That(measurements[0].Value, Is.EqualTo((long)WalSaturationState.Healthy),
            "the single series must report the current (recovered) regime");
    }

    /// <summary>
    /// Records the process-wide WAL saturation-state observable gauge on
    /// demand. The gauge reads the most-recently-constructed
    /// <see cref="WalSaturationSignal"/> (<c>_current</c>), which the
    /// caller has just constructed, so scoping the returned measurements
    /// to a unique tree id isolates them from any residual series.
    /// </summary>
    private sealed class GaugeCapture : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<(long Value, KeyValuePair<string, object?>[] Tags)> _records = new();

        public GaugeCapture()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                        && inst.Name == LatticeMetrics.WalSaturationStateGaugeName)
                    {
                        l.EnableMeasurementEvents(inst);
                    }
                },
            };
            _listener.SetMeasurementEventCallback<long>(
                (_, value, tags, _) => _records.Add((value, tags.ToArray())));
            _listener.Start();
        }

        public List<(long Value, KeyValuePair<string, object?>[] Tags)> RecordFor(string tree)
        {
            _records.Clear();
            _listener.RecordObservableInstruments();
            return _records
                .Where(r => r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == tree))
                .ToList();
        }

        public void Dispose() => _listener.Dispose();
    }
}
