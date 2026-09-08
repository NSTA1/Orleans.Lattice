using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the activation-failure observation added for issue #2280: the
/// second of the leaf's two observation sites, which counts activations that
/// threw out of <c>OnActivateAsync</c> by tree, temperature and reason.
/// <para>
/// This site is not a nicety. Orleans does not run <c>OnDeactivateAsync</c> when
/// <c>OnActivateAsync</c> throws - measured against a positive control in
/// <see cref="BPlusLeafGrainActivationFailureHookContractTests"/> - and issue
/// #2280's population is a cold WAL replay CANCELLED before it completes, which
/// leaves activation by throwing. A deactivation-sited instrument alone would
/// therefore report zero for that population at every rate of occurrence
/// including the highest, which is not a weak measurement but no measurement.
/// </para>
/// <para>
/// The fault is injected through the same throwing-logger-factory rig the
/// replay-permit-leak fixture uses (issue #2256), because that rig throws from
/// inside precisely the permit-guarded region this counter's catch wraps.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)> CaptureActivationFailures(
        out IDisposable listener)
    {
        var records = new ConcurrentBag<(long, KeyValuePair<string, object?>[])>();
        listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafActivationFailures,
            l => l.SetMeasurementEventCallback<long>(
                (_, value, tags, _) => records.Add((value, tags.ToArray()))));
        return records;
    }

    [Test]
    public void Failed_activation_is_counted_as_faulted_and_tagged_by_tree_and_temperature()
    {
        var treeId = UniqueReplayPermitTree();
        var (grain, state) = CreateGrainWithLoggerFactory(
            new ThrowingProbeLoggerFactory(() => throw new InvalidOperationException("activation-observation-probe")));
        state.State.TreeId = treeId;

        var records = CaptureActivationFailures(out var listener);
        using (listener)
        {
            Assert.ThrowsAsync<InvalidOperationException>(
                async () => await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None),
                "The observation must OBSERVE AND RETHROW. Swallowing the fault would bring the leaf "
                + "online over a half-applied projection, which is the issue #1535 no-loss violation "
                + "that 'failures propagate' exists to prevent.");
        }

        Assert.That(records, Has.Count.EqualTo(1),
            "A failed activation must be counted exactly once.");

        var tags = records.Single().Tags;
        Assert.That(
            tags.Select(t => t.Key),
            Is.EquivalentTo(new[]
            {
                LatticeMetrics.TagTree,
                LatticeMetrics.TagActivationTemperature,
                LatticeMetrics.TagReason,
                LatticeTenantLabel.TagTenant,
            }),
            "Exactly these four bounded tags - no leaf id, whose population is unbounded. The tenant "
            + "dimension is derived from the tree and so adds no cardinality of its own.");

        Assert.That(tags.Single(t => t.Key == LatticeMetrics.TagTree).Value, Is.EqualTo(treeId));
        Assert.That(
            tags.Single(t => t.Key == LatticeMetrics.TagReason).Value,
            Is.EqualTo(LatticeMetrics.ActivationFailureFaulted.Value),
            "A non-cancellation fault must be reported as faulted.");
        Assert.That(
            tags.Single(t => t.Key == LatticeMetrics.TagActivationTemperature).Value,
            Is.EqualTo(LatticeMetrics.ActivationTemperatureCold.Value),
            "This rig has no snapshot and an empty cache, so the activation replays the whole readable "
            + "WAL window: it is COLD, which is the arm issue #2280 is about.");
    }

    [Test]
    public void Cancelled_activation_is_counted_separately_from_a_generic_fault()
    {
        // The reason split is the point of the tag. Issue #2280's mechanism is
        // CANCELLATION specifically - an activation cut short while replaying -
        // and a counter that could not separate that from an ordinary fault
        // would not answer the question it was built for.
        var treeId = UniqueReplayPermitTree();
        var (grain, state) = CreateGrainWithLoggerFactory(
            new ThrowingProbeLoggerFactory(() => throw new OperationCanceledException()));
        state.State.TreeId = treeId;

        var records = CaptureActivationFailures(out var listener);
        using (listener)
        {
            Assert.ThrowsAsync<OperationCanceledException>(
                async () => await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None));
        }

        Assert.That(records, Has.Count.EqualTo(1));
        Assert.That(
            records.Single().Tags.Single(t => t.Key == LatticeMetrics.TagReason).Value,
            Is.EqualTo(LatticeMetrics.ActivationFailureCanceled.Value),
            "A cancelled activation must be reported as canceled, not folded in with generic faults.");
    }

    [Test]
    public async Task Successful_activation_is_not_counted_as_a_failure()
    {
        // The control. Without it, a counter wired to fire on every activation
        // would satisfy both tests above and be useless in the field - the
        // failure mode the wave's ruling on issue #2277 rejected outright.
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 0,
            walHead: 0);
        state.State.TreeId = UniqueReplayPermitTree();

        var records = CaptureActivationFailures(out var listener);
        using (listener)
        {
            await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        }

        Assert.That(records, Is.Empty,
            "A clean activation must record nothing at all.");
    }

    /// <summary>
    /// Faults <c>ResolveLogger</c>, which runs inside the permit-guarded region
    /// the activation-failure counter's catch wraps. Takes the throw as a
    /// delegate so a test can choose the exception type, which is what the
    /// canceled/faulted reason split turns on.
    /// </summary>
    private sealed class ThrowingProbeLoggerFactory(Func<ILogger> onCreate) : ILoggerFactory
    {
        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => onCreate();

        public void Dispose()
        {
        }
    }

    [Test]
    public async Task Activation_cancelled_while_queued_for_the_permit_is_counted_under_its_own_reason()
    {
        // The window this covers is the one that matters most under the
        // conditions issue #2280 describes: when replays are saturated, an
        // activation spends most of its life QUEUED for the permit, so that is
        // where a cancellation is most likely to land. It is reported under a
        // distinct reason rather than folded into `canceled` because the two
        // are different events - this activation never began replaying and had
        // no in-flight work to lose - and merging them would let a rise in
        // queueing read as a rise in abandoned replays.

        // Size the gate: it is created lazily by the first activation that
        // resolves options, so it must be made to exist before it can be
        // drained.
        var (warmGrain, warmState, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
        warmState.State.TreeId = UniqueReplayPermitTree();
        await ((IGrainBase)warmGrain).OnActivateAsync(CancellationToken.None);

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest;
        Assert.That(gate, Is.Not.Null);

        var treeId = UniqueReplayPermitTree();
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
        state.State.TreeId = treeId;

        var drained = 0;
        var records = CaptureActivationFailures(out var listener);
        using var cts = new CancellationTokenSource();
        try
        {
            // Hold every permit so the activation below cannot acquire one and
            // must wait - which is the state being measured.
            while (gate!.Wait(0))
                drained++;
            Assert.That(drained, Is.GreaterThan(0),
                "the gate must have had permits to drain, otherwise the activation below would be "
                + "waiting for a reason this test did not create");

            using (listener)
            {
                var activation = ((IGrainBase)grain).OnActivateAsync(cts.Token);

                // Confirm it really is parked on the gate before cancelling,
                // so a pass cannot come from an activation that had already
                // failed for some earlier reason.
                Assert.That(activation.IsCompleted, Is.False,
                    "the activation must be blocked on the drained permit gate");

                await cts.CancelAsync();
                Assert.That(async () => await activation, Throws.InstanceOf<OperationCanceledException>());
            }
        }
        finally
        {
            // The gate is process-wide and is never re-created, so failing to
            // restore it here would permanently reduce leaf-activation
            // concurrency for every test that runs afterwards - the very
            // failure mode issue #2256 is about.
            for (var i = 0; i < drained; i++)
                gate!.Release();
        }

        Assert.That(records, Has.Count.EqualTo(1),
            "A cancellation while queued for the permit must be counted, not silently dropped.");
        Assert.That(
            records.Single().Tags.Single(t => t.Key == LatticeMetrics.TagReason).Value,
            Is.EqualTo(LatticeMetrics.ActivationFailureCanceledAwaitingPermit.Value),
            "It must carry its own reason so it cannot be mistaken for a replay that was cut short.");
    }
}
