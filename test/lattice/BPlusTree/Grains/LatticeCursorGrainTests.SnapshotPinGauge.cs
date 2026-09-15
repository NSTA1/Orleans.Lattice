using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Acceptance coverage for issue #2700: the <c>orleans.lattice.snapshot.pins</c>
/// series must not ratchet upward when an activation is lost while holding a WAL
/// retention pin.
/// <para>
/// Before this fix the series was an <c>UpDownCounter</c> whose <c>+1</c> /
/// <c>-1</c> were guarded by a per-<i>activation</i> boolean on
/// <see cref="LatticeCursorGrain"/>. The counter is process-lifetime state and
/// the guard was activation-lifetime state, so the increment was repeatable
/// across activations while the compensating decrement was not guaranteed.
/// Three ways in: the activation is collected or deactivated holding a pin, the
/// silo fails or the grain migrates holding a pin, or <c>UnregisterAsync</c>
/// throws and the catch arm returns with the flag still set. In all three the
/// WAL retention pin itself is correct and only the gauge drifts - which is the
/// worse failure for the operator it serves, because a ratcheting value makes a
/// genuine pin leak indistinguishable from accumulated drift.
/// </para>
/// <para>
/// A lost activation is simulated the only way that actually binds the
/// behaviour: a second <see cref="LatticeCursorGrain"/> is built over the same
/// grain id and the first is discarded without letting it clean up, exactly as a
/// collected activation or a failed silo does. Reasoning about the guard proves
/// nothing, because the guard is the defect.
/// </para>
/// </summary>
public partial class LatticeCursorGrainTests
{
    private static string SnapshotPinConsumerId => LatticeCursorGrain.SnapshotConsumerIdPrefix + $"{TreeId}/{CursorId}";

    /// <summary>
    /// Observes the snapshot-pin gauge for one tree.
    /// <para>
    /// Matching is by instrument <i>name</i> on the Lattice meter: the gauge
    /// lives inside <see cref="SnapshotPinCensus"/> rather than in a static
    /// field, and going through
    /// <see cref="MeterListening.StartForMeter(Meter, IEnumerable{string}, Action{MeterListener})"/>
    /// also keeps the listener clear of the static-initialiser re-entrancy
    /// hazard that helper exists to remove.
    /// </para>
    /// </summary>
    private sealed class PinGauge : IDisposable
    {
        private readonly List<long> _observations = [];
        private readonly object _gate = new();
        private readonly MeterListener _listener;
        private readonly string _tree;

        public PinGauge(string tree)
        {
            _tree = tree;
            _listener = MeterListening.StartForMeter(
                LatticeMetrics.Meter,
                [LatticeMetrics.SnapshotPinsGaugeName],
                listener => listener.SetMeasurementEventCallback<long>(
                    (_, value, tags, _) => Capture(value, tags)));
        }

        /// <summary>The tags carried by this tree's most recent observation.</summary>
        public IReadOnlyList<KeyValuePair<string, object?>> Tags { get; private set; } = [];

        /// <summary>
        /// Forces one observation round and returns the value reported for this
        /// tree, or <see langword="null"/> when the tree exports no series at
        /// all - which is a different statement from reporting zero, and the
        /// distinction the priming convention exists to preserve.
        /// </summary>
        public long? Value()
        {
            lock (_gate) { _observations.Clear(); }
            _listener.RecordObservableInstruments();
            long[] observed;
            lock (_gate) { observed = _observations.ToArray(); }

            Assert.That(observed, Has.Length.LessThanOrEqualTo(1),
                "one tree must export exactly one snapshot-pin series");
            return observed.Length == 0 ? null : observed[0];
        }

        public void Dispose() => _listener.Dispose();

        private void Capture(long value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var captured = tags.ToArray();
            foreach (var tag in captured)
            {
                if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal)
                    && string.Equals(tag.Value as string, _tree, StringComparison.Ordinal))
                {
                    lock (_gate) { _observations.Add(value); }
                    Tags = captured;
                    return;
                }
            }
        }
    }

    /// <summary>
    /// A cursor registry backed by a real per-tree set, so a test can
    /// distinguish "the pin is still registered" from "the census merely thinks
    /// it is". A substitute returning an empty snapshot would let a reconcile
    /// assertion pass for the wrong reason.
    /// </summary>
    private sealed class PinTrackingRegistry : IWalCursorRegistry
    {
        private readonly Dictionary<string, HashSet<string>> _byTree = new(StringComparer.Ordinal);

        public Exception? ThrowOnUnregister { get; set; }

        public Task ReportCursorAsync(
            string treeName, string consumerId, HybridLogicalClock cursor,
            CancellationToken cancellationToken = default)
        {
            lock (_byTree)
            {
                if (!_byTree.TryGetValue(treeName, out var set))
                {
                    set = new HashSet<string>(StringComparer.Ordinal);
                    _byTree[treeName] = set;
                }

                set.Add(consumerId);
            }

            return Task.CompletedTask;
        }

        public Task ReportCursorAsync(
            string treeName, string consumerId, HybridLogicalClock cursor,
            HybridLogicalClock? blockedAtHlc, CancellationToken cancellationToken = default)
            => ReportCursorAsync(treeName, consumerId, cursor, cancellationToken);

        public Task ReportCursorAsync(
            string treeName, string consumerId, HybridLogicalClock cursor, VersionVector vector,
            CancellationToken cancellationToken = default)
            => ReportCursorAsync(treeName, consumerId, cursor, cancellationToken);

        public Task ReportCursorAsync(
            string treeName, string consumerId, HybridLogicalClock cursor, VersionVector vector,
            HybridLogicalClock? blockedAtHlc, CancellationToken cancellationToken = default)
            => ReportCursorAsync(treeName, consumerId, cursor, cancellationToken);

        public Task UnregisterAsync(
            string treeName, string consumerId, CancellationToken cancellationToken = default)
        {
            if (ThrowOnUnregister is not null)
            {
                return Task.FromException(ThrowOnUnregister);
            }

            lock (_byTree)
            {
                if (_byTree.TryGetValue(treeName, out var set))
                {
                    set.Remove(consumerId);
                }
            }

            return Task.CompletedTask;
        }

        public Task<HybridLogicalClock?> GetMinCursorAsync(
            string treeName, CancellationToken cancellationToken = default)
            => Task.FromResult<HybridLogicalClock?>(null);

        public Task<VersionVector?> GetCausalStableAsync(
            string treeName, CancellationToken cancellationToken = default)
            => Task.FromResult<VersionVector?>(null);

        public Task<HybridLogicalClock?> GetBlockedFloorAsync(
            string treeName, CancellationToken cancellationToken = default)
            => Task.FromResult<HybridLogicalClock?>(null);

        public Task<IReadOnlyList<WalCursorSnapshot>> SnapshotAsync(
            string treeName, CancellationToken cancellationToken = default)
        {
            lock (_byTree)
            {
                IReadOnlyList<WalCursorSnapshot> result = _byTree.TryGetValue(treeName, out var set)
                    ? set.Select(id => new WalCursorSnapshot(id, HybridLogicalClock.Zero, 0)).ToArray()
                    : [];
                return Task.FromResult(result);
            }
        }
    }

    /// <summary>
    /// Builds one activation of the snapshot cursor grain wired to
    /// <paramref name="census"/> and <paramref name="registry"/>. Every call
    /// produces a <i>new</i> activation of the <i>same</i> grain id, which is
    /// what lets a test discard an activation mid-pin.
    /// </summary>
    private static LatticeCursorGrain PinnedActivation(
        SnapshotPinCensus census, IWalCursorRegistry registry)
    {
        var (grain, _, _) = CreateSnapshotGrain(services =>
        {
            services.AddSingleton(registry);
            services.AddSingleton(census);
        });
        return grain;
    }

    /// <summary>
    /// A census with a clean slate. Constructing it also makes it the instance
    /// the process-wide gauge observes, matching the DI singleton model.
    /// </summary>
    private static SnapshotPinCensus NewPinCensus(IWalCursorRegistry? registry = null)
    {
        var census = new SnapshotPinCensus(registry);
        census.ResetForTesting();
        return census;
    }

    private static Task OpenPinnedSnapshotAsync(LatticeCursorGrain grain) =>
        grain.OpenSnapshotAsync(TreeId, SnapshotSpec(), MakeCoordinate(shards: (0, 5)));

    // --- A genuinely held pin is still reported (acceptance criterion 3) ------

    [Test]
    public async Task SnapshotPinGauge_reports_a_pin_while_it_is_held()
    {
        var registry = new PinTrackingRegistry();
        var census = NewPinCensus(registry);
        using var gauge = new PinGauge(TreeId);

        await OpenPinnedSnapshotAsync(PinnedActivation(census, registry));

        // The whole point of the instrument. A fix that reached accuracy by
        // reporting nothing would re-introduce the very defect issue #2694
        // removed from this same instrument, so this assertion guards the fix
        // against its own worst failure mode.
        Assert.That(gauge.Value(), Is.EqualTo(1),
            "a live snapshot pin must be visible to the operator asking whether one holds the WAL GC floor down");
    }

    [Test]
    public async Task SnapshotPinGauge_stops_reporting_a_released_pin()
    {
        var registry = new PinTrackingRegistry();
        var census = NewPinCensus(registry);
        using var gauge = new PinGauge(TreeId);

        var grain = PinnedActivation(census, registry);
        await OpenPinnedSnapshotAsync(grain);
        Assert.That(gauge.Value(), Is.EqualTo(1), "guard: the pin must be held before it can be released");

        await grain.CloseAsync();

        Assert.That(gauge.Value(), Is.Zero,
            "the closed cursor holds no pin, and the tree must still export a series saying so");
    }

    // --- A lost activation leaves no residual (acceptance criterion 1) -------

    [Test]
    public async Task SnapshotPinGauge_leaves_no_residual_when_an_activation_is_lost_holding_a_pin()
    {
        var registry = new PinTrackingRegistry();
        var census = NewPinCensus(registry);
        using var gauge = new PinGauge(TreeId);

        // Activation A opens a snapshot cursor and reports its pin.
        var lost = PinnedActivation(census, registry);
        await OpenPinnedSnapshotAsync(lost);
        Assert.That(gauge.Value(), Is.EqualTo(1), "guard: activation A must hold the pin");

        // A is now lost without unregistering - collected, deactivated, or taken
        // down with its silo. Nothing runs on its behalf, and in particular no
        // compensating decrement is emitted. The registry entry survives, so the
        // pin really is still holding the trim floor down and reporting it is
        // the truth rather than a residual.
        lost = null!;
        Assert.That(gauge.Value(), Is.EqualTo(1),
            "the registry entry outlives the activation, so the pin it represents is still truthfully reported");

        // The cursor reactivates and re-reports the same pin. Under the old
        // per-activation guard this second report emitted a second +1 that
        // nothing would ever compensate, and the series ratcheted to 2.
        var reactivated = PinnedActivation(census, registry);
        await OpenPinnedSnapshotAsync(reactivated);
        Assert.That(gauge.Value(), Is.EqualTo(1),
            "one pin held by one cursor; re-reporting it across activations must not inflate the series");

        // The reactivated cursor closes cleanly, so the pin is gone. The old
        // counter would sit at 1 for the life of the process here: the second
        // activation's -1 cancels its own +1 and the lost activation's +1 is
        // never compensated by anything.
        await reactivated.CloseAsync();

        Assert.That(gauge.Value(), Is.Zero,
            "with no pin held the series must return to zero; the lost activation must leave nothing behind");
    }

    [Test]
    public async Task SnapshotPinGauge_heals_from_the_registry_when_a_pin_is_lost_with_its_silo()
    {
        var registry = new PinTrackingRegistry();
        var census = NewPinCensus(registry);
        using var gauge = new PinGauge(TreeId);

        await OpenPinnedSnapshotAsync(PinnedActivation(census, registry));
        Assert.That(gauge.Value(), Is.EqualTo(1), "guard: the pin must be held before it can be lost");

        // The activation is gone and never runs again - an ungraceful silo loss,
        // the one way in that neither a deactivation hook nor a persisted flag
        // can cover, because there is no silo left to run either. Some later
        // actor drops the registry entry: the cursor's idle-TTL reminder on a
        // surviving silo, or an operator.
        await registry.UnregisterAsync(TreeId, SnapshotPinConsumerId);

        // No grain, no hook, no restart, no metric reset: the next WAL GC pass
        // re-derives the set from the registry and the series heals itself.
        await census.ReconcileAsync(TreeId);

        Assert.That(gauge.Value(), Is.Zero,
            "the gauge is derived from the registry, so a pin that left the registry leaves the series with it");
    }

    // --- A throwing unregister leaves no residual (acceptance criterion 2) ----

    [Test]
    public async Task SnapshotPinGauge_leaves_no_residual_when_the_unregister_throws()
    {
        var registry = new PinTrackingRegistry();
        var census = NewPinCensus(registry);
        using var gauge = new PinGauge(TreeId);

        var grain = PinnedActivation(census, registry);
        await OpenPinnedSnapshotAsync(grain);
        Assert.That(gauge.Value(), Is.EqualTo(1),
            "guard: the pin must be held before the unregister is attempted");

        // The close path's unregister throws. The catch arm logs and returns -
        // which under the old design left the per-activation flag set, so this
        // activation's +1 was never compensated even though the activation then
        // went away.
        registry.ThrowOnUnregister = new InvalidOperationException("registry unavailable");
        await grain.CloseAsync();

        Assert.That(gauge.Value(), Is.EqualTo(1),
            "the unregister failed, so the pin really does still hold the floor down and must still be reported");

        // The pin later falls out of the registry by its own TTL, exactly as the
        // logged warning says it will. Nothing on the grain side runs.
        registry.ThrowOnUnregister = null;
        await registry.UnregisterAsync(TreeId, SnapshotPinConsumerId);
        await census.ReconcileAsync(TreeId);

        Assert.That(gauge.Value(), Is.Zero,
            "once the pin is out of the registry the series must follow it down, with no compensating write to lose");
    }

    // --- The series is zero-primed per tree (acceptance criterion 4) ---------

    [Test]
    public void SnapshotPinGauge_exports_an_explicit_zero_for_a_tracked_tree_holding_no_pin()
    {
        var census = NewPinCensus();
        using var gauge = new PinGauge(TreeId);

        Assert.That(gauge.Value(), Is.Null,
            "guard: an untracked tree exports no series, which is the state the priming exists to replace");

        census.Track(TreeId);

        Assert.That(gauge.Value(), Is.Zero,
            "a primed tree must export an explicit zero, so 'no pins held' is distinguishable from 'not reporting'");
    }

    [Test]
    public void SnapshotPinGauge_carries_the_tree_and_tenant_dimensions()
    {
        var census = NewPinCensus();
        using var gauge = new PinGauge(TreeId);
        census.Track(TreeId);

        _ = gauge.Value();

        // Tag parity with the superseded up/down counter is load-bearing: a
        // changed tag set silently orphans every dashboard panel and recording
        // rule that already selects this series.
        var tenant = LatticeTenantLabel.ForTree(TreeId);
        Assert.Multiple(() =>
        {
            Assert.That(
                gauge.Tags.Any(t =>
                    string.Equals(t.Key, LatticeMetrics.TagTree, StringComparison.Ordinal)
                    && string.Equals(t.Value as string, TreeId, StringComparison.Ordinal)),
                Is.True,
                "the tree dimension must survive the counter-to-gauge conversion");
            Assert.That(
                gauge.Tags.Any(t => string.Equals(t.Key, tenant.Key, StringComparison.Ordinal)),
                Is.True,
                "the derived tenant dimension must survive the counter-to-gauge conversion");
        });
    }

    // --- Census-level derivation behaviour -----------------------------------

    [Test]
    public async Task SnapshotPinCensus_reconcile_adopts_a_pin_it_was_never_told_about()
    {
        var registry = new PinTrackingRegistry();
        var census = NewPinCensus(registry);
        census.Track(TreeId);

        // A pin reported by a silo whose mark this census never saw - a shared
        // registry, or a future consumer that does not route through the cursor
        // grain.
        await registry.ReportCursorAsync(
            TreeId, LatticeCursorGrain.SnapshotConsumerIdPrefix + "elsewhere", HybridLogicalClock.Zero);

        await census.ReconcileAsync(TreeId);

        Assert.That(census.CountFor(TreeId), Is.EqualTo(1),
            "the registry is the authority on which pins hold the floor down, so the reconcile must adopt them");
    }

    [Test]
    public async Task SnapshotPinCensus_reconcile_ignores_cursors_that_are_not_snapshot_pins()
    {
        var registry = new PinTrackingRegistry();
        var census = NewPinCensus(registry);
        census.Track(TreeId);

        await registry.ReportCursorAsync(TreeId, "materialiser-1", HybridLogicalClock.Zero);

        await census.ReconcileAsync(TreeId);

        Assert.That(census.CountFor(TreeId), Is.Zero,
            "the instrument counts snapshot-cursor pins, not every consumer cursor on the tree");
    }

    [Test]
    public async Task SnapshotPinCensus_reconcile_without_a_registry_leaves_the_marks_untouched()
    {
        var census = NewPinCensus();
        census.MarkHeld(TreeId, LatticeCursorGrain.SnapshotConsumerIdPrefix + "only-mark");

        await census.ReconcileAsync(TreeId);

        Assert.That(census.CountFor(TreeId), Is.EqualTo(1),
            "a host with no cursor registry has nothing to re-derive from; the marks must stand rather than be cleared");
    }

    [Test]
    public void SnapshotPinCensus_counts_a_repeated_mark_once()
    {
        var census = NewPinCensus();
        var consumer = LatticeCursorGrain.SnapshotConsumerIdPrefix + "repeat";

        census.MarkHeld(TreeId, consumer);
        census.MarkHeld(TreeId, consumer);

        Assert.That(census.CountFor(TreeId), Is.EqualTo(1),
            "a cursor re-reports its pin on every page; the census is a set, not a tally");
    }

    [Test]
    public void SnapshotPinCensus_release_of_an_unheld_pin_cannot_drive_the_count_negative()
    {
        var census = NewPinCensus();
        census.Track(TreeId);

        census.MarkReleased(TreeId, LatticeCursorGrain.SnapshotConsumerIdPrefix + "never-held");

        Assert.That(census.CountFor(TreeId), Is.Zero,
            "a release arriving on an activation that never reported is the common shape after a failover");
    }

    [Test]
    public void SnapshotPinCensus_forgetting_a_tree_removes_its_series()
    {
        var census = NewPinCensus();
        using var gauge = new PinGauge(TreeId);
        census.Track(TreeId);
        Assert.That(gauge.Value(), Is.Zero, "guard: the tree must export a series before it can be dropped");

        census.Forget(TreeId);

        Assert.That(gauge.Value(), Is.Null,
            "a retired tree must stop exporting, or its zero outlives the tree for the life of the silo");
    }
}
