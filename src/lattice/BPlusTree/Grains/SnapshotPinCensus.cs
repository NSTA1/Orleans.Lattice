using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-silo census of the WAL retention pins held by zero-observable-writes
/// snapshot cursors, and the source of the
/// <see cref="LatticeMetrics.SnapshotPinsGaugeName"/> observable gauge.
/// <para>
/// The census holds, per tree, the <i>set of live snapshot consumer ids</i>
/// registered against <see cref="IWalCursorRegistry"/> - not a running total.
/// That distinction is the whole point. The instrument was previously an
/// <c>UpDownCounter</c> whose <c>+1</c> / <c>-1</c> were emitted by
/// <c>LatticeCursorGrain</c> under a per-<i>activation</i> boolean. The counter
/// is process-lifetime state and the guard was activation-lifetime state, so
/// the increment was repeatable across activations while the compensating
/// decrement was not guaranteed: an activation collected, migrated, or lost
/// with its silo while holding a pin never emitted its <c>-1</c>, and the gauge
/// ratcheted permanently upward. A set keyed by the pin's own identity cannot
/// ratchet - re-reporting an existing pin is idempotent, and releasing one that
/// was never marked is a no-op (issue #2700).
/// </para>
/// <para>
/// <b>Nothing here is a compensating write.</b> Membership is asserted from the
/// two places that mutate the registry entry itself
/// (<see cref="MarkHeld"/> after a successful report,
/// <see cref="MarkReleased"/> after a successful unregister) and is then
/// <i>re-derived</i> from the registry by <see cref="ReconcileAsync"/> on every
/// WAL GC pass. Because the reconcile replaces membership with what the
/// registry currently holds, any mark that is lost - to a torn activation, to a
/// registry shared across silos, or to a future consumer that unregisters a
/// snapshot pin without going through the cursor grain - self-heals on the next
/// pass with no operator action, no restart, and no metric reset.
/// </para>
/// <para>
/// A pin that outlives its activation is <i>still reported</i>, and that is
/// correct rather than a residual: the registry entry genuinely still holds the
/// WAL GC floor down until the cursor's idle-TTL reminder reactivates the grain
/// and unregisters it. The gauge tracks the pin, not the activation, so it
/// falls to zero exactly when the pin does.
/// </para>
/// <para>
/// <b>The derived value is as of the last WAL GC pass for the tree, not as of
/// the scrape</b>, and the self-healing half is inert when WAL GC is disabled.
/// See <see cref="ObservePins"/> for the staleness bound and its consequences
/// before reading a pin count that disagrees with the registry.
/// </para>
/// </summary>
internal sealed class SnapshotPinCensus
{
    private static readonly object RegistrationLock = new();
    private static volatile SnapshotPinCensus? _current;
    private static bool _gaugeRegistered;

    private readonly IWalCursorRegistry? _cursorRegistry;

    /// <summary>
    /// Per-tree live snapshot-pin sets, keyed by tree id then by the consumer
    /// id the cursor reports under. The inner dictionary is used as a set (the
    /// value is ignored) so membership reads and writes are lock-free and the
    /// observable-gauge callback never blocks a scrape against a pin report.
    /// An empty inner set is meaningful and deliberately retained: it is the
    /// zero-primed series for a tree that holds no snapshot pin.
    /// </summary>
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _byTree
        = new(StringComparer.Ordinal);

    /// <summary>
    /// Initialises the census and ensures the observable snapshot-pin gauge is
    /// registered on the shared meter. Registration is process-wide and
    /// idempotent; the most recently constructed instance backs every gauge
    /// scrape, matching the DI singleton model used by <c>AddLattice</c> and
    /// the identical arrangement in <see cref="WalSaturationSignal"/>.
    /// </summary>
    /// <param name="cursorRegistry">
    /// The WAL cursor registry the census re-derives its membership from, or
    /// <see langword="null"/> on a host that registered none - in which case
    /// <see cref="ReconcileAsync"/> is a no-op and the marks stand alone.
    /// </param>
    public SnapshotPinCensus(IWalCursorRegistry? cursorRegistry = null)
    {
        _cursorRegistry = cursorRegistry;
        Publish(this);
    }

    /// <summary>
    /// Registers <paramref name="treeId"/> so the gauge exports a series for it
    /// even while it holds no snapshot pin, making "no pins held" distinguishable
    /// from "not reporting".
    /// <para>
    /// This is the observable-gauge equivalent of the <c>Add(0)</c> priming
    /// issue #2694 introduced for the WAL-retention counters, and it is called
    /// from the same place for the same reason: the WAL GC scheduler enumerates
    /// exactly the trees whose retention is being collected, which is the
    /// population the question is asked about.
    /// </para>
    /// </summary>
    public void Track(string treeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeId);
        _byTree.GetOrAdd(treeId, static _ => new ConcurrentDictionary<string, byte>(StringComparer.Ordinal));
    }

    /// <summary>
    /// Records that <paramref name="consumerId"/> holds a live snapshot pin on
    /// <paramref name="treeId"/>. Idempotent: a cursor re-reports its pin on
    /// every page, and a reactivated cursor reports under the same consumer id,
    /// so repeated calls cannot inflate the reported count.
    /// </summary>
    public void MarkHeld(string treeId, string consumerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeId);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);
        _byTree
            .GetOrAdd(treeId, static _ => new ConcurrentDictionary<string, byte>(StringComparer.Ordinal))
            .TryAdd(consumerId, 0);
    }

    /// <summary>
    /// Records that <paramref name="consumerId"/> no longer holds a snapshot pin
    /// on <paramref name="treeId"/>. Idempotent, and a no-op for a consumer that
    /// was never marked - so a release arriving on an activation that never
    /// reported (the common case after a failover) cannot drive the count
    /// negative. The tree's series is retained at zero rather than removed.
    /// </summary>
    public void MarkReleased(string treeId, string consumerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeId);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);
        if (_byTree.TryGetValue(treeId, out var pins))
        {
            pins.TryRemove(consumerId, out _);
        }
    }

    /// <summary>
    /// Drops every series for <paramref name="treeId"/>. Called when the tree
    /// registry stops reporting a tree, so a deleted tree does not export a
    /// gauge for the life of the silo.
    /// </summary>
    public void Forget(string treeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeId);
        _byTree.TryRemove(treeId, out _);
    }

    /// <summary>
    /// Re-derives <paramref name="treeId"/>'s live snapshot-pin set from the
    /// <see cref="IWalCursorRegistry"/> this census was constructed with. This
    /// is what makes the gauge self-healing: whatever the marks say, the
    /// registry is the authority on which pins are actually holding the WAL GC
    /// floor down, and after this call the gauge reports exactly those.
    /// <para>
    /// The removal half is restricted to the members observed <i>before</i> the
    /// registry read, so a pin reported concurrently with the read is added by
    /// its own <see cref="MarkHeld"/> and never removed by a snapshot that
    /// predates it. Without the registry (a host that never registered one) the
    /// call is a no-op and the marks stand on their own.
    /// </para>
    /// </summary>
    public async Task ReconcileAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeId);
        if (_cursorRegistry is null)
        {
            return;
        }

        var pins = _byTree.GetOrAdd(
            treeId, static _ => new ConcurrentDictionary<string, byte>(StringComparer.Ordinal));
        var observedBefore = pins.Keys;

        var registered = await _cursorRegistry.SnapshotAsync(treeId, cancellationToken).ConfigureAwait(false);

        var live = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < registered.Count; i++)
        {
            var consumerId = registered[i].ConsumerId;
            if (consumerId.StartsWith(LatticeCursorGrain.SnapshotConsumerIdPrefix, StringComparison.Ordinal))
            {
                live.Add(consumerId);
            }
        }

        foreach (var consumerId in observedBefore)
        {
            if (!live.Contains(consumerId))
            {
                pins.TryRemove(consumerId, out _);
            }
        }

        foreach (var consumerId in live)
        {
            pins.TryAdd(consumerId, 0);
        }
    }

    /// <summary>
    /// Current per-tree pin count. Exposed for the gauge callback and for tests
    /// that assert the reported value without standing up a meter listener.
    /// </summary>
    public int CountFor(string treeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeId);
        return _byTree.TryGetValue(treeId, out var pins) ? pins.Count : 0;
    }

    /// <summary>
    /// Emits one measurement per tracked tree, carrying the same
    /// <see cref="LatticeMetrics.TagTree"/> and tenant tags the superseded
    /// up/down counter carried, so existing dashboard panels and queries keep
    /// matching the series unchanged.
    /// <para>
    /// <b>Staleness bound.</b> This reports the pin set as of the <i>last WAL GC
    /// pass for that tree</i>, not as of the scrape. The two mark sites keep the
    /// graceful paths current at the instant they run, but the self-healing half
    /// - dropping a pin whose activation was lost with its silo, which no mark
    /// can ever report - happens only in <see cref="ReconcileAsync"/>, which the
    /// WAL GC scheduler calls once per pass. So the worst-case lag between a pin
    /// ceasing to hold the trim floor down and this gauge ceasing to report it
    /// is that tree's <i>effective</i> GC interval, which the scheduler varies
    /// within
    /// <c>[<see cref="LatticeOptions.WalGcMinInterval"/>,
    /// <see cref="LatticeOptions.WalGcInterval"/>]</c> (30 s to 1 h by default).
    /// A quiet tree relaxes toward the ceiling, so on a healthy tree the bound
    /// is the ceiling, not the floor. A tree blocked by an unusable pin holds at
    /// the floor (issue #2704), so the surface most likely to be under
    /// investigation is also the one that reconciles most often.
    /// </para>
    /// <para>
    /// <b>With <see cref="LatticeOptions.WalGcInterval"/> at or below zero the
    /// self-healing half is inert.</b> That setting disables the scheduler
    /// outright, so neither the per-tree zero-priming nor
    /// <see cref="ReconcileAsync"/> ever runs. <see cref="MarkHeld"/> and
    /// <see cref="MarkReleased"/> still keep every graceful path correct, and
    /// keying on the consumer id still makes the pre-#2700 ratchet
    /// unrepresentable, so the series remains strictly more accurate than the
    /// up/down counter it replaced - but a pin lost with its silo would never
    /// age out of this gauge, because nothing would re-read the registry. A
    /// deployment that disables WAL GC and needs this gauge to self-heal must
    /// re-enable it.
    /// </para>
    /// </summary>
    private IEnumerable<Measurement<long>> ObservePins()
    {
        foreach (var kv in _byTree)
        {
            yield return new Measurement<long>(
                kv.Value.Count,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, kv.Key),
                LatticeTenantLabel.ForTree(kv.Key));
        }
    }

    /// <summary>
    /// Installs <paramref name="instance"/> as the census the process-wide gauge
    /// observes, registering the gauge on first use.
    /// </summary>
    private static void Publish(SnapshotPinCensus instance)
    {
        lock (RegistrationLock)
        {
            _current = instance;
            if (_gaugeRegistered)
            {
                return;
            }

            LatticeMetrics.Meter.CreateObservableGauge(
                LatticeMetrics.SnapshotPinsGaugeName,
                static () => _current?.ObservePins() ?? Array.Empty<Measurement<long>>(),
                unit: "{pin}",
                description: "Live WAL retention pins held by zero-observable-writes snapshot cursors.");
            _gaugeRegistered = true;
        }
    }

    /// <summary>
    /// Test-only reset. Clears every tracked tree so a successor fixture sees a
    /// clean census. Intentionally <c>internal</c> so production code cannot
    /// call it.
    /// </summary>
    internal void ResetForTesting() => _byTree.Clear();
}
