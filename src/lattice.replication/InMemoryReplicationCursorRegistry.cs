using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default in-memory <see cref="ILatticeReplicationCursorRegistry"/>
/// implementation. Per-tree state is held in a dictionary protected by
/// a single lock; concurrent reports against different trees are
/// serialised through it but never block durable I/O so the lock is
/// only held for a few field assignments per call.
/// <para>
/// State is process-local and is lost on silo restart. After a
/// restart, every consumer must re-report its cursor before the GC
/// predicate trims past it; until then,
/// <see cref="GetMinCursorAsync"/> returns <see langword="null"/> and
/// the GC trims only by the optional
/// <see cref="LatticeReplicationOptions.WalRetention"/> hard ceiling.
/// This matches the "fall-off-the-log" detection seam later phases use
/// to trigger auto-bootstrap.
/// </para>
/// <para>
/// The causal-stable frontier returned by
/// <see cref="GetCausalStableAsync"/> is cached behind a per-tree
/// generation counter that bumps on every accepted mutation. A GC pass
/// that observes a stable registry therefore reads the cached frontier
/// in O(1); a recompute only happens after a consumer reports or
/// unregisters.
/// </para>
/// </summary>
public sealed class InMemoryReplicationCursorRegistry : ILatticeReplicationCursorRegistry
{
    private readonly object _gate = new();
    private readonly Dictionary<string, TreeState> _byTree = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public Task ReportCursorAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        CancellationToken cancellationToken = default)
        => ReportCursorCoreAsync(treeName, consumerId, cursor, vector: null, cancellationToken);

    /// <inheritdoc />
    public Task ReportCursorAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        VersionVector vector,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(vector);
        return ReportCursorCoreAsync(treeName, consumerId, cursor, vector, cancellationToken);
    }

    private Task ReportCursorCoreAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        VersionVector? vector,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);
        if (cursor <= HybridLogicalClock.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(cursor),
                cursor,
                "Cursor reports must be strictly greater than HybridLogicalClock.Zero. A consumer that has not yet "
                + "observed any entries should not register at all; once it consumes at least one non-range-delete "
                + "entry it has a non-zero cursor to report.");
        }
        cancellationToken.ThrowIfCancellationRequested();

        var nowTicks = DateTime.UtcNow.Ticks;
        // Defensive clone: callers may continue to mutate the supplied
        // VersionVector after the report returns (e.g. by ticking it on
        // the next applied entry). Cloning here keeps the registry's
        // copy stable for the lifetime of this consumer's entry.
        var defensiveClone = vector?.Clone();

        lock (_gate)
        {
            if (!_byTree.TryGetValue(treeName, out var state))
            {
                state = new TreeState();
                _byTree[treeName] = state;
            }

            if (state.PerConsumer.TryGetValue(consumerId, out var existing))
            {
                var advancedCursor = existing.Cursor;
                if (cursor > existing.Cursor)
                {
                    advancedCursor = cursor;
                }

                var mergedVector = existing.Vector;
                if (defensiveClone is not null)
                {
                    if (mergedVector is null)
                    {
                        mergedVector = defensiveClone;
                    }
                    else
                    {
                        // Pointwise-max coalescing keeps the registry
                        // monotonically non-decreasing per origin even
                        // when concurrent VC reports race or arrive out
                        // of order. Mutating the cached clone here is
                        // safe because the registry never hands the
                        // same instance to a caller.
                        mergedVector.MergeFrom(defensiveClone);
                    }
                }

                state.PerConsumer[consumerId] = new ReplicationCursorSnapshot(
                    consumerId,
                    advancedCursor,
                    nowTicks,
                    mergedVector);
            }
            else
            {
                state.PerConsumer[consumerId] = new ReplicationCursorSnapshot(
                    consumerId,
                    cursor,
                    nowTicks,
                    defensiveClone);
            }

            state.InvalidateCaches();
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task UnregisterAsync(
        string treeName,
        string consumerId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (_byTree.TryGetValue(treeName, out var state))
            {
                if (state.PerConsumer.Remove(consumerId))
                {
                    state.InvalidateCaches();
                }

                if (state.PerConsumer.Count == 0)
                {
                    _byTree.Remove(treeName);
                }
            }
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<HybridLogicalClock?> GetMinCursorAsync(
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_byTree.TryGetValue(treeName, out var state) || state.PerConsumer.Count == 0)
            {
                return Task.FromResult<HybridLogicalClock?>(null);
            }

            HybridLogicalClock? min = null;
            foreach (var snapshot in state.PerConsumer.Values)
            {
                if (min is null || snapshot.Cursor < min.Value)
                {
                    min = snapshot.Cursor;
                }
            }

            return Task.FromResult(min);
        }
    }

    /// <inheritdoc />
    public Task<VersionVector?> GetCausalStableAsync(
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_byTree.TryGetValue(treeName, out var state) || state.PerConsumer.Count == 0)
            {
                return Task.FromResult<VersionVector?>(null);
            }

            if (state.HasCachedCausalStable)
            {
                // A clone is returned to callers so they cannot mutate
                // the cached frontier and corrupt subsequent reads.
                return Task.FromResult(state.CachedCausalStable?.Clone());
            }

            var computed = ComputeCausalStable(state.PerConsumer.Values);
            state.HasCachedCausalStable = true;
            state.CachedCausalStable = computed;
            return Task.FromResult(computed?.Clone());
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<ReplicationCursorSnapshot>> SnapshotAsync(
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_byTree.TryGetValue(treeName, out var state) || state.PerConsumer.Count == 0)
            {
                return Task.FromResult<IReadOnlyList<ReplicationCursorSnapshot>>(Array.Empty<ReplicationCursorSnapshot>());
            }

            var copy = new ReplicationCursorSnapshot[state.PerConsumer.Count];
            var i = 0;
            foreach (var snapshot in state.PerConsumer.Values)
            {
                // Clone the embedded vector so callers cannot mutate
                // the registry's internal copy.
                copy[i++] = snapshot with { Vector = snapshot.Vector?.Clone() };
            }
            return Task.FromResult<IReadOnlyList<ReplicationCursorSnapshot>>(copy);
        }
    }

    /// <summary>
    /// Computes the pointwise-min <see cref="VersionVector"/> across
    /// every consumer that has reported a non-<see langword="null"/>
    /// vector. An origin is retained only when every reporting
    /// consumer has named it; the value is the smallest
    /// <see cref="HybridLogicalClock"/> across those reports.
    /// Consumers reporting HLC-only are skipped entirely. Returns
    /// <see langword="null"/> when no consumer has reported a vector.
    /// </summary>
    private static VersionVector? ComputeCausalStable(IEnumerable<ReplicationCursorSnapshot> snapshots)
    {
        VersionVector? meet = null;
        var reportingCount = 0;
        foreach (var snapshot in snapshots)
        {
            if (snapshot.Vector is null)
            {
                continue;
            }
            reportingCount++;

            if (meet is null)
            {
                meet = snapshot.Vector.Clone();
                continue;
            }

            // Pointwise-min: keep only origins present in BOTH the
            // running meet and the next consumer's vector, with the
            // smaller HLC at each origin. Origins absent from either
            // side are dropped because we cannot prove every consumer
            // has observed them.
            var next = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
            foreach (var (origin, lhsClock) in meet.Entries)
            {
                if (snapshot.Vector.Entries.TryGetValue(origin, out var rhsClock))
                {
                    next[origin] = lhsClock < rhsClock ? lhsClock : rhsClock;
                }
            }

            meet.Entries = next;
        }

        return reportingCount == 0 ? null : meet;
    }

    /// <summary>
    /// Per-tree mutable state held under <see cref="_gate"/>. The
    /// causal-stable cache is invalidated by
    /// <see cref="InvalidateCaches"/> on every accepted mutation so a
    /// stable registry serves <see cref="GetCausalStableAsync"/> in
    /// O(1).
    /// </summary>
    private sealed class TreeState
    {
        public Dictionary<string, ReplicationCursorSnapshot> PerConsumer { get; } =
            new(StringComparer.Ordinal);

        public bool HasCachedCausalStable { get; set; }

        public VersionVector? CachedCausalStable { get; set; }

        public void InvalidateCaches()
        {
            HasCachedCausalStable = false;
            CachedCausalStable = null;
        }
    }
}
