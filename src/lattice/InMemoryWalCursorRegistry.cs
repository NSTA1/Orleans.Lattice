using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Default in-memory <see cref="IWalCursorRegistry"/>
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
/// <see cref="LatticeOptions.WalRetention"/> hard ceiling.
/// This matches the "fall-off-the-log" detection seam later phases use
/// to trigger auto-bootstrap.
/// </para>
/// <para>
/// The causal-stable frontier returned by
/// <see cref="GetCausalStableAsync"/> and the blocked-floor
/// returned by <see cref="GetBlockedFloorAsync"/> are both cached
/// behind a per-tree generation counter that bumps on every accepted
/// mutation. A GC pass that observes a stable registry therefore
/// reads each cached value in O(1); a recompute only happens after a
/// consumer reports or unregisters.
/// </para>
/// </summary>
public sealed class InMemoryWalCursorRegistry : IWalCursorRegistry
{
    private readonly object _gate = new();
    private readonly Dictionary<string, TreeState> _byTree = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public Task ReportCursorAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        CancellationToken cancellationToken = default)
        => ReportCursorCoreAsync(
            treeName, consumerId, cursor, vector: null,
            blockedAtHlc: null, blockedAtHlcSpecified: false, cancellationToken);

    /// <inheritdoc />
    public Task ReportCursorAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        HybridLogicalClock? blockedAtHlc,
        CancellationToken cancellationToken = default)
        => ReportCursorCoreAsync(
            treeName, consumerId, cursor, vector: null,
            blockedAtHlc, blockedAtHlcSpecified: true, cancellationToken);

    /// <inheritdoc />
    public Task ReportCursorAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        VersionVector vector,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(vector);
        return ReportCursorCoreAsync(
            treeName, consumerId, cursor, vector,
            blockedAtHlc: null, blockedAtHlcSpecified: false, cancellationToken);
    }

    /// <inheritdoc />
    public Task ReportCursorAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        VersionVector vector,
        HybridLogicalClock? blockedAtHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(vector);
        return ReportCursorCoreAsync(
            treeName, consumerId, cursor, vector,
            blockedAtHlc, blockedAtHlcSpecified: true, cancellationToken);
    }

    private Task ReportCursorCoreAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        VersionVector? vector,
        HybridLogicalClock? blockedAtHlc,
        bool blockedAtHlcSpecified,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);
        // The blocked-floor overloads relax the cursor precondition
        // to allow HybridLogicalClock.Zero so a blocked-floor-only
        // consumer (typically the receiver-side applier) can register
        // without polluting the GC's HLC min(cursor) branch. Strictly
        // negative cursors remain rejected; legacy overloads that do
        // not pass a blocked-floor still require cursor > Zero.
        if (cursor < HybridLogicalClock.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(cursor),
                cursor,
                "Cursor reports must not be negative.");
        }
        if (!blockedAtHlcSpecified && cursor <= HybridLogicalClock.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(cursor),
                cursor,
                "Cursor reports must be strictly greater than HybridLogicalClock.Zero. A consumer that has not yet "
                + "observed any entries should not register at all; once it consumes at least one non-range-delete "
                + "entry it has a non-zero cursor to report. Use the blocked-floor overload to register a "
                + "buffer-pin-only consumer with cursor=Zero.");
        }
        if (blockedAtHlc is { } pin && pin < HybridLogicalClock.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(blockedAtHlc),
                pin,
                "Blocked-floor reports must not be negative; pass null to clear the pin.");
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

                // Blocked-floor uses replace semantics. The
                // consumer is the authority on its own buffer pin and
                // must be able to clear it (transition to null) when
                // the buffer drains. Untouched when the caller did not
                // pass the parameter (legacy overload).
                var nextBlockedAtHlc = existing.BlockedAtHlc;
                if (blockedAtHlcSpecified)
                {
                    nextBlockedAtHlc = blockedAtHlc;
                }

                // Decide whether the report changed anything observable
                // BEFORE writing back so a no-op re-report (same cursor,
                // no new vector, no blocked-floor delta) preserves the
                // memoised causal-stable / blocked-floor caches. The
                // alternative - invalidating unconditionally - paid an
                // O(consumers) recompute on the next GC pass for every
                // idempotent ping the leaf reporter coalesces.
                var cursorAdvanced = advancedCursor > existing.Cursor;
                var vectorReported = defensiveClone is not null;
                var blockedFloorChanged = blockedAtHlcSpecified
                    && !Nullable.Equals(nextBlockedAtHlc, existing.BlockedAtHlc);
                var shouldInvalidate = cursorAdvanced || vectorReported || blockedFloorChanged;

                state.PerConsumer[consumerId] = new WalCursorSnapshot(
                    consumerId,
                    advancedCursor,
                    nowTicks,
                    mergedVector,
                    nextBlockedAtHlc);

                if (shouldInvalidate)
                {
                    state.InvalidateCaches();
                }
            }
            else
            {
                state.PerConsumer[consumerId] = new WalCursorSnapshot(
                    consumerId,
                    cursor,
                    nowTicks,
                    defensiveClone,
                    blockedAtHlcSpecified ? blockedAtHlc : null);

                // A new consumer always affects the meet - its absence
                // was the previous floor. Invalidate unconditionally.
                state.InvalidateCaches();
            }
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
                // Skip blocked-floor-only consumers (registered
                // with cursor=Zero) so a buffer pin does not disable
                // the cursor branch of the GC predicate.
                if (snapshot.Cursor <= HybridLogicalClock.Zero)
                {
                    continue;
                }
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
    public Task<HybridLogicalClock?> GetBlockedFloorAsync(
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

            if (state.HasCachedBlockedFloor)
            {
                return Task.FromResult(state.CachedBlockedFloor);
            }

            var computed = ComputeBlockedFloor(state.PerConsumer.Values);
            state.HasCachedBlockedFloor = true;
            state.CachedBlockedFloor = computed;
            return Task.FromResult(computed);
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<WalCursorSnapshot>> SnapshotAsync(
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_byTree.TryGetValue(treeName, out var state) || state.PerConsumer.Count == 0)
            {
                return Task.FromResult<IReadOnlyList<WalCursorSnapshot>>(Array.Empty<WalCursorSnapshot>());
            }

            var copy = new WalCursorSnapshot[state.PerConsumer.Count];
            var i = 0;
            foreach (var snapshot in state.PerConsumer.Values)
            {
                // Clone the embedded vector so callers cannot mutate
                // the registry's internal copy.
                copy[i++] = snapshot with { Vector = snapshot.Vector?.Clone() };
            }
            return Task.FromResult<IReadOnlyList<WalCursorSnapshot>>(copy);
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
    private static VersionVector? ComputeCausalStable(IEnumerable<WalCursorSnapshot> snapshots)
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
    /// Computes the pointwise-min <see cref="HybridLogicalClock"/>
    /// across every consumer that has reported a non-<see langword="null"/>
    /// blocked-floor pin. Consumers reporting <see langword="null"/>
    /// (the majority - leaf materialisers, peer ship loops) are
    /// skipped. Returns <see langword="null"/> when no consumer
    /// currently reports a buffer pin.
    /// </summary>
    private static HybridLogicalClock? ComputeBlockedFloor(IEnumerable<WalCursorSnapshot> snapshots)
    {
        HybridLogicalClock? min = null;
        foreach (var snapshot in snapshots)
        {
            min = WalBlockedFloorCore.Meet(min, snapshot.BlockedAtHlc);
        }
        return min;
    }

    /// <summary>
    /// Per-tree mutable state held under <see cref="_gate"/>. The
    /// causal-stable and blocked-floor caches are invalidated by
    /// <see cref="InvalidateCaches"/> on every accepted mutation so a
    /// stable registry serves <see cref="GetCausalStableAsync"/> and
    /// <see cref="GetBlockedFloorAsync"/> in O(1).
    /// </summary>
    private sealed class TreeState
    {
        public Dictionary<string, WalCursorSnapshot> PerConsumer { get; } =
            new(StringComparer.Ordinal);

        public bool HasCachedCausalStable { get; set; }

        public VersionVector? CachedCausalStable { get; set; }

        public bool HasCachedBlockedFloor { get; set; }

        public HybridLogicalClock? CachedBlockedFloor { get; set; }

        public void InvalidateCaches()
        {
            HasCachedCausalStable = false;
            CachedCausalStable = null;
            HasCachedBlockedFloor = false;
            CachedBlockedFloor = null;
        }
    }
}
