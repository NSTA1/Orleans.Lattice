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
/// </summary>
public sealed class InMemoryReplicationCursorRegistry : ILatticeReplicationCursorRegistry
{
    private readonly object _gate = new();
    private readonly Dictionary<string, Dictionary<string, ReplicationCursorSnapshot>> _byTree = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public Task ReportCursorAsync(
        string treeName,
        string consumerId,
        HybridLogicalClock cursor,
        CancellationToken cancellationToken = default)
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
        lock (_gate)
        {
            if (!_byTree.TryGetValue(treeName, out var perConsumer))
            {
                perConsumer = new Dictionary<string, ReplicationCursorSnapshot>(StringComparer.Ordinal);
                _byTree[treeName] = perConsumer;
            }

            if (perConsumer.TryGetValue(consumerId, out var existing) && existing.Cursor >= cursor)
            {
                // Coalesce: the registry is monotonically non-decreasing
                // per (tree, consumer). A late-arriving report with a
                // stale cursor is silently dropped rather than rolling
                // the consumer backwards (which would un-trim entries
                // a later GC pass already considered safe to trim).
                perConsumer[consumerId] = existing with { LastReportedAtTicks = nowTicks };
                return Task.CompletedTask;
            }

            perConsumer[consumerId] = new ReplicationCursorSnapshot(consumerId, cursor, nowTicks);
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
            if (_byTree.TryGetValue(treeName, out var perConsumer))
            {
                perConsumer.Remove(consumerId);
                if (perConsumer.Count == 0)
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
            if (!_byTree.TryGetValue(treeName, out var perConsumer) || perConsumer.Count == 0)
            {
                return Task.FromResult<HybridLogicalClock?>(null);
            }

            HybridLogicalClock? min = null;
            foreach (var snapshot in perConsumer.Values)
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
    public Task<IReadOnlyList<ReplicationCursorSnapshot>> SnapshotAsync(
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_byTree.TryGetValue(treeName, out var perConsumer) || perConsumer.Count == 0)
            {
                return Task.FromResult<IReadOnlyList<ReplicationCursorSnapshot>>(Array.Empty<ReplicationCursorSnapshot>());
            }

            var copy = new ReplicationCursorSnapshot[perConsumer.Count];
            var i = 0;
            foreach (var snapshot in perConsumer.Values)
            {
                copy[i++] = snapshot;
            }
            return Task.FromResult<IReadOnlyList<ReplicationCursorSnapshot>>(copy);
        }
    }
}
