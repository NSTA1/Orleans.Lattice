using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree dead-letter queue grain. See
/// <see cref="IReplicationDeadLetterGrain"/> for the contract.
/// <para>
/// Storage, caching and monotonic-id assignment are
/// delegated to the shared <see cref="LatticeQueueCore"/> engine bound to a
/// reserved system tree named <c>_lattice_replog_dlq_{treeId}</c>
/// (<see cref="LatticeConstants.WalTreePrefix"/> + <c>dlq_</c>). The grain
/// is a thin specialisation: it parks <see cref="DeadLetterEntry"/> payloads
/// serialized through the Orleans binary <see cref="Serializer{T}"/>, tags
/// the <c>dead_letter.removed</c> counter with the appropriate reason, and
/// pins the engine to its historical <c>e/</c> row-key scheme with the
/// head-cursor row disabled so the on-disk format is preserved byte-for-byte
/// across upgrades.
/// </para>
/// <para>
/// The queue never evicts (#4603). Every parked entry was acknowledged without
/// being applied, so it is the only copy of its write: a full queue refuses the
/// next enqueue with <see cref="ReplicationDeadLetterQueueFullException"/> and
/// the caller keeps that entry unacknowledged instead. Enqueue is idempotent by
/// the entry's <c>(origin, timestamp, key, op)</c> identity, so a re-shipped
/// copy of an already-parked entry does not take a second slot. Discarding a
/// foreign-origin entry records its identity as lost on the tree's
/// high-water-mark grain before the row is deleted, so an entry that depends on
/// it is never released.
/// </para>
/// </summary>
internal sealed class ReplicationDeadLetterGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
    Serializer<DeadLetterEntry> serializer) : IReplicationDeadLetterGrain, IGrainBase
{
    /// <summary>Inclusive prefix every parked-entry key carries inside the system tree.</summary>
    private const string EntryKeyPrefix = "e/";

    private string _treeId = "";
    private LatticeQueueCore _core = null!;
    private bool _initialized;

    /// <summary>
    /// Identity of every parked entry, keyed by the entry's identity and mapped
    /// to its queue id, so an enqueue is idempotent without deserializing the
    /// queue. Rebuilt from the queue on activation.
    /// </summary>
    private readonly Dictionary<ParkedIdentity, long> _parked = new();

    /// <inheritdoc />
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var key = context.GrainId.Key.ToString();
        if (string.IsNullOrEmpty(key))
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationDeadLetterGrain)} activation key is empty; expected the replicated tree id.");
        }

        _treeId = key;
        var store = grainFactory.GetGrain<ISystemLattice>(BackingTreeId(_treeId));
        _core = CreateCore(store);
        await _core.InitializeAsync(cancellationToken).ConfigureAwait(true);
        RebuildParkedIdentities();
        _initialized = true;
    }

    /// <summary>
    /// Test-only initialisation seam. Bypasses Orleans activation by
    /// supplying the tree id and a pre-bound <see cref="ISystemLattice"/>
    /// store, then runs the same bulk-load
    /// <see cref="OnActivateAsync(CancellationToken)"/> uses.
    /// </summary>
    internal async Task InitializeForTestingAsync(
        string treeId,
        ISystemLattice store,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(store);

        _treeId = treeId;
        _core = CreateCore(store);
        await _core.InitializeAsync(cancellationToken).ConfigureAwait(true);
        RebuildParkedIdentities();
        _initialized = true;
    }

    private static LatticeQueueCore CreateCore(ISystemLattice store) =>
        new(store, EntryKeyPrefix, persistHeadCursor: false);

    private void RebuildParkedIdentities()
    {
        _parked.Clear();
        foreach (var (id, bytes) in _core.Snapshot())
        {
            _parked[ParkedIdentity.From(serializer.Deserialize(bytes).Entry)] = id;
        }
    }

    /// <inheritdoc />
    public async Task<long> EnqueueAsync(
        WalRecord entry,
        string failureReason,
        int retryCount,
        string reasonTag,
        CancellationToken cancellationToken,
        ReplicationSourceLineageStamp? sourceLineage = null)
    {
        ArgumentNullException.ThrowIfNull(failureReason);
        ArgumentException.ThrowIfNullOrEmpty(reasonTag);
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        var identity = ParkedIdentity.From(entry);
        if (_parked.TryGetValue(identity, out var existing))
        {
            // A re-shipped copy of an entry already parked (the first delivery was
            // parked but its batch was deferred for another entry). One slot each.
            // Publish again: the first enqueue's publication may have failed.
            await PublishHeldAsync(entry.OriginClusterId, cancellationToken).ConfigureAwait(true);
            return existing;
        }

        var capacity = optionsMonitor.Get(_treeId).DeadLetterQueueCapacity;
        if (_core.Count >= capacity)
        {
            LatticeReplicationMetrics.DeadLetterRefused.Add(
                1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reasonTag),
                LatticeTenantLabel.ForTree(_treeId));
            throw new ReplicationDeadLetterQueueFullException(_treeId, capacity);
        }

        var enqueuedAtTicks = DateTime.UtcNow.Ticks;

        var assigned = await _core.EnqueueAsync(
            id => serializer.SerializeToArray(new DeadLetterEntry
            {
                EntryId = id,
                Entry = entry,
                FailureReason = failureReason,
                RetryCount = retryCount,
                EnqueuedAtTicks = enqueuedAtTicks,
                SourceLineageClusterId = sourceLineage?.SourceClusterId,
                SourceLineage = sourceLineage?.Lineage,
            }),
            capacity: null,
            cancellationToken).ConfigureAwait(true);
        _parked[identity] = assigned;

        // Issue #4586: the origin's frontier must list the write as held before
        // the caller acknowledges it, or a dependent could be released while the
        // write sits here unapplied.
        await PublishHeldAsync(entry.OriginClusterId, cancellationToken).ConfigureAwait(true);

        LatticeReplicationMetrics.DeadLetterEnqueued.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reasonTag),
            LatticeTenantLabel.ForTree(_treeId));

        return assigned;
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<DeadLetterEntry>> ListAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        var snapshot = _core.Snapshot();
        var result = new DeadLetterEntry[snapshot.Count];
        for (var i = 0; i < snapshot.Count; i++)
        {
            result[i] = serializer.Deserialize(snapshot[i].Value);
        }
        return Task.FromResult<IReadOnlyList<DeadLetterEntry>>(result);
    }

    /// <inheritdoc />
    public Task<int> CountAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        return Task.FromResult(_core.Count);
    }

    /// <inheritdoc />
    public async Task<bool> DiscardAsync(long entryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        var bytes = _core.TryGet(entryId);
        if (bytes is null)
        {
            return false;
        }

        // A discarded foreign-origin entry was acknowledged to its sender and is
        // never applied: its write is lost on this cluster for good. Record that
        // durably first, so an entry that depends on it is never released (#4603).
        // A local-origin entry is one the outbound shipper parked; it was never
        // applied here in the first place and is not a receiver-side loss.
        var entry = serializer.Deserialize(bytes).Entry;
        var localClusterId = optionsMonitor.Get(_treeId).ClusterId;
        if (!string.IsNullOrEmpty(entry.OriginClusterId)
            && !string.Equals(entry.OriginClusterId, localClusterId, StringComparison.Ordinal))
        {
            await grainFactory.GetGrain<IReplicationOriginFrontierGrain>(entry.OriginClusterId)
                .RecordLostAsync([entry.Timestamp], cancellationToken)
                .ConfigureAwait(true);
        }

        return await RemoveAsync(entryId, LatticeReplicationMetrics.ReasonDiscarded, cancellationToken).ConfigureAwait(true);
    }

    /// <inheritdoc />
    public Task<bool> RemoveReplayedAsync(long entryId, CancellationToken cancellationToken) =>
        RemoveAsync(entryId, LatticeReplicationMetrics.ReasonReplayed, cancellationToken);

    /// <inheritdoc />
    public Task<DeadLetterEntry?> TryGetAsync(long entryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        var bytes = _core.TryGet(entryId);
        DeadLetterEntry? result = bytes is null ? null : serializer.Deserialize(bytes);
        return Task.FromResult(result);
    }

    /// <summary>
    /// Internal removal helper used by both <see cref="DiscardAsync"/>
    /// and the post-replay cleanup path. The reason tag distinguishes the
    /// two callers in the <c>dead_letter.removed</c> counter; the counter is
    /// only emitted when an entry was actually removed.
    /// </summary>
    internal async Task<bool> RemoveAsync(long entryId, string reason, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        var bytes = _core.TryGet(entryId);
        var removed = await _core.RemoveAsync(entryId, cancellationToken).ConfigureAwait(true);
        if (!removed)
        {
            return false;
        }

        if (bytes is not null)
        {
            var removedEntry = serializer.Deserialize(bytes).Entry;
            _parked.Remove(ParkedIdentity.From(removedEntry));

            // Best effort: a write still listed after its removal only delays a
            // dependent until the origin frontier confirms it with this queue.
            try
            {
                await PublishHeldAsync(removedEntry.OriginClusterId, cancellationToken).ConfigureAwait(true);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
            }
        }

        LatticeReplicationMetrics.DeadLetterRemoved.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reason),
            LatticeTenantLabel.ForTree(_treeId));

        return true;
    }

    /// <inheritdoc />
    public Task<bool> IsHoldingAsync(string originClusterId, HybridLogicalClock timestamp, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        foreach (var identity in _parked.Keys)
        {
            if (identity.Timestamp == timestamp
                && string.Equals(identity.OriginClusterId, originClusterId, StringComparison.Ordinal))
            {
                return Task.FromResult(true);
            }
        }

        return Task.FromResult(false);
    }

    /// <summary>
    /// Publishes every write of <paramref name="originClusterId"/> this queue
    /// holds to the origin's frontier (issue #4586). Skipped for a local-origin
    /// entry - one the outbound shipper parked - which no dependency check here
    /// ever names.
    /// </summary>
    private Task PublishHeldAsync(string? originClusterId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(originClusterId)
            || string.Equals(originClusterId, optionsMonitor.Get(_treeId).ClusterId, StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        var held = new HashSet<HybridLogicalClock>();
        foreach (var identity in _parked.Keys)
        {
            if (string.Equals(identity.OriginClusterId, originClusterId, StringComparison.Ordinal))
            {
                held.Add(identity.Timestamp);
            }
        }

        return grainFactory.GetGrain<IReplicationOriginFrontierGrain>(originClusterId)
            .SetHeldAsync(ReplicationOriginFrontierGrain.DeadLetterSource(_treeId), held, cancellationToken);
    }

    /// <summary>The identity that makes a dead-letter enqueue idempotent.</summary>
    private readonly record struct ParkedIdentity(
        string OriginClusterId,
        HybridLogicalClock Timestamp,
        string Key,
        string? EndExclusiveKey,
        MutationKind Op,
        Guid TransactionId)
    {
        public static ParkedIdentity From(WalRecord entry) =>
            new(
                entry.OriginClusterId ?? string.Empty,
                entry.Timestamp,
                entry.Key ?? string.Empty,
                entry.EndExclusiveKey,
                entry.Op,
                entry.TransactionId);
    }

    private void EnsureInitialized()
    {
        if (!_initialized)
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationDeadLetterGrain)} for tree '{_treeId}' has not completed activation.");
        }
    }

    /// <summary>
    /// Composes the system-tree id used to back the dead-letter queue
    /// for <paramref name="treeId"/>. Lives inside the reserved
    /// <c>_lattice_replog_</c> namespace so user trees cannot collide
    /// with it.
    /// </summary>
    internal static string BackingTreeId(string treeId) => $"{LatticeConstants.WalTreePrefix}dlq_{treeId}";

    /// <summary>
    /// Builds the system-tree key for the parked entry with the supplied id
    /// (<c>"e/" + 19-digit-id</c>). Delegates to the shared queue engine so
    /// the row-key scheme stays identical to the generic queue primitive.
    /// </summary>
    internal static string EntryKey(long entryId) => LatticeQueueCore.FormatEntryKey(EntryKeyPrefix, entryId);
}
