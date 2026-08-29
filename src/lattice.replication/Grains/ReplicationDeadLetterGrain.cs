using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree dead-letter queue grain. See
/// <see cref="IReplicationDeadLetterGrain"/> for the contract.
/// <para>
/// Storage, caching, monotonic-id assignment, and FIFO eviction are
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
        _initialized = true;
    }

    private LatticeQueueCore CreateCore(ISystemLattice store) =>
        new(store, EntryKeyPrefix, persistHeadCursor: false, onEvicted: OnEvicted);

    /// <inheritdoc />
    public async Task<long> EnqueueAsync(
        WalRecord entry,
        string failureReason,
        int retryCount,
        string reasonTag,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(failureReason);
        ArgumentException.ThrowIfNullOrEmpty(reasonTag);
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        var capacity = optionsMonitor.Get(_treeId).DeadLetterQueueCapacity;
        var enqueuedAtTicks = DateTime.UtcNow.Ticks;

        var assigned = await _core.EnqueueAsync(
            id => serializer.SerializeToArray(new DeadLetterEntry
            {
                EntryId = id,
                Entry = entry,
                FailureReason = failureReason,
                RetryCount = retryCount,
                EnqueuedAtTicks = enqueuedAtTicks,
            }),
            capacity,
            cancellationToken).ConfigureAwait(true);

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
    public Task<bool> DiscardAsync(long entryId, CancellationToken cancellationToken) =>
        RemoveAsync(entryId, LatticeReplicationMetrics.ReasonDiscarded, cancellationToken);

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

        var removed = await _core.RemoveAsync(entryId, cancellationToken).ConfigureAwait(true);
        if (!removed)
        {
            return false;
        }

        LatticeReplicationMetrics.DeadLetterRemoved.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reason),
            LatticeTenantLabel.ForTree(_treeId));

        return true;
    }

    private void OnEvicted(long entryId) =>
        LatticeReplicationMetrics.DeadLetterRemoved.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, LatticeReplicationMetrics.ReasonEvicted),
            LatticeTenantLabel.ForTree(_treeId));

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
