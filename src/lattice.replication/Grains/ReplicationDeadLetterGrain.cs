using System.Globalization;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree dead-letter queue grain. See
/// <see cref="IReplicationDeadLetterGrain"/> for the contract.
/// <para>
/// Storage is delegated to a reserved system tree named
/// <c>_lattice_replog_dlq_{treeId}</c> resolved through the internal
/// <see cref="ISystemLattice"/> surface, so the queue inherits the
/// scaling, sharding, and persistence of the core B+ tree rather than
/// living inside a single grain''s persistent-state row. This avoids
/// the storage-row size limit a List-in-state design would hit under
/// sustained apply failure.
/// </para>
/// <para>
/// On activation the grain bulk-loads every parked entry into an
/// in-memory cache; subsequent reads (List / Count / TryGet) are
/// served from memory and writes (Enqueue / Discard) are applied to
/// the cache and written through to the system tree. The bound on
/// cache size is <see cref="LatticeReplicationOptions.DeadLetterQueueCapacity"/>,
/// which the validator pins to a positive value.
/// </para>
/// </summary>
internal sealed class ReplicationDeadLetterGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
    Serializer<DeadLetterEntry> serializer) : IReplicationDeadLetterGrain, IGrainBase
{
    /// <summary>Width of the zero-padded entry-id segment in stored keys (matches the WAL row-key style).</summary>
    private const int EntryIdWidth = 19;

    /// <summary>Inclusive prefix every parked-entry key carries inside the system tree.</summary>
    private const string EntryKeyPrefix = "e/";

    /// <summary>Exclusive end key for a prefix range scan over <see cref="EntryKeyPrefix"/>.</summary>
    private const string EntryKeyPrefixEnd = "e0"; // "e/" < "e0" lexicographically; ASCII '/' (0x2F) < '0' (0x30).

    private string _treeId = "";
    private ISystemLattice _store = null!;
    private readonly List<DeadLetterEntry> _cache = new();
    private long _nextEntryId = 1;
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
        _store = grainFactory.GetGrain<ISystemLattice>(BackingTreeId(_treeId));

        // Bulk-load existing parked entries into the in-memory cache.
        // The DLQ is bounded by DeadLetterQueueCapacity (default 1000),
        // so the load is bounded and a one-shot pass is acceptable.
        await foreach (var kvp in _store.EntriesAsync(
            startInclusive: EntryKeyPrefix,
            endExclusive: EntryKeyPrefixEnd,
            cancellationToken: cancellationToken).ConfigureAwait(true))
        {
            if (!TryParseEntryId(kvp.Key, out var entryId))
            {
                // Defensive: an unrecognised key under our prefix is
                // skipped rather than crashing activation.
                continue;
            }

            var parked = serializer.Deserialize(kvp.Value);
            _cache.Add(parked);
            if (entryId >= _nextEntryId)
            {
                _nextEntryId = entryId + 1;
            }
        }

        _cache.Sort(static (a, b) => a.EntryId.CompareTo(b.EntryId));
        _initialized = true;
    }

    /// <summary>
    /// Test-only initialisation seam. Bypasses Orleans activation by
    /// supplying the tree id and pre-bound <see cref="ISystemLattice"/>
    /// store directly, then running the same bulk-load logic
    /// <see cref="OnActivateAsync(CancellationToken)"/> uses. Tests
    /// that exercise the grain in isolation use this in lieu of the
    /// activation lifecycle.
    /// </summary>
    internal async Task InitializeForTestingAsync(
        string treeId,
        ISystemLattice store,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(store);

        _treeId = treeId;
        _store = store;
        _cache.Clear();
        _nextEntryId = 1;

        await foreach (var kvp in store.EntriesAsync(
            startInclusive: EntryKeyPrefix,
            endExclusive: EntryKeyPrefixEnd,
            cancellationToken: cancellationToken).ConfigureAwait(true))
        {
            if (!TryParseEntryId(kvp.Key, out var entryId))
            {
                continue;
            }

            var parked = serializer.Deserialize(kvp.Value);
            _cache.Add(parked);
            if (entryId >= _nextEntryId)
            {
                _nextEntryId = entryId + 1;
            }
        }

        _cache.Sort(static (a, b) => a.EntryId.CompareTo(b.EntryId));
        _initialized = true;
    }

    /// <inheritdoc />
    public async Task<long> EnqueueAsync(
        ReplogEntry entry,
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

        // FIFO eviction: trim from the head until strictly below capacity,
        // then append. Capacity >= 1 is enforced by the options validator.
        while (_cache.Count >= capacity)
        {
            var oldest = _cache[0];
            await _store.DeleteAsync(EntryKey(oldest.EntryId), cancellationToken).ConfigureAwait(true);
            _cache.RemoveAt(0);
            LatticeReplicationMetrics.DeadLetterRemoved.Add(
                1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, LatticeReplicationMetrics.ReasonEvicted));
        }

        var assigned = _nextEntryId;
        var parked = new DeadLetterEntry
        {
            EntryId = assigned,
            Entry = entry,
            FailureReason = failureReason,
            RetryCount = retryCount,
            EnqueuedAtTicks = DateTime.UtcNow.Ticks,
        };

        var encoded = serializer.SerializeToArray(parked);
        await _store.SetAsync(EntryKey(assigned), encoded, cancellationToken).ConfigureAwait(true);

        _cache.Add(parked);
        _nextEntryId = checked(assigned + 1);

        LatticeReplicationMetrics.DeadLetterEnqueued.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reasonTag));

        return assigned;
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<DeadLetterEntry>> ListAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        IReadOnlyList<DeadLetterEntry> snapshot = _cache.ToArray();
        return Task.FromResult(snapshot);
    }

    /// <inheritdoc />
    public Task<int> CountAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        return Task.FromResult(_cache.Count);
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

        var index = IndexOf(entryId);
        DeadLetterEntry? result = index < 0 ? null : _cache[index];
        return Task.FromResult(result);
    }

    /// <summary>
    /// Internal removal helper used by both <see cref="DiscardAsync"/>
    /// and the post-replay cleanup path on
    /// <see cref="ILatticeReplicationDeadLetters.ReplayAsync(string, long, CancellationToken)"/>.
    /// The reason tag distinguishes the two callers in the
    /// <c>dead_letter.removed</c> counter.
    /// </summary>
    internal async Task<bool> RemoveAsync(long entryId, string reason, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        var index = IndexOf(entryId);
        if (index < 0)
        {
            return false;
        }

        await _store.DeleteAsync(EntryKey(entryId), cancellationToken).ConfigureAwait(true);
        _cache.RemoveAt(index);

        LatticeReplicationMetrics.DeadLetterRemoved.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reason));

        return true;
    }

    private int IndexOf(long entryId)
    {
        for (var i = 0; i < _cache.Count; i++)
        {
            if (_cache[i].EntryId == entryId)
            {
                return i;
            }
        }
        return -1;
    }

    private void EnsureInitialized()
    {
        if (!_initialized)
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationDeadLetterGrain)} for tree ''{_treeId}'' has not completed activation.");
        }
    }

    /// <summary>
    /// Composes the system-tree id used to back the dead-letter queue
    /// for <paramref name="treeId"/>. Lives inside the reserved
    /// <c>_lattice_replog_</c> namespace so user trees cannot collide
    /// with it.
    /// </summary>
    internal static string BackingTreeId(string treeId) => $"{LatticeConstants.ReplogTreePrefix}dlq_{treeId}";

    /// <summary>
    /// Builds the system-tree key for the parked entry with the supplied
    /// id. Uses <see cref="string.Create{TState}(int, TState, System.Buffers.SpanAction{char, TState})"/>
    /// to produce the <c>"e/" + 19-digit-id</c> row key in a single
    /// allocation, avoiding the intermediate <see cref="long.ToString(string, IFormatProvider)"/>
    /// + concat that an interpolated <c>$"e/{id:D19}"</c> would emit.
    /// Called per <see cref="EnqueueAsync(ReplogEntry, string, int, CancellationToken)"/>
    /// (terminal failure path) and per FIFO eviction, so the saving is
    /// modest but non-zero.
    /// </summary>
    internal static string EntryKey(long entryId) =>
        string.Create(
            EntryKeyPrefix.Length + EntryIdWidth,
            entryId,
            static (span, id) =>
            {
                span[0] = 'e';
                span[1] = '/';
                var ok = id.TryFormat(span[EntryKeyPrefix.Length..], out var written, "D" + EntryIdWidth, CultureInfo.InvariantCulture);
                // The buffer is sized to EntryKeyPrefix.Length + EntryIdWidth
                // and the "D19" format produces exactly 19 characters for any
                // non-negative long, so TryFormat must succeed and fill the tail.
                if (!ok || written != EntryIdWidth)
                {
                    throw new InvalidOperationException(
                        "EntryKey formatting produced an unexpected width; entry-id width contract violated.");
                }
            });

    private static bool TryParseEntryId(string storedKey, out long entryId)
    {
        if (storedKey is null || !storedKey.StartsWith(EntryKeyPrefix, StringComparison.Ordinal))
        {
            entryId = 0;
            return false;
        }
        return long.TryParse(
            storedKey.AsSpan(EntryKeyPrefix.Length),
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out entryId);
    }
}

