using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-shard write-ahead-log grain. Stores every captured
/// <see cref="ReplogEntry"/> destined for downstream shippers in a
/// monotonically-sequenced, append-only log via the configured
/// <see cref="IWalStorageProvider"/>. The append is the commit point -
/// a WAL failure surfaces to the originating writer rather than being
/// silently dropped.
/// <para>
/// Grain key format: <c>{treeId}/{partition}</c>. The
/// <see cref="ShardedReplogSink"/> producer hashes
/// <see cref="ReplogEntry.Key"/> modulo
/// <see cref="LatticeReplicationOptions.ReplogPartitions"/> to pick
/// the partition.
/// </para>
/// <para>
/// Implements the turn-safe batching protocol from the WAL design doc
/// (§4): callers receive a per-call <see cref="TaskCompletionSource{TResult}"/>
/// that completes once the containing batch is durably persisted by the
/// configured <see cref="IWalStorageProvider"/>. Batch limits
/// (<see cref="LatticeReplicationOptions.WalMaxBatchEntries"/> and
/// <see cref="LatticeReplicationOptions.WalMaxBatchBytes"/>) flush the
/// current batch before enqueueing an entry that would overflow it; a
/// single in-flight flush at a time keeps the protocol simple in v1.
/// On flush failure the affected batch's offsets are rolled back and
/// every TCS in the failed batch (and the currently-accumulating
/// pending batch, whose offsets are now stale) is faulted with the
/// underlying storage exception so the originating writers see the
/// failure inline.
/// </para>
/// </summary>
internal sealed class ReplogShardGrain(
    IGrainContext context,
    IServiceProvider services,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor) : IReplogShardGrain, IGrainBase
{
    /// <summary>Per-entry serialised-size estimate overhead in bytes (envelope + HLC + origin id + slot tags).</summary>
    private const int EntrySizeOverhead = 128;

    private string _treeId = "";
    private int _shardIndex;
    private IWalStorageProvider _provider = null!;
    private LatticeReplicationOptions _options = null!;
    private long _nextOffset;
    private bool _initialized;

    private List<WalEntry> _pendingBatch = new();
    private List<TaskCompletionSource<long>> _pendingAcks = new();
    private long _pendingBatchSizeBytes;
    private Task? _inFlightFlush;

    /// <inheritdoc />
    IGrainContext IGrainBase.GrainContext => context;

    /// <summary>
    /// Recovers <c>_nextOffset</c> from the configured
    /// <see cref="IWalStorageProvider"/> on activation. The contract
    /// requires offsets to be dense and gap-free, so
    /// <c>_nextOffset = GetHighestOffsetAsync() + 1</c> is sufficient.
    /// </summary>
    public async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var key = context.GrainId.Key.ToString();
        if (string.IsNullOrEmpty(key))
        {
            throw new InvalidOperationException(
                $"{nameof(ReplogShardGrain)} activation key is empty; expected '{{treeId}}/{{partition}}'.");
        }

        var slash = key.LastIndexOf('/');
        if (slash <= 0 || slash >= key.Length - 1)
        {
            throw new InvalidOperationException(
                $"{nameof(ReplogShardGrain)} activation key '{key}' is not in the expected '{{treeId}}/{{partition}}' format.");
        }

        _treeId = key[..slash];
        if (!int.TryParse(key.AsSpan(slash + 1), out _shardIndex) || _shardIndex < 0)
        {
            throw new InvalidOperationException(
                $"{nameof(ReplogShardGrain)} activation key '{key}' has a non-integer or negative shard index suffix.");
        }

        _options = optionsMonitor.Get(_treeId);
        _provider = _options.WalStorageProvider?.Invoke(_treeId)
            ?? services.GetRequiredService<IWalStorageProvider>();
        var highest = await _provider.GetHighestOffsetAsync(_treeId, _shardIndex, cancellationToken).ConfigureAwait(true);
        _nextOffset = highest + 1;
        _initialized = true;
    }

    /// <summary>
    /// Drains the in-flight flush and any pending batch before
    /// returning, so a graceful deactivation never leaves callers
    /// observing a hung <see cref="TaskCompletionSource{TResult}"/>
    ///.
    /// </summary>
    public async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        if (_inFlightFlush is { } running)
        {
            try { await running.ConfigureAwait(true); } catch { /* failures already surfaced to TCSs */ }
        }

        if (_pendingBatch.Count > 0)
        {
            StartFlush();
            if (_inFlightFlush is { } draining)
            {
                try { await draining.ConfigureAwait(true); } catch { /* see above */ }
            }
        }
    }

    /// <inheritdoc />
    public async Task<long> AppendAsync(ReplogEntry entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        var size = EstimateSize(entry);
        var maxEntries = _options.WalMaxBatchEntries;
        var maxBytes = _options.WalMaxBatchBytes;

        // Flush the current pending batch when adding `entry` would
        // overflow either limit. The loop tolerates concurrent appends
        // arriving across the await: each iteration re-checks capacity
        // against the current pending state.
        while (_pendingBatch.Count > 0
            && (_pendingBatch.Count + 1 > maxEntries || _pendingBatchSizeBytes + size > maxBytes))
        {
            if (_inFlightFlush is { } running)
            {
                try { await running.ConfigureAwait(true); } catch { /* TCSs in the failed batch already saw the exception */ }
            }
            else
            {
                StartFlush();
                // After StartFlush, pending is empty; loop guard exits.
            }
        }

        var offset = _nextOffset++;
        _pendingBatch.Add(new WalEntry { Offset = offset, Entry = entry });
        _pendingBatchSizeBytes += size;

        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingAcks.Add(tcs);

        if (_inFlightFlush is null)
        {
            StartFlush();
        }

        return await tcs.Task.ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async Task<ReplogShardPage> ReadAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (fromSequence < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fromSequence),
                fromSequence,
                "Sequence numbers start at 0; negative values are not valid.");
        }

        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries),
                maxEntries,
                "At least one entry must be requested per page.");
        }

        EnsureInitialized();

        var collected = new List<ReplogShardEntry>(Math.Min(maxEntries, 64));
        var fromOffsetExclusive = fromSequence - 1;
        await foreach (var walEntry in _provider
            .ReadAsync(_treeId, _shardIndex, fromOffsetExclusive, maxEntries, cancellationToken)
            .ConfigureAwait(true))
        {
            collected.Add(new ReplogShardEntry { Sequence = walEntry.Offset, Entry = walEntry.Entry });
            if (collected.Count >= maxEntries)
            {
                break;
            }
        }

        var nextSequence = collected.Count == 0 ? fromSequence : collected[^1].Sequence + 1;
        return new ReplogShardPage
        {
            Entries = collected,
            NextSequence = nextSequence,
        };
    }

    /// <inheritdoc />
    public Task<long> GetNextSequenceAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        return Task.FromResult(_nextOffset);
    }

    /// <inheritdoc />
    public Task<long> GetEntryCountAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        return Task.FromResult(_nextOffset);
    }

    /// <summary>
    /// Captures the current pending batch into an in-flight flush task
    /// and resets the pending state for new arrivals. Single in-flight
    /// per shard in v1; <see cref="LatticeReplicationOptions.WalMaxPendingBatches"/>
    /// caps the in-memory backlog.
    /// </summary>
    private void StartFlush()
    {
        if (_pendingBatch.Count == 0)
        {
            return;
        }

        var batch = _pendingBatch;
        var acks = _pendingAcks;
        _pendingBatch = new List<WalEntry>();
        _pendingAcks = new List<TaskCompletionSource<long>>();
        _pendingBatchSizeBytes = 0;
        _inFlightFlush = FlushAsync(batch, acks);
    }

    private async Task FlushAsync(List<WalEntry> batch, List<TaskCompletionSource<long>> acks)
    {
        // Yield once before doing any work so this task is observably
        // incomplete by the time the caller (StartFlush) assigns it to
        // _inFlightFlush. Without this, a synchronously-completing
        // provider runs the entire body inline before the assignment,
        // and the finally block's `_inFlightFlush = null` gets immediately
        // overwritten by the completed-task assignment in StartFlush -
        // which would leave _inFlightFlush permanently non-null and
        // every subsequent append would hang on its TCS.
        await Task.Yield();

        try
        {
            await _provider.AppendBatchAsync(_treeId, _shardIndex, batch, CancellationToken.None).ConfigureAwait(true);
            for (var i = 0; i < acks.Count; i++)
            {
                acks[i].TrySetResult(batch[i].Offset);
            }
        }
        catch (Exception ex)
        {
            // Roll the offset counter back to the start of the failed
            // batch so subsequent appends do not leave a permanent gap
            // (the in-memory provider's dense-offset validation rejects
            // non-contiguous offsets, and a future durable provider must
            // make the same guarantee per the IWalStorageProvider
            // contract).
            _nextOffset = batch[0].Offset;

            for (var i = 0; i < acks.Count; i++)
            {
                acks[i].TrySetException(ex);
            }

            // The pending batch (entries that arrived during the in-flight
            // flush) now has stale offsets above the gap. Fail those TCSs
            // too and reset pending state so subsequent appends restart
            // from `_nextOffset` cleanly. Each affected caller sees the
            // exception inline and retries by re-calling AppendAsync.
            var stalePending = _pendingAcks;
            _pendingBatch = new List<WalEntry>();
            _pendingAcks = new List<TaskCompletionSource<long>>();
            _pendingBatchSizeBytes = 0;
            for (var i = 0; i < stalePending.Count; i++)
            {
                stalePending[i].TrySetException(ex);
            }
        }
        finally
        {
            _inFlightFlush = null;
        }

        // Drain a follow-on batch that accumulated while we were in
        // flight. Done outside the try/finally so a fresh flush failure
        // is observed cleanly by its own callers.
        if (_pendingBatch.Count > 0)
        {
            StartFlush();
        }
    }

    /// <summary>
    /// Approximates the serialised size of a captured
    /// <see cref="ReplogEntry"/> for batch-byte-budget accounting. The
    /// estimate covers the key bytes (UTF-16 worst case), the value
    /// bytes, and a constant overhead for the record envelope, HLC,
    /// origin cluster id, and Orleans slot tags. Documented as
    /// approximate in <see cref="LatticeReplicationOptions.WalMaxBatchBytes"/>.
    /// </summary>
    private static long EstimateSize(ReplogEntry entry)
    {
        var keyBytes = entry.Key is { } k ? k.Length * 2 : 0;
        var valueBytes = entry.Value?.Length ?? 0;
        return keyBytes + valueBytes + EntrySizeOverhead;
    }

    private void EnsureInitialized()
    {
        if (!_initialized)
        {
            throw new InvalidOperationException(
                $"{nameof(ReplogShardGrain)} has not been initialized. The grain is normally activated by Orleans, "
                + $"which calls {nameof(OnActivateAsync)}; unit tests may bypass that by calling {nameof(InitializeForTestingAsync)}.");
        }
    }

    /// <summary>
    /// Test seam that bypasses Orleans activation: configures the grain
    /// for direct instantiation in unit tests without standing up a
    /// silo. Tests pre-load any persisted state into the supplied
    /// <paramref name="provider"/> before calling this method.
    /// </summary>
    internal async Task InitializeForTestingAsync(
        string treeId,
        int shardIndex,
        IWalStorageProvider provider,
        LatticeReplicationOptions? options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(provider);

        _treeId = treeId;
        _shardIndex = shardIndex;
        _provider = provider;
        _options = options ?? new LatticeReplicationOptions { ClusterId = "test" };
        var highest = await provider.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(true);
        _nextOffset = highest + 1;
        _initialized = true;
    }
}
