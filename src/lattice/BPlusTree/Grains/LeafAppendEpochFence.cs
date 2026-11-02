using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// One leaf activation's side of the WAL append fence (epic #5605, design note
/// <c>spec/wal/design/LiveAdvanceDesign.md</c>, section 1). Claims the
/// activation's append epoch eagerly - persisted once to the leaf's own epoch row,
/// then claimed at every WAL partition before the activation's first append there
/// - and stamps every append with it while the fence gate is open.
/// </summary>
/// <remarks>
/// <para>
/// <b>Step 1, the epoch row.</b> The activation reads its leaf's epoch row and
/// writes the value plus one as a strict conditional write on the ETag it read.
/// Only claims write that row, so any conflict - an empty-ETag create race
/// included - means another activation claimed: this one is superseded, stops,
/// and deactivates. It never adopts the winner's row
/// (<c>TopologySeedPersist.WriteAdoptingBenignFirstCreateRaceAsync</c> is the
/// shape of #5593).
/// </para>
/// <para>
/// <b>Steps 2 and 3</b> run at the partition
/// (<see cref="IWalShardGrain.ClaimAppendEpochAsync"/>). A refusal returns the
/// value the claim must exceed; the activation persists that value plus one,
/// conditionally on the ETag it last wrote. If the row has not moved the refusal
/// came from the partition's retired floor and it claims again above it; if it has,
/// another activation claimed and this one is superseded.
/// </para>
/// <para>
/// <b>Fail safe.</b> Wherever the claim cannot complete for any reason other than
/// being superseded - a storage fault, a timeout - the append goes unstamped, which
/// the partition always admits, and the activation reports it to itself. So the
/// fence adds no failure a caller can observe; only a superseded activation fails,
/// with <see cref="WalAppendEpochStaleException"/>, which the seam that called the
/// leaf retries against the live activation.
/// </para>
/// <para>
/// <b>Concurrency.</b> The commit-log writer calls this from its outbound dispatch,
/// off the grain turn, so every field is guarded by <see cref="_sync"/>. Claims are
/// serialised per partition: concurrent appends to one partition share one claim.
/// </para>
/// </remarks>
internal sealed class LeafAppendEpochFence : ILeafAppendFence
{
    /// <summary>How many claim rounds an append makes at a partition before it goes unstamped.</summary>
    internal const int MaxClaimRounds = 8;

    /// <summary>How long an activation keeps its view of the gate closed after a "method unknown" failure.</summary>
    internal static readonly TimeSpan MethodUnknownBackoff = TimeSpan.FromSeconds(30);

    private readonly object _sync = new();
    private readonly Dictionary<int, PartitionClaim> _partitions = new();
    private readonly Guid _leafId;
    private readonly IWalAppendFenceGate _gate;
    private readonly IGrainStorage _storage;
    private readonly Action<long> _onSuperseded;
    private readonly KeyValuePair<string, object?> _treeTag;
    private readonly KeyValuePair<string, object?> _tenantTag;
    private GrainState<LeafAppendEpochState>? _row;
    private long _epoch;
    private Task<long>? _rowClaim;
    private Task? _bump;
    private bool _stale;
    private long _gateClosedUntilTicks;

    /// <summary>Creates the fence for one leaf activation.</summary>
    /// <param name="leafId">The leaf's grain key.</param>
    /// <param name="gate">The silo's view of the fence gate.</param>
    /// <param name="storage">The tree's grain storage provider, for the leaf's epoch row.</param>
    /// <param name="onSuperseded">Called once, with the epoch held, when the activation is found superseded; it deactivates the leaf.</param>
    /// <param name="treeTag">The leaf's tree tag, for its metrics.</param>
    /// <param name="tenantTag">The leaf's tenant tag, for its metrics.</param>
    public LeafAppendEpochFence(
        Guid leafId,
        IWalAppendFenceGate gate,
        IGrainStorage storage,
        Action<long> onSuperseded,
        KeyValuePair<string, object?> treeTag,
        KeyValuePair<string, object?> tenantTag)
    {
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(onSuperseded);
        _leafId = leafId;
        _gate = gate;
        _storage = storage;
        _onSuperseded = onSuperseded;
        _treeTag = treeTag;
        _tenantTag = tenantTag;
    }

    /// <summary>The epoch this activation holds; 0 before its first claim.</summary>
    internal long Epoch
    {
        get
        {
            lock (_sync)
            {
                return _epoch;
            }
        }
    }

    /// <summary><see langword="true"/> once the activation has been found superseded.</summary>
    internal bool IsStale
    {
        get
        {
            lock (_sync)
            {
                return _stale;
            }
        }
    }

    /// <summary>The claim head at <paramref name="partition"/>, or <see langword="null"/> when it is not claimed under the current epoch.</summary>
    internal long? ClaimHead(int partition)
    {
        lock (_sync)
        {
            return _partitions.TryGetValue(partition, out var p) && _epoch > 0 && p.ClaimedEpoch == _epoch ? p.Head : null;
        }
    }

    /// <summary>The highest unstamped offset reported to this activation at <paramref name="partition"/>; -1 when none.</summary>
    internal long ReportedUnstamped(int partition)
    {
        lock (_sync)
        {
            return _partitions.TryGetValue(partition, out var p) ? p.ReportedUnstamped : -1;
        }
    }

    /// <inheritdoc />
    public ValueTask<WalAppendStamp?> StampForAsync(IWalShardGrain shard, int partition, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(shard);
        lock (_sync)
        {
            if (_stale)
            {
                throw new WalAppendEpochStaleException(_leafId, _epoch);
            }

            if (!IsGateOpenLocked())
            {
                return new ValueTask<WalAppendStamp?>((WalAppendStamp?)null);
            }

            if (_epoch > 0 && _partitions.TryGetValue(partition, out var claimed) && claimed.ClaimedEpoch == _epoch)
            {
                return new ValueTask<WalAppendStamp?>(WalAppendStamp.ForLeaf(_leafId, _epoch));
            }
        }

        return ClaimAsync(shard, partition, cancellationToken);
    }

    /// <inheritdoc />
    public void OnStampedAppended(int partition, long unstampedWatermark)
    {
        bool report;
        lock (_sync)
        {
            var p = PartitionLocked(partition);
            report = unstampedWatermark > p.ReportedUnstamped && unstampedWatermark >= p.Head;
            if (unstampedWatermark > p.ReportedUnstamped)
            {
                p.ReportedUnstamped = unstampedWatermark;
            }
        }

        if (report)
        {
            LatticeMetrics.WalAppendFenceUnstampedReports.Add(1, _treeTag, _tenantTag);
        }
    }

    /// <inheritdoc />
    public void OnUnstampedAppended(int partition, long highestOffset)
    {
        lock (_sync)
        {
            var p = PartitionLocked(partition);
            if (highestOffset > p.ReportedUnstamped)
            {
                p.ReportedUnstamped = highestOffset;
            }
        }
    }

    /// <inheritdoc />
    public void OnUnloaded(int partition, WalAppendStamp stamp)
    {
        lock (_sync)
        {
            if (_partitions.TryGetValue(partition, out var p) && p.ClaimedEpoch == stamp.Epoch)
            {
                p.ClaimedEpoch = 0;
            }
        }
    }

    /// <inheritdoc />
    public WalAppendEpochStaleException OnStale(WalAppendStamp stamp)
    {
        MarkStale();
        return new WalAppendEpochStaleException(_leafId, stamp.Epoch);
    }

    /// <inheritdoc />
    public void OnStampedMethodUnknown()
    {
        lock (_sync)
        {
            _gateClosedUntilTicks = Environment.TickCount64 + (long)MethodUnknownBackoff.TotalMilliseconds;
        }
    }

    private bool IsGateOpenLocked() =>
        (_gateClosedUntilTicks == 0 || Environment.TickCount64 >= _gateClosedUntilTicks) && _gate.IsOpen;

    private async ValueTask<WalAppendStamp?> ClaimAsync(IWalShardGrain shard, int partition, CancellationToken cancellationToken)
    {
        try
        {
            for (var round = 1; round <= MaxClaimRounds; round++)
            {
                Task claim;
                lock (_sync)
                {
                    if (_stale)
                    {
                        throw new WalAppendEpochStaleException(_leafId, _epoch);
                    }

                    var p = PartitionLocked(partition);
                    if (_epoch > 0 && p.ClaimedEpoch == _epoch)
                    {
                        return WalAppendStamp.ForLeaf(_leafId, _epoch);
                    }

                    claim = p.Claiming ??= ClaimAtPartitionAsync(shard, p, cancellationToken);
                }

                try
                {
                    await claim.ConfigureAwait(false);
                }
                finally
                {
                    lock (_sync)
                    {
                        if (_partitions.TryGetValue(partition, out var p) && ReferenceEquals(p.Claiming, claim))
                        {
                            p.Claiming = null;
                        }
                    }
                }
            }
        }
        catch (WalAppendEpochStaleException)
        {
            throw;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            // Fail safe: an append that cannot be stamped goes unstamped, which the
            // partition always admits and reports. Nothing new reaches the caller.
            return null;
        }

        // Every round was refused without this activation being superseded; the
        // append goes unstamped rather than spin.
        return null;
    }

    /// <summary>One claim round at a partition: steps 2 and 3, and the bump on a refusal.</summary>
    private async Task ClaimAtPartitionAsync(IWalShardGrain shard, PartitionClaim partition, CancellationToken cancellationToken)
    {
        var epoch = await EnsureEpochAsync().ConfigureAwait(false);
        var result = await shard.ClaimAppendEpochAsync(_leafId, epoch, cancellationToken).ConfigureAwait(false);
        if (result.Accepted)
        {
            lock (_sync)
            {
                if (_epoch == epoch)
                {
                    partition.ClaimedEpoch = epoch;
                    partition.Head = result.Head;
                }
            }

            return;
        }

        await RaiseEpochAboveAsync(epoch, result.Refusal).ConfigureAwait(false);
    }

    /// <summary>Step 1, once per activation: persists the epoch the row held plus one.</summary>
    private Task<long> EnsureEpochAsync()
    {
        lock (_sync)
        {
            if (_epoch > 0)
            {
                return Task.FromResult(_epoch);
            }

            return _rowClaim ??= ClaimRowAsync();
        }
    }

    private async Task<long> ClaimRowAsync()
    {
        try
        {
            var row = await WalAppendFenceRows.ReadLeafEpochAsync(_storage, _leafId).ConfigureAwait(false);
            var epoch = row.State.Epoch + 1;
            row.State.Epoch = epoch;
            await WriteRowOrSupersedeAsync(row).ConfigureAwait(false);
            lock (_sync)
            {
                _row = row;
                _epoch = epoch;
                _rowClaim = null;
            }

            return epoch;
        }
        catch
        {
            lock (_sync)
            {
                _rowClaim = null;
            }

            throw;
        }
    }

    /// <summary>
    /// After a refusal: persists the refused threshold plus one, conditionally on the
    /// ETag this activation last wrote. A conflict means another activation claimed.
    /// </summary>
    private Task RaiseEpochAboveAsync(long epoch, long refusal)
    {
        lock (_sync)
        {
            if (_epoch != epoch)
            {
                // Another partition's refusal already raised the epoch; claim again under it.
                return Task.CompletedTask;
            }

            return _bump ??= BumpAsync(epoch, refusal);
        }
    }

    private async Task BumpAsync(long epoch, long refusal)
    {
        try
        {
            GrainState<LeafAppendEpochState> row;
            lock (_sync)
            {
                row = _row!;
            }

            var raised = Math.Max(refusal, epoch) + 1;
            row.State.Epoch = raised;
            await WriteRowOrSupersedeAsync(row).ConfigureAwait(false);
            lock (_sync)
            {
                _epoch = raised;
            }
        }
        finally
        {
            lock (_sync)
            {
                _bump = null;
            }
        }
    }

    private async Task WriteRowOrSupersedeAsync(GrainState<LeafAppendEpochState> row)
    {
        try
        {
            await WalAppendFenceRows.WriteAsync(
                _storage,
                WalAppendFenceRows.LeafEpochGrainType,
                WalAppendFenceRows.LeafEpochKey(_leafId),
                row).ConfigureAwait(false);
        }
        catch (InconsistentStateException)
        {
            // Only claims write the epoch row, so a conflict means another
            // activation claimed since this one read or last wrote it.
            MarkStale();
            throw new WalAppendEpochStaleException(_leafId, row.State.Epoch);
        }
    }

    private void MarkStale()
    {
        long epoch;
        lock (_sync)
        {
            if (_stale)
            {
                return;
            }

            _stale = true;
            epoch = _epoch;
        }

        _onSuperseded(epoch);
    }

    private PartitionClaim PartitionLocked(int partition)
    {
        if (!_partitions.TryGetValue(partition, out var p))
        {
            p = new PartitionClaim();
            _partitions[partition] = p;
        }

        return p;
    }

    /// <summary>The activation's claim state at one partition. Guarded by <see cref="_sync"/>.</summary>
    private sealed class PartitionClaim
    {
        /// <summary>The epoch whose claim was raised here; 0 when none, or invalidated by an unloaded rejection.</summary>
        public long ClaimedEpoch;

        /// <summary>The allocator head the accepted claim returned.</summary>
        public long Head;

        /// <summary>The highest unstamped offset reported to the activation here, its own included.</summary>
        public long ReportedUnstamped = -1;

        /// <summary>The claim round in flight here, shared by concurrent appends.</summary>
        public Task? Claiming;
    }
}
