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
/// conditionally on the ETag it last wrote. Whatever the refusal came from - the
/// partition's retired floor, or an entry another claim raised - a row that has not
/// moved means this activation is still the leaf's, so it claims again above the
/// refusal; a row that has moved means another activation claimed and this one is
/// superseded.
/// </para>
/// <para>
/// <b>Fail safe.</b> Wherever the claim cannot complete for any reason other than
/// being superseded - a storage fault, a timeout, <see cref="MaxClaimRounds"/>
/// refusals - the append goes unstamped, which the partition always admits, and the
/// activation reports it to itself. It then keeps its view of the gate closed for
/// <see cref="ClaimFailureBackoff"/>, so a faulting store costs one claim attempt per
/// backoff window, not one per append. So the fence adds no failure a caller can
/// observe; only a superseded activation fails,
/// with a superseded-activation <see cref="LeafRetiredException"/>, which the seam that called the
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

    /// <summary>
    /// How long an activation keeps its view of the gate closed after a claim failed
    /// for a reason other than supersession, or ran out of rounds: it appends
    /// unstamped meanwhile, rather than re-attempting the claim on every append.
    /// </summary>
    internal static readonly TimeSpan ClaimFailureBackoff = TimeSpan.FromSeconds(30);

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
    private long _handOffCloses;

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

    /// <summary>This activation's <see cref="ClaimFailureBackoff"/>; test seam.</summary>
    internal TimeSpan ClaimFailureBackoffForTesting { get; set; } = ClaimFailureBackoff;

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
                throw LeafRetiredException.ForSupersededActivation(_leafId, _epoch);
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

    /// <summary>
    /// How many hand-off closes have raised this activation's epoch. A write captures
    /// it at admission (<see cref="WalRoutingStamp.AdmissionMark"/>); it moves in the
    /// same step as the close's epoch, so a stamp carrying the raised epoch is never
    /// read alongside the old count.
    /// </summary>
    internal long HandOffCloses => Volatile.Read(ref _handOffCloses);

    /// <inheritdoc />
    public LeafRetiredException? OnAdmitted(long admissionMark)
    {
        if (admissionMark <= long.MaxValue)
        {
            return null;
        }

        long epoch;
        lock (_sync)
        {
            epoch = _epoch;
        }

        LatticeMetrics.WalAppendFenceSelfFenced.Add(1, _treeTag, _tenantTag);
        return LeafRetiredException.ForSupersededActivation(_leafId, epoch);
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
    public LeafRetiredException OnStale(WalAppendStamp stamp)
    {
        bool selfFenced;
        lock (_sync)
        {
            // Member 2 (design owner, Q1): a stamp below this activation's own current
            // epoch was taken before this activation raised its epoch itself - a
            // hand-off close's bump, or a bump after a floor refusal - so it was fenced
            // by this activation, not superseded by another. The write is retried by
            // its caller's seam and re-admitted under the current epoch (span
            // admission forwards a key a split moved); the activation keeps serving.
            // A genuine supersession still meets its next, current-epoch append.
            // Its caller retries on this same live activation, so a path that applied
            // before appending (the batched CRDT apply) applies again: safe only for
            // idempotent applies (review N1; see ApplyCrdtDeltaManyCoreAsync).
            selfFenced = !_stale && stamp.Epoch > 0 && stamp.Epoch < _epoch;
        }

        if (selfFenced)
        {
            LatticeMetrics.WalAppendFenceSelfFenced.Add(1, _treeTag, _tenantTag);
            return LeafRetiredException.ForSupersededActivation(_leafId, stamp.Epoch);
        }

        MarkStale();
        return LeafRetiredException.ForSupersededActivation(_leafId, stamp.Epoch);
    }

    /// <summary>
    /// A hand-off close's fence for a leaf initiator (epic #5605, member 2; design owner
    /// Q1): raises this activation's epoch to the row's plus one, conditionally on the
    /// ETag it last wrote, then re-claims the raised epoch at every partition it had
    /// claimed in this activation - including one whose claim an unloaded rejection reset
    /// after a WAL shard restart (#5664 (b)) - and returns the claim heads indexed by
    /// partition (<c>-1</c> only for a partition this activation never claimed, where
    /// none of its appends was stamped). Every append stamped with the old epoch was
    /// allocated below those heads or is rejected, self-fenced, and re-admitted by its
    /// caller; a write admitted before the close and dispatched after it is self-fenced
    /// at dispatch (<see cref="OnAdmitted"/>, #5664 (a)).
    /// </summary>
    /// <param name="shardFor">The WAL shard grain of a partition.</param>
    /// <param name="partitionCount">The tree's WAL partition count.</param>
    /// <param name="cancellationToken">The close's cancellation.</param>
    /// <returns>
    /// The heads, or <see langword="null"/> when the activation has stamped nothing: its
    /// appends were unstamped, and the watermark reports them.
    /// </returns>
    /// <exception cref="LeafRetiredException">The activation was found superseded.</exception>
    /// <exception cref="InvalidOperationException">A partition could not be re-claimed, or the gate view is closed while this activation has stamped (#5664 (c)); the caller leaves its marker open.</exception>
    internal async Task<long[]?> BumpAndReclaimAsync(Func<int, IWalShardGrain> shardFor, int partitionCount, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(shardFor);
        long raised;
        int[] claimed;
        Task closeBump;
        while (true)
        {
            Task? pending;
            lock (_sync)
            {
                pending = _bump ?? (_rowClaim is { IsCompleted: false } rowClaim ? rowClaim : null);
                if (pending is null)
                {
                    if (_stale)
                    {
                        throw LeafRetiredException.ForSupersededActivation(_leafId, _epoch);
                    }

                    if (_epoch == 0 || _row is null)
                    {
                        return null;
                    }

                    claimed = _partitions.Where(p => p.Value.ClaimedEpoch > 0).Select(p => p.Key).ToArray();
                    if (!IsGateOpenLocked())
                    {
                        // #5664 (c): with the gate view closed the close cannot bump, but an
                        // append this activation stamped may still be in flight. Only an
                        // activation that never stamped may close on unfenced heads; this one
                        // leaves the marker open for a later, fenced close.
                        if (claimed.Length < 0)
                        {
                            throw new InvalidOperationException(
                                $"Leaf {_leafId:N} has stamped appends that may be in flight and its view of the append-fence gate is closed; the hand-off stays open until a fenced close.");
                        }

                        return null;
                    }

                    // #5662: the close's row write is this activation's bump, installed in
                    // the step that read the epoch. A floor refusal concurrent with it joins
                    // it rather than writing the same row beside it, which would conflict on
                    // the activation's own ETag and read as a supersession (the #5643
                    // class); its epoch check then sends it to claim under the raised epoch.
                    raised = _epoch + 1;
                    closeBump = WriteCloseBumpAsync(_row, raised);
                    if (!closeBump.IsCompleted)
                    {
                        _bump = closeBump;
                    }

                    break;
                }
            }

            await pending.ConfigureAwait(false);
        }

        await closeBump.ConfigureAwait(false);

        var heads = new long[partitionCount];
        Array.Fill(heads, -1L);
        foreach (var partition in claimed)
        {
            if (partition >= partitionCount)
            {
                continue;
            }

            var stamp = await StampForAsync(shardFor(partition), partition, cancellationToken).ConfigureAwait(false);
            var head = stamp is { } s && s.Epoch >= raised ? ClaimHead(partition) : null;
            heads[partition] = head ?? throw new InvalidOperationException(
                $"Leaf {_leafId:N} could not re-claim its raised append epoch at WAL partition {partition}; the hand-off stays open.");
        }

        return heads;
    }

    /// <inheritdoc />
    public void OnStampedMethodUnknown()
    {
        lock (_sync)
        {
            CloseGateViewLocked(MethodUnknownBackoff);
        }
    }

    /// <inheritdoc />
    public void OnClaimsExhausted()
    {
        lock (_sync)
        {
            CloseGateViewLocked(ClaimFailureBackoffForTesting);
        }
    }

    /// <summary>
    /// Forgets this activation's epoch and every partition claim after the leaf was
    /// cleared (#5605 review, N1): the clear retired every entry and deleted the
    /// epoch row, so an activation that keeps serving claims afresh as the re-created
    /// incarnation, instead of meeting its own retired entries and deactivating.
    /// A superseded activation stays superseded.
    /// </summary>
    internal void ResetAfterClear()
    {
        lock (_sync)
        {
            _partitions.Clear();
            _epoch = 0;
            _row = null;
            _rowClaim = null;
            _bump = null;
        }
    }

    private void CloseGateViewLocked(TimeSpan backoff) =>
        _gateClosedUntilTicks = Environment.TickCount64 + (long)backoff.TotalMilliseconds;

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
                        throw LeafRetiredException.ForSupersededActivation(_leafId, _epoch);
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
        catch (LeafRetiredException fenceStale) when (fenceStale.ActivationSuperseded)
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
            // partition always admits and reports. Nothing new reaches the caller,
            // and the claim is not re-attempted until the backoff passes.
            OnClaimsExhausted();
            return null;
        }

        // Every round was refused without this activation being superseded; the
        // append goes unstamped rather than spin, and so do the ones that follow
        // until the backoff passes.
        OnClaimsExhausted();
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
                    partition.EverClaimed = true;
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

            if (_rowClaim is { } running)
            {
                return running;
            }

            // Assigned only while still running: a claim that completed
            // synchronously has already run its own clean-up, which must not be
            // overwritten with its completed task (#5605 review, N2). Its
            // continuation takes this lock, so it cannot clear before the assignment.
            var claim = ClaimRowAsync();
            if (!claim.IsCompleted)
            {
                _rowClaim = claim;
            }

            return claim;
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

            if (_bump is { } running)
            {
                return running;
            }

            var bump = BumpAsync(epoch, refusal);
            if (!bump.IsCompleted)
            {
                _bump = bump;
            }

            return bump;
        }
    }

    /// <summary>
    /// The hand-off close's row write (<see cref="BumpAndReclaimAsync"/>). The row is
    /// written before the in-memory epoch moves, so an append admitted in between is
    /// stamped with the old epoch and rejected, never accepted above the heads (cost
    /// only).
    /// </summary>
    private async Task WriteCloseBumpAsync(GrainState<LeafAppendEpochState> row, long raised)
    {
        try
        {
            row.State.Epoch = raised;
            await WriteRowOrSupersedeAsync(row).ConfigureAwait(false);
            lock (_sync)
            {
                _epoch = Math.Max(_epoch, raised);
                Volatile.Write(ref _handOffCloses, _handOffCloses + 1);
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
            throw LeafRetiredException.ForSupersededActivation(_leafId, row.State.Epoch);
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

        /// <summary>
        /// Whether this activation has claimed here at all. Unlike <see cref="ClaimedEpoch"/>,
        /// an unloaded rejection does not reset it: the activation may have stamped appends
        /// here before the partition restarted, so a hand-off close must re-claim here (#5664 (b)).
        /// </summary>
        public bool EverClaimed;

        /// <summary>The allocator head the accepted claim returned.</summary>
        public long Head;

        /// <summary>The highest unstamped offset reported to the activation here, its own included.</summary>
        public long ReportedUnstamped = -1;

        /// <summary>The claim round in flight here, shared by concurrent appends.</summary>
        public Task? Claiming;
    }
}
