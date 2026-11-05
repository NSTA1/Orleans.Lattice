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

    // #5833, revision 2, B1: this activation's token, attributed to every append it makes.
    private readonly Guid _activation = Guid.NewGuid();
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

    // #5840 review S4: pre-arm split closes this activation counted and has not yet spent
    // on a probed re-claim, and the highest epoch it relinquished at such a close.
    private int _reclaimAllowance;
    private long _relinquishedEpoch;

    // Bumped by every relinquish; a row claim that began under an older value is discarded
    // (R2). The epochs a row claim or a bump is writing, while one is in flight.
    private int _relinquishes;
    private long _claimingEpoch;
    private long _bumpingEpoch;
    private int _refusedUnallocated;
    private readonly bool _requireReachability;
    private int _reachability;

    private const int ReachabilityUnproven = 0;
    private const int ReachabilityProven = 1;
    private const int ReachabilityUnreachable = 2;

    /// <summary>Creates the fence for one leaf activation.</summary>
    /// <param name="leafId">The leaf's grain key.</param>
    /// <param name="gate">The silo's view of the fence gate.</param>
    /// <param name="storage">The tree's grain storage provider, for the leaf's epoch row.</param>
    /// <param name="onSuperseded">Called once, with the epoch held, when the activation is found superseded; it deactivates the leaf.</param>
    /// <param name="treeTag">The leaf's tree tag, for its metrics.</param>
    /// <param name="tenantTag">The leaf's tenant tag, for its metrics.</param>
    /// <param name="requireReachabilityProof">
    /// Whether the activation may stamp only once it has proven it is reachable by descent
    /// (#5709): until then, and for good once it is found unreachable, its view of the gate
    /// reads closed, so it claims nothing and appends unstamped.
    /// </param>
    /// <param name="loadedStateCurrent">
    /// Whether the leaf's durable state row is still the one this activation loaded or last
    /// wrote, read fresh from storage (#5841, rule A). A claim's row write is followed by
    /// this read: an activation whose leaf another activation has since persisted - a split
    /// donor's narrowing above all - holds a stale span and must not claim. <see langword="null"/>
    /// skips the check (a fence built without a leaf, in tests).
    /// </param>
    public LeafAppendEpochFence(
        Guid leafId,
        IWalAppendFenceGate gate,
        IGrainStorage storage,
        Action<long> onSuperseded,
        KeyValuePair<string, object?> treeTag,
        KeyValuePair<string, object?> tenantTag,
        bool requireReachabilityProof = false,
        Func<Task<bool>>? loadedStateCurrent = null)
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
        _requireReachability = requireReachabilityProof;
        _loadedStateCurrent = loadedStateCurrent;
    }

    private readonly Func<Task<bool>>? _loadedStateCurrent;

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

    /// <summary>
    /// The live advance's view of <paramref name="partition"/> (epic #5605, member 4),
    /// read in one step: the claim head under the current epoch and the highest
    /// unstamped offset reported to this activation there. <see langword="false"/>
    /// when the activation cannot arm on the partition at all: it is superseded, its
    /// view of the gate is closed, or the partition is not claimed under its current
    /// epoch (never claimed, reset by an unloaded rejection, or awaiting a re-claim
    /// after a bump).
    /// </summary>
    internal bool TryGetArmingView(int partition, out long claimHead, out long reportedUnstamped)
    {
        lock (_sync)
        {
            if (!_stale
                && IsGateOpenLocked()
                && _epoch > 0
                && _partitions.TryGetValue(partition, out var p)
                && p.ClaimedEpoch == _epoch)
            {
                // #5833, revision 2 (B1): an arm's view is bounded by its foreign high,
                // presented as the head below which the activation must have read.
                claimHead = p.OnDemand ? p.ForeignHigh + 1 : p.Head;
                reportedUnstamped = p.ReportedUnstamped;
                return true;
            }
        }

        claimHead = -1;
        reportedUnstamped = -1;
        return false;
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
                return new ValueTask<WalAppendStamp?>(WalAppendStamp.ForLeaf(_leafId, _epoch, _activation));
            }

            if (_gate.IsPreArmOpen)
            {
                // #5833, revision 2: no claim until the leaf arms here (ArmAsync); the
                // append goes pre-arm, which the partition admits on its routing
                // generation alone and attributes to this activation.
                return new ValueTask<WalAppendStamp?>(WalAppendStamp.PreArm(_leafId, _activation));
            }
        }

        return ClaimAsync(shard, partition, cancellationToken);
    }

    /// <summary>Whether this activation has claimed at any partition, eagerly or by an arm.</summary>
    internal bool HasClaimedAnywhere
    {
        get
        {
            lock (_sync)
            {
                foreach (var p in _partitions.Values)
                {
                    if (p.EverClaimed)
                    {
                        return true;
                    }
                }

                return false;
            }
        }
    }

    /// <summary>
    /// Arms this activation at each requested partition (#5833, revision 2): persists the
    /// activation's epoch once, then claims it at each partition through
    /// <see cref="IWalShardGrain.ArmAppendEpochAsync"/>, in parallel, resolving a deferred
    /// hand-off close's generation where one is given. Afterwards the activation stamps
    /// its appends there, and its arming view reads the foreign high the arm returned.
    /// </summary>
    /// <param name="shardFor">The WAL shard grain of a partition.</param>
    /// <param name="requests">The partitions to arm, each with the deferred generation to resolve there, if any.</param>
    /// <param name="cancellationToken">The arm's cancellation.</param>
    /// <returns>
    /// Per request, the re-claim head of a resolved deferred generation and the generation
    /// the re-claim claimed (review N2), a head of <c>-1</c> when none was given, or
    /// <see langword="null"/> when the partition could not be armed; it then stays
    /// pre-arm, which holds it on the replay-based advance.
    /// </returns>
    /// <exception cref="LeafRetiredException">The activation was found superseded.</exception>
    internal async Task<(long Head, long Generation)?[]> ArmAsync(
        Func<int, IWalShardGrain> shardFor,
        IReadOnlyList<(int Partition, WalRoutingInitiator Initiator, long Generation)> requests,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(shardFor);
        ArgumentNullException.ThrowIfNull(requests);
        var arms = new Task<(long Head, long Generation)?>[requests.Count];
        for (var i = 0; i < requests.Count; i++)
        {
            var (partition, initiator, generation) = requests[i];
            arms[i] = ArmPartitionAsync(shardFor(partition), partition, initiator, generation, cancellationToken);
        }

        await Task.WhenAll(arms).ConfigureAwait(false);
        var heads = new (long Head, long Generation)?[arms.Length];
        for (var i = 0; i < arms.Length; i++)
        {
            heads[i] = arms[i].Result;
        }

        return heads;
    }

    /// <summary>One partition's arm: claim rounds as <see cref="ClaimAsync"/> makes, through the arm call.</summary>
    private async Task<(long Head, long Generation)?> ArmPartitionAsync(
        IWalShardGrain shard,
        int partition,
        WalRoutingInitiator initiator,
        long generation,
        CancellationToken cancellationToken)
    {
        try
        {
            for (var round = 1; round <= MaxClaimRounds; round++)
            {
                lock (_sync)
                {
                    if (_stale)
                    {
                        throw LeafRetiredException.ForSupersededActivation(_leafId, _epoch);
                    }
                }

                var epoch = await EnsureEpochAsync().ConfigureAwait(false);
                var result = await shard.ArmAppendEpochAsync(_leafId, epoch, _activation, initiator, generation, cancellationToken).ConfigureAwait(false);
                if (result.Accepted)
                {
                    lock (_sync)
                    {
                        if (_epoch != epoch)
                        {
                            // A concurrent bump raised the epoch: arm again under it.
                            continue;
                        }

                        var p = PartitionLocked(partition);
                        p.ClaimedEpoch = epoch;
                        p.EverClaimed = true;
                        p.Head = result.Head;
                        p.ForeignHigh = result.ForeignHigh;
                        p.OnDemand = true;
                    }

                    return (result.ReclaimHead, result.ReclaimGeneration);
                }

                await RaiseEpochAboveAsync(epoch, result.Refusal).ConfigureAwait(false);
            }
        }
        catch (LeafRetiredException superseded) when (superseded.ActivationSuperseded)
        {
            throw;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            // Fail safe: the partition stays pre-arm and advances by replay.
        }

        // Review N1: a failed arm is counted, never silent; the leaf logs it.
        LatticeMetrics.WalAppendFenceArmFailures.Add(1, _treeTag, _tenantTag);
        return null;
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
        if (admissionMark <= 0 || admissionMark - 1 == Volatile.Read(ref _handOffCloses))
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
            selfFenced = !_stale && stamp.Epoch > 0 && (stamp.Epoch < _epoch || stamp.Epoch <= _relinquishedEpoch);

            // #5840 review S4: in pre-arm the shard root fences a split's donor at link time,
            // raising the leaf's entry above this activation's current epoch. Once per split
            // close it counted, the activation relinquishes the epoch and re-claims afresh -
            // a fresh row read, row+1 and the leaf-state probe - instead of superseding
            // itself; a duplicate counted no close, so it still supersedes here.
            if (!selfFenced && !_stale && stamp.Epoch > 0 && stamp.Epoch == _epoch && _reclaimAllowance > 0)
            {
                _reclaimAllowance = 0;
                RelinquishLocked();
                selfFenced = true;
            }
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
    /// A split close's fence (#5841, rule A, the split's half), run by the donor's live
    /// activation once its narrowed state is durable: raises the leaf's epoch row above every
    /// epoch issued and claims the raised epoch at every partition
    /// (<see cref="LeafSplitDuplicateFence"/>), so every other activation of the donor -
    /// one that claimed before the split, armed or not - is refused. This activation adopts
    /// the raised row, epoch and claims (A3), and counts the close in either case, so a write
    /// it admitted before the narrowing and dispatches after it is self-fenced and re-admitted
    /// (<see cref="OnAdmitted"/>, #5664 (a)).
    /// </summary>
    /// <param name="shardFor">The WAL shard grain of a partition.</param>
    /// <param name="partitionCount">The tree's WAL partition count.</param>
    /// <param name="cancellationToken">The close's cancellation.</param>
    /// <returns>The heads read after the raised claims, indexed by partition, or <see langword="null"/> when no activation of the leaf ever claimed.</returns>
    /// <exception cref="LeafRetiredException">This activation was already found superseded.</exception>
    internal async Task<long[]?> FenceSplitAsync(Func<int, IWalShardGrain> shardFor, int partitionCount, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(shardFor);
        TaskCompletionSource installed;
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

                    if (!IsGateOpenLocked())
                    {
                        // #5664 (c): with the gate view closed the fence cannot claim, but an
                        // append this activation stamped may still be in flight. Only an
                        // activation that never stamped may close on unfenced heads; this one
                        // leaves the marker open for a later, fenced close.
                        if (_partitions.Values.Any(p => p.EverClaimed))
                        {
                            throw new InvalidOperationException(
                                $"Leaf {_leafId:N} has stamped appends that may be in flight and its view of the append-fence gate is closed; the hand-off stays open until a fenced close.");
                        }

                        // Never stamped: its own appends were unstamped. The durable fence
                        // below still runs for a duplicate whose silo's view is open; it is
                        // one row read when nothing ever claimed.
                    }

                    // #5662: the fence's row write is this activation's bump. A floor
                    // refusal concurrent with it joins it rather than writing the row beside
                    // it; its epoch check then sends it to claim under the raised epoch.
                    installed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    _bump = installed.Task;
                    break;
                }
            }

            try
            {
                await pending.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // A claim or bump that failed has already settled this activation's state;
                // the loop re-checks it.
            }
        }

        try
        {
            var heads = await LeafSplitDuplicateFence.FenceAsync(
                _storage,
                shardFor,
                partitionCount,
                _leafId,
                row =>
                {
                    lock (_sync)
                    {
                        if (!_stale)
                        {
                            // The epoch and the close count move in one step, so a stamp
                            // carrying the raised epoch is never read alongside the old count.
                            _row = row;
                            _epoch = row.State.Epoch;
                            Volatile.Write(ref _handOffCloses, _handOffCloses + 1);
                        }
                    }
                },
                (partition, head) =>
                {
                    lock (_sync)
                    {
                        if (!_stale && _row is { } row && _epoch == row.State.Epoch)
                        {
                            var p = PartitionLocked(partition);
                            if (p.ClaimedEpoch <= _epoch)
                            {
                                p.ClaimedEpoch = _epoch;
                                p.EverClaimed = true;
                                p.Head = head;
                                p.OnDemand = false;
                            }
                        }
                    }
                },
                cancellationToken).ConfigureAwait(false);
            if (heads is null)
            {
                // Nothing ever claimed: no duplicate holds an epoch to refuse, but a write
                // this activation admitted before the narrowing must still not claim and
                // append after it.
                lock (_sync)
                {
                    if (!_stale)
                    {
                        Volatile.Write(ref _handOffCloses, _handOffCloses + 1);
                    }
                }
            }

            return heads;
        }
        finally
        {
            lock (_sync)
            {
                if (ReferenceEquals(_bump, installed.Task))
                {
                    _bump = null;
                }
            }

            installed.TrySetResult();
        }
    }

    /// <summary>
    /// Counts a split close that another grain fences durably (#5841, pre-arm): the shard
    /// root raises this leaf's epoch row and claims it when it links the split. Locally,
    /// only the close count moves, so a write this activation admitted before its narrowing
    /// and dispatches after it is self-fenced and re-admitted (<see cref="OnAdmitted"/>).
    /// </summary>
    internal void CountSplitClose()
    {
        lock (_sync)
        {
            if (!_stale)
            {
                Volatile.Write(ref _handOffCloses, _handOffCloses + 1);

                // #5840 review S4: the shard root raises this leaf's epoch row when it links
                // the split. The activation drops its claims now, with no I/O, so its next
                // appends go pre-arm and its next arm re-claims above the raise through the
                // probed claim; a stamped append still in flight at the dropped epoch is
                // self-fenced if the raise refuses it. One allowance covers an arm that
                // races the link: its refusal or row conflict re-claims once instead of
                // superseding.
                _reclaimAllowance = 1;
                RelinquishLocked();
            }
        }
    }

    /// <summary>
    /// Drops this activation's epoch, cached row and claims locally (#5840 review S4, R1),
    /// recording as relinquished the highest epoch it held or was writing: a stamp at or below
    /// it is self-fenced (R3), and the next claim takes <see cref="ClaimRowAsync"/> - a fresh
    /// row read, row+1 and the leaf-state probe under the persist gate - never a write on the
    /// cached ETag. A row claim or bump in flight installs nothing when it completes (R2).
    /// Called under <see cref="_sync"/>.
    /// </summary>
    private void RelinquishLocked()
    {
        _relinquishes++;
        _relinquishedEpoch = Math.Max(_relinquishedEpoch, Math.Max(_epoch, Math.Max(_claimingEpoch, _bumpingEpoch)));
        _epoch = 0;
        _row = null;
        foreach (var p in _partitions.Values)
        {
            p.ClaimedEpoch = 0;
        }
    }

    /// <summary>The pre-arm split closes this activation may still spend on a probed re-claim; test seam (#5840 review S4).</summary>
    internal int ReclaimAllowanceForTesting
    {
        get
        {
            lock (_sync)
            {
                return _reclaimAllowance;
            }
        }
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

    /// <inheritdoc />
    public void OnRefusedUnallocated() => Interlocked.Increment(ref _refusedUnallocated);

    /// <summary>
    /// Takes the count of append calls refused without allocating anything since the
    /// last take (#5708). The leaf releases that many counted appends on its own turn.
    /// </summary>
    internal int TakeRefusedUnallocated() => Interlocked.Exchange(ref _refusedUnallocated, 0);

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
        IsGateViewOpenLocked()
        && (_reachability == ReachabilityProven || (!_requireReachability && _reachability == ReachabilityUnproven));

    private bool IsGateViewOpenLocked() =>
        (_gateClosedUntilTicks == 0 || Environment.TickCount64 >= _gateClosedUntilTicks) && _gate.IsOpen;

    /// <summary>
    /// Whether the activation must prove it is reachable by descent before it may claim
    /// (#5709): the proof is required, not yet decided, and the activation would
    /// otherwise stamp. The leaf proves it on its own turn before its next append.
    /// </summary>
    internal bool NeedsReachabilityProof
    {
        get
        {
            lock (_sync)
            {
                return _requireReachability && _reachability == ReachabilityUnproven && !_stale && IsGateViewOpenLocked();
            }
        }
    }

    /// <summary>Whether the activation has stopped stamping for good because it was found unreachable (#5709).</summary>
    internal bool IsUnreachable
    {
        get
        {
            lock (_sync)
            {
                return _reachability == ReachabilityUnreachable;
            }
        }
    }

    /// <summary>The activation proved it is reachable by descent, or was born under an open hand-off marker; it may claim.</summary>
    internal void OnReachabilityProven()
    {
        lock (_sync)
        {
            if (_reachability == ReachabilityUnproven)
            {
                _reachability = ReachabilityProven;
            }
        }
    }

    /// <summary>
    /// The proof could not be decided (the shard root could not be asked): the activation
    /// appends unstamped for a backoff, then proves again.
    /// </summary>
    internal void OnReachabilityUndecided()
    {
        lock (_sync)
        {
            CloseGateViewLocked(ClaimFailureBackoffForTesting);
        }
    }

    /// <summary>
    /// The activation is not reachable by descent and no hand-off marker is open on it
    /// (#5709): it never stamps again, so every append it makes is unstamped and reported
    /// to every leaf sharing the partition, whose replay applies it by key range. Returns
    /// whether this call latched it.
    /// </summary>
    internal bool StopStampingUnreachable()
    {
        lock (_sync)
        {
            if (_reachability == ReachabilityUnreachable)
            {
                return false;
            }

            _reachability = ReachabilityUnreachable;
            return true;
        }
    }

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
                        return WalAppendStamp.ForLeaf(_leafId, _epoch, _activation);
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
                    partition.OnDemand = false;
                }
            }

            return;
        }

        lock (_sync)
        {
            // #5840 review S4, R7: a write whose claim round ran under an epoch a split close
            // relinquished meanwhile never bumps on its refusal; it self-fences, and its seam
            // re-admits it - pre-arm, or under a fresh probed claim. Only a round under the
            // current epoch, above the relinquished one, may bump.
            if (!_stale && epoch != _epoch && epoch <= _relinquishedEpoch)
            {
                LatticeMetrics.WalAppendFenceSelfFenced.Add(1, _treeTag, _tenantTag);
                throw LeafRetiredException.ForSupersededActivation(_leafId, epoch);
            }
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
        int relinquishes;
        lock (_sync)
        {
            relinquishes = _relinquishes;
        }

        try
        {
            var row = await WalAppendFenceRows.ReadLeafEpochAsync(_storage, _leafId).ConfigureAwait(false);
            row.State.Epoch++;
            lock (_sync)
            {
                _claimingEpoch = row.State.Epoch;
            }

            row = await WriteRowOrSupersedeAsync(row).ConfigureAwait(false);

            // #5841, rule A (the claim's half of a Dekker pair with the split fence): only
            // after the row write has landed, read the leaf's state fresh. A split persists
            // its narrowing before it reads this row, so either the split saw this write and
            // fences it, or this read sees the narrowing. An activation whose loaded state is
            // no longer current holds a stale span: it is superseded here, before it claims
            // at any partition, and every caller awaiting this claim observes that (A4).
            if (_loadedStateCurrent is { } current && !await current().ConfigureAwait(false))
            {
                MarkStale();
                throw LeafRetiredException.ForSupersededActivation(_leafId, row.State.Epoch);
            }

            var epoch = row.State.Epoch;
            lock (_sync)
            {
                _claimingEpoch = 0;
                _rowClaim = null;
                if (relinquishes != _relinquishes)
                {
                    // #5840 review S4, R2 and R5: a split close relinquished while this claim was
                    // in flight. Checked, discarded and recorded as relinquished in this one
                    // section (the relinquish may have run before the claim recorded its epoch);
                    // the next claim reads the row afresh. A stale probe above has already
                    // superseded the activation (R6). The refusal has the self-fence shape, so
                    // the seam retries.
                    _relinquishedEpoch = Math.Max(_relinquishedEpoch, epoch);
                    LatticeMetrics.WalAppendFenceSelfFenced.Add(1, _treeTag, _tenantTag);
                    throw LeafRetiredException.ForSupersededActivation(_leafId, epoch);
                }

                _row = row;
                _epoch = epoch;
            }

            return epoch;
        }
        catch
        {
            lock (_sync)
            {
                _claimingEpoch = 0;
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

    private async Task BumpAsync(long epoch, long refusal)
    {
        try
        {
            GrainState<LeafAppendEpochState> row;
            lock (_sync)
            {
                row = _row!;
            }

            row.State.Epoch = Math.Max(refusal, epoch) + 1;
            lock (_sync)
            {
                _bumpingEpoch = row.State.Epoch;
            }

            row = await WriteRowOrSupersedeAsync(row, relinquishOnConflict: true).ConfigureAwait(false);
            lock (_sync)
            {
                // #5840 review S4, R2: a relinquish while this bump was in flight dropped the
                // epoch it raised from; the next claim reads the row afresh instead.
                if (_epoch == epoch)
                {
                    _row = row;
                    _epoch = row.State.Epoch;
                }
            }
        }
        finally
        {
            lock (_sync)
            {
                _bump = null;
                _bumpingEpoch = 0;
            }
        }
    }

    /// <summary>
    /// Writes this activation's epoch row conditionally on the ETag it last read or wrote,
    /// and returns the row written. Only claims and split fences write the row, so a
    /// conflict means another activation claimed since, or a split fenced this one: the
    /// activation is superseded - unless <paramref name="relinquishOnConflict"/> (a bump) and
    /// it holds the allowance of a counted pre-arm split close (#5840 review S4, R4), when it
    /// relinquishes its epoch and its next claim re-claims afresh through the probe.
    /// </summary>
    private async Task<GrainState<LeafAppendEpochState>> WriteRowOrSupersedeAsync(GrainState<LeafAppendEpochState> row, bool relinquishOnConflict = false)
    {
        try
        {
            await WalAppendFenceRows.WriteAsync(
                _storage,
                WalAppendFenceRows.LeafEpochGrainType,
                WalAppendFenceRows.LeafEpochKey(_leafId),
                row).ConfigureAwait(false);
            return row;
        }
        catch (InconsistentStateException)
        {
            lock (_sync)
            {
                // #5840 review S4: once per counted pre-arm split close, a conflict - the
                // shard root's link-time raise, or a duplicate's row write after it - drops
                // the epoch and leaves the next claim to read the row afresh and run the
                // probe. The refusal has the self-fence shape, so the caller's seam retries.
                if (relinquishOnConflict && !_stale && _reclaimAllowance > 0)
                {
                    _reclaimAllowance = 0;
                    RelinquishLocked();
                    LatticeMetrics.WalAppendFenceSelfFenced.Add(1, _treeTag, _tenantTag);
                    throw LeafRetiredException.ForSupersededActivation(_leafId, row.State.Epoch);
                }
            }

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

        /// <summary>
        /// Whether the claim was made by an arm (#5833, revision 2): the activation then
        /// must have read to <see cref="ForeignHigh"/>, not to <see cref="Head"/>, before it
        /// may advance live, since its own pre-arm appends below the head are its own.
        /// </summary>
        public bool OnDemand;

        /// <summary>The arm's foreign high: the highest offset another activation of the leaf may have allocated here.</summary>
        public long ForeignHigh = -1;

        /// <summary>The highest unstamped offset reported to the activation here, its own included.</summary>
        public long ReportedUnstamped = -1;

        /// <summary>The claim round in flight here, shared by concurrent appends.</summary>
        public Task? Claiming;
    }
}
