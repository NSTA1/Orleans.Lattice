using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The bulk switch's hand-off (epic #5605, member 3; design note section 3, "The bulk
/// birth hint", and the hand-off table's bulk row): the birth-head hint every leaf a
/// bulk load or bulk append creates starts reading from, and the shard-level marker
/// that holds those leaves until the switch is closed by fence.
/// </summary>
/// <remarks>
/// <para>
/// <b>Begin.</b> Before the first leaf it creates, the operation stores its hint,
/// together with the open marker <c>bulk:{operationId}</c>, in one write of the shard's
/// <c>_lattice_shard-handoff</c> row. A resume of the same operation reuses the stored
/// hint, so a crash between the capture and the switch cannot move it later than the
/// window it covers.
/// </para>
/// <para>
/// <b>The hint of a run (review C1 of #5702).</b> A hint may be any head read before the
/// operation's first leaf is born: a lower hint is never above a record the leaf has not
/// applied, it only lengthens the replay. So a chunk reuses the previous chunk's hint, or
/// the heads of the fence it folds, and reads the partitions' heads only when the shard
/// holds neither - the first chunk of a run. The reuse is bounded by the fence cadence,
/// and the replay it adds is paid only by a leaf rehydrated before its checkpoint has
/// advanced past it.
/// </para>
/// <para>
/// <b>Why the hint is applied only at the switch's end.</b> A leaf whose checkpoint is
/// the hint never replays a record allocated before it. Every row the new leaves own
/// whose record lies below the hint is a row the shard held before the switch: the
/// old rightmost leaf's rows in the appended range (issue #5610), or a batch write
/// stored on the root a bulk load replaces (issue #5642). Both are re-appended above
/// the hint when they are relocated. So the hint is applied once the relocation has
/// completed through the seams that guarantee it, and never otherwise: when the old
/// leaf could not be narrowed and relocated (an older silo hosts it, or the gate is
/// closed), the new leaves keep reading from the log's start, which is what recovers
/// such rows in 10.2.0 (review N2 of #5646).
/// </para>
/// <para>
/// <b>Close by fence, amortised over a run (review C1 of #5702).</b> A close raises the
/// shard root's routing generation, re-claims it at every WAL partition and records the
/// claim heads as the close of every switch it covers, removing their hints in the same
/// write. Every write admitted before the close was then either allocated below the
/// heads or is rejected and re-admitted under the new routing. A bulk load closes at its
/// end. A bulk append does not close its own switch: a streaming import sends one
/// append per chunk, and a close per chunk would cost a claim per partition and two
/// row writes, and reject every concurrent stamped write once, per chunk. Instead the
/// completed switches stay open, which only holds their leaves on the replay-based
/// advance a little longer, and are closed together by one fence: when
/// <see cref="BulkSwitchesPerFence"/> have accumulated (folded into the next chunk's
/// open write), when the shard has seen no bulk chunk for
/// <see cref="BulkSwitchIdleClose"/>, or by the next bulk load. With the gate closed a
/// close records the allocator heads instead, as a migration's does. Nothing here arms
/// a leaf: member 3 is inert.
/// </para>
/// </remarks>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// How many completed bulk-append switches a shard keeps open before the next chunk
    /// closes them with one fence (review C1 of #5702).
    /// </summary>
    internal const int BulkSwitchesPerFence = 8;

    /// <summary>
    /// How long a shard waits after its last bulk-append chunk before it closes the
    /// switches still open, ending the run.
    /// </summary>
    internal static readonly TimeSpan BulkSwitchIdleClose = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The hint the current run of bulk chunks reuses; <see langword="null"/> between runs
    /// and on a fresh activation, when the next chunk reads the heads or folds a fence.
    /// </summary>
    private long[]? _bulkRunHint;

    /// <summary>The one-shot timer that ends a run; registered once, re-armed per chunk.</summary>
    private IGrainTimer? _bulkIdleCloseTimer;

    /// <summary>Set when no timer can be registered (a test harness without a runtime).</summary>
    private bool _bulkIdleCloseUnavailable;

    /// <summary>The token a bulk operation opens on its shard's marker.</summary>
    internal static string BulkHandOffToken(string operationId) => string.Concat("bulk:", operationId);

    /// <summary>
    /// Begins bulk operation <paramref name="operationId"/>'s switch while the append-fence
    /// gate is open: stores its birth hint and opens its marker over
    /// <c>[<paramref name="lowInclusive"/>, open)</c>, in one write. The hint is the one a
    /// previous attempt stored, else the heads of the fence this call folds, else the run's,
    /// else the partitions' heads read now. When <see cref="BulkSwitchesPerFence"/> completed
    /// switches are open, or any are and the activation holds no run hint, they are closed
    /// by one fence and the close lands in the same write as the open. Called in the
    /// operation's serial turn, before it creates its first leaf. Returns the stored hint,
    /// or <see langword="null"/> while the gate is closed or the silo provides no marker
    /// storage.
    /// </summary>
    private async Task<long[]?> BeginBulkSwitchAsync(string operationId, string? lowInclusive)
    {
        if (!IsAppendFenceGateOpen() || !HasHandOffStorage())
        {
            return null;
        }

        var marker = await LoadHandOffMarkerAsync();
        marker.BulkBirthHints.TryGetValue(operationId, out var stored);

        // Review N1 of #5702: an abandoned switch counts here, so a fresh activation,
        // which holds no run hint, closes it before it opens anything.
        var completed = CountCompletedBulkSwitches(marker, operationId);
        List<string>? closing = null;
        long[]? fenceHeads = null;
        if (completed >= BulkSwitchesPerFence || (completed > 0 && stored is null && _bulkRunHint is null))
        {
            closing = CollectCompletedBulkSwitches(marker, operationId);
            fenceHeads = await FenceBulkSwitchesAsync(drainBatchWrites: true);
        }

        var hint = stored ?? fenceHeads ?? _bulkRunHint ?? await CaptureBulkBirthHeadsAsync();
        var token = BulkHandOffToken(operationId);
        var opened = false;
        var closed = 0;
        var updated = await UpdateHandOffMarkerAsync(m =>
        {
            var changed = false;
            if (closing is not null)
            {
                foreach (var op in closing)
                {
                    if (HandOffMarkerCore.Close(m, BulkHandOffToken(op), fenceHeads!))
                    {
                        closed++;
                    }

                    changed |= m.BulkBirthHints.Remove(op);
                }
            }

            changed |= m.BulkBirthHints.TryAdd(operationId, hint);
            opened = HandOffMarkerCore.Open(m, token, lowInclusive, null);
            return changed | closed > 0 | opened;
        });

        RecordBulkSwitchMarkers(opened ? 1 : 0, closed);

        // A stored array is never mutated once stored (review C2), so none is cloned.
        var durable = updated.BulkBirthHints.TryGetValue(operationId, out var d) ? d : null;
        _bulkRunHint = durable;
        return durable;
    }

    /// <summary>
    /// Ends bulk operation <paramref name="operationId"/>'s switch: applies its stored birth
    /// hint to <paramref name="leaves"/> when <paramref name="relocated"/>, then either
    /// closes every completed bulk switch of the shard by one fence
    /// (<paramref name="closeNow"/>) or leaves the switch for the run's fence and re-arms the
    /// idle close. A no-op when the operation stored no hint and opened no marker.
    /// Idempotent, so a resumed switch re-runs it.
    /// </summary>
    /// <param name="operationId">The bulk operation.</param>
    /// <param name="leaves">Every leaf the operation created.</param>
    /// <param name="relocated">
    /// Whether every row the shard held in the operation's range before the switch has been
    /// re-appended above the hint. When <see langword="false"/> the hint is not applied and
    /// the leaves read from the log's start.
    /// </param>
    /// <param name="closeNow">
    /// <see langword="true"/> for a bulk load, which is not part of a run;
    /// <see langword="false"/> for a bulk append, whose switch the run's fence closes.
    /// </param>
    private async Task FinishBulkSwitchAsync(
        string operationId,
        IReadOnlyList<GrainId> leaves,
        bool relocated,
        bool closeNow)
    {
        if (!HasHandOffStorage())
        {
            return;
        }

        var marker = await LoadHandOffMarkerAsync();
        marker.BulkBirthHints.TryGetValue(operationId, out var hint);
        if (hint is null && !marker.Open.ContainsKey(BulkHandOffToken(operationId)))
        {
            return;
        }

        if (hint is not null && relocated && leaves.Count > 0)
        {
            await BoundedFanOut.ForEachAsync(
                leaves,
                BoundedFanOut.DefaultWidth,
                leafId => grainFactory.GetGrain<IBPlusLeafGrain>(leafId).SetCheckpointOffsetHintsAsync(hint));
        }

        if (closeNow)
        {
            await CloseBulkSwitchesAsync(drainBatchWrites: false);
            return;
        }

        ArmBulkIdleClose();
    }

    /// <summary>
    /// Closes, by one fence and one marker write, every bulk switch open on this shard
    /// except a still-pending graft's: the completed switches of a run, and any a previous
    /// operation opened and abandoned (review N1 of #5702). Their hints are removed and
    /// none is applied: a completed switch applied its own, and an abandoned append's
    /// leaves were never grafted, so a later re-drive of it re-merges every entry above
    /// the hint it is then given. Ends the run.
    /// </summary>
    /// <param name="drainBatchWrites">
    /// Whether to wait for the batch writes admitted before the close first. Only a turn
    /// that is not a batch write may pass <see langword="true"/>, or it would wait for
    /// itself. The wait only lowers how many writes the fence rejects.
    /// </param>
    private async Task CloseBulkSwitchesAsync(bool drainBatchWrites)
    {
        _bulkRunHint = null;
        if (!HasHandOffStorage())
        {
            return;
        }

        var marker = await LoadHandOffMarkerAsync();
        if (CountCompletedBulkSwitches(marker, currentOperationId: null) == 0)
        {
            return;
        }

        var closing = CollectCompletedBulkSwitches(marker, currentOperationId: null);
        var heads = await FenceBulkSwitchesAsync(drainBatchWrites);
        var closed = 0;
        await UpdateHandOffMarkerAsync(m =>
        {
            var changed = false;
            foreach (var op in closing)
            {
                if (HandOffMarkerCore.Close(m, BulkHandOffToken(op), heads))
                {
                    closed++;
                }

                changed |= m.BulkBirthHints.Remove(op);
            }

            return changed | closed > 0;
        });

        RecordBulkSwitchMarkers(0, closed);
    }

    /// <summary>
    /// The heads a bulk close records: the routing generation's re-claim at every partition
    /// while the gate is open, else the allocator heads.
    /// </summary>
    private async Task<long[]> FenceBulkSwitchesAsync(bool drainBatchWrites)
    {
        if (drainBatchWrites)
        {
            await DrainBatchWritesAdmittedBeforeAsync();
        }

        return IsAppendFenceGateOpen()
            ? await RaiseAndReclaimRoutingGenerationAsync()
            : await BPlusLeafGrain.CaptureAllocatorHeadsAsync(
                grainFactory, TreeId, Math.Max(1, await optionsResolver.GetWalPartitionsAsync(TreeId)));
    }

    /// <summary>
    /// Re-arms the one-shot timer that closes the run's switches once no bulk chunk has
    /// reached the shard for <see cref="BulkSwitchIdleClose"/>. The timer is registered
    /// once per activation and re-armed in place, so a chunk allocates nothing here.
    /// </summary>
    private void ArmBulkIdleClose()
    {
        if (_bulkIdleCloseTimer is { } timer)
        {
            timer.Change(BulkSwitchIdleClose, Timeout.InfiniteTimeSpan);
            return;
        }

        if (_bulkIdleCloseUnavailable)
        {
            return;
        }

        try
        {
            _bulkIdleCloseTimer = this.RegisterGrainTimer(
                OnBulkIdleCloseAsync,
                new GrainTimerCreationOptions(dueTime: BulkSwitchIdleClose, period: Timeout.InfiniteTimeSpan));
        }
        catch (Exception ex)
        {
            // A test harness without a grain runtime: the switches stay open until the
            // next bulk operation closes them.
            _bulkIdleCloseUnavailable = true;
            logger.LogDebug(ex, "Could not register the bulk switch idle close on shard {ShardKey}.", context.GrainId.Key.ToString());
        }
    }

    private async Task OnBulkIdleCloseAsync(CancellationToken cancellationToken)
    {
        try
        {
            await CloseBulkSwitchesAsync(drainBatchWrites: true);
        }
        catch (Exception ex)
        {
            // The switches stay open, which only holds their leaves on the replay-based
            // advance; the next bulk operation or the next idle close retries.
            logger.LogWarning(ex, "The bulk switch idle close failed on shard {ShardKey}.", context.GrainId.Key.ToString());
        }
    }

    /// <summary>Test seam: opens bulk operation <paramref name="operationId"/>'s switch.</summary>
    internal Task<long[]?> BeginBulkSwitchForTestingAsync(string operationId, string? lowInclusive) =>
        BeginBulkSwitchAsync(operationId, lowInclusive);

    /// <summary>Test seam: the run's idle close, as its timer runs it.</summary>
    internal Task CloseBulkSwitchesForTestingAsync() => OnBulkIdleCloseAsync(CancellationToken.None);

    /// <summary>
    /// How many bulk switches are open on <paramref name="marker"/> other than
    /// <paramref name="currentOperationId"/>'s and a still-pending graft's. Compares as
    /// spans, so it allocates nothing.
    /// </summary>
    private int CountCompletedBulkSwitches(ShardHandOffState marker, string? currentOperationId)
    {
        var count = 0;
        foreach (var token in marker.Open.Keys)
        {
            if (IsCompletedBulkSwitch(token, currentOperationId))
            {
                count++;
            }
        }

        return count;
    }

    private List<string> CollectCompletedBulkSwitches(ShardHandOffState marker, string? currentOperationId)
    {
        var operations = new List<string>();
        foreach (var token in marker.Open.Keys)
        {
            if (IsCompletedBulkSwitch(token, currentOperationId))
            {
                operations.Add(token["bulk:".Length..]);
            }
        }

        return operations;
    }

    private bool IsCompletedBulkSwitch(string token, string? currentOperationId)
    {
        if (!token.StartsWith("bulk:", StringComparison.Ordinal))
        {
            return false;
        }

        var operationId = token.AsSpan("bulk:".Length);
        return !operationId.SequenceEqual(currentOperationId)
            && !operationId.SequenceEqual(state.State.PendingBulkGraft?.OperationId);
    }

    private void RecordBulkSwitchMarkers(int opened, int closed)
    {
        if (opened > 0)
        {
            LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindBulk, opened: true, TreeId);
        }

        for (var i = 0; i < closed; i++)
        {
            LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindBulk, opened: false, TreeId);
        }
    }

    /// <summary>
    /// Every WAL partition's head, read as a split reads its donor's
    /// (<c>WalHeadsAtBirth</c>): the read position a leaf created now may start from.
    /// </summary>
    private async Task<long[]> CaptureBulkBirthHeadsAsync()
    {
        var partitions = Math.Max(1, await optionsResolver.GetWalPartitionsAsync(TreeId));
        var reads = new Task<long>[partitions];
        for (var p = 0; p < partitions; p++)
        {
            reads[p] = grainFactory
                .GetGrain<ILeafReplayCoordinatorGrain>(string.Concat(TreeId, "/", p.ToString(System.Globalization.CultureInfo.InvariantCulture)))
                .GetHeadOffsetAsync(CancellationToken.None);
        }

        return await Task.WhenAll(reads);
    }

    /// <summary>Whether this shard root can reach its hand-off marker row.</summary>
    private bool HasHandOffStorage() =>
        _handOffStorage is not null || WalAppendFenceStorage.Resolve(context.ActivationServices) is not null;
}
