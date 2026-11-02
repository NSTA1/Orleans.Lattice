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
/// <b>Begin.</b> Before the first leaf it creates, the operation captures every WAL
/// partition's head and stores it as the operation's hint, together with the open
/// marker <c>bulk:{operationId}</c>, in one write of the shard's
/// <c>_lattice_shard-handoff</c> row. A resume of the same operation reuses the stored
/// hint, so a crash between the capture and the switch cannot move it later than the
/// window it covers.
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
/// <b>Close by fence.</b> Once the switch has published the new routing, the shard
/// root raises its routing generation, re-claims it at every WAL partition and records
/// the claim heads as the token's close, removing the hint in the same write. Every
/// write admitted before the switch was then either allocated below the heads or is
/// rejected and re-admitted under the new routing. With the gate closed the close
/// records the allocator heads instead, as a migration's does. Nothing here arms a
/// leaf: member 3 is inert.
/// </para>
/// </remarks>
internal sealed partial class ShardRootGrain
{
    /// <summary>The token a bulk operation opens on its shard's marker.</summary>
    internal static string BulkHandOffToken(string operationId) => string.Concat("bulk:", operationId);

    /// <summary>
    /// Begins bulk operation <paramref name="operationId"/>'s switch while the append-fence
    /// gate is open: stores its birth hint, captured now unless a previous attempt stored
    /// one, and opens its marker over <c>[<paramref name="lowInclusive"/>, open)</c>, in one
    /// write. Called before the operation creates its first leaf. Returns the stored hint,
    /// or <see langword="null"/> while the gate is closed or the silo provides no marker
    /// storage.
    /// </summary>
    private async Task<long[]?> BeginBulkSwitchAsync(string operationId, string? lowInclusive)
    {
        if (!IsAppendFenceGateOpen() || !HasHandOffStorage())
        {
            return null;
        }

        await CloseAbandonedBulkSwitchesAsync(operationId);

        var hint = await ReadBulkBirthHintAsync(operationId) ?? await CaptureBulkBirthHeadsAsync();
        var token = BulkHandOffToken(operationId);
        var opened = false;
        var stored = await UpdateHandOffMarkerAsync(marker =>
        {
            var added = marker.BulkBirthHints.TryAdd(operationId, (long[])hint.Clone());
            opened = HandOffMarkerCore.Open(marker, token, lowInclusive, null);
            return added | opened;
        });

        if (opened)
        {
            LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindBulk, opened: true, TreeId);
        }

        return stored.BulkBirthHints.TryGetValue(operationId, out var durable) ? (long[])durable.Clone() : null;
    }

    /// <summary>
    /// Ends bulk operation <paramref name="operationId"/>'s switch: applies its stored birth
    /// hint to <paramref name="leaves"/> when <paramref name="relocated"/>, optionally waits
    /// out the batch writes admitted before the switch, then closes its marker by fence and
    /// removes the hint. A no-op when the operation stored no hint and opened no marker.
    /// Idempotent, so a resumed switch re-runs it.
    /// </summary>
    /// <param name="operationId">The bulk operation.</param>
    /// <param name="leaves">Every leaf the operation created.</param>
    /// <param name="relocated">
    /// Whether every row the shard held in the operation's range before the switch has been
    /// re-appended above the hint. When <see langword="false"/> the hint is not applied and
    /// the leaves read from the log's start.
    /// </param>
    /// <param name="drainBatchWrites">
    /// Whether to wait for the batch writes admitted before the call first. Only a serial
    /// turn may pass <see langword="true"/>: a batch write that resumes the switch would
    /// otherwise wait for itself. The wait is an optional latency step; the fence is what
    /// bounds them.
    /// </param>
    private async Task FinishBulkSwitchAsync(
        string operationId,
        IReadOnlyList<GrainId> leaves,
        bool relocated,
        bool drainBatchWrites)
    {
        if (!HasHandOffStorage())
        {
            return;
        }

        var token = BulkHandOffToken(operationId);
        var marker = await LoadHandOffMarkerAsync();
        var hint = marker.BulkBirthHints.TryGetValue(operationId, out var stored) ? (long[])stored.Clone() : null;
        if (hint is null && !marker.Open.ContainsKey(token))
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

        if (drainBatchWrites)
        {
            await DrainBatchWritesAdmittedBeforeAsync();
        }

        var heads = IsAppendFenceGateOpen()
            ? await RaiseAndReclaimRoutingGenerationAsync()
            : await BPlusLeafGrain.CaptureAllocatorHeadsAsync(
                grainFactory, TreeId, Math.Max(1, await optionsResolver.GetWalPartitionsAsync(TreeId)));

        var closed = false;
        await UpdateHandOffMarkerAsync(m =>
        {
            closed = HandOffMarkerCore.Close(m, token, heads);
            return m.BulkBirthHints.Remove(operationId) | closed;
        });

        if (closed)
        {
            LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindBulk, opened: false, TreeId);
        }
    }

    /// <summary>Test seam: opens bulk operation <paramref name="operationId"/>'s switch.</summary>
    internal Task<long[]?> BeginBulkSwitchForTestingAsync(string operationId, string? lowInclusive) =>
        BeginBulkSwitchAsync(operationId, lowInclusive);

    /// <summary>
    /// Closes, without applying their hints, the bulk switches a previous operation
    /// opened on this shard and never finished (review N1 of #5702): an append that
    /// stored its hint and opened its marker, then stopped before it recorded its graft
    /// and was never re-driven. Its marker would otherwise hold the leaves it covers on
    /// the replay-based advance for good. Called from the next bulk operation's serial
    /// turn, which no other bulk operation on this shard can overlap; the operation
    /// <paramref name="currentOperationId"/> and a graft still pending are left alone.
    /// No hint is applied: the leaves an abandoned append seeded were never grafted,
    /// and a later re-drive of the same operation captures a fresh hint and re-merges
    /// every entry above it.
    /// </summary>
    private async Task CloseAbandonedBulkSwitchesAsync(string currentOperationId)
    {
        var marker = await LoadHandOffMarkerAsync();
        List<string>? abandoned = null;
        foreach (var token in marker.Open.Keys)
        {
            if (!token.StartsWith("bulk:", StringComparison.Ordinal))
                continue;

            var operationId = token["bulk:".Length..];
            if (string.Equals(operationId, currentOperationId, StringComparison.Ordinal)
                || string.Equals(operationId, state.State.PendingBulkGraft?.OperationId, StringComparison.Ordinal))
            {
                continue;
            }

            (abandoned ??= []).Add(operationId);
        }

        if (abandoned is null)
            return;

        foreach (var operationId in abandoned)
        {
            await FinishBulkSwitchAsync(operationId, [], relocated: false, drainBatchWrites: false);
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
