namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The arm at a WAL partition (#5833, revision 2): the claim a leaf activation makes only
/// when it needs a live advance there, instead of eagerly before its first append.
/// </summary>
internal sealed partial class WalShardGrain
{
    /// <summary>How many times an arm re-presents a deferred generation a concurrent re-claim overtook.</summary>
    internal const int MaxDeferredResolveAttempts = 4;

    /// <inheritdoc />
    public async Task<WalAppendFenceArmResult> ArmAppendEpochAsync(
        Guid leafId,
        long epoch,
        Guid activation,
        WalRoutingInitiator deferredInitiator,
        long deferredGeneration,
        CancellationToken cancellationToken)
    {
        if (activation == Guid.Empty)
        {
            throw new ArgumentException("An arm names its activation's token.", nameof(activation));
        }

        if (deferredInitiator.IsSet && deferredInitiator.Kind != WalRoutingInitiatorKind.ShardRoot)
        {
            throw new ArgumentException("A deferred hand-off close is a shard root's.", nameof(deferredInitiator));
        }

        var claim = await ClaimAppendEpochAsync(leafId, epoch, cancellationToken).ConfigureAwait(true);
        if (!claim.Accepted)
        {
            return WalAppendFenceArmResult.Refuse(claim.Refusal);
        }

        var (reclaimHead, reclaimGeneration) = deferredInitiator.IsSet
            ? await ResolveDeferredGenerationAsync(deferredInitiator, deferredGeneration, cancellationToken).ConfigureAwait(true)
            : (-1L, 0L);

        long foreignHigh;
        lock (_stateGate)
        {
            // Read after the raise: any other activation's record allocated later is
            // reported on this activation's own appends (NoteLeafAuthorLocked).
            foreignHigh = ForeignHighLocked(leafId, activation);
        }

        return new WalAppendFenceArmResult(true, claim.Head, 0, foreignHigh, reclaimHead, reclaimGeneration);
    }

    /// <summary>
    /// Re-claims a deferred close's generation (C2) and returns the re-claim's head and the
    /// generation it claimed (review N2). A
    /// generation below the one this partition has loaded is presented at the loaded
    /// value (C5): that higher re-claim already refuses every write admitted under the
    /// deferred one, and the current head lies above it, so the arm takes the fast path
    /// and the current head instead of being refused and wedged.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The generation stayed refused: it lies at or below the routing floor (the tree was
    /// purged since the close), so the arm does not hold at this partition.
    /// </exception>
    private async Task<(long Head, long Generation)> ResolveDeferredGenerationAsync(WalRoutingInitiator initiator, long generation, CancellationToken cancellationToken)
    {
        await LoadRoutingGenerationAsync(initiator, cancellationToken).ConfigureAwait(true);
        var present = generation;
        for (var attempt = 1; attempt <= MaxDeferredResolveAttempts; attempt++)
        {
            lock (_stateGate)
            {
                if (GenerationEntries().TryGetValue(initiator, out var loaded) && loaded.Memory != WalAppendFenceCore.Unloaded)
                {
                    present = Math.Max(present, loaded.Memory);
                }
            }

            var result = await ClaimRoutingGenerationGatedAsync(initiator, present, cancellationToken).ConfigureAwait(true);
            RecordFenceClaim(LatticeMetrics.AppendFenceKindGeneration, result.Accepted);
            if (result.Accepted)
            {
                return (result.Head, generation); // RED PROOF: the requested generation (review N2)
            }

            present = Math.Max(present, result.Refusal);
        }

        throw new InvalidOperationException(
            $"WAL shard {_treeId}/{_shardIndex}: the deferred generation {generation} of {WalAppendFenceRows.InitiatorTag(initiator)} could not be re-claimed.");
    }
}
