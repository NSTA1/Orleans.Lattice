using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The shard root's hand-off marker (epic #5605, member 2; design note section 2):
/// the durable <c>_lattice_shard-handoff</c> row (<see cref="ShardHandOffState"/>),
/// its open and close, the shard's routing generation and the close fence that
/// re-claims it, and the bulk birth-hint slot member 3 writes.
/// </summary>
/// <remarks>
/// <para>
/// The shard root is the row's only writer. Every update goes through
/// <see cref="_handOffGate"/>, so the marker methods are safe to interleave: they are
/// <c>[AlwaysInterleave]</c> on the interface because the tree's range-delete fence
/// calls them while a walk of this shard root may itself be waiting on that fence.
/// </para>
/// <para>
/// <b>The close is a fence, not a wait.</b> <see cref="CloseShardMarkerAsync"/>
/// raises the routing generation durably, re-claims it at every WAL partition and
/// records the heads the re-claims return: every write admitted under an older
/// generation was either allocated below those heads or is rejected when it arrives
/// (<c>ReadPositionHonestDrainWithoutFence</c>). Nothing here arms a leaf; member 2 is
/// inert.
/// </para>
/// </remarks>
internal sealed partial class ShardRootGrain
{
    /// <summary>Serialises this activation's marker updates.</summary>
    private readonly SemaphoreSlim _handOffGate = new(1, 1);

    /// <summary>The marker as last read or written by this activation, or <see langword="null"/> before the first read.</summary>
    private ShardHandOffState? _handOffMarker;

    /// <summary>The tree's grain storage for the marker row; resolved on first use.</summary>
    private IGrainStorage? _handOffStorage;

    /// <summary>Overrides the grain storage the marker row goes through; test seam.</summary>
    internal void UseHandOffStorageForTesting(IGrainStorage storage) => _handOffStorage = storage;

    /// <inheritdoc />
    public async Task<ShardHandOffState> GetHandOffMarkerAsync()
    {
        // Fresh from storage, never from this activation's cache: the leaf reading it
        // must see every open and close made before its read (design note section 2).
        var storage = HandOffStorage();
        var row = await HandOffMarkerRows.ReadShardAsync(storage, TreeId, ShardIndex);
        return row.State;
    }

    /// <inheritdoc />
    public Task OpenHandOffMarkerAsync(string token, string? lowInclusive, string? highExclusive)
    {
        ArgumentException.ThrowIfNullOrEmpty(token);
        return OpenShardMarkerAsync(token, lowInclusive, highExclusive);
    }

    /// <inheritdoc />
    public Task CloseHandOffMarkerAsync(string token, long[] heads)
    {
        ArgumentException.ThrowIfNullOrEmpty(token);
        ArgumentNullException.ThrowIfNull(heads);
        return UpdateHandOffMarkerAsync(marker => HandOffMarkerCore.Close(marker, token, heads));
    }

    /// <summary>
    /// Opens shard-level hand-off <paramref name="token"/> over <c>[low, high)</c>,
    /// idempotently, before the hand-off's first admission of a write the covered
    /// leaves will own. Every resume path of an in-flight hand-off calls it before its
    /// next admission (rule 1 of "Hand-offs in flight across the gate").
    /// </summary>
    internal Task OpenShardMarkerAsync(string token, string? lowInclusive, string? highExclusive) =>
        UpdateHandOffMarkerAsync(marker => HandOffMarkerCore.Open(marker, token, lowInclusive, highExclusive));

    /// <summary>
    /// Closes shard-level hand-off <paramref name="token"/> by fence: raises the
    /// shard's routing generation durably, re-claims it at every WAL partition, and
    /// records the claim heads in the marker as the token's close. Called once the
    /// hand-off has stopped admitting writes for its range. Returns the heads, indexed
    /// by partition.
    /// </summary>
    internal async Task<long[]> CloseShardMarkerAsync(string token)
    {
        ArgumentException.ThrowIfNullOrEmpty(token);
        var raised = await UpdateHandOffMarkerAsync(marker =>
        {
            marker.RoutingGeneration++;
            return true;
        });
        var heads = await ClaimRoutingGenerationEverywhereAsync(raised.RoutingGeneration);
        await UpdateHandOffMarkerAsync(marker => HandOffMarkerCore.Close(marker, token, heads));
        return heads;
    }

    /// <summary>Reads bulk operation <paramref name="operationId"/>'s stored birth hint, or <see langword="null"/>.</summary>
    internal async Task<long[]?> ReadBulkBirthHintAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var marker = await LoadHandOffMarkerAsync();
        return marker.BulkBirthHints.TryGetValue(operationId, out var heads) ? (long[])heads.Clone() : null;
    }

    /// <summary>
    /// Persists bulk operation <paramref name="operationId"/>'s birth hint before any
    /// bulk leaf is created; a no-op when one is stored already, so a resume reuses it.
    /// </summary>
    internal Task WriteBulkBirthHintAsync(string operationId, long[] heads)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(heads);
        return UpdateHandOffMarkerAsync(marker => marker.BulkBirthHints.TryAdd(operationId, (long[])heads.Clone()));
    }

    /// <summary>Removes bulk operation <paramref name="operationId"/>'s birth hint once its switch's marker has closed.</summary>
    internal Task RemoveBulkBirthHintAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return UpdateHandOffMarkerAsync(marker => marker.BulkBirthHints.Remove(operationId));
    }

    /// <summary>
    /// Sets the marker's initialised flag (rule 2, "absence never means closed"), but
    /// only when this shard root has no hand-off in flight that may have started
    /// without a marker: no adaptive split, bulk graft, shadow forward or pending
    /// promotion. Called from a serial turn; a no-op once set.
    /// </summary>
    internal async Task TryInitialiseHandOffMarkerAsync()
    {
        if (state.State.SplitInProgress is not null
            || state.State.PendingBulkGraft is not null
            || state.State.ShadowForward is not null
            || !IsAppendFenceGateOpen())
        {
            return;
        }

        await UpdateHandOffMarkerAsync(marker =>
        {
            if (marker.Initialised)
            {
                return false;
            }

            marker.Initialised = true;
            marker.Sequence++;
            return true;
        });
    }

    /// <summary><see langword="true"/> while this silo's append-fence gate is open.</summary>
    private bool IsAppendFenceGateOpen() =>
        context.ActivationServices?.GetService<IWalAppendFenceGate>() is { IsOpen: true };

    /// <summary>
    /// Counts a range-delete record a leaf appended unstamped by design while the gate
    /// was open (member 2, behaviour change 7): an unfenced walk, or a fenced one whose
    /// own intent closed under it.
    /// </summary>
    private void RecordUnfencedRangeDeleteAppend(KeyValuePair<string, object?> source)
    {
        if (!IsAppendFenceGateOpen())
        {
            return;
        }

        LatticeMetrics.WalAppendFenceUnstampedAppends.Add(
            1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            LatticeTenantLabel.ForTree(TreeId),
            source);
    }

    private IGrainStorage HandOffStorage() =>
        _handOffStorage ??= WalAppendFenceStorage.Resolve(context.ActivationServices)
            ?? throw new InvalidOperationException(
                $"Shard root {TreeId}/{ShardIndex} cannot keep a hand-off marker: no grain storage provider is "
                + $"registered under '{LatticeOptions.StorageProviderName}'.");

    private async Task<ShardHandOffState> LoadHandOffMarkerAsync()
    {
        if (_handOffMarker is { } cached)
        {
            return cached;
        }

        var row = await HandOffMarkerRows.ReadShardAsync(HandOffStorage(), TreeId, ShardIndex);
        return _handOffMarker = row.State;
    }

    private async Task<ShardHandOffState> UpdateHandOffMarkerAsync(Func<ShardHandOffState, bool> mutate)
    {
        await _handOffGate.WaitAsync();
        try
        {
            var stored = await HandOffMarkerRows.UpdateShardAsync(HandOffStorage(), TreeId, ShardIndex, mutate);
            _handOffMarker = stored;
            return stored;
        }
        finally
        {
            _handOffGate.Release();
        }
    }

    /// <summary>
    /// Claims shard routing generation <paramref name="generation"/> at every WAL
    /// partition of the tree and returns the claim heads. A refusal means a partition
    /// already holds a higher generation for this shard root (the marker row was lost
    /// or reset): the generation is raised above it and claimed again everywhere.
    /// </summary>
    private async Task<long[]> ClaimRoutingGenerationEverywhereAsync(long generation)
    {
        var partitions = await optionsResolver.GetWalPartitionsAsync(TreeId);
        var initiator = WalRoutingInitiator.ShardRoot(ShardIndex);
        for (var round = 1; ; round++)
        {
            var claims = new Task<WalAppendFenceClaimResult>[partitions];
            for (var p = 0; p < partitions; p++)
            {
                claims[p] = grainFactory
                    .GetGrain<IWalShardGrain>(string.Concat(TreeId, "/", p.ToString(System.Globalization.CultureInfo.InvariantCulture)))
                    .ClaimRoutingGenerationAsync(initiator, generation, CancellationToken.None);
            }

            var results = await Task.WhenAll(claims);
            var refusedAbove = 0L;
            var heads = new long[partitions];
            for (var p = 0; p < partitions; p++)
            {
                if (results[p].Accepted)
                {
                    heads[p] = results[p].Head;
                }
                else
                {
                    refusedAbove = Math.Max(refusedAbove, results[p].Refusal);
                }
            }

            if (refusedAbove == 0)
            {
                return heads;
            }

            if (round >= TreeRangeDeleteFenceGrain.MaxClaimRounds)
            {
                throw new InvalidOperationException(
                    $"Shard root {TreeId}/{ShardIndex}: its routing generation could not be claimed at every WAL partition.");
            }

            var raised = await UpdateHandOffMarkerAsync(marker =>
            {
                marker.RoutingGeneration = Math.Max(marker.RoutingGeneration, refusedAbove) + 1;
                return true;
            });
            generation = raised.RoutingGeneration;
        }
    }
}
