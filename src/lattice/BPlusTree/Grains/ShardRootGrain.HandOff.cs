using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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

    /// <summary>
    /// Whether this activation has seen its marker row exist since it last seeded the
    /// shard's root, and the highest routing generation it saw there (#5705). A row
    /// seen and then found absent, under a routing floor that covers that generation,
    /// was retired by a purge, so an update refuses rather than re-create it for the
    /// purged incarnation.
    /// </summary>
    private bool _handOffMarkerSeen;

    /// <summary>The highest routing generation this activation has seen in its marker row; see <see cref="_handOffMarkerSeen"/>.</summary>
    private long _handOffMarkerSeenGeneration;

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
        var heads = await RaiseAndReclaimRoutingGenerationAsync();
        await UpdateHandOffMarkerAsync(marker => HandOffMarkerCore.Close(marker, token, heads));
        return heads;
    }

    /// <summary>
    /// The shard-level close fence: raises this shard root's routing generation
    /// durably, then re-claims it at every WAL partition and returns the claim heads.
    /// Every write admitted under the old generation was either allocated below the
    /// heads or is rejected when it arrives.
    /// </summary>
    private async Task<long[]> RaiseAndReclaimRoutingGenerationAsync()
    {
        var raised = await UpdateHandOffMarkerAsync(marker =>
        {
            marker.RoutingGeneration++;
            return true;
        });
        return await ClaimRoutingGenerationEverywhereAsync(raised.RoutingGeneration);
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
    /// The token an adaptive split or consolidation fold from this shard opens on its
    /// target's marker. Unique per migration: a donor never moves the same slot twice.
    /// </summary>
    private string MigrationHandOffToken(ShardSplitInProgress sip) =>
        string.Concat(
            "migrate:",
            ShardIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ">",
            sip.ShadowTargetShardIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ":",
            sip.VirtualShardCount.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ":",
            string.Join(',', sip.MovedSlots));

    private IShardRootGrain TargetShardRoot(int shardIndex) =>
        grainFactory.GetGrain<IShardRootGrain>(
            string.Concat(TreeId, "/", shardIndex.ToString(System.Globalization.CultureInfo.InvariantCulture)));

    /// <summary>
    /// Opens this shard's migration hand-off on the target shard root's marker
    /// (epic #5605, member 2; the hand-off table's adaptive split and shard fold rows),
    /// idempotently. Called when the migration begins and on every resume path before
    /// its next admission (rule 1). A target on an older build that answers "method
    /// unknown" is skipped; any other failure propagates, so the migration does not
    /// proceed without its marker.
    /// </summary>
    private async Task OpenMigrationHandOffAsync(ShardSplitInProgress sip)
    {
        if (!IsAppendFenceGateOpen())
        {
            return;
        }

        try
        {
            await TargetShardRoot(sip.ShadowTargetShardIndex).OpenHandOffMarkerAsync(MigrationHandOffToken(sip), null, null);
            LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindShardMigration, opened: true, TreeId);
        }
        catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
        {
        }
    }

    /// <summary>
    /// Closes this shard's migration hand-off on the target by fence, once admission
    /// for the moved slots has stopped (the reject phase) and the drain has finished:
    /// raises this shard root's routing generation, re-claims it at every WAL partition,
    /// and records the heads on the target's marker. With the gate closed the
    /// migration's writes were unstamped, which the watermark reports, so the current
    /// allocator heads close it. A failure propagates and the completion is re-driven.
    /// </summary>
    private async Task CloseMigrationHandOffAsync(ShardSplitInProgress sip)
    {
        long[] heads;
        if (IsAppendFenceGateOpen())
        {
            heads = await RaiseAndReclaimRoutingGenerationAsync();
        }
        else
        {
            heads = await BPlusLeafGrain.CaptureAllocatorHeadsAsync(
                grainFactory, TreeId, await optionsResolver.GetWalPartitionsAsync(TreeId));
        }

        try
        {
            await TargetShardRoot(sip.ShadowTargetShardIndex).CloseHandOffMarkerAsync(MigrationHandOffToken(sip), heads);
            LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindShardMigration, opened: false, TreeId);
        }
        catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
        {
        }
    }

    /// <summary>The token a reclaim of <paramref name="absorbedLeafId"/> opens on its survivor's marker.</summary>
    internal static string ReclaimHandOffToken(GrainId absorbedLeafId) =>
        string.Concat("reclaim:", absorbedLeafId.GetGuidKey().ToString("N"));

    /// <summary>
    /// Opens the reclaim hand-off on <paramref name="survivorLeafId"/>'s marker before
    /// its range widens onto <paramref name="absorbedLeafId"/>'s (epic #5605, member 2;
    /// the hand-off table's leaf reclaim row). A no-op while the gate is closed; a
    /// failure is logged and the fold goes ahead, since the survivor then has an
    /// unchanged, possibly uninitialised marker, which never reads as closed.
    /// </summary>
    private async Task OpenReclaimHandOffAsync(GrainId survivorLeafId, GrainId absorbedLeafId)
    {
        if (!IsAppendFenceGateOpen() || WalAppendFenceStorage.Resolve(context.ActivationServices) is not { } storage)
        {
            return;
        }

        try
        {
            var token = ReclaimHandOffToken(absorbedLeafId);
            var opened = false;
            await HandOffMarkerRows.UpdateLeafAsync(storage, survivorLeafId.GetGuidKey(), marker => opened = HandOffMarkerCore.Open(marker, token, null, null));
            if (opened)
            {
                LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindReclaim, opened: true, TreeId);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Shard root {TreeId}/{ShardIndex}: could not open the reclaim marker on survivor {Survivor} (epic #5605).",
                TreeId,
                ShardIndex,
                survivorLeafId);
        }
    }

    /// <summary>
    /// Closes the reclaim hand-off on <paramref name="survivorLeafId"/> by fence: retires
    /// <paramref name="absorbedLeafId"/>'s append-epoch entry at every WAL partition, so
    /// its later stamped appends are rejected, then records the allocator heads read
    /// after the retirement. Idempotent; a no-op when the marker is not open. A failure
    /// leaves the marker open, which only holds the survivor on the replay-based advance;
    /// the reclaim's pending marker re-drives the completion.
    /// </summary>
    private async Task CloseReclaimHandOffAsync(GrainId survivorLeafId, GrainId absorbedLeafId)
    {
        if (WalAppendFenceStorage.Resolve(context.ActivationServices) is not { } storage)
        {
            return;
        }

        var token = ReclaimHandOffToken(absorbedLeafId);
        var survivorKey = survivorLeafId.GetGuidKey();
        try
        {
            var marker = await HandOffMarkerRows.ReadLeafAsync(storage, survivorKey);
            if (!marker.State.Open.ContainsKey(token))
            {
                return;
            }

            var partitions = await optionsResolver.GetWalPartitionsAsync(TreeId);
            var heads = await RetireAbsorbedAndCaptureHeadsAsync(grainFactory, TreeId, partitions, absorbedLeafId.GetGuidKey());
            var closed = false;
            await HandOffMarkerRows.UpdateLeafAsync(storage, survivorKey, m => closed = HandOffMarkerCore.Close(m, token, heads));
            if (closed)
            {
                LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindReclaim, opened: false, TreeId);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Shard root {TreeId}/{ShardIndex}: could not close the reclaim marker on survivor {Survivor}; it stays on the replay-based advance until the reclaim completion is re-driven (epic #5605).",
                TreeId,
                ShardIndex,
                survivorLeafId);
        }
    }

    /// <summary>
    /// The reclaim close's fence: retires <paramref name="absorbedLeafId"/>'s
    /// append-epoch entry at every WAL partition, then reads every partition's
    /// allocator head. The heads are read only after every retirement has returned, so
    /// each append the absorbed leaf made is either allocated below them or rejected.
    /// Backstops the absorbed leaf's own clear, whose retirement failure is logged.
    /// </summary>
    internal static async Task<long[]> RetireAbsorbedAndCaptureHeadsAsync(IGrainFactory grainFactory, string treeId, int partitions, Guid absorbedLeafId)
    {
        var retirements = new Task<long>[partitions];
        for (var p = 0; p < partitions; p++)
        {
            retirements[p] = grainFactory
                .GetGrain<IWalShardGrain>(string.Concat(treeId, "/", p.ToString(System.Globalization.CultureInfo.InvariantCulture)))
                .RetireAppendEpochAsync(absorbedLeafId, WalAppendFenceRows.RowEpochUnread, CancellationToken.None);
        }

        await Task.WhenAll(retirements);
        return await BPlusLeafGrain.CaptureAllocatorHeadsAsync(grainFactory, treeId, partitions);
    }

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
        NoteHandOffMarkerRead(row);
        return _handOffMarker = row.State;
    }

    /// <summary>Records that this activation read its marker row, when the row exists (#5705).</summary>
    private void NoteHandOffMarkerRead(GrainState<ShardHandOffState> row)
    {
        if (row.RecordExists)
        {
            _handOffMarkerSeen = true;
            _handOffMarkerSeenGeneration = Math.Max(_handOffMarkerSeenGeneration, row.State.RoutingGeneration);
        }
    }

    /// <summary>
    /// Whether this shard root's routing floor covers every generation this activation
    /// saw in its marker row, so the row it found absent was retired by a purge (#5705).
    /// </summary>
    private async Task<bool> IsRetiredByPurgeAsync()
    {
        var floor = await WalAppendFenceRows.ReadRoutingFloorAsync(HandOffStorage(), TreeId, WalRoutingInitiator.ShardRoot(ShardIndex));
        return floor > 0 && floor >= _handOffMarkerSeenGeneration;
    }

    /// <summary>
    /// The re-seed catch-up (#5703 review, S2; #5716). It runs when this shard re-seeds
    /// a purged root for a re-created incarnation, before the tombstone is lifted, and
    /// stamps the marker with <paramref name="reseedOf"/>: the storage ETag of the
    /// tombstone being lifted. A marker that already carries it was caught up by
    /// another activation lifting the same tombstone (a duplicate activation that
    /// re-seeded first, and may since have seeded and fenced its generation), so it is
    /// left alone. Any other marker predates the re-created incarnation. A 10.2.0 silo
    /// that finished the purge retired nothing, so such a marker can hold a generation
    /// or read initialised. The catch-up raises this shard root's own routing floor
    /// over its generation, then rewrites it as uninitialised with no generation. Its
    /// first initialisation then seeds above the floor through the first-initialisation
    /// fence.
    /// </summary>
    /// <param name="reseedOf">
    /// The ETag of the purge tombstone being lifted. Empty only on a storage double that
    /// issues no ETags; the catch-up then infers "predates" from the generation and the
    /// initialised flag, as it did before #5716. Production providers enforce ETags.
    /// </param>
    /// <remarks>
    /// <para>
    /// Only this shard root's own initiator floor rises. The re-created incarnation is
    /// live while its shards re-seed one at a time, so raising any other initiator's
    /// floor could cover a generation that initiator already holds. The open hand-offs
    /// and closes the marker carries are kept: an open token only holds the shard's
    /// leaves (rule 2), and a close's heads only make a leaf replay further.
    /// </para>
    /// <para>
    /// A marker the new incarnation's range-delete fence opened on this shard before it
    /// re-seeded holds neither a generation nor the initialised flag, so it is only
    /// stamped. Cost: one marker-row read and, unless a sibling already stamped it, one
    /// marker-row write, once per shard per re-creation. No grain call. Mixed-version
    /// residue: a re-seed run by a 10.2.0 silo, and a range-delete fence state an earlier
    /// incarnation left, are not caught up.
    /// </para>
    /// </remarks>
    private async Task RetirePredatingHandOffMarkerAsync(string? reseedOf)
    {
        if (WalAppendFenceStorage.Resolve(context.ActivationServices) is not { } storage)
        {
            return;
        }

        var stamped = false && !string.IsNullOrEmpty(reseedOf); // RED-PROOF M15: infer predates, ignore the re-seed stamp
        var self = WalRoutingInitiator.ShardRoot(ShardIndex);
        await _handOffGate.WaitAsync();
        try
        {
            for (var attempt = 1; ; attempt++)
            {
                var row = await HandOffMarkerRows.ReadShardAsync(storage, TreeId, ShardIndex);
                if (row.RecordExists ? IsCaughtUp(row.State) : !stamped)
                {
                    break;
                }

                var floor = HoldsGeneration(row.State)
                    ? await WalAppendFenceRows.RaiseRoutingFloorAsync(storage, TreeId, self, row.State.RoutingGeneration)
                    : 0;
                var stored = await HandOffMarkerRows.UpdateShardAsync(storage, TreeId, ShardIndex, m =>
                {
                    if (IsCaughtUp(m) || m.RoutingGeneration > floor)
                    {
                        // Caught up by a sibling, or raised since the read: re-read and cover it.
                        return false;
                    }

                    m.RoutingGeneration = 0;
                    m.Initialised = false;
                    m.ReseedOf = stamped ? reseedOf : m.ReseedOf;
                    m.Sequence++;
                    return true;
                });

                if (IsCaughtUp(stored))
                {
                    break;
                }

                if (attempt >= HandOffMarkerRows.MaxUpdateAttempts)
                {
                    throw new InvalidOperationException(
                        $"Shard root {TreeId}/{ShardIndex}: its predating hand-off marker could not be retired.");
                }
            }

            ForgetHandOffMarker();
        }
        finally
        {
            _handOffGate.Release();
        }

        bool IsCaughtUp(ShardHandOffState marker) =>
            stamped ? string.Equals(marker.ReseedOf, reseedOf, StringComparison.Ordinal) : !HoldsGeneration(marker);

        static bool HoldsGeneration(ShardHandOffState marker) => marker.RoutingGeneration > 0 || marker.Initialised;
    }

    /// <summary>Runs the re-seed catch-up as a purged root's re-seed of tombstone <paramref name="reseedOf"/> does; test seam (#5716).</summary>
    internal Task RetirePredatingHandOffMarkerForTestingAsync(string? reseedOf) => RetirePredatingHandOffMarkerAsync(reseedOf);
    /// <summary>Forgets every marker read, when the shard re-seeds a purged root for a re-created incarnation (#5705).</summary>
    private void ForgetHandOffMarker()
    {
        _handOffMarker = null;
        _handOffMarkerSeen = false;
        _handOffMarkerSeenGeneration = 0;
    }

    private async Task<ShardHandOffState> UpdateHandOffMarkerAsync(Func<ShardHandOffState, bool> mutate)
    {
        await _handOffGate.WaitAsync();
        try
        {
            ShardHandOffState stored;
            if (_handOffMarkerSeen)
            {
                // #5705: an absent row this activation has seen was either lost, which
                // the update re-creates as before (#5663), or retired by a purge, whose
                // routing floor then covers every generation this activation saw. That
                // incarnation's marker is never re-created: a re-created incarnation
                // re-seeds the root first, which forgets the reads.
                stored = await HandOffMarkerRows.UpdateExistingShardAsync(HandOffStorage(), TreeId, ShardIndex, mutate)
                    ?? (await IsRetiredByPurgeAsync()
                        ? throw PurgedIncarnation()
                        : await HandOffMarkerRows.UpdateShardAsync(HandOffStorage(), TreeId, ShardIndex, mutate));
            }
            else
            {
                stored = await HandOffMarkerRows.UpdateShardAsync(HandOffStorage(), TreeId, ShardIndex, mutate);
            }

            _handOffMarkerSeen = true;
            _handOffMarkerSeenGeneration = Math.Max(_handOffMarkerSeenGeneration, stored.RoutingGeneration);
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
