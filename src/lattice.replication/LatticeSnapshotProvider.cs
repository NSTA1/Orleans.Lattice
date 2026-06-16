using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
///
/// Default <see cref="ISnapshotProvider"/> implementation. Enumerates
/// every live entry in the source tree via the public
/// <see cref="ILattice.EntriesAsync"/> surface and stamps each with its
/// commit-time <see cref="HybridLogicalClock"/> via
/// <see cref="ILattice.GetWithVersionAsync"/>. The snapshot's
/// <see cref="SnapshotStream.CausalStableFrontier"/> is read once
/// up-front from the
/// <see cref="IWalCursorRegistry"/> via
/// <see cref="IWalCursorRegistry.GetCausalStableAsync"/>:
/// the snapshot is cut at the producer's causal-stable frontier
/// (<c>min(consumer VC)</c>), so a receiver pinning that frontier on
/// <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/> can
/// safely accept the first incremental entry under the dependency
/// check without parking it. When no consumer has reported a vector
/// yet (the common case for a single-peer cluster, a fresh deployment
/// before the first ack-with-VC, or a host that has not wired up the
/// causal+ overload), the provider falls back to the producer's
/// per-tree local vector clock from
/// <see cref="IReplicationHighWaterMarkGrain.GetVectorAsync"/>; this
/// is a strict superset of the causal-stable meet and is safe as a
/// snapshot cut-point because there are no entries above the
/// producer's local VC at snapshot time.
/// <para>
/// <b>Atomic visibility across the bootstrap boundary.</b> The export
/// freezes a tree-wide view of <see cref="ITxRegistryGrain"/> saga
/// decisions via <see cref="ITxRegistryGrain.SnapshotAsync"/> at the
/// start of the export and stamps it on every leaf the export visits
/// via <see cref="LatticeRegistrySnapshotContext"/>. Sagas the
/// snapshot recorded as <see cref="TxStatus.Committed"/> or
/// <see cref="TxStatus.Aborted"/> are folded into the committed
/// projection by the per-leaf scan (Committed surfaces the prepared
/// value as the live one; Aborted drops the prepared mutation
/// entirely). Sagas the snapshot recorded as
/// <see cref="TxStatus.InFlight"/> have their per-key prepared
/// mutations emitted explicitly with
/// <see cref="SnapshotEntry.IsPrepared"/> set, routed on the receiver
/// through
/// <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyPreparedSetAsync"/>
/// / <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyPreparedDeleteAsync"/>
/// into the per-tx pending bucket; the matching terminal record
/// arrives subsequently via the post-snapshot incremental WAL stream
/// and flips visibility atomically per saga via
/// <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyTxTerminalAsync"/>.
/// This means a saga that lands a prepare-commit pair concurrent with
/// the export is observed by the bootstrapped peer either at every
/// key or at none, never at a strict subset.
/// </para>
/// <para>
/// <b>Performance note.</b> The default implementation pays one
/// per-key <see cref="ILattice.GetWithVersionAsync"/> round-trip on
/// top of the leaf-chain enumeration. This is correct but not
/// optimal at large key counts; a future revision can swap to a
/// streaming HLC-threshold leaf scan once the core library exposes
/// a version-bearing entries-newer-than primitive in a single pass.
/// Hosts that need a faster export today can register their own <see cref="ISnapshotProvider"/>
/// via DI before calling
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// </para>
/// </summary>
internal sealed class LatticeSnapshotProvider(
    IGrainFactory grainFactory,
    IWalCursorRegistry cursors,
    IOptionsMonitor<LatticeReplicationOptions> options) : ISnapshotProvider
{
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly IWalCursorRegistry _cursors = cursors ?? throw new ArgumentNullException(nameof(cursors));
    private readonly IOptionsMonitor<LatticeReplicationOptions> _options = options ?? throw new ArgumentNullException(nameof(options));

    /// <inheritdoc />
    public async Task<SnapshotStream> ExportAsync(
        string treeName,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        // Read the producer's causal-stable frontier once up-front.
        // The cursor registry's GetCausalStableAsync is the canonical
        // snapshot cut-point per the causal+ design (snapshot_frontier
        // = causal_stable). When the registry has not yet observed a
        // VC-shaped report from any consumer (new deployment, single-
        // peer cluster, host using the legacy HLC-only overload), fall
        // back to the producer's per-tree local vector clock - a strict
        // superset of the meet that is safe as a snapshot cut because
        // no entry can have a VC component above the producer's own
        // local VC at the moment of capture.
        _ = _options.Get(treeName);
        var frontier = await _cursors
            .GetCausalStableAsync(treeName, cancellationToken)
            .ConfigureAwait(false);

        if (frontier is null)
        {
            var hwm = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeName);
            frontier = await hwm.GetVectorAsync(cancellationToken).ConfigureAwait(false);
        }

        var entries = EnumerateAsync(treeName, asOfHlc, cancellationToken);
        return new SnapshotStream(treeName, asOfHlc, frontier, entries);
    }

    private async IAsyncEnumerable<SnapshotEntry> EnumerateAsync(
        string treeName,
        HybridLogicalClock asOfHlc,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var lattice = _grainFactory.GetGrain<ILattice>(treeName);
        var hasUpperBound = asOfHlc != HybridLogicalClock.Zero;

        // Freeze a tree-wide view of saga decisions for the duration of
        // this export. The TxRegistry is the single tree-wide
        // linearization point for atomic-write saga commit/abort
        // decisions; capturing one snapshot up-front and stamping it
        // on every per-shard / per-leaf call via
        // <see cref="LatticeRegistrySnapshotContext"/> means every
        // export sees a single decision view.
        var registry = _grainFactory.GetGrain<ITxRegistryGrain>(treeName);
        var snap0 = await registry.SnapshotAsync().ConfigureAwait(false);

        // The prepared-row pass runs BEFORE the committed-projection
        // pass. Order matters because a source-side terminal that
        // drains a pending bucket between the two passes would
        // otherwise erase the saga's prepared rows from the export
        // entirely: the committed pass with the snap0 ambient hides
        // the saga's keys (snap0 says InFlight), the prepared pass
        // finds the bucket already drained, and the prepares - which
        // were stamped at HLC <= asOfHlc - never re-arrive via the
        // post-snapshot incremental WAL stream (it starts at asOfHlc).
        // Capturing prepared rows first guarantees that every saga
        // snap0 had as InFlight is shipped to the receiver's
        // pending-tx bucket, with the matching terminal record
        // delivered subsequently by the incremental stream to flip
        // visibility atomically.
        //
        // Residual race (a saga snap0 had as InFlight that commits on
        // the source after the prepared pass visited its leaves but
        // before the committed pass emitted the post-saga value at
        // HLC > asOfHlc): in that case the prepared pass has already
        // captured the prepared rows, so the receiver routes them
        // into its pending-tx bucket; the terminal WAL record arrives
        // via the post-snapshot incremental stream, drains the
        // bucket, and the saga becomes atomically visible on the
        // receiver. The committed projection row the committed pass
        // may emit for the same keys at HLC > asOfHlc is filtered out
        // by the hasUpperBound check; when asOfHlc is Zero (cold
        // bootstrap) it is emitted and LWW dominates the prepare-time
        // HLC stamped on the pending bucket, so the post-saga value
        // is the steady-state result either way.
        await foreach (var prepared in EnumeratePreparedAsync(treeName, snap0, asOfHlc, cancellationToken)
            .ConfigureAwait(false))
        {
            yield return prepared;
        }

        using (LatticeRegistrySnapshotContext.BeginScope(snap0))
        {
            // Committed-projection pass. Every leaf in the scan reads
            // the ambient snap0 via
            // <see cref="LatticeRegistrySnapshotContext.Current"/> when
            // resolving the visibility of any pending-tx bucket on
            // each requested key, so the committed view is
            // linearizable against snap0. Sagas snap0 had as
            // Committed surface their prepared (post-saga) value on
            // the matching key; sagas snap0 had as Aborted are
            // dropped; sagas snap0 had as InFlight are hidden
            // (already covered by the prepared-row pass above).
            await foreach (var pair in lattice
                .EntriesAsync(cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var versioned = await lattice
                    .GetWithVersionAsync(pair.Key, cancellationToken)
                    .ConfigureAwait(false);

                if (versioned.Value is null)
                {
                    // Tombstoned between EntriesAsync emitting the key and
                    // the per-key version read; skip - the snapshot reflects
                    // the live state at that read point.
                    continue;
                }

                if (hasUpperBound && versioned.Version > asOfHlc)
                {
                    continue;
                }

                yield return new SnapshotEntry
                {
                    Key = pair.Key,
                    Value = versioned.Value,
                    Timestamp = versioned.Version,
                };
            }
        }
    }

    /// <summary>
    /// Walks every shard's leaf chain on the source tree and emits a
    /// <see cref="SnapshotEntry"/> with <see cref="SnapshotEntry.IsPrepared"/>
    /// set for every <c>(transactionId, key)</c> pair in any leaf's
    /// pending-tx bucket whose <paramref name="snap0"/> status is
    /// <see cref="TxStatus.InFlight"/> or absent. Sagas snap0 had as
    /// <see cref="TxStatus.Committed"/> / <see cref="TxStatus.Aborted"/>
    /// are intentionally skipped here because the committed-projection
    /// pass under the same registry snapshot has already folded their
    /// per-key visibility into its emitted rows.
    /// </summary>
    private async IAsyncEnumerable<SnapshotEntry> EnumeratePreparedAsync(
        string treeName,
        Dictionary<Guid, TxStatus> snap0,
        HybridLogicalClock asOfHlc,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var registry = _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(treeName).ConfigureAwait(false);

        // The registry's shard map is the producer-side authority on
        // virtual-slot / physical-shard layout. A tree that has been
        // written to always has a persisted map; the null fallback to
        // <see cref="LatticeConstants.DefaultShardCount"/> /
        // <see cref="LatticeConstants.DefaultVirtualShardCount"/>
        // covers a tree that exists in the registry but has not yet
        // had its map materialised (an empty pending-prepare scan in
        // that case is a no-op anyway).
        var shardMap = await registry.GetShardMapAsync(treeName).ConfigureAwait(false)
            ?? ShardMap.GetOrCreateDefaultShared(
                LatticeConstants.DefaultVirtualShardCount,
                LatticeConstants.DefaultShardCount);
        var virtualShardCount = shardMap.VirtualShardCount;

        // Slot range covering every virtual slot. The leaf-side
        // <see cref="IBPlusLeafGrain.GetPendingMutationsForSlotsAsync"/>
        // primitive is slot-filtered (designed for shard splits that
        // migrate a subset of slots); for snapshot export we want every
        // pending mutation across every slot, so we pass the full
        // ascending slot array. The leaf bounds the scan by its own
        // pending-tx footprint, so the steady-state cost is dominated
        // by the saga in-flight set, not the virtual slot fan-out.
        var allSlots = new int[virtualShardCount];
        for (var i = 0; i < virtualShardCount; i++)
        {
            allSlots[i] = i;
        }

        var hasUpperBound = asOfHlc != HybridLogicalClock.Zero;
        var physicalShardIndices = shardMap.GetPhysicalShardIndices();

        foreach (var shardIndex in physicalShardIndices)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var shard = _grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync().ConfigureAwait(false);
            while (leafId is not null)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var leaf = _grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
                var pending = await leaf
                    .GetPendingMutationsForSlotsAsync(allSlots, virtualShardCount)
                    .ConfigureAwait(false);

                foreach (var m in pending)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // Skip sagas snap0 already had as decided. The
                    // committed-projection pass under the same snapshot
                    // already folded them in (Committed -> prepared
                    // value surfaced as committed; Aborted -> dropped).
                    // We emit prepared rows only for sagas that snap0
                    // had as InFlight or absent, so the receiver routes
                    // them into its per-tx pending bucket where the
                    // post-snapshot incremental WAL's terminal record
                    // will flip them atomically.
                    if (snap0.TryGetValue(m.TransactionId, out var status)
                        && status != TxStatus.InFlight)
                    {
                        continue;
                    }

                    if (hasUpperBound && m.Timestamp > asOfHlc)
                    {
                        // The prepared mutation was authored after the
                        // snapshot's as-of cut; defer it to the
                        // post-snapshot incremental WAL stream rather
                        // than leaking it across the cut.
                        continue;
                    }

                    yield return new SnapshotEntry
                    {
                        Key = m.Key,
                        Value = m.Value ?? Array.Empty<byte>(),
                        Timestamp = m.Timestamp,
                        IsPrepared = true,
                        IsTombstone = m.IsTombstone,
                        TransactionId = m.TransactionId,
                        ExpiresAtTicks = m.ExpiresAtTicks,
                        // Carry the typed CRDT delta + merge mode so a
                        // bootstrap-restored prepared CRDT entry folds its
                        // per-replica delta on the receiver's terminal commit
                        // (the union) instead of installing the prepared LWW
                        // value. Plain LWW prepares carry Delta=null /
                        // Mode=LwwRegister and stay on the unchanged path.
                        Delta = m.Delta,
                        Mode = m.Mode,
                    };
                }

                leafId = await leaf.GetNextSiblingAsync().ConfigureAwait(false);
            }
        }
    }
}

