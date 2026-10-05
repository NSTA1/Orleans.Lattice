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
/// <see cref="IReplicationHighWaterMarkGrain.MergeBootstrapFrontierAsync"/> can
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
/// decisions via <see cref="ITxRegistryGrain.SnapshotAsync"/>, unioned
/// across every registry shard of the tree up to its durable shard
/// high-water, at the start of the export and stamps it on every leaf the export visits
/// via <see cref="LatticeRegistrySnapshotContext"/>. Sagas the
/// snapshot recorded as <see cref="TxStatus.Committed"/> or
/// <see cref="TxStatus.Aborted"/> are folded into the committed
/// projection by the per-leaf scan (Committed surfaces the prepared
/// value as the live one; Aborted drops the prepared mutation
/// entirely). Sagas the snapshot recorded as
/// <see cref="TxStatus.InFlight"/> or
/// <see cref="TxStatus.Indeterminate"/>, and sagas it has no row for
/// at all, have their per-key prepared
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
/// <b>Aged-out decisions over a resident prepare.</b> A saga snap0 reports as
/// <see cref="TxStatus.Indeterminate"/> because its decision tombstone outlived
/// the retention window is resolved to the verdict the registry still stores
/// (<see cref="ITxRegistryGrain.GetRecordedStatusAsync"/>, the read the source
/// leaf's self-terminalise sweep finishes such a prepare by) before either pass
/// runs over it, so a saga whose terminal drained some keys and stranded others
/// ships whole rather than split (#4481). An Indeterminate saga with no stored
/// verdict (an unreachable cross-tree delegation) still ships as prepared rows,
/// and a row already purged reads as absent and exports the split the source
/// itself serves (#4508).
/// </para>
/// <para>
/// <b>Deletes.</b> The committed projection enumerates live keys only, so the
/// export ends with a tombstone pass that ships every tombstone a source leaf
/// still holds as a committed tombstone row (<see cref="SnapshotEntry.IsTombstone"/>
/// set, <see cref="SnapshotEntry.IsPrepared"/> clear), and the prepared-row pass
/// ships a pending delete of a saga the frozen registry view recorded as
/// <see cref="TxStatus.Committed"/> the same way. A receiver that bootstraps in
/// place over an existing copy - a peer that fell off the log - applies each as a
/// Delete, so a key deleted while it was behind does not keep its old value
/// (#4504). A tombstone the source has already reaped past
/// <c>TombstoneGracePeriod</c> cannot ship (#4537).
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

        // Number the export before its registry snap0, which the enumeration
        // takes when it starts (#4534): an epoch greater than the one a
        // shipper recorded when it took a peer off the log therefore proves
        // this export's snap0 came after that point.
        var epoch = await _grainFactory
            .GetGrain<Orleans.Lattice.Replication.Grains.IReplicationExportEpochGrain>(treeName)
            .AdvanceAsync()
            .ConfigureAwait(false);

        var entries = EnumerateAsync(treeName, asOfHlc, cancellationToken);
        return new SnapshotStream(treeName, asOfHlc, frontier, entries) { ExportEpoch = epoch };
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
        var snap0 = await Orleans.Lattice.BPlusTree.Grains.TxRegistryFanOut
            .StableSnapshotAsync(_grainFactory, treeName)
            .ConfigureAwait(false);

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
        var recordedResolved = new HashSet<Guid>();
        await foreach (var prepared in EnumeratePreparedAsync(
                treeName, snap0, recordedResolved, asOfHlc, cancellationToken)
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
            // dropped; sagas snap0 had as InFlight or Indeterminate are
            // hidden (already covered by the prepared-row pass above).
            //
            // Resilience: this is a long-running export - cross-cluster
            // bootstrap drains it over a (potentially proxied, WAN)
            // gRPC stream and interleaves a per-key
            // GetWithVersionAsync round-trip between pulls, which
            // widens the gap between successive MoveNextAsync calls and
            // makes idle-expiry of the source grain enumerator likely.
            // ScanEntriesAsync wraps EntriesAsync with
            // EnumerationAbortedException recovery and deterministic
            // resume from the last yielded key, so a reclaimed
            // enumerator is transparently re-opened instead of aborting
            // the whole snapshot stream (which would fail the receiver's
            // bootstrap drain and trap replication in a re-bootstrap
            // loop with the high-water-mark pinned at HLC(0:0)).
            await foreach (var pair in lattice
                .ScanEntriesAsync(cancellationToken: cancellationToken)
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

                // Carry the entry's absolute expiry. Last-writer-wins receivers
                // install it verbatim; typed CRDT committed rows currently
                // merge through a path that writes the resulting key durable.
                yield return new SnapshotEntry
                {
                    Key = pair.Key,
                    Value = versioned.Value,
                    Timestamp = versioned.Version,
                    ExpiresAtTicks = versioned.ExpiresAtTicks,
                };
            }
        }

        // Tombstone pass (#4504). The committed projection enumerates live
        // keys only, so on its own a snapshot never ships a delete. A
        // receiver that bootstraps in place - a peer that fell off the log
        // re-bootstraps over its existing copy, which the drain does not
        // clear - would keep the old value of every key the source deleted
        // while it was behind, and the delete's WAL record is behind the trim
        // point, so the incremental stream never delivers it either. Every
        // tombstone the source still holds therefore ships as a committed
        // tombstone row, which the drain applies as a Delete. Pass order is
        // immaterial: a tombstone and a live row for the same key resolve by
        // HLC under last-writer-wins on the receiver. A tombstone the source
        // has already reaped (CompactTombstonesAsync, past
        // TombstoneGracePeriod) cannot ship and remains a residual (#4537).
        await foreach (var tombstone in EnumerateTombstonesAsync(treeName, asOfHlc, cancellationToken)
            .ConfigureAwait(false))
        {
            yield return tombstone;
        }

        // Decision rows (#4482): every saga the source still STORES a decision
        // for, settled as of this export. The source's write-ahead log can
        // still retain a saga record from before the cut - a prepare in one
        // partition whose terminal's partition was already trimmed - and the
        // incremental stream re-ships it after the bootstrap. The receiver
        // records these outcomes in its registry and settles a re-shipped
        // prepare against them instead of staging it where no terminal will
        // drain it. An aged-out row with no resident bucket is resolved to its
        // recorded verdict here, exactly as the prepared pass resolves one
        // over a bucket (#4481). A row the source has already purged cannot
        // be exported; a re-seed's receiver decides such a saga's stale
        // pending buckets aborted (#4533), so a saga the source still knows
        // but cannot settle (an unresolved Indeterminate row) ships as a
        // value-less row naming it, which keeps the receiver from treating it
        // as purged. A receiver that predates it skips a row with no value.
        foreach (var (txid, decided) in snap0?.ToList() ?? [])
        {
            cancellationToken.ThrowIfCancellationRequested();
            var status = decided;
            if (status == TxStatus.Indeterminate && recordedResolved.Add(txid))
            {
                var recorded = await Orleans.Lattice.BPlusTree.Grains.TxRegistryRouting
                    .GetRegistry(_grainFactory, treeName, txid)
                    .GetRecordedStatusAsync(txid)
                    .ConfigureAwait(false);
                if (recorded is TxStatus.Committed or TxStatus.Aborted)
                {
                    snap0![txid] = recorded;
                    status = recorded;
                }
            }

            if (status is TxStatus.Committed or TxStatus.Aborted)
            {
                yield return new SnapshotEntry
                {
                    Key = string.Empty,
                    // No value: a receiver that predates the decision slot
                    // skips a committed row that carries none.
                    Value = null!,
                    TransactionId = txid,
                    SettledDecision = status == TxStatus.Committed,
                };
            }
            else
            {
                yield return new SnapshotEntry
                {
                    Key = string.Empty,
                    Value = null!,
                    TransactionId = txid,
                };
            }
        }
    }

    /// <summary>
    /// Walks every shard's leaf chain on the source tree and emits a
    /// committed tombstone row (<see cref="SnapshotEntry.IsTombstone"/> set,
    /// <see cref="SnapshotEntry.IsPrepared"/> clear) for every key a leaf
    /// still holds as a tombstone, stamped with the tombstone's own HLC. A
    /// tombstone authored after a bounded export's <paramref name="asOfHlc"/>
    /// is left to the incremental stream.
    /// </summary>
    private async IAsyncEnumerable<SnapshotEntry> EnumerateTombstonesAsync(
        string treeName,
        HybridLogicalClock asOfHlc,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var registry = _grainFactory.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(treeName).ConfigureAwait(false);
        var shardMap = await registry.GetShardMapAsync(treeName).ConfigureAwait(false)
            ?? ShardMap.GetOrCreateDefaultShared(
                LatticeConstants.DefaultVirtualShardCount,
                LatticeConstants.DefaultShardCount);

        var hasUpperBound = asOfHlc != HybridLogicalClock.Zero;

        // An empty version vector dominates nothing, so every leaf answers
        // with every entry it holds, tombstones included.
        var everything = new VersionVector();

        foreach (var shardIndex in shardMap.GetPhysicalShardIndices())
        {
            cancellationToken.ThrowIfCancellationRequested();

            var shard = _grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync().ConfigureAwait(false);
            while (leafId is not null)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var leaf = _grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
                var delta = await leaf.GetDeltaSinceAsync(everything).ConfigureAwait(false);
                foreach (var (key, lww) in delta.Entries)
                {
                    if (!lww.IsTombstone)
                    {
                        continue;
                    }

                    if (hasUpperBound && lww.Timestamp > asOfHlc)
                    {
                        continue;
                    }

                    yield return new SnapshotEntry
                    {
                        Key = key,
                        Value = Array.Empty<byte>(),
                        Timestamp = lww.Timestamp,
                        IsTombstone = true,
                    };
                }

                leafId = await leaf.GetNextSiblingAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Walks every shard's leaf chain on the source tree and emits a
    /// <see cref="SnapshotEntry"/> with <see cref="SnapshotEntry.IsPrepared"/>
    /// set for every <c>(transactionId, key)</c> pair in any leaf's
    /// pending-tx bucket whose <paramref name="snap0"/> status is
    /// <see cref="TxStatus.InFlight"/>, <see cref="TxStatus.Indeterminate"/>,
    /// or absent. Sagas snap0 had as
    /// <see cref="TxStatus.Committed"/> / <see cref="TxStatus.Aborted"/>
    /// are intentionally skipped here because the committed-projection
    /// pass under the same registry snapshot has already folded their
    /// per-key visibility into its emitted rows.
    /// <para>
    /// <b>What absence means to the receiver, and why it changed.</b> A txid
    /// absent from <paramref name="snap0"/> means the source has no decision
    /// for it, so the receiver is right to treat it as still preparing. That
    /// reading used to be unsound for one case: a saga that <i>committed</i> and
    /// whose decision then aged out of the retention window was dropped from the
    /// snapshot, so it arrived at the receiver as absence and was read as still
    /// preparing. The terminal that would have corrected it was already outside
    /// the incremental stream the receiver drains after the snapshot, so nothing
    /// on either side could repair the divergence: the source held "committed",
    /// the receiver held "preparing", permanently. The registry now carries such
    /// a row explicitly as <see cref="TxStatus.Indeterminate"/> instead of
    /// dropping it, so absence in this payload once again means only what it
    /// says, and the aged-out case is visible as the distinct thing it is.
    /// </para>
    /// </summary>
    private async IAsyncEnumerable<SnapshotEntry> EnumeratePreparedAsync(
        string treeName,
        Dictionary<Guid, TxStatus> snap0,
        HashSet<Guid> recordedResolved,
        HybridLogicalClock asOfHlc,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var registry = _grainFactory.GetLatticeRegistry();
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
                    // had as InFlight, Indeterminate, or absent, so the
                    // receiver routes them into its per-tx pending bucket
                    // where the post-snapshot incremental WAL's terminal
                    // record will flip them atomically.
                    //
                    // Indeterminate must be on the SHIPPING side of this test,
                    // not the skipping side. The committed pass runs under the
                    // same snap0 and does not surface an indeterminate saga's
                    // keys, so skipping here too would drop the prepared rows
                    // from the export entirely and lose the write.
                    //
                    // An Indeterminate row whose decision is still STORED
                    // (its tombstone aged out of the readable window) is
                    // resolved to that recorded verdict first (#4481). The
                    // receiver's registry has no row for the saga, so a key
                    // shipped as a prepared row there reads as in flight and
                    // serves its pre-saga value beside the keys the saga's
                    // terminal already drained - a split no mechanism on
                    // either side repairs. The recorded verdict is what the
                    // source's own leaf sweep finishes the stranded prepare
                    // by, so overriding snap0 with it for both passes ships
                    // the saga whole, as committed rows (or not at all on an
                    // abort), exactly as a saga snap0 had as decided. This is
                    // a transfer of state the source owns, not a disclosure
                    // to a reader, so the read path's retention mask does not
                    // apply. An Indeterminate with no stored verdict (an
                    // unreachable cross-tree delegation) still ships as
                    // prepared rows, and a row already purged reads as absent
                    // and exports the split the source itself serves (#4508).
                    if (snap0.TryGetValue(m.TransactionId, out var status)
                        && status == TxStatus.Indeterminate
                        && recordedResolved.Add(m.TransactionId))
                    {
                        var recorded = await Orleans.Lattice.BPlusTree.Grains.TxRegistryRouting
                            .GetRegistry(_grainFactory, treeName, m.TransactionId)
                            .GetRecordedStatusAsync(m.TransactionId)
                            .ConfigureAwait(false);
                        if (recorded is TxStatus.Committed or TxStatus.Aborted)
                        {
                            snap0[m.TransactionId] = recorded;
                        }
                    }

                    var committedBucket = snap0.TryGetValue(m.TransactionId, out status)
                        && status == TxStatus.Committed;

                    if (hasUpperBound && m.Timestamp > asOfHlc)
                    {
                        // The prepared mutation was authored after the
                        // snapshot's as-of cut; defer it to the
                        // post-snapshot incremental WAL stream rather
                        // than leaking it across the cut.
                        continue;
                    }

                    if (committedBucket)
                    {
                        // A resident bucket of a committed saga - one snap0
                        // has as Committed whose terminal has not drained this
                        // leaf yet, or a recorded commit behind an aged-out
                        // row - ships as the committed value the terminal (or
                        // the source's leaf sweep) will install. The
                        // committed-projection pass need not enumerate a key
                        // held only in a pending bucket, so this pass emits
                        // it; without it such a saga left the export entirely
                        // and the receiver depended on the incremental stream
                        // re-shipping its prepares. A committed delete ships as a
                        // committed tombstone row, not as an absence: a
                        // bootstrap can land on a receiver copy that still
                        // holds the key's older value (a peer that fell off
                        // the log re-bootstraps in place), and an absence
                        // would leave that value beside the saga's other keys.
                        yield return new SnapshotEntry
                        {
                            Key = m.Key,
                            Value = m.IsTombstone ? Array.Empty<byte>() : (m.Value ?? Array.Empty<byte>()),
                            Timestamp = m.Timestamp,
                            IsTombstone = m.IsTombstone,
                            ExpiresAtTicks = m.ExpiresAtTicks,
                            Delta = m.Delta,
                            Mode = m.Mode,
                        };

                        continue;
                    }

                    if (snap0.TryGetValue(m.TransactionId, out status)
                        && status is TxStatus.Committed or TxStatus.Aborted)
                    {
                        if (status == TxStatus.Committed && m.IsTombstone)
                        {
                            // The committed pass reads a committed saga's
                            // pending delete as an absent key and emits
                            // nothing, so a receiver re-bootstrapping over a
                            // copy that still holds the key would keep it
                            // (#4504). Ship the delete as a committed
                            // tombstone row instead.
                            yield return new SnapshotEntry
                            {
                                Key = m.Key,
                                Value = Array.Empty<byte>(),
                                Timestamp = m.Timestamp,
                                IsTombstone = true,
                            };
                        }

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

