using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Saga records the source's write-ahead log retained from before a snapshot
/// bootstrap's cut, re-shipped by the incremental stream afterwards (issue
/// #4482). The source shipper resumes from its own per-partition cursors, so a
/// peer bootstrapped after falling off the log can be sent a prepare from
/// before the cut whose terminal's partition was already trimmed. The receiver
/// must settle it against the outcome the snapshot exported for the saga, never
/// stage it in a pending bucket no terminal will drain.
/// </summary>
public partial class BootstrapAtomicVisibilityTests
{
    private const string PreCutOrigin = "snap-precut-origin";

    private IReplicationApplier ReceiverApplier =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First()
            .SiloHost.Services.GetRequiredService<IReplicationApplier>();

    /// <summary>
    /// Runs a two-key saga on <paramref name="sourceTree"/> to completion
    /// (both shard terminals drained), optionally lets its decision age out,
    /// and bootstraps <paramref name="receiverTree"/> from the export through
    /// the real drain seams.
    /// </summary>
    private async Task<(string KeyA, string KeyB, Guid TxId)> BootstrapReceiverOverSettledSagaAsync(
        string sourceTree, string receiverTree, bool committed, bool ageOut)
    {
        var (keyA, keyB) = AgedKeysOnDistinctShards();
        keyA += $"-{receiverTree}";
        keyB += $"-{receiverTree}";
        while (LatticeSharding.GetShardIndex(keyA, LatticeConstants.DefaultShardCount)
            == LatticeSharding.GetShardIndex(keyB, LatticeConstants.DefaultShardCount))
        {
            keyB += "x";
        }

        var txid = Guid.NewGuid();
        var source = _cluster.Client.GetGrain<IReplicationApplyGrain>(sourceTree);
        await source.ApplyPreparedSetAsync(
            keyA, new byte[] { 1 }, Hlc(6_000), ClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 0);
        await source.ApplyPreparedSetAsync(
            keyB, new byte[] { 2 }, Hlc(6_000), ClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 1);
        foreach (var key in new[] { keyA, keyB })
        {
            await source.ApplyTxTerminalAsync(
                txid, committed, LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount),
                Hlc(6_100), ClusterId);
        }

        if (ageOut)
        {
            await _cluster.Client.GetGrain<ITxRegistryGrain>(sourceTree).ForgetAsync(txid);
            await Task.Delay(TimeSpan.FromMilliseconds(700));
        }

        var stream = await _provider.ExportAsync(sourceTree, HybridLogicalClock.Zero);
        var entries = await DrainAsync(stream);
        Assert.That(entries.Any(e => e.IsDecision && e.TransactionId == txid), Is.True,
            "precondition: the export carries a decision row for the settled saga");

        var applier = ReceiverApplier;
        using (LatticeBootstrapApplyContext.BeginScope())
        {
            foreach (var entry in entries.Where(e => e.IsDecision ? e.TransactionId == txid : e.Key == keyA || e.Key == keyB))
            {
                if (entry.IsDecision)
                {
                    await LatticeBootstrapCoordinatorGrain.ApplySettledDecisionAsync(_cluster.Client, receiverTree, entry);
                }
                else if (LatticeBootstrapCoordinatorGrain.ToSnapshotWalRecord(
                    entry, receiverTree, PreCutOrigin, LatticeMergeMode.LwwRegister) is { } record)
                {
                    await applier.ApplyAsync(record);
                }
            }
        }

        return (keyA, keyB, txid);
    }

    /// <summary>The pre-cut prepare the source's retained log re-ships after the bootstrap; its terminal is trimmed.</summary>
    private Task ReshipPreCutPrepareAsync(string receiverTree, string key, byte value, Guid txid, int index, HybridLogicalClock hlc) =>
        ReceiverApplier.ApplyAsync(new WalRecord
        {
            TreeId = receiverTree,
            Op = MutationKind.Set,
            Key = key,
            Value = new[] { value },
            Timestamp = hlc,
            OriginClusterId = PreCutOrigin,
            TransactionId = txid,
            IsPrepared = true,
            AtomicBatchSize = 2,
            AtomicBatchIndex = index,
        });

    /// <summary>Every pending-bucket mutation the receiver tree's leaves hold for <paramref name="txid"/>.</summary>
    private async Task<List<string>> PendingKeysForAsync(string tree, Guid txid)
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physical = await registry.ResolveAsync(tree);
        var map = await registry.GetShardMapAsync(tree)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var slots = Enumerable.Range(0, map.VirtualShardCount).ToArray();
        var keys = new List<string>();
        foreach (var shardIndex in map.GetPhysicalShardIndices())
        {
            var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{physical}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync();
            while (leafId is not null)
            {
                var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(leafId.Value);
                foreach (var m in await leaf.GetPendingMutationsForSlotsAsync(slots, map.VirtualShardCount))
                {
                    if (m.TransactionId == txid)
                    {
                        keys.Add(m.Key);
                    }
                }

                leafId = await leaf.GetNextSiblingAsync();
            }
        }

        return keys;
    }

    [Test]
    public async Task Export_ships_a_committed_saga_whose_buckets_are_still_resident_as_committed_rows()
    {
        // The saga is decided Committed but its terminal has not drained the
        // leaves yet, so both keys exist only in pending buckets. The
        // committed-projection pass does not enumerate such a key, and the
        // prepared pass skips a decided saga, so the saga used to leave the
        // export entirely.
        const string tree = "snap-committed-resident";
        var (keyA, keyB) = AgedKeysOnDistinctShards();
        var txid = Guid.NewGuid();
        var source = _cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        await source.ApplyPreparedSetAsync(
            keyA, new byte[] { 1 }, Hlc(7_000), ClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 0);
        await source.ApplyPreparedSetAsync(
            keyB, new byte[] { 2 }, Hlc(7_000), ClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 1);
        await _cluster.Client.GetGrain<ITxRegistryGrain>(tree).MarkCommittedAsync(txid);

        var entries = await DrainAsync(await _provider.ExportAsync(tree, HybridLogicalClock.Zero));

        var committed = entries
            .Where(e => !e.IsPrepared && !e.IsDecision && (e.Key == keyA || e.Key == keyB))
            .Select(e => (e.Key, e.Value[0]))
            .Distinct()
            .OrderBy(r => r.Key, StringComparer.Ordinal)
            .Select(r => r.Item2)
            .ToArray();
        Assert.That(committed, Is.EqualTo(string.CompareOrdinal(keyA, keyB) < 0 ? new byte[] { 1, 2 } : new byte[] { 2, 1 }),
            "both keys of the committed saga ship as committed rows");
    }

    [TestCase(false, TestName = "Pre_cut_prepare_reshipped_after_bootstrap_settles_against_the_exported_commit")]
    [TestCase(true, TestName = "Pre_cut_prepare_reshipped_after_bootstrap_settles_against_an_aged_out_commit_with_no_resident_bucket")]
    public async Task Pre_cut_prepare_reshipped_after_bootstrap_settles_against_the_exported_commit(bool ageOut)
    {
        var sourceTree = ageOut ? AgedTree : "snap-precut-source";
        var receiverTree = ageOut ? "snap-precut-aged-receiver" : "snap-precut-receiver";
        var (_, keyB, txid) = await BootstrapReceiverOverSettledSagaAsync(sourceTree, receiverTree, committed: true, ageOut);

        await ReshipPreCutPrepareAsync(receiverTree, keyB, 2, txid, index: 1, Hlc(6_000));

        var receiver = _cluster.Client.GetGrain<ILattice>(receiverTree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await PendingKeysForAsync(receiverTree, txid), Is.Empty,
                "a re-shipped prepare of a saga the snapshot settled must not be staged where no terminal will drain it");
            Assert.That(await receiver.GetAsync(keyB), Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task Pre_cut_prepare_reshipped_after_bootstrap_of_an_aborted_saga_is_dropped()
    {
        const string receiverTree = "snap-precut-abort-receiver";
        var (_, keyB, txid) = await BootstrapReceiverOverSettledSagaAsync(
            "snap-precut-abort-source", receiverTree, committed: false, ageOut: false);

        await ReshipPreCutPrepareAsync(receiverTree, keyB, 2, txid, index: 1, Hlc(6_000));

        var receiver = _cluster.Client.GetGrain<ILattice>(receiverTree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await PendingKeysForAsync(receiverTree, txid), Is.Empty);
            Assert.That(await receiver.GetAsync(keyB), Is.Null, "an aborted saga's prepare never becomes visible");
        });
    }

    [Test]
    public async Task Pre_cut_prepare_settled_as_committed_stays_below_a_newer_write_on_the_key()
    {
        // The re-shipped prepare is materialised at its own source clock, so
        // last-writer-wins keeps it below a write the key took after the saga.
        const string receiverTree = "snap-precut-newer-receiver";
        var (_, keyB, txid) = await BootstrapReceiverOverSettledSagaAsync(
            "snap-precut-newer-source", receiverTree, committed: true, ageOut: false);
        await ReceiverApplier.ApplyAsync(new WalRecord
        {
            TreeId = receiverTree,
            Op = MutationKind.Set,
            Key = keyB,
            Value = new byte[] { 77 },
            Timestamp = Hlc(9_000),
            OriginClusterId = PreCutOrigin,
        });

        await ReshipPreCutPrepareAsync(receiverTree, keyB, 2, txid, index: 1, Hlc(6_000));

        var receiver = _cluster.Client.GetGrain<ILattice>(receiverTree);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await receiver.GetAsync(keyB), Is.EqualTo(new byte[] { 77 }),
                "the settled prepare must not overwrite the newer write");
            Assert.That(await PendingKeysForAsync(receiverTree, txid), Is.Empty);
        });
    }
}
