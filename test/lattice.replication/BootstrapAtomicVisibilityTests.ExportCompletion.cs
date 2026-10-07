using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4685: the export reads each leaf at its own instant, so a saga that
/// decides while it reads - one the export's registry snapshot (snap0) never
/// saw - could ship with some keys post-saga (drained) and the rest pre-saga or
/// not at all, and no decision row naming it. The bootstrapped receiver then
/// served it split until the incremental stream delivered the saga's last
/// source-shard terminal. The export's completion step ships every such saga
/// whole from the source's write-ahead log. Runs the real export, registry,
/// shard roots, WAL and receiver apply path.
/// </summary>
public partial class BootstrapAtomicVisibilityTests
{
    private const string CrdtTreePrefix = "snap-crdt-";

    private async Task StageSagaAsync(string sourceTree, Guid txid, IReadOnlyList<string> keys, byte value)
    {
        var source = _cluster.Client.GetGrain<IReplicationApplyGrain>(sourceTree);
        for (var i = 0; i < keys.Count; i++)
        {
            await source.ApplyPreparedSetAsync(
                keys[i], new byte[] { value }, Hlc(DateTime.UtcNow.Ticks), ClusterId, sourceVectorClock: null,
                expiresAtTicks: 0, txid, atomicBatchSize: keys.Count, atomicBatchIndex: i);
        }
    }

    /// <summary>Commits <paramref name="txid"/> and drains only <paramref name="drainedKey"/>'s shard.</summary>
    private async Task CommitAndDrainOneShardAsync(string sourceTree, Guid txid, string drainedKey)
    {
        var physical = await _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(sourceTree);
        await TxRegistryRouting.GetRegistry(_cluster.Client, sourceTree, txid).MarkCommittedAsync(txid);
        await _cluster.Client
            .GetGrain<IShardRootGrain>($"{physical}/{LatticeSharding.GetShardIndex(drainedKey, LatticeConstants.DefaultShardCount)}")
            .AppendTxTerminalAsync(txid, true);
    }

    /// <summary>Bootstraps <paramref name="receiverTree"/> from <paramref name="entries"/> through the drain's seams.</summary>
    private async Task ImportAsync(List<SnapshotEntry> entries, string receiverTree, LatticeMergeMode mode = LatticeMergeMode.LwwRegister)
    {
        var applier = ReceiverApplier;
        using (LatticeBootstrapApplyContext.BeginScope())
        {
            foreach (var entry in entries)
            {
                if (entry.IsDecision)
                {
                    await LatticeBootstrapCoordinatorGrain.ApplySettledDecisionAsync(_cluster.Client, receiverTree, entry);
                }
                else if (LatticeBootstrapCoordinatorGrain.ToSnapshotWalRecord(entry, receiverTree, PreCutOrigin, mode) is { } record)
                {
                    await applier.ApplyAsync(record);
                }
            }
        }
    }

    private async Task<List<SnapshotEntry>> ExportAsync(string sourceTree)
    {
        try
        {
            return await DrainAsync(await _provider.ExportAsync(sourceTree, HybridLogicalClock.Zero));
        }
        finally
        {
            _provider.AfterPreparedPassForTesting = null;
            _provider.BeforeCommittedEntryForTesting = null;
        }
    }

    private async Task AssertWholeAsync(string receiverTree, IReadOnlyList<string> keys, byte expected)
    {
        var read = await _cluster.Client.GetGrain<ILattice>(receiverTree).GetManyAsync(keys.ToList());
        Assert.That(read.Count, Is.EqualTo(keys.Count),
            "the bootstrapped receiver serves the saga split to one atomic read: "
            + string.Join(", ", keys.Select(k => $"{k} {(read.ContainsKey(k) ? "visible" : "absent")}")));
        Assert.That(read.Values.All(v => v.SequenceEqual(new[] { expected })), Is.True, "every key holds the saga's committed value");
    }

    [Test]
    public async Task Export_never_ships_part_of_a_saga_that_decides_after_the_prepared_pass_so_the_receiver_never_serves_it_split()
    {
        // A saga absent from snap0 stages its prepares after the prepared pass
        // has read their leaves, commits, and drains one key before the
        // committed pass reads it: the other key is still a bucket.
        var suffix = Guid.NewGuid().ToString("N");
        var sourceTree = $"snap-postsnap-src-{suffix}";
        var receiverTree = $"snap-postsnap-rcv-{suffix}";
        var keys = KeysOnDistinctShards($"post-{suffix}", 2);
        var txid = Guid.NewGuid();
        await _cluster.Client.GetGrain<IReplicationApplyGrain>(sourceTree)
            .ApplySetAsync("seed", new byte[] { 0 }, Hlc(DateTime.UtcNow.Ticks), ClusterId, sourceVectorClock: null, expiresAtTicks: 0);

        _provider.AfterPreparedPassForTesting = async () =>
        {
            await StageSagaAsync(sourceTree, txid, keys, 7);
            await CommitAndDrainOneShardAsync(sourceTree, txid, keys[0]);
        };
        var entries = await ExportAsync(sourceTree);

        Assert.That(entries.Any(e => e.IsPrepared && e.TransactionId == txid), Is.False,
            "precondition: the prepared pass ran before the saga staged its prepares");
        await ImportAsync(entries, receiverTree);
        await AssertWholeAsync(receiverTree, keys, 7);
    }

    [TestCase(true, TestName = "Export_ships_whole_a_saga_that_decides_between_two_committed_pass_reads_drained_key_read_first")]
    [TestCase(false, TestName = "Export_ships_whole_a_saga_that_decides_between_two_committed_pass_reads_drained_key_read_second")]
    public async Task Export_ships_whole_a_saga_that_decides_between_two_committed_pass_reads(bool drainKeyReadFirst)
    {
        // The saga runs just before the committed pass reads its first key and
        // drains one shard. Drained key read first: only that key holds a
        // pre-saga row, so the pass reads it post-saga and never reaches the
        // other, a bucket of a saga snap0 does not know (post, then absent).
        // Drained key read second: both keys hold a pre-saga row, so the pass
        // reads the first pre-saga and the second post-saga (pre, then post).
        var suffix = Guid.NewGuid().ToString("N");
        var sourceTree = $"snap-walk-src-{suffix}";
        var receiverTree = $"snap-walk-rcv-{suffix}";
        var keys = KeysOnDistinctShards($"walk-{suffix}", 2);
        var txid = Guid.NewGuid();
        var source = _cluster.Client.GetGrain<IReplicationApplyGrain>(sourceTree);
        foreach (var key in drainKeyReadFirst ? keys.Take(1) : keys)
        {
            await source.ApplySetAsync(key, new byte[] { 1 }, Hlc(DateTime.UtcNow.Ticks), ClusterId, sourceVectorClock: null, expiresAtTicks: 0);
        }

        var ran = false;
        _provider.BeforeCommittedEntryForTesting = async key =>
        {
            if (ran || !keys.Contains(key))
            {
                return;
            }

            ran = true;
            var other = keys.Single(k => k != key);
            await StageSagaAsync(sourceTree, txid, keys, 9);
            await CommitAndDrainOneShardAsync(sourceTree, txid, drainKeyReadFirst ? key : other);
        };
        var entries = await ExportAsync(sourceTree);

        Assert.That(ran, Is.True, "precondition: the saga ran inside the committed pass");
        await ImportAsync(entries, receiverTree);
        await AssertWholeAsync(receiverTree, keys, 9);
    }

    [Test]
    public async Task Export_completion_ships_a_CRDT_saga_delta_without_double_counting_a_key_already_drained()
    {
        // A PN-counter saga drains one key before the committed pass reads it,
        // so the pass ships that key's folded state and the completion ships the
        // saga's delta for it again. The delta is a join, so the receiver counts
        // the increment once.
        var suffix = Guid.NewGuid().ToString("N");
        var sourceTree = $"{CrdtTreePrefix}src-{suffix}";
        var receiverTree = $"{CrdtTreePrefix}rcv-{suffix}";
        var keys = KeysOnDistinctShards($"pn-{suffix}", 2);
        var txid = Guid.NewGuid();
        var delta = new PnCounterDelta
        {
            Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["A"] = 1 },
            Decrements = new Dictionary<string, long>(StringComparer.Ordinal),
        };
        var state = new PnCounter();
        state.MergeDelta(delta);
        var source = _cluster.Client.GetGrain<IReplicationApplyGrain>(sourceTree);
        await source.ApplySetAsync("seed", JsonLatticeSerializer<PnCounter>.Default.Serialize(new PnCounter()), Hlc(DateTime.UtcNow.Ticks), ClusterId, sourceVectorClock: null, expiresAtTicks: 0);

        _provider.AfterPreparedPassForTesting = async () =>
        {
            for (var i = 0; i < keys.Length; i++)
            {
                await source.ApplyPreparedSetAsync(
                    keys[i], JsonLatticeSerializer<PnCounter>.Default.Serialize(state), Hlc(DateTime.UtcNow.Ticks), ClusterId,
                    sourceVectorClock: null, expiresAtTicks: 0, txid, atomicBatchSize: keys.Length, atomicBatchIndex: i,
                    delta: JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(delta), mode: LatticeMergeMode.PnCounter);
            }

            await CommitAndDrainOneShardAsync(sourceTree, txid, keys[0]);
        };
        var entries = await ExportAsync(sourceTree);

        Assert.That(entries.Any(e => !e.IsPrepared && !e.IsDecision && e.Key == keys[0] && e.Delta is null), Is.True,
            "precondition: the committed pass shipped the drained key's folded state");
        await ImportAsync(entries, receiverTree, LatticeMergeMode.PnCounter);

        var lattice = _cluster.Client.GetGrain<ILattice>(receiverTree);
        foreach (var key in keys)
        {
            var bytes = await lattice.GetAsync(key);
            Assert.That(bytes, Is.Not.Null, $"{key}: the saga's increment reached the receiver");
            Assert.That(JsonLatticeSerializer<PnCounter>.Default.Deserialize(bytes!).Value, Is.EqualTo(1),
                $"{key}: the increment is counted exactly once");
        }
    }

    [Test]
    public async Task Export_holds_the_tree_decision_purges_while_it_reads_and_releases_them_when_done()
    {
        // A saga that decides during the export must still be recorded when the
        // completion reads the registry, so the export holds decision purges.
        var suffix = Guid.NewGuid().ToString("N");
        var sourceTree = $"snap-hold-src-{suffix}";
        await _cluster.Client.GetGrain<IReplicationApplyGrain>(sourceTree)
            .ApplySetAsync("seed", new byte[] { 0 }, Hlc(DateTime.UtcNow.Ticks), ClusterId, sourceVectorClock: null, expiresAtTicks: 0);
        var physical = await _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(sourceTree);
        var holds = _cluster.Client.GetGrain<IWalPurgeHoldGrain>(physical);

        var heldDuring = false;
        _provider.AfterPreparedPassForTesting = async () =>
            heldDuring = (await holds.GetAsync()).Keys.Any(k => k.StartsWith(LatticeSnapshotProvider.ExportPurgeHoldPrefix, StringComparison.Ordinal));
        await ExportAsync(sourceTree);

        Assert.That(heldDuring, Is.True, "the export holds the tree's decision purges while it reads");
        Assert.That((await holds.GetAsync()).Keys.Any(k => k.StartsWith(LatticeSnapshotProvider.ExportPurgeHoldPrefix, StringComparison.Ordinal)),
            Is.False, "the export releases its hold when it ends");
    }

    [Test]
    public async Task Export_fails_closed_when_the_log_no_longer_holds_a_saga_it_must_complete()
    {
        // The saga decides during the export, but a trim removes its prepares
        // from the log before the completion reads them: the export cannot ship
        // the saga whole, so it fails with a retryable fault.
        var suffix = Guid.NewGuid().ToString("N");
        var sourceTree = $"snap-trim-src-{suffix}";
        var keys = KeysOnDistinctShards($"trim-{suffix}", 2);
        var txid = Guid.NewGuid();
        await _cluster.Client.GetGrain<IReplicationApplyGrain>(sourceTree)
            .ApplySetAsync("seed", new byte[] { 0 }, Hlc(DateTime.UtcNow.Ticks), ClusterId, sourceVectorClock: null, expiresAtTicks: 0);
        var physical = await _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(sourceTree);
        var storage = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services.GetRequiredService<IWalStorageProvider>();
        var partitions = Math.Max(1, LatticeSnapshotProviderUnitTests.TestOptions().Get(sourceTree).ReplogPartitions);

        _provider.AfterPreparedPassForTesting = async () =>
        {
            await StageSagaAsync(sourceTree, txid, keys, 5);
            await CommitAndDrainOneShardAsync(sourceTree, txid, keys[0]);
            for (var partition = 0; partition < partitions; partition++)
            {
                var head = await _cluster.Client.GetGrain<IWalShardGrain>($"{physical}/{partition}").GetNextSequenceAsync(CancellationToken.None);
                if (head > 0)
                {
                    await storage.TrimAsync(physical, partition, head - 1, CancellationToken.None);
                }
            }
        };

        Assert.That(async () => await ExportAsync(sourceTree), Throws.InstanceOf<TimeoutException>(),
            "an export that cannot complete a saga from the log must fail rather than ship it split");
    }
}
