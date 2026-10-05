using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4618 on a real in-process cluster: the online-resize (and online
/// snapshot) mirror carries the writes a source shard accepts after the drain
/// has passed their keys onto the destination copy R - including a typed CRDT
/// delta apply, a CRDT delta batch, and a bulk append, which it used to drop -
/// and joins a CRDT row into the one R folded itself, so a saga's terminal fold
/// on R at R's own (later) stamp does not discard a non-atomic contribution the
/// source mirrored below it.
/// <para>
/// Each test switches a source shard to mirror onto a destination shard before
/// any write, so nothing reaches R except through the mirror - exactly the
/// position of a write that arrives after the drain has read past its key. R's
/// leaf clock is pushed an hour ahead of the source's, as a destination's can
/// be, so a CRDT fold R mints sorts above every stamp the source issues.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ResizeMirrorCrdtAndBulkIntegrationTests
{
    private const string OperationId = "op-mirror-crdt";
    private const string Origin = "mirror-crdt-origin";
    private const string GCounterPrefix = "rmcrdt-gc-";
    private const string OrSetPrefix = "rmcrdt-os-";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    private sealed record Mirror(ILattice Source, IShardRootGrain SourceShard, ILattice Destination, IShardRootGrain DestinationShard);

    /// <summary>
    /// Registers a source tree and a destination tree, optionally fills the
    /// source past one leaf so its root is an internal node, switches the
    /// source's shard to mirror onto the destination, and pushes the
    /// destination's leaf clock an hour ahead of the source's.
    /// </summary>
    private async Task<Mirror> CreateMirrorAsync(string prefix, int fill = 0)
    {
        var source = $"{prefix}{Guid.NewGuid():N}";
        var destination = $"{source}-r";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(source, new TreeRegistryEntry { ShardCount = 1 });
        await registry.RegisterAsync(destination, new TreeRegistryEntry { ShardCount = 1 });
        var sourceShard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{source}/0");
        var destinationShard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{destination}/0");

        var ahead = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.AddHours(1).UtcTicks, Counter = 0 };
        await destinationShard.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["zz-clock"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("ahead"), ahead),
        });
        await sourceShard.SetAsync("aa-seed", Encoding.UTF8.GetBytes("seed"));
        if (fill > 0)
        {
            await sourceShard.SetManyAsync(Enumerable.Range(0, fill)
                .Select(i => new KeyValuePair<string, byte[]>($"ab-fill-{i:D4}", Encoding.UTF8.GetBytes("f")))
                .ToList());
        }

        await sourceShard.BeginShadowForwardAsync(destination, OperationId, source);

        return new Mirror(
            _cluster.GrainFactory.GetGrain<ILattice>(source), sourceShard,
            _cluster.GrainFactory.GetGrain<ILattice>(destination), destinationShard);
    }

    private static byte[] Increment(string replica, long by = 1) =>
        JsonLatticeSerializer<GCounterDelta>.Default.Serialize(
            new GCounterDelta { Increments = new Dictionary<string, long>(StringComparer.Ordinal) { [replica] = by } });

    private static async Task<GCounter?> CounterAsync(ILattice tree, string key) =>
        await tree.GetAsync(key) is { } bytes ? JsonLatticeSerializer<GCounter>.Default.Deserialize(bytes) : null;

    [Test]
    public async Task A_typed_CRDT_delta_applied_after_the_drain_passed_its_key_reaches_the_destination()
    {
        var m = await CreateMirrorAsync(GCounterPrefix);

        await m.Source.ApplyCrdtDeltaAsync("k", LatticeMergeMode.GCounter, Increment("A", 3));

        Assert.That((await CounterAsync(m.Source, "k"))?.Value, Is.EqualTo(3), "PRECONDITION: the source applied the delta");
        Assert.That((await CounterAsync(m.Destination, "k"))?.Value, Is.EqualTo(3),
            "the destination must hold the CRDT delta the source applied while it mirrored");
    }

    [TestCase(0, TestName = "A_CRDT_delta_batch_applied_after_the_drain_passed_its_keys_reaches_the_destination_flat")]
    [TestCase(300, TestName = "A_CRDT_delta_batch_applied_after_the_drain_passed_its_keys_reaches_the_destination_deep")]
    public async Task A_CRDT_delta_batch_applied_after_the_drain_passed_its_keys_reaches_the_destination(int fill)
    {
        var m = await CreateMirrorAsync(GCounterPrefix, fill);
        var keys = new[] { "b1", "b2", "zb3" };

        await m.Source.ApplyCrdtDeltaManyAsync(
            keys.Select((k, i) => new KeyValuePair<string, byte[]>(k, Increment("A", i + 1))).ToList(),
            LatticeMergeMode.GCounter);

        for (var i = 0; i < keys.Length; i++)
        {
            Assert.That((await CounterAsync(m.Source, keys[i]))?.Value, Is.EqualTo(i + 1), $"PRECONDITION: {keys[i]} on the source");
            Assert.That((await CounterAsync(m.Destination, keys[i]))?.Value, Is.EqualTo(i + 1),
                $"{keys[i]}: the destination must hold the batch the source applied while it mirrored");
        }
    }

    [Test]
    public async Task A_bulk_append_after_the_drain_passed_its_keys_reaches_the_destination()
    {
        var m = await CreateMirrorAsync("rmcrdt-bulk-", fill: 300);
        var chunk = Enumerable.Range(0, 400)
            .Select(i => new KeyValuePair<string, byte[]>($"bk-{i:D4}", Encoding.UTF8.GetBytes($"v{i}")))
            .ToList();

        Assert.That(await m.Source.BulkAppendChunkAsync("bulk-op", chunk), Is.EqualTo(chunk.Count));

        foreach (var key in new[] { "bk-0000", "bk-0199", "bk-0399" })
        {
            Assert.That(await m.Source.GetAsync(key), Is.Not.Null, $"PRECONDITION: {key} on the source");
            var onDestination = await m.Destination.GetAsync(key);
            Assert.That(onDestination is null ? null : Encoding.UTF8.GetString(onDestination), Is.EqualTo($"v{int.Parse(key[3..])}"),
                $"{key}: the destination must hold the row the source's bulk append stored while it mirrored");
        }

        Assert.That(await m.Destination.CountAsync(), Is.EqualTo(chunk.Count + 1),
            "the destination holds every appended row (plus its own clock row)");
    }

    [Test]
    public async Task A_retried_bulk_append_mirrors_the_rows_its_first_attempt_stored()
    {
        // The first attempt completes on the source before the mirror begins
        // (standing in for an attempt whose mirror was lost to a crash after the
        // shard recorded completion); the caller's same-operation retry is a
        // no-op locally and must still mirror the rows.
        var source = $"rmcrdt-bulkretry-{Guid.NewGuid():N}";
        var destination = $"{source}-r";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(source, new TreeRegistryEntry { ShardCount = 1 });
        await registry.RegisterAsync(destination, new TreeRegistryEntry { ShardCount = 1 });
        var sourceShard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{source}/0");
        var chunk = Enumerable.Range(0, 5)
            .Select(i => new KeyValuePair<string, byte[]>($"bk-{i}", Encoding.UTF8.GetBytes($"v{i}")))
            .ToList();
        await sourceShard.BulkAppendAsync("retry-op", chunk);
        await sourceShard.BeginShadowForwardAsync(destination, OperationId, source);

        await sourceShard.BulkAppendAsync("retry-op", chunk);

        var dest = _cluster.GrainFactory.GetGrain<ILattice>(destination);
        foreach (var (key, value) in chunk)
        {
            var onDestination = await dest.GetAsync(key);
            Assert.That(onDestination is null ? null : Encoding.UTF8.GetString(onDestination), Is.EqualTo(Encoding.UTF8.GetString(value)),
                $"{key}: the retry must mirror the row the append stored");
        }
    }

    [TestCase(true, TestName = "A_saga_prepared_CRDT_delta_and_a_non_atomic_increment_both_reach_the_destination_terminal_first")]
    [TestCase(false, TestName = "A_saga_prepared_CRDT_delta_and_a_non_atomic_increment_both_reach_the_destination_increment_first")]
    public async Task A_saga_prepared_CRDT_delta_and_a_non_atomic_increment_both_reach_the_destination(bool terminalFirst)
    {
        // The saga's CRDT delta is prepared on the source and mirrored as a
        // prepare; its terminal folds it on R at R's own stamp, an hour ahead.
        // A non-atomic increment on the source is mirrored as the source's row,
        // stamped below that fold: a last-writer-wins merge on R kept only the
        // fold, losing the increment once R became authoritative.
        var m = await CreateMirrorAsync(GCounterPrefix);
        var key = "k";
        var saga = new GCounterDelta { Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["A"] = 1 } };
        var sagaState = new GCounter();
        sagaState.MergeDelta(saga);
        var txid = Guid.NewGuid();
        var apply = _cluster.GrainFactory.GetGrain<IReplicationApplyGrain>(m.Source.GetPrimaryKeyString());

        await apply.ApplyPreparedSetAsync(
            key, JsonLatticeSerializer<GCounter>.Default.Serialize(sagaState), Hlc(5_000), Origin, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 1, atomicBatchIndex: 0,
            delta: JsonLatticeSerializer<GCounterDelta>.Default.Serialize(saga), mode: LatticeMergeMode.GCounter);

        Assert.That((await PendingAsync(m.DestinationShard)).Select(p => p.Key), Does.Contain(key),
            "the saga's CRDT delta must reach the destination as a prepare");

        async Task TerminalAsync() =>
            await apply.ApplyTxTerminalAsync(txid, committed: true, 0, Hlc(5_100), Origin);
        async Task IncrementAsync() =>
            await m.Source.ApplyCrdtDeltaAsync(key, LatticeMergeMode.GCounter, Increment("B"));

        if (terminalFirst)
        {
            await TerminalAsync();
            await IncrementAsync();
        }
        else
        {
            await IncrementAsync();
            await TerminalAsync();
        }

        Assert.That((await CounterAsync(m.Source, key))?.Value, Is.EqualTo(2), "PRECONDITION: the source holds both contributions");
        var onDestination = await CounterAsync(m.Destination, key);
        Assert.Multiple(() =>
        {
            Assert.That(onDestination?.Increments.GetValueOrDefault("A"), Is.EqualTo(1), "the saga's committed increment was lost on the destination");
            Assert.That(onDestination?.Increments.GetValueOrDefault("B"), Is.EqualTo(1), "the non-atomic increment was lost on the destination");
        });
    }

    [Test]
    public async Task An_OR_set_add_mirrored_below_the_destinations_fold_is_joined_not_dropped()
    {
        var m = await CreateMirrorAsync(OrSetPrefix);
        var key = "s";
        static OrSetDelta Add(string element, string replica) => new()
        {
            Adds = [new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes(element), ReplicaId = replica, Counter = 1 }],
            Removes = [],
        };
        var saga = Add("x", "A");
        var sagaState = new OrSet();
        sagaState.MergeDelta(saga);
        var txid = Guid.NewGuid();
        var apply = _cluster.GrainFactory.GetGrain<IReplicationApplyGrain>(m.Source.GetPrimaryKeyString());

        await apply.ApplyPreparedSetAsync(
            key, JsonLatticeSerializer<OrSet>.Default.Serialize(sagaState), Hlc(5_000), Origin, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 1, atomicBatchIndex: 0,
            delta: JsonLatticeSerializer<OrSetDelta>.Default.Serialize(saga), mode: LatticeMergeMode.OrSet);
        await apply.ApplyTxTerminalAsync(txid, committed: true, 0, Hlc(5_100), Origin);
        await m.Source.ApplyCrdtDeltaAsync(key, LatticeMergeMode.OrSet, JsonLatticeSerializer<OrSetDelta>.Default.Serialize(Add("y", "B")));

        var bytes = await m.Destination.GetAsync(key);
        var set = bytes is null ? null : JsonLatticeSerializer<OrSet>.Default.Deserialize(bytes);
        Assert.Multiple(() =>
        {
            Assert.That(set?.Contains(Encoding.UTF8.GetBytes("x")), Is.True, "the saga's committed add was lost on the destination");
            Assert.That(set?.Contains(Encoding.UTF8.GetBytes("y")), Is.True, "the non-atomic add was lost on the destination");
        });
    }

    [Test]
    public async Task A_merge_outside_the_mirror_keeps_last_writer_wins_for_a_CRDT_row()
    {
        // The join is opt-in: a whole-row merge that is not a resize or snapshot
        // mirror (a restore, a tree merge) still resolves last-writer-wins.
        var tree = $"{GCounterPrefix}{Guid.NewGuid():N}";
        var lattice = _cluster.GrainFactory.GetGrain<ILattice>(tree);
        await lattice.ApplyCrdtDeltaAsync("k", LatticeMergeMode.GCounter, Increment("A"));
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{tree}/0");
        var lower = new GCounter();
        lower.MergeDelta(new GCounterDelta { Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["B"] = 1 } });

        await shard.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k"] = LwwValue<byte[]>.Create(JsonLatticeSerializer<GCounter>.Default.Serialize(lower), Hlc(1)),
        });

        var counter = await CounterAsync(lattice, "k");
        Assert.That(counter?.Increments.GetValueOrDefault("A"), Is.EqualTo(1));
        Assert.That(counter?.Increments.ContainsKey("B"), Is.False, "a merge outside the mirror must not join");
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private async Task<List<PendingMutationSnapshot>> PendingAsync(IShardRootGrain shard)
    {
        var leafId = (await shard.GetLeftmostLeafIdAsync())!.Value;
        return await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId)
            .GetPendingMutationsForSlotsAsync(new[] { 0 }, 1);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, CrdtTreeResolver>();
        }
    }

    /// <summary>
    /// Declares each CRDT test tree's mode by its name prefix, as a CRDT tree is
    /// declared in production, so a prepared CRDT write records its typed delta
    /// for the terminal to fold.
    /// </summary>
    private sealed class CrdtTreeResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            treeId.StartsWith(GCounterPrefix, StringComparison.Ordinal) ? LatticeMergeMode.GCounter
            : treeId.StartsWith(OrSetPrefix, StringComparison.Ordinal) ? LatticeMergeMode.OrSet
            : null;
    }
}
