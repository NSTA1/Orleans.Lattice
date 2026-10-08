using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4522 on a real in-process cluster: the online-resize mirror ships each
/// write to the destination copy R at the source copy T's own stamps.
/// <para>
/// R's leaf clock can run ahead of T's - here it is pushed an hour ahead - so a
/// write R re-mints sorts above every stamp T issues, including a saga's prepare
/// stamp P. The mirror used to forward the operation itself, so R re-minted it:
/// a plain write acknowledged on T below P landed on R above P (b0c753b8's
/// depth-10 trace), and an unmarked mirrored prepare drained on R at a fresh
/// stamp over a write acknowledged after it (the depth-18/19 traces). Now a
/// plain write is mirrored as the row T stored, a prepare carries P, and the
/// terminal carries P, so R orders every write exactly as T does.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ResizeMirrorOriginalStampIntegrationTests
{
    private const string OperationId = "op-mirror";
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
        TerminalHold.Release();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void ReleaseHold() => TerminalHold.Release();

    private sealed record Mirror(ILattice Source, IShardRootGrain SourceShard, ILattice Destination, IShardRootGrain DestinationShard);

    /// <summary>
    /// Registers a source tree and a destination tree, switches the source's
    /// shard to mirror onto the destination, and pushes the destination's leaf
    /// clock an hour ahead of the source's.
    /// </summary>
    private async Task<Mirror> CreateMirrorAsync(string prefix)
    {
        var source = $"{prefix}-{Guid.NewGuid():N}";
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
        await sourceShard.BeginShadowForwardAsync(destination, OperationId, source);

        return new Mirror(
            _cluster.GrainFactory.GetGrain<ILattice>(source), sourceShard,
            _cluster.GrainFactory.GetGrain<ILattice>(destination), destinationShard);
    }

    private static async Task<string?> ReadAsync(ILattice tree, string key) =>
        await tree.GetAsync(key) is { } bytes ? Encoding.UTF8.GetString(bytes) : null;

    private async Task<LwwEntry?> RawAsync(IShardRootGrain shard, string key)
    {
        var leafId = (await shard.GetLeftmostLeafIdAsync())!.Value;
        return await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId).GetRawEntryAsync(key);
    }

    [Test]
    public async Task A_plain_write_is_mirrored_at_the_source_copys_own_stamp()
    {
        var m = await CreateMirrorAsync("mirror-plain");

        await m.Source.SetAsync("k", Encoding.UTF8.GetBytes("w0"));
        await m.Source.DeleteAsync("gone");
        await m.Source.SetAsync("gone", Encoding.UTF8.GetBytes("x"));
        await m.Source.DeleteAsync("gone");

        var onSource = await RawAsync(m.SourceShard, "k");
        var onDestination = await RawAsync(m.DestinationShard, "k");
        Assert.That(onDestination, Is.Not.Null);
        Assert.That(onDestination!.Value.Timestamp, Is.EqualTo(onSource!.Value.Timestamp),
            "the destination must hold the row at the source's stamp, not re-mint it on its own clock");
        Assert.That(Encoding.UTF8.GetString(onDestination.Value.Value!), Is.EqualTo("w0"));

        var tombSource = await RawAsync(m.SourceShard, "gone");
        var tombDestination = await RawAsync(m.DestinationShard, "gone");
        Assert.That(tombDestination!.Value.IsTombstone, Is.True);
        Assert.That(tombDestination.Value.Timestamp, Is.EqualTo(tombSource!.Value.Timestamp));
    }

    [Test]
    public async Task A_mirrored_prepare_is_bucketed_on_the_destination_at_its_original_stamp()
    {
        var m = await CreateMirrorAsync("mirror-prepare");
        var write = await StartHeldAtomicBatchAsync(m.Source, ["k1", "k2"]);
        try
        {
            var sourcePending = await PendingAsync(m.SourceShard);
            var destinationPending = await PendingAsync(m.DestinationShard);

            Assert.That(destinationPending.Select(p => p.Key), Is.EquivalentTo(new[] { "k1", "k2" }));
            foreach (var prepared in destinationPending)
            {
                Assert.That(prepared.StampIsOriginal, Is.True, $"{prepared.Key}: the mirrored prepare is marked");
                Assert.That(prepared.Timestamp, Is.EqualTo(sourcePending.Single(p => p.Key == prepared.Key).Timestamp),
                    $"{prepared.Key}: bucketed at the source's prepare stamp");
            }
        }
        finally
        {
            TerminalHold.Release();
            await write;
        }

        Assert.That(await ReadAsync(m.Destination, "k1"), Is.EqualTo("saga-k1"));
        Assert.That(await ReadAsync(m.Destination, "k2"), Is.EqualTo("saga-k2"));
    }

    [Test]
    public async Task A_write_acknowledged_before_the_prepare_never_beats_the_saga_on_the_destination()
    {
        // b0c753b8's depth-10 trace: W0 is acknowledged on the source below P.
        // Re-minted on the destination's clock it sorted above P there, so the
        // destination served k1 from the saga and k2 from W0 - a torn batch, and
        // after the swap a lost committed write.
        var m = await CreateMirrorAsync("mirror-before");
        await m.Source.SetAsync("k2", Encoding.UTF8.GetBytes("w0"));

        await m.Source.SetManyAtomicAsync(
            new[] { "k1", "k2" }.Select(k => new KeyValuePair<string, byte[]>(k, Encoding.UTF8.GetBytes($"saga-{k}"))).ToList());

        Assert.That(await ReadAsync(m.Source, "k2"), Is.EqualTo("saga-k2"));
        Assert.That(await ReadAsync(m.Destination, "k1"), Is.EqualTo("saga-k1"));
        Assert.That(await ReadAsync(m.Destination, "k2"), Is.EqualTo("saga-k2"),
            "the destination must order W0 below the saga, as the source does");
    }

    [Test]
    public async Task A_write_acknowledged_after_the_prepare_survives_the_terminal_on_the_destination()
    {
        // The depth-18/19 shape: a write W acknowledged on the source after the
        // prepare is stamped above P. The destination must keep it through the
        // saga's terminal, exactly as the source does.
        var m = await CreateMirrorAsync("mirror-after");
        var write = await StartHeldAtomicBatchAsync(m.Source, ["k1", "k2"]);
        try
        {
            await m.Source.SetAsync("k2", Encoding.UTF8.GetBytes("later"));
        }
        finally
        {
            TerminalHold.Release();
        }

        await write.WaitAsync(TimeSpan.FromSeconds(60));

        Assert.That(await ReadAsync(m.Source, "k2"), Is.EqualTo("later"), "PRECONDITION: the source keeps the later write");
        Assert.That(await ReadAsync(m.Destination, "k1"), Is.EqualTo("saga-k1"));
        Assert.That(await ReadAsync(m.Destination, "k2"), Is.EqualTo("later"),
            "the destination must keep the write acknowledged after the prepare, as the source does");
    }

    [Test]
    public async Task A_resize_never_lets_a_decided_sagas_backstop_overwrite_a_later_write()
    {
        // b0c753b8's depth-11 trace, through a real online resize. A saga
        // decides with its terminal held, so its buckets stay pending on the
        // source, and a write W acknowledged after its prepare is stamped above
        // P. The resize copy's sweep finds the decided bucket and backstops the
        // saga's value on the destination; the drain copies W there at its own
        // stamp. A backstop at a fresh dominating stamp sorted above W, so once
        // the tree resolved to the resized copy W was lost. At P it loses to W.
        var tree = $"resize-backstop-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(tree, new TreeRegistryEntry { ShardCount = 1 });
        var router = _cluster.GrainFactory.GetGrain<ILattice>(tree);

        var write = await StartHeldAtomicBatchAsync(router, ["k1", "k2"]);
        try
        {
            await router.SetAsync("k2", Encoding.UTF8.GetBytes("later")).WaitAsync(TimeSpan.FromSeconds(30));
            await router.ResizeAsync(64, 64);
            await TestPoll.UntilAsync(() => router.IsResizeCompleteAsync(), "the resize to complete",
                timeout: TimeSpan.FromSeconds(60));
            Assert.That(await registry.ResolveAsync(tree), Is.Not.EqualTo(tree),
                "PRECONDITION: the tree resolves to its resized copy");
        }
        finally
        {
            TerminalHold.Release();
        }

        await write.WaitAsync(TimeSpan.FromSeconds(60));

        Assert.That(await ReadAsync(router, "k1"), Is.EqualTo("saga-k1"));
        Assert.That(await ReadAsync(router, "k2"), Is.EqualTo("later"),
            "the resized copy must keep the write acknowledged after the saga's prepare");
    }

    private async Task<List<PendingMutationSnapshot>> PendingAsync(IShardRootGrain shard)
    {
        var leafId = (await shard.GetLeftmostLeafIdAsync())!.Value;
        return await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId)
            .GetPendingMutationsForSlotsAsync(new[] { 0 }, 1);
    }

    private static async Task<Task> StartHeldAtomicBatchAsync(ILattice router, IReadOnlyList<string> keys)
    {
        TerminalHold.Arm();
        var write = router.SetManyAtomicAsync(
            keys.Select(k => new KeyValuePair<string, byte[]>(k, Encoding.UTF8.GetBytes($"saga-{k}"))).ToList());
        var reached = await Task.WhenAny(TerminalHold.Reached, write, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.That(reached, Is.SameAs(TerminalHold.Reached),
            "PRECONDITION: the saga's terminal broadcast must have been reached and held");
        return write;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IOutgoingGrainCallFilter, TerminalHoldFilter>();
        }
    }

    /// <summary>
    /// Holds the atomic-write coordinator's <c>AppendTxTerminalAsync</c> calls
    /// while armed. Static because the TestingHost silo runs in-process.
    /// </summary>
    private static class TerminalHold
    {
        private static TaskCompletionSource _reached = NewSource();
        private static TaskCompletionSource _released = NewSource();
        private static int _armed;

        private static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal static Task Reached => _reached.Task;

        internal static void Arm()
        {
            _reached = NewSource();
            _released = NewSource();
            Volatile.Write(ref _armed, 1);
        }

        internal static void Release()
        {
            Volatile.Write(ref _armed, 0);
            _released.TrySetResult();
        }

        internal static Task WaitIfArmedAsync()
        {
            if (Volatile.Read(ref _armed) == 0)
                return Task.CompletedTask;
            _reached.TrySetResult();
            return _released.Task;
        }
    }

    private sealed class TerminalHoldFilter : IOutgoingGrainCallFilter
    {
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.InterfaceMethod?.Name == "AppendTxTerminalAsync"
                && context.SourceId is { } source
                && source.Type.ToString()?.Contains("atomicwrite", StringComparison.OrdinalIgnoreCase) == true)
            {
                await TerminalHold.WaitIfArmedAsync();
            }

            await context.Invoke();
        }
    }
}
