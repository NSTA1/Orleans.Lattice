using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4522 on a real in-process cluster: a saga's terminal used to install
/// its committed values at a fresh stamp minted at terminal time, above every
/// row the leaf held, so a write acknowledged after the prepare that reached the
/// leaf as a cross-shard migration import (a split's shadow-forward of a plain
/// write, which carries <c>IsMigrated</c>) was overwritten by the saga's older
/// value. The fix applies a marked prepare at its own original stamp P under
/// last-writer-wins, and the read gate treats a row stamped at or above P as
/// superseding the prepare.
/// <para>
/// The saga's terminal is held at the coordinator (an outgoing filter on
/// <c>AppendTxTerminalAsync</c>), after the decision is recorded and before any
/// shard drains. In that window the test reads the routing tier's mark off the
/// leaf - which proves the routing tier stamps the shard a prepare is dispatched
/// to in the same <c>{physicalTreeId}/{shardIndex}</c> form the leaf compares
/// against - and imports a later write as a migration, exactly as a split's
/// shadow-forward delivers it.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class OriginalPrepareStampIntegrationTests
{
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

    private async Task<(ILattice Router, IShardRootGrain Shard)> CreateTreeAsync(string treeName)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry { ShardCount = 1 });
        return (_cluster.GrainFactory.GetGrain<ILattice>(treeName),
                _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0"));
    }

    private async Task<List<PendingMutationSnapshot>> PendingAsync(IShardRootGrain shard)
    {
        var leafId = (await shard.GetLeftmostLeafIdAsync())!.Value;
        return await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId)
            .GetPendingMutationsForSlotsAsync(new[] { 0 }, 1);
    }

    private static async Task<string?> ReadAsync(ILattice router, string key) =>
        await router.GetAsync(key) is { } bytes ? Encoding.UTF8.GetString(bytes) : null;

    /// <summary>
    /// Starts an atomic batch over <c>a</c> and <c>b</c> with its terminal held,
    /// and returns once the coordinator has decided and reached the broadcast.
    /// </summary>
    private static async Task<Task> StartHeldAtomicBatchAsync(ILattice router)
    {
        TerminalHold.Arm();
        var write = router.SetManyAtomicAsync(
        [
            new("a", Encoding.UTF8.GetBytes("saga-a")),
            new("b", Encoding.UTF8.GetBytes("saga-b")),
        ]);
        var reached = await Task.WhenAny(TerminalHold.Reached, write, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.That(reached, Is.SameAs(TerminalHold.Reached),
            "PRECONDITION: the saga's terminal broadcast must have been reached and held");
        return write;
    }

    [Test]
    public async Task The_routing_tier_marks_every_prepare_it_dispatches_to_a_shard()
    {
        var (router, shard) = await CreateTreeAsync($"stamp-mark-{Guid.NewGuid():N}");
        var write = await StartHeldAtomicBatchAsync(router);

        var pending = await PendingAsync(shard);

        TerminalHold.Release();
        await write;

        Assert.That(pending.Select(p => p.Key), Is.EquivalentTo(new[] { "a", "b" }));
        Assert.That(pending.All(p => p.StampIsOriginal), Is.True,
            "a prepare the routing tier dispatched to this shard carries its original stamp");
    }

    [Test]
    public async Task A_committed_saga_never_overwrites_a_later_write_imported_as_a_migration()
    {
        var (router, shard) = await CreateTreeAsync($"stamp-migrated-{Guid.NewGuid():N}");
        var write = await StartHeldAtomicBatchAsync(router);
        var prepareA = (await PendingAsync(shard)).Single(p => p.Key == "a");

        // A plain write acknowledged after the prepare, delivered the way a
        // split's shadow-forward delivers it: a migration import stamped above
        // the prepare (property H holds at the source, whose clock minted P).
        var later = new HybridLogicalClock { WallClockTicks = prepareA.Timestamp.WallClockTicks, Counter = prepareA.Timestamp.Counter + 1 };
        await shard.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["a"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("later-a"), later),
        }, isCrossShardMigration: true);

        // Decided but not yet drained: the read gate must already serve the
        // later write, so a reader never sees the saga's older value surface
        // between the later write and the drain.
        var readWhileHeld = await ReadAsync(router, "a");

        TerminalHold.Release();
        await write;

        Assert.That(readWhileHeld, Is.EqualTo("later-a"));
        Assert.That(await ReadAsync(router, "a"), Is.EqualTo("later-a"),
            "the saga's terminal must not overwrite a write acknowledged after its prepare");
        Assert.That(await ReadAsync(router, "b"), Is.EqualTo("saga-b"));
    }

    [Test]
    public async Task Writes_issued_while_a_terminal_is_held_still_complete()
    {
        var (router, _) = await CreateTreeAsync($"stamp-live-{Guid.NewGuid():N}");
        var write = await StartHeldAtomicBatchAsync(router);

        var plain = router.SetAsync("c", Encoding.UTF8.GetBytes("plain"));
        var completed = await Task.WhenAny(plain, Task.Delay(TimeSpan.FromSeconds(10)));

        TerminalHold.Release();
        await write;

        Assert.That(completed, Is.SameAs(plain));
        Assert.That(await ReadAsync(router, "c"), Is.EqualTo("plain"));
        Assert.That(await ReadAsync(router, "a"), Is.EqualTo("saga-a"));
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
    /// Holds the first armed <c>AppendTxTerminalAsync</c> call until released.
    /// Static because the TestingHost silo runs in-process.
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
            if (context.InterfaceMethod?.Name == "AppendTxTerminalAsync")
                await TerminalHold.WaitIfArmedAsync();
            await context.Invoke();
        }
    }
}
