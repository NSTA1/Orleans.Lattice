using System.Text;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4564, on a real cluster driving a real adaptive split. A saga decides
/// while its prepared buckets are still on shard 0, with its terminal broadcast
/// held at the coordinator. The split then opens, and its retroactive sweep
/// resolves each moved key's decided bucket at the destination through a terminal
/// whose committed-values backstop carries the prepare's original stamp P. A plain
/// write acknowledged afterwards on the source reaches the destination as a
/// migration import (the live shadow-forward, then the final moved-slot drain).
/// The destination used to drop every such import over its non-migrated saga
/// value, so once the map moved the key read the saga's older value and the
/// acknowledged write was lost. A value stored at a carried P is now on the
/// source's clock lineage, and the later write wins by last-writer-wins.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SplitMigrationImportAfterSagaIntegrationTests
{
    private const int KeyCount = 32;
    private const int ShardCount = 4;

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
        SagaTerminalHold.Release();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private static string KeyOf(int i) => $"import-{i:D2}";

    private static string? Text(byte[]? value) => value is null ? null : Encoding.UTF8.GetString(value);

    [Test]
    public async Task A_write_acknowledged_after_the_sweep_resolved_a_decided_saga_survives_the_split()
    {
        var treeId = $"split-import-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = ShardCount });
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        for (var i = 0; i < KeyCount; i++)
            await tree.SetAsync(KeyOf(i), Encoding.UTF8.GetBytes("seed"));

        // The split moves the upper half of shard 0's virtual slots.
        var map = (await tree.GetRoutingAsync(forceRefresh: true)).Map;
        var owned = Enumerable.Range(0, map.Slots.Length).Where(slot => map.Slots[slot] == 0).ToList();
        var moved = owned.Skip(owned.Count / 2).ToHashSet();
        var movedKeys = Enumerable.Range(0, KeyCount).Select(KeyOf)
            .Where(key => moved.Contains(ShardMap.GetVirtualSlot(key, map.Slots.Length))).ToList();
        Assert.That(movedKeys, Is.Not.Empty, "precondition: some keys move to the destination");

        // 1. The saga decides; its terminal broadcast is held, so its buckets stay
        //    prepared on shard 0.
        SagaTerminalHold.Arm();
        var saga = tree.SetManyAtomicAsync(Enumerable.Range(0, KeyCount)
            .Select(i => new KeyValuePair<string, byte[]>(KeyOf(i), Encoding.UTF8.GetBytes("saga")))
            .ToList());
        try
        {
            var reached = await Task.WhenAny(SagaTerminalHold.Reached, saga, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.That(reached, Is.SameAs(SagaTerminalHold.Reached), "precondition: the saga decided and its terminal is held");

            // 2. The split of shard 0 opens; its sweep resolves the decided buckets
            //    of the moved keys at the destination.
            var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
            await split.SplitAsync(sourceShardIndex: 0);
            var source = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/0");
            List<int> targets = [];
            await TestPoll.UntilAsync(async () => (targets = await source.GetSplitForwardTargetsAsync()).Count > 0,
                "the split's shadow-write window to open", timeout: TimeSpan.FromSeconds(30));
            var destination = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{targets[0]}");
            await TestPoll.UntilAsync(
                async () =>
                {
                    foreach (var key in movedKeys)
                    {
                        if (Text(await destination.GetAsync(key)) != "saga") return false;
                    }

                    return true;
                },
                "the sweep to resolve every moved key's decided bucket at the destination",
                timeout: TimeSpan.FromSeconds(30));
        }
        finally
        {
            SagaTerminalHold.Release();
        }

        await saga;

        // 3. A write of every key acknowledged after the saga, still routed to
        //    shard 0 while the split is open.
        for (var i = 0; i < KeyCount; i++)
            await tree.SetAsync(KeyOf(i), Encoding.UTF8.GetBytes("later"));

        // 4. The split completes and the moved keys route to the destination.
        var coordinator = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        while (!await coordinator.IsIdleAsync())
        {
            budget.Token.ThrowIfCancellationRequested();
            await coordinator.RunSplitPassAsync();
            await Task.Delay(20, budget.Token);
        }

        var lost = new List<string>();
        for (var i = 0; i < KeyCount; i++)
        {
            var read = Text(await tree.GetAsync(KeyOf(i)));
            if (read != "later")
                lost.Add($"{KeyOf(i)}={read ?? "<null>"}");
        }

        Assert.That(lost, Is.Empty, "every write acknowledged after the saga must survive the split");
    }

    /// <summary>
    /// Holds the saga coordinator's terminal broadcast - and only that: the split
    /// sweep's own terminals pass through.
    /// </summary>
    private static class SagaTerminalHold
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

    private sealed class SagaTerminalHoldFilter : IOutgoingGrainCallFilter
    {
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.InterfaceMethod?.Name == nameof(IShardRootGrain.AppendTxTerminalAsync)
                && context.SourceId is { } sourceId
                && sourceId.Type.ToString()?.Contains("atomicwrite", StringComparison.OrdinalIgnoreCase) == true)
            {
                await SagaTerminalHold.WaitIfArmedAsync();
            }

            await context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddOutgoingGrainCallFilter<SagaTerminalHoldFilter>();
        }
    }
}
