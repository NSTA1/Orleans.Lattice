using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// #6166: a bulk-append chunk that fails after seeding some of its leaves, and is re-driven
/// under the same operation id, must keep the layout its first attempt gave them. The
/// leaves' ids are derived from the operation and each keeps the range it was first born
/// declaring, while the old rightmost leaf - not yet narrowed, so still owning the
/// appended range - replays the first attempt's seeds and moves its right edge up. A
/// retry laid out from that moved edge re-seeded the same leaves under other ranges, so
/// keys landed in leaves whose declared range excludes them and read back absent: on the
/// rig, one leaf's worth of keys per affected shard. Each case fails one leaf call of the
/// chunk part-way through, before the call runs or after it lands (a timeout's ambiguous
/// outcome), waits out the run's idle close as the rig's retry did, retries, appends more,
/// and reads every key back, then again after every leaf reactivates.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BulkAppendChunkRetryIntegrationTests
{
    private const int MaxLeafKeys = 32;
    private const int ChunkSize = 512;
    private const int ChunksBefore = 4;
    private const int ChunksAfter = 2;

    /// <summary>
    /// The leaf call of the faulted chunk that fails: one shard, so its sixteen leaves are
    /// created in order and the first eleven are seeded before it.
    /// </summary>
    private const int FaultedLeafCall = 12;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        await _cluster.WaitForAppendFenceGateAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        SeamFault.Disarm();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TestCase(nameof(IBPlusLeafGrain.InitializeSiblingAsync), false)]
    [TestCase(nameof(IBPlusLeafGrain.InitializeSiblingAsync), true)]
    [TestCase(nameof(IBPlusLeafGrain.MergeBulkSeedAsync), false)]
    [TestCase(nameof(IBPlusLeafGrain.MergeBulkSeedAsync), true)]
    public async Task A_chunk_retried_after_a_seam_fault_keeps_its_first_layout_and_every_key(string method, bool afterLanding)
    {
        var tree = $"bulk-retry-{method}-{(afterLanding ? "landed" : "refused")}".ToLowerInvariant();
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(tree, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = MaxLeafKeys, WalPartitions = 2 });
        var lattice = _cluster.GrainFactory.GetGrain<ILattice>(tree);
        var total = (ChunksBefore + 1 + ChunksAfter) * ChunkSize;
        for (var c = 0; c < ChunksBefore; c++)
        {
            await lattice.BulkAppendChunkAsync($"chunk-{c}", Chunk(c));
        }

        SeamFault.Arm(typeof(IBPlusLeafGrain), method, afterLanding, FaultedLeafCall);
        var faulted = Chunk(ChunksBefore);
        Assert.That(async () => await lattice.BulkAppendChunkAsync($"chunk-{ChunksBefore}", faulted), Throws.Exception);
        var fired = SeamFault.Fired;
        SeamFault.Disarm();

        // Past the run's idle close, as the rig's retry came after its saturation backoff:
        // the abandoned switch is closed, so the retry cannot read its layout from it.
        await Task.Delay(ShardRootGrain.BulkSwitchIdleClose + TimeSpan.FromSeconds(2));
        await lattice.BulkAppendChunkAsync($"chunk-{ChunksBefore}", faulted);
        for (var c = ChunksBefore + 1; c < ChunksBefore + 1 + ChunksAfter; c++)
        {
            await lattice.BulkAppendChunkAsync($"chunk-{c}", Chunk(c));
        }

        var beforeReactivation = await CountPresentAsync(lattice, total);
        foreach (var leaf in await ListLeavesAsync(tree))
        {
            await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leaf).ForceDeactivateAsync();
        }

        await Task.Delay(500);
        var afterReactivation = await CountPresentAsync(lattice, total);

        Assert.Multiple(() =>
        {
            Assert.That(fired, Is.True, "precondition: the fault failed the chunk part-way through");
            Assert.That(beforeReactivation, Is.EqualTo(total), "every key reads back after the retry");
            Assert.That(afterReactivation, Is.EqualTo(total), "every key reads back after every leaf reactivates");
        });
    }
    private static List<KeyValuePair<string, byte[]>> Chunk(int c) =>
        Enumerable.Range(c * ChunkSize, ChunkSize)
            .Select(n => new KeyValuePair<string, byte[]>(Key(n), Encoding.UTF8.GetBytes($"v{n}")))
            .ToList();

    private static string Key(int n) => $"k{n:D9}";

    private static async Task<int> CountPresentAsync(ILattice lattice, int total)
    {
        var found = await lattice.GetManyAsync(Enumerable.Range(0, total).Select(Key).ToList());
        return Enumerable.Range(0, total).Count(n => found.TryGetValue(Key(n), out var v) && Encoding.UTF8.GetString(v) == $"v{n}");
    }

    private async Task<List<GrainId>> ListLeavesAsync(string tree)
    {
        var leaves = new List<GrainId>();
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{tree}/0");
        for (GrainId? id = await shard.GetLeftmostLeafIdAsync(); id is { } current;)
        {
            leaves.Add(current);
            id = (await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(current).GetKeyRangeAndNextSiblingAsync()).NextSibling;
        }

        return leaves;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, SeamFaultFilter>();
        }
    }

    /// <summary>The one armed seam fault; static because the TestingHost silo runs in-process.</summary>
    private static class SeamFault
    {
        private static Type? _seam;
        private static string? _method;
        private static bool _afterLanding;
        private static int _remaining;
        private static int _fired;

        internal static bool Fired => Volatile.Read(ref _fired) == 1;

        internal static void Arm(Type seam, string method, bool afterLanding, int ordinal)
        {
            _seam = seam;
            _method = method;
            _afterLanding = afterLanding;
            Volatile.Write(ref _fired, 0);
            Volatile.Write(ref _remaining, ordinal);
        }

        internal static void Disarm() => Volatile.Write(ref _remaining, 0);

        internal static bool Matches(IIncomingGrainCallContext context, out bool afterLanding)
        {
            afterLanding = _afterLanding;
            return Volatile.Read(ref _remaining) > 0
                && context.InterfaceMethod?.DeclaringType == _seam
                && context.InterfaceMethod?.Name == _method
                && Interlocked.Decrement(ref _remaining) == 0
                && Interlocked.Exchange(ref _fired, 1) == 0;
        }
    }

    private sealed class SeamFaultFilter : IIncomingGrainCallFilter
    {
        public async Task Invoke(IIncomingGrainCallContext context)
        {
            ArgumentNullException.ThrowIfNull(context);
            if (!SeamFault.Matches(context, out var afterLanding))
            {
                await context.Invoke();
                return;
            }

            if (afterLanding)
            {
                await context.Invoke();
            }

            throw new TimeoutException($"Injected seam fault on {context.InterfaceMethod!.Name} ({(afterLanding ? "after" : "before")} it ran).");
        }
    }
}
