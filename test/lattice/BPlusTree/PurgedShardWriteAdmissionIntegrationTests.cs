using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Wal;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Real-grain detector for issue #5458, found by the confirmation review of the
/// WAL pin retirement model (#5351). A purge keeps the tree's registry entry
/// until the last step of its tail, while every shard it has walked is already
/// tombstoned. A purged shard used to lift its tombstone for any write once the
/// registry named the id live - and the purged tree's own entry did. A write to
/// the deleted tree was then acknowledged while the purge's registry removal was
/// owed; it re-seeded the shard's root leaf under the purged entry's lineage, the
/// purge's re-driven tail took that lineage for its own and trimmed the log,
/// cleared the pin store and removed the entry under the live leaf, and the
/// leaf's acknowledged writes were gone once it replayed cold.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class PurgedShardWriteAdmissionIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [TearDown]
    public void TearDown() => UnregisterFault.Disarm();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string? Text(byte[]? bytes) => bytes is null ? null : Encoding.UTF8.GetString(bytes);

    private static async Task<Exception?> TrySetAsync(ILattice tree, string key, string value)
    {
        try
        {
            await tree.SetAsync(key, Bytes(value));
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private async Task WaitAsync(Func<Task<bool>> condition, string what)
    {
        var deadline = Environment.TickCount64 + 30_000;
        while (!await condition())
        {
            if (Environment.TickCount64 > deadline)
                Assert.Fail($"timed out waiting for {what}");
            await Task.Delay(50);
        }
    }

    /// <summary>
    /// Reads <paramref name="key"/> after the shard's root leaf has been
    /// deactivated, so it answers from durable state and the log alone.
    /// </summary>
    private async Task<string?> ReadColdAsync(ILattice tree, IShardRootGrain shard, string key)
    {
        if (await shard.GetLeftmostLeafIdAsync() is { } leaf)
        {
            await _cluster.Client.GetGrain<IBPlusLeafGrain>(leaf).ForceDeactivateAsync();
            await Task.Delay(200);
        }

        return Text(await tree.GetAsync(key));
    }

    [Test]
    public async Task A_write_to_a_tree_whose_purge_still_owes_its_registry_removal_is_never_acknowledged_and_lost()
    {
        var treeId = $"purge-admission-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1 });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        await tree.SetAsync("a", Bytes("1"));
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{treeId}/0");
        var deletion = _cluster.Client.GetGrain<ITreeDeletionGrain>(treeId);

        await tree.DeleteTreeAsync();
        Assert.That(await TrySetAsync(tree, "x", "0"), Is.Not.Null, "precondition: a deleted tree refuses writes");

        // The purge walks every shard and records its completion, and its registry
        // removal faults: the removal is owed, and the purged entry stays.
        UnregisterFault.Arm(treeId);
        await tree.PurgeTreeAsync();
        await WaitAsync(() => Task.FromResult(UnregisterFault.Fired), "the purge's registry removal");
        await Task.Delay(200);
        Assert.That(await registry.ExistsAsync(treeId), Is.True, "precondition: the purged tree's entry is still registered");

        // The window: every shard is purged, and the registry names the id.
        var refused = await TrySetAsync(tree, "b", "2");

        // The purge's tail is re-driven and removes the entry.
        await tree.PurgeTreeAsync();
        await WaitAsync(async () => (await deletion.GetDeletionStatusAsync()).PurgeComplete, "the purge to finish");

        string? b = null;
        if (refused is null)
        {
            b = await ReadColdAsync(tree, shard, "b");
        }

        // A write after the purge has finished is the deliberate reuse of #3940,
        // and keeps its data like any other.
        await tree.SetAsync("c", Bytes("3"));
        var c = await ReadColdAsync(tree, shard, "c");

        Assert.Multiple(() =>
        {
            if (refused is null)
            {
                Assert.That(b, Is.EqualTo("2"),
                    "the write was acknowledged on the purged shard, re-seeding its root leaf under the purged "
                    + "entry's lineage; the purge's re-driven tail then trimmed its log and cleared its pins, and "
                    + "the leaf lost the acknowledged write when it replayed cold");
            }

            Assert.That(refused, Is.Not.Null, "a write to a tree whose purge still holds it must be refused");
            Assert.That(c, Is.EqualTo("3"), "a write after the purge has finished reuses the id and keeps its data");
        });
    }

    /// <summary>Faults the next outgoing registry removal of the armed tree once.</summary>
    private sealed class UnregisterFault : IOutgoingGrainCallFilter
    {
        private static volatile string? _treeId;
        private static int _fired;

        public static bool Fired => Volatile.Read(ref _fired) != 0;

        public static void Arm(string treeId)
        {
            Volatile.Write(ref _fired, 0);
            _treeId = treeId;
        }

        public static void Disarm()
        {
            _treeId = null;
            Volatile.Write(ref _fired, 0);
        }

        public Task Invoke(IOutgoingGrainCallContext context)
        {
            if (_treeId is { } treeId
                && context.MethodName == nameof(ILatticeRegistry.UnregisterAsync)
                && context.InterfaceMethod?.DeclaringType == typeof(ILatticeRegistry)
                && context.Request.GetArgumentCount() > 0
                && context.Request.GetArgument(0) as string == treeId
                && Interlocked.Exchange(ref _fired, 1) == 0)
            {
                throw new TimeoutException("test: the purge's registry removal faulted");
            }

            return context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<IGrainStorage>(name, (_, _) => new EnumerableMemoryGrainStorage()));
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.AddLatticeWalGc();
            siloBuilder.ConfigureLattice(o =>
            {
                o.TombstoneGracePeriod = TimeSpan.Zero;
                o.DigestCoalescingWindowMs = 0;
                o.MaterialiserCheckpointInterval = TimeSpan.Zero;
                o.WalPartitions = 1;
            });
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IOutgoingGrainCallFilter, UnregisterFault>();
        }
    }
}
