using System.Collections.Concurrent;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end coverage for snapshot-open admission control
/// (<see cref="LatticeOptions.ShedSnapshotOpensWhenSaturated"/>, issue #1053).
/// When a tree is <see cref="WalSaturationState.Saturated"/> and the option is
/// enabled (the default), opening a snapshot-isolated cursor must shed fast with
/// a retryable <see cref="LatticeSaturatedException"/> carrying the tree id -
/// before the expensive per-shard baseline capture is fanned out onto the
/// collapsing shard roots. A healthy tree opens normally, and disabling the
/// option restores the always-open behaviour even under saturation.
/// <para>
/// Saturation is forced deterministically by overriding
/// <see cref="IWalSaturationSignal"/> with a per-tree switchable fake, so the
/// shed path is exercised without driving a real account into back-pressure.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SnapshotOpenAdmissionControlIntegrationTests
{
    private const int ShardCount = 4;
    private const int MaxLeafKeys = 4;

    private TestCluster _shedOnCluster = null!;
    private TestCluster _shedOffCluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var shedOn = new TestClusterBuilder();
        shedOn.AddSiloBuilderConfigurator<ShedOnConfigurator>();
        _shedOnCluster = shedOn.Build();
        await _shedOnCluster.DeployAsync();

        var shedOff = new TestClusterBuilder();
        shedOff.AddSiloBuilderConfigurator<ShedOffConfigurator>();
        _shedOffCluster = shedOff.Build();
        await _shedOffCluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        SwitchableWalSaturationSignal.Reset();
        await _shedOnCluster.StopAllSilosAsync();
        await _shedOnCluster.DisposeAsync();
        await _shedOffCluster.StopAllSilosAsync();
        await _shedOffCluster.DisposeAsync();
    }

    [TearDown]
    public void TearDown() => SwitchableWalSaturationSignal.Reset();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static async Task<ILattice> CreateSeededTreeAsync(TestCluster cluster, string treeId)
    {
        var registry = cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = MaxLeafKeys,
            ShardCount = ShardCount,
        });
        var tree = cluster.Client.GetGrain<ILattice>(treeId);

        // Seed while Healthy so the writes admit; the shed only gates the
        // subsequent snapshot open.
        for (var i = 0; i < 40; i++)
        {
            var k = $"key-{i:D4}";
            await tree.SetAsync(k, Bytes(k));
        }

        return tree;
    }

    [Test]
    public async Task Saturated_tree_sheds_snapshot_open_with_LatticeSaturatedException()
    {
        var treeId = $"admission-shed-{Guid.NewGuid():N}";
        var tree = await CreateSeededTreeAsync(_shedOnCluster, treeId);

        // Force the tree into the Saturated regime, then attempt the open.
        SwitchableWalSaturationSignal.Set(treeId, WalSaturationState.Saturated);

        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(
            () => tree.OpenSnapshotEntryCursorAsync());
        Assert.That(ex!.TreeId, Is.EqualTo(treeId),
            "the shed must carry the originating tree id for caller-side attribution");
    }

    [Test]
    public async Task Healthy_tree_opens_snapshot_cursor_normally()
    {
        var treeId = $"admission-healthy-{Guid.NewGuid():N}";
        var tree = await CreateSeededTreeAsync(_shedOnCluster, treeId);

        // Signal left at the default Healthy: the open must proceed and return
        // the full snapshot.
        var cursorId = await tree.OpenSnapshotEntryCursorAsync();
        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 16);
            collected.AddRange(page.Entries.Select(kv => kv.Key));
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected, Has.Count.EqualTo(40),
            "a healthy tree must open normally and return every seeded entry");
    }

    [Test]
    public async Task Saturated_tree_opens_when_shed_option_is_disabled()
    {
        var treeId = $"admission-optoff-{Guid.NewGuid():N}";
        var tree = await CreateSeededTreeAsync(_shedOffCluster, treeId);

        // Even under Saturated, with ShedSnapshotOpensWhenSaturated = false the
        // open must proceed (the prior always-open behaviour).
        SwitchableWalSaturationSignal.Set(treeId, WalSaturationState.Saturated);

        var cursorId = await tree.OpenSnapshotEntryCursorAsync();
        var collected = new List<string>();
        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 16);
            collected.AddRange(page.Entries.Select(kv => kv.Key));
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected, Has.Count.EqualTo(40),
            "with the shed option disabled a saturated tree must still open the snapshot");
    }

    private sealed class ShedOnConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.ShedSnapshotOpensWhenSaturated = true;
            });
            // Override the interface after AddLattice so the grain resolves the
            // switchable fake (last registration wins for GetService).
            siloBuilder.Services.AddSingleton<IWalSaturationSignal, SwitchableWalSaturationSignal>();
            siloBuilder.UseInMemoryReminderService();
        }
    }

    private sealed class ShedOffConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.ShedSnapshotOpensWhenSaturated = false;
            });
            siloBuilder.Services.AddSingleton<IWalSaturationSignal, SwitchableWalSaturationSignal>();
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

/// <summary>
/// Test double for <see cref="IWalSaturationSignal"/> whose per-tree regime is
/// switchable at runtime from the test body. Backed by process-static state so
/// the in-process TestingHost silo and the test share it. Defaults every tree to
/// <see cref="WalSaturationState.Healthy"/> so seeding writes admit normally.
/// </summary>
internal sealed class SwitchableWalSaturationSignal : IWalSaturationSignal
{
    private static readonly ConcurrentDictionary<string, WalSaturationState> States = new();

    public static void Set(string treeId, WalSaturationState state) => States[treeId] = state;

    public static void Reset() => States.Clear();

    public WalSaturationState GetCurrentState(string treeId)
        => States.TryGetValue(treeId, out var state) ? state : WalSaturationState.Healthy;

    public WalSaturationState GetAggregateState()
        => States.IsEmpty ? WalSaturationState.Healthy : (WalSaturationState)States.Values.Cast<int>().Max();

    public Task WaitForHealthyAsync(string treeId, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}
