using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Wal;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Real-grain detector for issue #4654: a leaf whose own state row vanishes -
/// lost storage, or a row deleted outside the lattice - while the shard still
/// routes to it must not come up empty and report acknowledged keys absent.
/// <para>
/// The row is the leaf's only link to its tree, key range, checkpoint and
/// kept-snapshot record (issue #4634), so with it gone the leaf had nothing to
/// replay from and nothing to fail closed on: it activated unbound and served an
/// empty cache, and every key whose only durable copy was the trimmed WAL prefix
/// or the leaf's snapshot read as absent with no error.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class VanishedLeafRowIntegrationTests
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

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static IServiceProvider Services => SiloServiceProviderCaptureForWalTests.Captured
        ?? throw new InvalidOperationException("Silo IServiceProvider was not captured by the fixture.");

    private async Task<(ILattice Tree, string TreeId, GrainId LeafId)> CreateSnapshottedTrimmedLeafAsync(string prefix)
    {
        var logical = $"{prefix}-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 1024 });
        var tree = _cluster.Client.GetGrain<ILattice>(logical);
        var treeId = (await tree.GetRoutingAsync()).PhysicalTreeId;
        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"k-{i:D2}", Bytes($"v-{i}"));
        }

        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{treeId}/0");
        var leafId = (await shard.GetLeftmostLeafIdAsync())!.Value;
        var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey());

        // Cover the written prefix with a snapshot, so the next checkpoint flush
        // publishes the durable pin that licenses the WAL GC to trim it.
        var head = await _cluster.Client.GetGrain<ILeafReplayCoordinatorGrain>($"{treeId}/0").GetHeadOffsetAsync(CancellationToken.None);
        await leaf.SetCheckpointOffsetHintsAsync([head - 1]);
        await leaf.CaptureSnapshotAsync();
        await tree.SetAsync("k-after-capture", Bytes("x"));

        var trimmed = await TrimWalAsync(treeId);
        Assert.That(trimmed, Is.GreaterThan(0),
            "precondition: the WAL GC trimmed the prefix the snapshot covers; without a trim the test is vacuous");

        await leaf.ForceDeactivateAsync();
        await Task.Delay(200);
        return (tree, treeId, leafId);
    }

    private static async Task<long> TrimWalAsync(string treeId)
    {
        var registry = Services.GetRequiredService<IWalCursorRegistry>();
        var deadline = Environment.TickCount64 + 30_000;
        HybridLogicalClock? last = null;
        var stable = 0;
        while (Environment.TickCount64 < deadline)
        {
            var min = await registry.GetMinCursorAsync(treeId);
            if (min is { } floor && floor.CompareTo(HybridLogicalClock.Zero) > 0)
            {
                if (last is { } prev && floor.CompareTo(prev) == 0)
                {
                    if (++stable >= 2) break;
                }
                else
                {
                    stable = 0;
                }

                last = floor;
            }

            await Task.Delay(100);
        }

        var report = await Services.GetRequiredService<ILatticeWalGc>().RunOnceAsync(treeId);
        TestContext.Out.WriteLine($"gc: trimmed={report.EntriesTrimmed} state={report.CursorFloorState} blocking={report.BlockingConsumerId}");
        return report.EntriesTrimmed;
    }

    /// <summary>Deletes the leaf's own state row straight from grain storage, as lost storage would.</summary>
    private static async Task DeleteLeafRowAsync(GrainId leafId)
    {
        var storage = Services.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);
        var row = new GrainState<LeafNodeState>();
        await storage.ReadStateAsync("leaf", leafId, row);
        Assert.That(row.RecordExists, Is.True, "precondition: the leaf row exists before it is deleted");
        await storage.ClearStateAsync("leaf", leafId, row);
    }

    private static async Task<(byte[]? Read, Exception? Failure)> TryGetAsync(ILattice tree, string key)
    {
        try
        {
            return (await tree.GetAsync(key), null);
        }
        catch (Exception ex)
        {
            return (null, ex);
        }
    }

    [TestCase(false, TestName = "A_leaf_whose_state_row_vanishes_does_not_silently_report_acknowledged_keys_absent")]
    [TestCase(true, TestName = "A_leaf_whose_state_row_and_snapshot_both_vanish_does_not_silently_report_acknowledged_keys_absent")]
    public async Task Vanished_leaf_row(bool snapshotAlsoVanishes)
    {
        var (tree, _, leafId) = await CreateSnapshottedTrimmedLeafAsync("vanished-row");

        await DeleteLeafRowAsync(leafId);
        if (snapshotAlsoVanishes)
        {
            await _cluster.Client.GetGrain<ILeafSnapshotStorageGrain>(leafId.GetGuidKey()).ClearAsync(CancellationToken.None);
        }

        var (read, failure) = await TryGetAsync(tree, "k-00");

        TestContext.Out.WriteLine($"read={(read is null ? "null" : Encoding.UTF8.GetString(read))}; "
            + $"failure={failure?.GetType().Name}: {failure?.Message}");
        Assert.That(read is not null || failure is not null, Is.True,
            "k-00 was acknowledged, and the leaf that owned it lost its state row: the leaf must fail closed "
            + "rather than come up empty and report the key absent");
        if (read is not null)
        {
            Assert.That(read, Is.EqualTo(Bytes("v-0")));
        }
        else
        {
            Assert.That(failure, Is.InstanceOf<ILatticeLeafUnavailable>(),
                "the fault must be recognisable as an unavailable leaf, not an arbitrary error");
        }
    }

    /// <summary>
    /// The control: a leaf removed deliberately (the clear every purge, retirement,
    /// reclaim and orphan repair funnels through) is not failed closed, so routing
    /// that still reaches it keeps self-healing as issue #1744 requires.
    /// </summary>
    [Test]
    public async Task A_deliberately_cleared_leaf_is_not_failed_closed()
    {
        var (tree, _, leafId) = await CreateSnapshottedTrimmedLeafAsync("cleared-row");

        await _cluster.Client.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey()).ClearGrainStateAsync();
        await Task.Delay(200);

        var (_, failure) = await TryGetAsync(tree, "k-00");

        Assert.That(failure, Is.Null, $"a deliberate clear accepted the data's removal; got {failure}");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.AddLatticeWalGc();
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.MaterialiserCheckpointInterval = TimeSpan.Zero;
                // One WAL partition, so the single leaf's pin covers the whole log
                // and no unwritten partition holds a blocking pin.
                o.WalPartitions = 1;
            });
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<SiloServiceProviderCaptureForWalTests>();
            siloBuilder.Services.AddHostedService(sp => sp.GetRequiredService<SiloServiceProviderCaptureForWalTests>());
        }
    }
}
