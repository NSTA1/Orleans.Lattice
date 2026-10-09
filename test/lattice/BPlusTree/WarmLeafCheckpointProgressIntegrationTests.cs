using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #3314: foreground writes on a resident leaf earn durable snapshot
/// coverage and WAL reclamation through the real timer, not checkpoint hints,
/// explicit capture, a GC starvation drive, or deactivation.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class WarmLeafCheckpointProgressIntegrationTests
{
    private const string SubjectTree = "warm-checkpoint-subject";
    private const string ControlTree = "warm-checkpoint-control";
    private const string SiblingTree = "warm-checkpoint-sibling";
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(40);
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        foreach (var tree in new[] { SubjectTree, ControlTree, SiblingTree })
            await registry.RegisterAsync(tree, new TreeRegistryEntry
            {
                ShardCount = 1,
                MaxLeafKeys = 128,
                WalPartitions = 1,
            });
    }

    /// <summary>Sibling birth must arm the same timer before its first checkpoint.</summary>
    [Test]
    public async Task Sibling_birth_arms_progress_before_any_checkpoint()
    {
        var key = Guid.NewGuid();
        var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(key);
        using (LatticeNewLeafIntentContext.BeginScope(leaf.GetGrainId()))
            await leaf.InitializeSiblingAsync(new SiblingInitialization
            {
                TreeId = SiblingTree,
                ShardIndex = 0,
                LowKeyInclusive = "a",
            });
        Assert.That(await leaf.GetProjectionCheckpointOffsetAsync(), Is.Zero);
        await leaf.SetAsync("stable", [42]);
        var storage = _cluster.Client.GetGrain<ILeafSnapshotStorageGrain>(key);
        LeafSnapshotBlob? snapshot = null;
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < Budget)
        {
            await leaf.SetAsync("hot", [1]);
            Assert.That(await leaf.GetAsync("stable"), Is.EqualTo(new byte[] { 42 }));
            snapshot = await storage.LoadAsync(CancellationToken.None);
            if (snapshot is not null)
                break;
            await Task.Delay(100);
        }
        Assert.That(snapshot, Is.Not.Null, "sibling seeding must not wait for a checkpoint to register the timer");
        Assert.That(snapshot!.SnapshotOffsetsByPartition![0], Is.GreaterThanOrEqualTo(0));
        Assert.That(ContainsStableRow(snapshot), Is.True);
    }

    [OneTimeTearDown]
    public async Task TearDownAsync()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task Foreground_only_warm_leaf_banks_loadable_coverage_and_reclaims_its_wal()
    {
        var subject = _cluster.Client.GetGrain<ILattice>(SubjectTree);
        var control = _cluster.Client.GetGrain<ILattice>(ControlTree);
        // Finish activation over an empty WAL before any foreground append.
        await subject.GetAsync("stable");
        await control.GetAsync("stable");
        var (subjectLeaf, subjectSnapshot) = await ResolveLeafAsync(SubjectTree);
        var (controlLeaf, controlSnapshot) = await ResolveLeafAsync(ControlTree);
        // The administration accessor exposes the legacy scalar's birth
        // default, not the presence-aware replay sentinel.
        Assert.That(await subjectLeaf.GetProjectionCheckpointOffsetAsync(), Is.Zero);
        Assert.That(await controlLeaf.GetProjectionCheckpointOffsetAsync(), Is.Zero);

        await subject.SetAsync("stable", [42]);
        await control.SetAsync("stable", [42]);
        await subject.SetAsync("hot", [1]);
        await control.SetAsync("hot", [1]);
        var head = await _cluster.Client.GetGrain<ILeafReplayCoordinatorGrain>($"{SubjectTree}/0")
            .GetHeadOffsetAsync(CancellationToken.None);
        Assert.That(head, Is.GreaterThan(1), "the accepted foreground writes must be in the WAL");

        LeafSnapshotBlob? snapshot = null;
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < Budget)
        {
            // Continual foreground traffic keeps both leaves warm. It must not
            // manufacture a checkpoint from the latest append offset.
            await subject.SetAsync("hot", [1]);
            await control.SetAsync("hot", [1]);
            Assert.That(await subject.GetAsync("stable"), Is.EqualTo(new byte[] { 42 }));
            Assert.That(await control.GetAsync("stable"), Is.EqualTo(new byte[] { 42 }));
            snapshot = await subjectSnapshot.LoadAsync(CancellationToken.None);
            if (snapshot?.SnapshotOffsetsByPartition is { Length: > 0 } coverage
                && coverage[0] >= head - 1)
                break;
            await Task.Delay(100);
        }

        Assert.That(snapshot, Is.Not.Null,
            "the actual coverage-lag callback must replay and bank a loadable snapshot on a warm leaf");
        Assert.That(snapshot!.SnapshotOffsetsByPartition![0], Is.GreaterThanOrEqualTo(head - 1));
        Assert.That(ContainsStableRow(snapshot), Is.True,
            "loadable coverage must include the acknowledged row, not merely a non-negative offset");
        Assert.That(await subjectLeaf.GetProjectionCheckpointOffsetAsync(), Is.GreaterThanOrEqualTo(head - 1));
        Assert.That(await controlLeaf.GetProjectionCheckpointOffsetAsync(), Is.Zero,
            "without the timer, foreground writes alone must not fabricate a processed shared-WAL prefix");
        Assert.That(await controlSnapshot.LoadAsync(CancellationToken.None), Is.Null);

        var services = ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;
        var gc = services.GetRequiredService<ILatticeWalGc>();
        Assert.That((await gc.RunOnceAsync(ControlTree)).EntriesTrimmed, Is.Zero,
            "the never-covered control must retain the sole durable copy of its accepted writes");

        long trimmed = 0;
        LatticeWalGcReport report = default;
        clock.Restart();
        while (clock.Elapsed < Budget && trimmed == 0)
        {
            report = await gc.RunOnceAsync(SubjectTree);
            trimmed += report.EntriesTrimmed;
            await subject.GetAsync("stable");
            await Task.Delay(100);
        }
        Assert.That(trimmed, Is.GreaterThan(0),
            $"the timer's durable coverage must make real GC progress possible: {report}");
        var provider = services.GetRequiredService<IWalStorageProvider>();
        Assert.That(await provider.GetTrimWatermarkAsync(SubjectTree, 0, CancellationToken.None),
            Is.GreaterThanOrEqualTo(head - 1),
            "verify the provider's durable trim watermark, not only the GC report");
        Assert.That(await subject.GetAsync("stable"), Is.EqualTo(new byte[] { 42 }));
        Assert.That(ContainsStableRow((await subjectSnapshot.LoadAsync(CancellationToken.None))!), Is.True);
    }

    private static bool ContainsStableRow(LeafSnapshotBlob snapshot)
    {
        foreach (var row in snapshot.EnumerateRows())
            if (row.Key == "stable" && !row.Value.IsTombstone
                && row.Value.Value.AsSpan().SequenceEqual(new byte[] { 42 }))
                return true;
        return false;
    }

    private async Task<(IBPlusLeafGrain Leaf, ILeafSnapshotStorageGrain Snapshot)> ResolveLeafAsync(string tree)
    {
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{tree}/0");
        var id = await shard.GetLeftmostLeafIdAsync();
        Assert.That(id, Is.Not.Null);
        var key = id!.Value.GetGuidKey();
        return (_cluster.Client.GetGrain<IBPlusLeafGrain>(key),
            _cluster.Client.GetGrain<ILeafSnapshotStorageGrain>(key));
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.AddLatticeWalGc();
            siloBuilder.ConfigureLattice(options => options.WalGcInterval = TimeSpan.Zero);
            siloBuilder.UseInMemoryReminderService();
            foreach (var tree in new[] { SubjectTree, ControlTree, SiblingTree })
                siloBuilder.ConfigureLattice(tree, options =>
                {
                    options.WalPartitions = 1;
                    options.LeafSnapshotMaxCoverageLagSeconds = tree == ControlTree ? 0 : 1;
                    options.LeafSnapshotReClassifyEveryNCheckpoints = 0;
                    options.MaterialiserCheckpointInterval = TimeSpan.FromHours(1);
                    options.MaterialiserCheckpointEntries = 1_000_000;
                    options.WalMaterialiserMaxConcurrentReplays = 4;
                });
        }
    }
}
