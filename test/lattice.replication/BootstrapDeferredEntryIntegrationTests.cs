using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4604 on a real in-process cluster: a coordinated restore's durable
/// receive fence (#1173) makes the replication applier defer every entry for
/// the tree, the snapshot drain included. A deferred row cannot be published
/// from the shadow copy: once its bounded attempt fails, the original remains
/// readable, and the bootstrap can be started again after the fence lifts.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BootstrapDeferredEntryIntegrationTests
{
    private const string ClusterId = "deferred-receiver";
    private const string SourceCluster = "deferred-source";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    private ILattice Tree(string name) => _cluster.Client.GetGrain<ILattice>(name);

    private ILatticeBootstrapCoordinator Coordinator => new LatticeBootstrapCoordinator(_cluster.Client);

    private static string Str(byte[]? value) => value is null ? "<none>" : System.Text.Encoding.UTF8.GetString(value);

    private static byte[] Bytes(string value) => System.Text.Encoding.UTF8.GetBytes(value);

    [Test]
    public async Task A_deferred_shadow_import_is_discarded_and_can_restart_after_the_receive_fence_lifts()
    {
        var treeName = $"deferred-fence-{Guid.NewGuid():N}";
        await Tree(treeName).SetAsync("k1", Bytes("pre"));
        await Tree(treeName).SetAsync("k2", Bytes("pre"));
        var fence = _cluster.Client.GetGrain<ITreeReceiveFenceGrain>(treeName);
        await fence.PauseAsync("restore-saga");

        await Coordinator.BootstrapAsync(treeName, SourceCluster);

        // While the fence holds, every row the drain applies is deferred, so the
        // shadow copy must not reach the handoff.
        var held = DateTime.UtcNow + TimeSpan.FromSeconds(3);
        var phasesWhileFenced = new HashSet<LatticeBootstrapState>();
        while (DateTime.UtcNow < held)
        {
            phasesWhileFenced.Add((await Coordinator.GetStatusAsync(treeName)).Phase);
            await Task.Delay(100);
        }

        var failed = await Coordinator.GetStatusAsync(treeName);
        var beforeRetry = await Tree(treeName).GetManyAsync(["k1", "k2"]);
        await fence.ResumeAsync("restore-saga");
        await Coordinator.BootstrapAsync(treeName, SourceCluster);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        BootstrapCoordinatorStatus status;
        while (true)
        {
            status = await Coordinator.GetStatusAsync(treeName);
            if (status.Phase == LatticeBootstrapState.LiveIncremental || DateTime.UtcNow > deadline) break;
            await Task.Delay(200);
        }

        var after = await Tree(treeName).GetManyAsync(["k1", "k2"]);
        Assert.Multiple(() =>
        {
            Assert.That(phasesWhileFenced, Does.Not.Contain(LatticeBootstrapState.LiveIncremental),
                "the bootstrap completed while the receive fence deferred every row it applied");
            Assert.That(failed.Phase, Is.EqualTo(LatticeBootstrapState.Failed),
                "a bounded deferred apply failure discards the held shadow copy");
            Assert.That(failed.ReadFenced, Is.False);
            Assert.That(Str(beforeRetry.GetValueOrDefault("k1")), Is.EqualTo("pre"));
            Assert.That(Str(beforeRetry.GetValueOrDefault("k2")), Is.EqualTo("pre"));
            Assert.That(status.Phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental),
                "a fresh bootstrap imports the rows once the fence lifts");
            Assert.That(Str(after.GetValueOrDefault("k1")), Is.EqualTo("snapshot"), "k1's snapshot row was dropped");
            Assert.That(Str(after.GetValueOrDefault("k2")), Is.EqualTo("snapshot"), "k2's snapshot row was dropped");
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IBootstrapSnapshotSource, TwoRowSnapshotSource>();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = ClusterId;
                opts.BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
                {
                    MaxAttempts = 1,
                    InitialDelay = TimeSpan.Zero,
                    MaxDelay = TimeSpan.Zero,
                };
            });
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>A snapshot source exporting committed rows for <c>k1</c> and <c>k2</c>.</summary>
    private sealed class TwoRowSnapshotSource : IBootstrapSnapshotSource
    {
        public Task<SnapshotStream> ExportAsync(string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default) =>
            Task.FromResult(new SnapshotStream(treeName, asOfHlc, new VersionVector(), RowsAsync()));

        private static async IAsyncEnumerable<SnapshotEntry> RowsAsync()
        {
            await Task.CompletedTask;
            var stamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 0 };
            yield return new SnapshotEntry { Key = "k1", Value = Bytes("snapshot"), Timestamp = stamp };
            yield return new SnapshotEntry { Key = "k2", Value = Bytes("snapshot"), Timestamp = stamp };
        }
    }
}
