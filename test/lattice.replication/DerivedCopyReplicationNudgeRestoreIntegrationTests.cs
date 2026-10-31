using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// The restored-copy case of <see cref="DerivedCopyReplicationNudgeIntegrationTests"/>
/// (the #5300 / #5306 class, epic #4908): after a coordinated restore swaps a
/// replicated tree onto its restored physical copy (<c>{tree}-bkprestore-{op}</c>),
/// a local write committed on the copy is published to observers under the logical
/// tree, so the real exact-name <see cref="ConfiguredLatticeMergeModeResolver"/>
/// resolves it and the doorbell rings the logical tree's shipper, never a shipper
/// keyed by the copy's physical id.
/// <para>
/// Kept in its own fixture because the backup add-on's replicated-sink startup
/// guard rejects the in-cluster sink once trees are declared replicated; the
/// membership seam is overridden to report nothing to the guard, as
/// <c>CoordinatedRestoreClusterFixture</c> does.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class DerivedCopyReplicationNudgeRestoreIntegrationTests
{
    private const string RestoredTree = "dcn-restored";
    private const string ClusterId = "dcn-restore-local";

    private static readonly ConcurrentQueue<string> Rung = new();
    private static readonly ConcurrentQueue<LatticeMutation> Published = new();

    private TestCluster _cluster = null!;

    private IServiceProvider Services => _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

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

    [Test]
    public async Task Write_on_a_restored_copy_nudges_the_logical_tree_shipper()
    {
        var lattice = _cluster.Client.GetGrain<ILattice>(RestoredTree);
        await lattice.SetAsync("cut", Encoding.UTF8.GetBytes("cut"));
        var backup = await Services.GetRequiredService<ILatticeBackupCaptureService>().CaptureAsync(
            new LatticeBackupCaptureRequest("dcn", BackupScopeSelector.WholeTree(RestoredTree)));
        await lattice.SetAsync("after-cut", Encoding.UTF8.GetBytes("after"));

        var participant = new RestoreParticipant(
            Services.GetRequiredService<ILatticeCoordinatedRestoreEngine>(),
            Services.GetRequiredService<ILatticeBackupRestoreService>(),
            Services.GetRequiredService<IRestoreCapacityProbe>(),
            Services.GetRequiredService<IGrainFactory>(),
            NullLogger<RestoreParticipant>.Instance);
        var request = new SagaControlRequest
        {
            SagaId = "dcn-restore-saga",
            TargetTree = RestoredTree,
            ManifestId = backup.BackupId,
            CoordinatorClusterId = ClusterId,
        };
        Assert.That((await participant.PrepareAsync(request)).Vote, Is.EqualTo(SagaVote.Commit), "precondition: the restore prepared");
        await participant.CommitAsync(request);
        var physical = (await _cluster.Client.GetLatticeRegistry().GetEntryAsync(RestoredTree))?.PhysicalTreeId;
        Assert.That(physical, Is.Not.Null.And.Not.EqualTo(RestoredTree), "precondition: the tree resolves to the restored copy");
        Rung.Clear();
        Published.Clear();

        await lattice.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        Assert.Multiple(() =>
        {
            Assert.That(Rung, Does.Contain(RestoredTree), "the write nudged the logical tree's shipper");
            Assert.That(Rung, Does.Not.Contain(physical), "no nudge is keyed by the copy's physical id");
            Assert.That(Published.Where(m => m.Key == "k").Select(m => m.TreeId), Is.All.EqualTo(RestoredTree).And.Not.Empty,
                "the write committed on the restored copy is published under the logical tree");
        });
    }

    private sealed class RecordingReplogSink : IReplogSink
    {
        public Task WriteAsync(string treeId, CancellationToken cancellationToken)
        {
            Rung.Enqueue(treeId);
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingMutationObserver : IMutationObserver
    {
        public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
        {
            Published.Enqueue(mutation);
            return Task.CompletedTask;
        }
    }

    /// <summary>Reports the restored tree replicated, and nothing to the backup sink guard.</summary>
    private sealed class GuardSilentMembership : IReplicatedTreeMembership
    {
        public IReadOnlyCollection<string> ReplicatedTrees => Array.Empty<string>();

        public bool IsReplicated(string treeId) => string.Equals(treeId, RestoredTree, StringComparison.Ordinal);
    }

    /// <summary>The participant is driven directly, so the control channel is never used.</summary>
    private sealed class UnusedSagaControlChannel : ISagaControlChannel
    {
        public Task<SagaControlResponse> PrepareAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<SagaControlResponse> CommitAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<SagaControlResponse> AbortAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<SagaControlResponse> GetStatusAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.Services.AddSingleton<IReplogSink, RecordingReplogSink>();
            siloBuilder.Services.AddSingleton<IMutationObserver, RecordingMutationObserver>();
            siloBuilder.Services.AddSingleton<ISagaControlChannel, UnusedSagaControlChannel>();
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = ClusterId;
                opts.AutoBootstrapOnFallOffLog = false;
                opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
                {
                    [RestoredTree] = LatticeMergeMode.LwwRegister,
                };
            });
            siloBuilder.AddLatticeBackup();
            siloBuilder.Services.AddSingleton<IReplicatedTreeMembership, GuardSilentMembership>();
        }
    }
}
