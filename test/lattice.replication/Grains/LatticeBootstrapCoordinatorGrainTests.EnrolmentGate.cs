using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #5731. The applier drops every row of a tree its enrolment gate does not
/// admit and reports it as not applied, so a drain into a gate that refuses the
/// tree used to complete having imported nothing. The drain refreshes this
/// silo's enrolment view once and, if the gate still refuses, fails the attempt;
/// a gate that stops admitting the tree mid-drain fails it too.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    private static LatticeReplicationOptions EnrolledOptions(params string[] trees) => new()
    {
        ClusterId = "test-cluster",
        ReplicatedTrees = trees.ToDictionary(t => t, _ => LatticeMergeMode.LwwRegister, StringComparer.Ordinal),
        BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
        {
            MaxAttempts = 1,
            InitialDelay = TimeSpan.Zero,
            MaxDelay = TimeSpan.Zero,
        },
    };

    private static IServiceProvider Services(ILatticeReplicationContext context, IRefreshableReplicatedTreeMembership? refreshable = null)
    {
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ILatticeReplicationContext)).Returns(context);
        services.GetService(typeof(IRefreshableReplicatedTreeMembership)).Returns(refreshable);
        return services;
    }

    private static SnapshotEntry GateRow(string key, long hlc) =>
        new() { Key = key, Value = [1], Timestamp = Hlc(hlc) };

    [Test]
    public async Task A_drain_into_a_gate_that_refuses_the_tree_fails_rather_than_completing_empty()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, _, provider, _, apply, _, _) = Create(fake, replicationOptions: EnrolledOptions("other-tree"));
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(GateRow("k", 5)))));

        Assert.That(async () => await grain.ProcessNextPhaseAsync(), Throws.InstanceOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed),
                "a bootstrap whose every row the receiver would drop is never reported complete");
            Assert.That(fake.State.CompletedExportEpochs.ContainsKey(SourceCluster), Is.False);
        });
        await apply.DidNotReceive().ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_drain_refreshes_a_stale_enrolment_view_once_and_then_imports()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        var context = Substitute.For<ILatticeReplicationContext>();
        var refreshable = Substitute.For<IRefreshableReplicatedTreeMembership>();
        LatticeMergeMode? compiled = null;
        resolver.Resolve(Tree).Returns(_ => compiled);
        context.ResolveMergeMode(Tree).Returns(_ => compiled);
        refreshable.RefreshAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            compiled = LatticeMergeMode.LwwRegister;
            return Task.CompletedTask;
        });
        var (grain, _, _, provider, _, apply, _, _) = Create(
            fake, mergeResolver: resolver, activationServices: Services(context, refreshable));
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(GateRow("k", 5)))));

        await grain.ProcessNextPhaseAsync();

        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
        await refreshable.Received(1).RefreshAsync(Arg.Any<CancellationToken>());
        await apply.Received(1).ApplyAsync(
            Arg.Is<WalRecord>(r => r.Key == "k" && r.Mode == LatticeMergeMode.LwwRegister),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_drain_whose_tree_is_disenrolled_while_it_runs_fails()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        var context = Substitute.For<ILatticeReplicationContext>();
        LatticeMergeMode? compiled = LatticeMergeMode.LwwRegister;
        resolver.Resolve(Tree).Returns(_ => compiled);
        context.ResolveMergeMode(Tree).Returns(_ => compiled);
        var (grain, _, _, provider, _, apply, _, _) = Create(
            fake, mergeResolver: resolver, activationServices: Services(context));
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(_ =>
        {
            // A disable lands mid-drain: from here the gate drops the rows.
            compiled = null;
            return Task.FromResult(new ApplyResult { Applied = false });
        });
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(GateRow("k", 5)))));

        Assert.That(async () => await grain.ProcessNextPhaseAsync(), Throws.InstanceOf<InvalidOperationException>());
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
    }

    [Test]
    public async Task A_drain_of_an_enrolled_tree_makes_no_refresh()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        var context = Substitute.For<ILatticeReplicationContext>();
        var refreshable = Substitute.For<IRefreshableReplicatedTreeMembership>();
        resolver.Resolve(Tree).Returns(LatticeMergeMode.LwwRegister);
        context.ResolveMergeMode(Tree).Returns(LatticeMergeMode.LwwRegister);
        var (grain, _, _, provider, _, _, _, _) = Create(
            fake, mergeResolver: resolver, activationServices: Services(context, refreshable));
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(GateRow("k", 5)))));

        await grain.ProcessNextPhaseAsync();

        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
        await refreshable.DidNotReceive().RefreshAsync(Arg.Any<CancellationToken>());
    }
}
