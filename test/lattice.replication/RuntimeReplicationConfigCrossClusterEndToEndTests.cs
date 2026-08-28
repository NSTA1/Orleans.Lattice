using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// The genuinely multi-cluster end-to-end scenario for runtime replication
/// configuration: an enable authored on cluster A's dogfooded
/// <c>sys-replication-config</c> OR-Map tree converges to cluster B, whose
/// compiled snapshot then reports the tree as replicated under the authored
/// mode. Both sites are real two-silo <see cref="TwoSiteClusterFixture"/>
/// clusters; the enable is authored through the real
/// <see cref="LatticeReplicationConfigAuthority"/> writing the real
/// <see cref="LatticeReplicationConfigStore"/> over cluster A's client.
/// <para>
/// Convergence is deterministic and sleep-free: the config OR-Map entry authored
/// on A is folded into B's config tree explicitly (modelling the config-tree
/// replication delivery), then B's
/// <see cref="CompiledReplicationConfigSnapshotMaintainer"/> is rebuilt
/// synchronously via <see cref="CompiledReplicationConfigSnapshotMaintainer.EnsureWarmAsync"/>
/// - there is no background pump, wall-clock wait, or ordering race. This
/// mirrors how the sibling cross-cluster integration fixtures drive the receiver
/// seam directly rather than pumping a transport.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class RuntimeReplicationConfigCrossClusterEndToEndTests
{
    private TwoSiteClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        // The fixture's stub merge-mode resolver defaults every tree to
        // LwwRegister; declare the dogfooded config tree under its real OrMap
        // mode so the OR-Map config writes below are permitted, exactly as the
        // enableRuntimeConfig anchor enrols it on a real silo.
        TwoSiteClusterFixture.TreeModeOverrides[LatticeSystemTreeNames.ReplicationConfig] =
            LatticeMergeMode.OrMap;

        _fixture = new TwoSiteClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Enable_authored_on_site_A_converges_to_site_B_snapshot_membership()
    {
        const string tree = "orders-cross-cluster";

        // Cluster A and cluster B each dogfood their own sys-replication-config
        // OR-Map tree, addressed through their own cluster client.
        var storeA = new LatticeReplicationConfigStore(_fixture.SiteA.Client);
        var storeB = new LatticeReplicationConfigStore(_fixture.SiteB.Client);

        // Before anything is authored, cluster B does not replicate the tree.
        var membershipBefore = Membership(await WarmMaintainerAsync(storeB));
        Assert.That(membershipBefore.IsReplicated(tree), Is.False,
            "cluster B must not replicate a tree no cluster has enabled yet");

        // Author the enable on cluster A through the real engine authority, which
        // writes the enablement flag + mode into A's config OR-Map.
        var authorityA = CreateAuthority(storeA, TwoSiteClusterFixture.SiteAClusterId);
        var enable = await authorityA.EnableReplicationAsync(tree, LatticeMergeMode.OrSet);
        Assert.That(enable.Mode, Is.EqualTo(LatticeMergeMode.OrSet));

        // Deliver A's converged config entry to cluster B's config tree, exactly
        // as replicating the sys-replication-config OR-Map would. B's store folds
        // it in through the same MergeFrom the wire delivery uses.
        var authored = await storeA.ReadEntryAsync(tree);
        Assert.That(authored, Is.Not.Null);
        await storeB.WriteEntryAsync(tree, TwoSiteClusterFixture.SiteAClusterId, authored!);

        // Cluster B now compiles a snapshot from its own converged config tree
        // and begins replicating the tree under the authored mode.
        var maintainerB = await WarmMaintainerAsync(storeB);
        var membershipB = Membership(maintainerB);
        var resolverB = Resolver(maintainerB);

        Assert.Multiple(() =>
        {
            Assert.That(membershipB.IsReplicated(tree), Is.True,
                "the enable authored on cluster A must make cluster B replicate the tree");
            Assert.That(membershipB.ReplicatedTrees, Does.Contain(tree));
            Assert.That(resolverB.Resolve(tree), Is.EqualTo(LatticeMergeMode.OrSet),
                "cluster B must resolve the merge mode authored on cluster A");
            Assert.That(maintainerB.Current.TryGetTree(tree, out var projection), Is.True);
            Assert.That(projection.Enabled, Is.True);
            Assert.That(projection.Ambiguous, Is.False);
        });
    }

    private static LatticeReplicationConfigAuthority CreateAuthority(
        ILatticeReplicationConfigStore store, string localReplicaId)
    {
        var context = Substitute.For<ILatticeReplicationContext>();
        context.LocalReplicaId.Returns(localReplicaId);
        context.IsReplicationEnabled.Returns(true);

        var preconditions = new LatticeReplicationPreconditionValidator(context);
        var admin = Substitute.For<ILatticeReplicationAdmin>();
        var probe = Substitute.For<ILatticeTreeContentProbe>();
        probe.HasContentAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(false));

        return new LatticeReplicationConfigAuthority(store, preconditions, context, admin, probe);
    }

    private static async Task<CompiledReplicationConfigSnapshotMaintainer> WarmMaintainerAsync(
        ILatticeReplicationConfigStore store)
    {
        var maintainer = new CompiledReplicationConfigSnapshotMaintainer(
            store, NullLogger<CompiledReplicationConfigSnapshotMaintainer>.Instance);
        await maintainer.EnsureWarmAsync();
        return maintainer;
    }

    private static IOptionsMonitor<LatticeReplicationOptions> EmptyMonitor()
    {
        var options = new LatticeReplicationOptions { ClusterId = "x", ReplicatedTrees = null };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        monitor.OnChange(Arg.Any<Action<LatticeReplicationOptions, string?>>())
            .Returns(Substitute.For<IDisposable>());
        return monitor;
    }

    private static SnapshotReplicatedTreeMembership Membership(
        CompiledReplicationConfigSnapshotMaintainer maintainer) =>
        new(maintainer, EmptyMonitor());

    private static SnapshotLatticeMergeModeResolver Resolver(
        CompiledReplicationConfigSnapshotMaintainer maintainer) =>
        new(maintainer, new ConfiguredLatticeMergeModeResolver(EmptyMonitor()));
}
