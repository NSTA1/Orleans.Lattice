using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="SnapshotReplicatedTreeMembership"/>: a tree is
/// replicated when enabled in the compiled snapshot <b>or</b> present in the
/// static <see cref="LatticeReplicationOptions.ReplicatedTrees"/> seed map.
/// </summary>
[TestFixture]
public sealed class SnapshotReplicatedTreeMembershipTests
{
    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(
        IReadOnlyDictionary<string, LatticeMergeMode>? staticTrees)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ReplicatedTrees = staticTrees });
        return monitor;
    }

    [Test]
    public async Task IsReplicated_true_when_enabled_in_snapshot_only()
    {
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>
            {
                ["orders"] = ReplicationConfigSnapshotTestHelpers.Enabled(LatticeMergeMode.LwwRegister),
            });
        var membership = new SnapshotReplicatedTreeMembership(maintainer, Monitor(null));

        Assert.That(membership.IsReplicated("orders"), Is.True);
    }

    [Test]
    public async Task IsReplicated_true_when_in_static_seed_only()
    {
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>());
        var membership = new SnapshotReplicatedTreeMembership(
            maintainer,
            Monitor(new Dictionary<string, LatticeMergeMode> { ["inventory"] = LatticeMergeMode.OrSet }));

        Assert.That(membership.IsReplicated("inventory"), Is.True);
    }

    [Test]
    public async Task IsReplicated_false_when_disabled_in_snapshot_and_absent_from_seed()
    {
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>
            {
                ["orders"] = ReplicationConfigSnapshotTestHelpers.DisabledWithMode(LatticeMergeMode.LwwRegister),
            });
        var membership = new SnapshotReplicatedTreeMembership(maintainer, Monitor(null));

        Assert.That(membership.IsReplicated("orders"), Is.False);
    }

    [Test]
    public async Task IsReplicated_true_when_disabled_in_snapshot_but_present_in_seed()
    {
        // The static map is a floor: a runtime disable does not remove a
        // statically-declared tree in this sub-issue's union semantics.
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>
            {
                ["orders"] = ReplicationConfigSnapshotTestHelpers.DisabledWithMode(LatticeMergeMode.LwwRegister),
            });
        var membership = new SnapshotReplicatedTreeMembership(
            maintainer,
            Monitor(new Dictionary<string, LatticeMergeMode> { ["orders"] = LatticeMergeMode.LwwRegister }));

        Assert.That(membership.IsReplicated("orders"), Is.True);
    }

    [Test]
    public async Task ReplicatedTrees_unions_snapshot_enabled_and_static_seed()
    {
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>
            {
                ["orders"] = ReplicationConfigSnapshotTestHelpers.Enabled(LatticeMergeMode.LwwRegister),
                ["disabled"] = ReplicationConfigSnapshotTestHelpers.DisabledWithMode(LatticeMergeMode.OrSet),
            });
        var membership = new SnapshotReplicatedTreeMembership(
            maintainer,
            Monitor(new Dictionary<string, LatticeMergeMode> { ["inventory"] = LatticeMergeMode.OrSet }));

        Assert.That(membership.ReplicatedTrees, Is.EquivalentTo(new[] { "orders", "inventory" }));
    }

    [Test]
    public async Task ReplicatedTrees_deduplicates_a_tree_in_both_sources()
    {
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>
            {
                ["orders"] = ReplicationConfigSnapshotTestHelpers.Enabled(LatticeMergeMode.LwwRegister),
            });
        var membership = new SnapshotReplicatedTreeMembership(
            maintainer,
            Monitor(new Dictionary<string, LatticeMergeMode> { ["orders"] = LatticeMergeMode.LwwRegister }));

        Assert.That(membership.ReplicatedTrees, Is.EquivalentTo(new[] { "orders" }));
    }

    [Test]
    public async Task IsReplicated_throws_on_null_tree_id()
    {
        var maintainer = await ReplicationConfigSnapshotTestHelpers.WarmMaintainerAsync(
            new Dictionary<string, LatticeReplicationConfigEntry>());
        var membership = new SnapshotReplicatedTreeMembership(maintainer, Monitor(null));

        Assert.That(() => membership.IsReplicated(null!), Throws.ArgumentNullException);
    }
}
