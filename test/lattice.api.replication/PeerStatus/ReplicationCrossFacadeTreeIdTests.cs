using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Tests.PeerStatus;

/// <summary>
/// Cross-facade regression tests for issue #4000: under an asserted, non-default
/// tenant the status report (<see cref="ILatticeReplicationStatus"/>) and the
/// enrolment report (<see cref="ILatticeReplicationControl.GetReplicationConfigAsync"/>)
/// must name the caller's own trees by the SAME id - the effective,
/// tenant-qualified <c>t/{tenant}/{name}</c> id - so a consumer can join a
/// tree's links to its enrolment. Both facades are built over one tenant
/// resolver and one access gate, and neither may widen what the gate admits.
/// </summary>
[TestFixture]
public sealed class ReplicationCrossFacadeTreeIdTests
{
    private const string Tenant = "acme";
    private const string OwnAppTree = "t/acme/a/task-board/tasks";
    private const string OwnPlainTree = "t/acme/orders";
    private const string ForeignTree = "t/globex/orders";
    private const string DefaultTree = "orders";

    private static (LatticeReplicationControl Control, LatticeReplicationStatus Status) CreateFacades(
        ILatticeAccessGate gate,
        ITenantContextResolver resolver,
        IEnumerable<string> enrolledTrees,
        IEnumerable<string> linkedTrees)
    {
        var statuses = enrolledTrees.ToDictionary(
            tree => tree,
            tree => new LatticeReplicationTreeStatus(tree, true, LatticeMergeMode.LwwRegister, false),
            StringComparer.Ordinal);
        var authority = Substitute.For<ILatticeReplicationConfigAuthority>();
        authority
            .GetAllTreeStatusesAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyDictionary<string, LatticeReplicationTreeStatus>>(statuses));

        var reader = new StatsBackedPeerStatusReader();
        foreach (var tree in linkedTrees)
        {
            reader.Stats.RecordSuccess(tree, "east");
        }

        var replicationOptions = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        replicationOptions.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = "west" });
        var statusOptions = Substitute.For<IOptionsMonitor<LatticeReplicationStatusOptions>>();
        statusOptions.CurrentValue.Returns(new LatticeReplicationStatusOptions());
        var authorizer = new ReplicationAccessAuthorizer(gate, membership: null);

        return (
            new LatticeReplicationControl(
                authority, authorizer, resolver, Substitute.For<ILatticeReplicationPeerDecommissioner>()),
            new LatticeReplicationStatus(reader, authorizer, resolver, replicationOptions, statusOptions));
    }

    [Test]
    public async Task A_tenant_tree_has_the_same_id_in_the_status_and_enrolment_reports()
    {
        var (control, status) = CreateFacades(
            new AllowingAccessGate(),
            FixedTenantContextResolver.For(Tenant),
            enrolledTrees: [OwnAppTree],
            linkedTrees: [OwnAppTree]);

        var enrolment = await control.GetReplicationConfigAsync();
        var links = await status.GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        var enrolledIds = enrolment.Trees.Select(tree => tree.TreeId).ToArray();
        var linkedIds = links.Peers.Select(link => link.TreeId).Distinct(StringComparer.Ordinal).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(enrolledIds, Is.EqualTo(new[] { OwnAppTree }));
            Assert.That(linkedIds, Is.EqualTo(enrolledIds), "the status report must name the tree by its enrolment id");
            Assert.That(
                enrolment.Trees.Join(links.Peers, tree => tree.TreeId, link => link.TreeId, (tree, _) => tree.TreeId, StringComparer.Ordinal),
                Is.EqualTo(new[] { OwnAppTree }),
                "each enrolled tree must join to its links on tree id");
        });
    }

    [Test]
    public async Task A_tenant_tree_id_from_the_enrolment_report_filters_the_status_report()
    {
        var (control, status) = CreateFacades(
            new AllowingAccessGate(),
            FixedTenantContextResolver.For(Tenant),
            enrolledTrees: [OwnAppTree, OwnPlainTree],
            linkedTrees: [OwnAppTree, OwnPlainTree]);

        var enrolled = (await control.GetReplicationConfigAsync()).Trees.Single(tree => tree.TreeId == OwnAppTree);
        var links = await status.GetPeerStatusAsync(new ReplicationPeerStatusQuery { TreeId = enrolled.TreeId });

        Assert.That(links.Peers.Select(link => link.TreeId), Is.EqualTo(new[] { OwnAppTree }));
    }

    [Test]
    public async Task Neither_report_shows_a_tree_the_gate_does_not_admit_to_the_tenant()
    {
        // The gate models the tenancy enforcer: acme may manage only its own trees.
        string[] all = [OwnAppTree, OwnPlainTree, ForeignTree, DefaultTree];
        var (control, status) = CreateFacades(
            new TreeScopedAccessGate(OwnAppTree, OwnPlainTree),
            FixedTenantContextResolver.For(Tenant),
            enrolledTrees: all,
            linkedTrees: all);

        var enrolledIds = (await control.GetReplicationConfigAsync()).Trees.Select(tree => tree.TreeId).Order(StringComparer.Ordinal).ToArray();
        var linkedIds = (await status.GetPeerStatusAsync(ReplicationPeerStatusQuery.All)).Peers.Select(link => link.TreeId).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(enrolledIds, Is.EqualTo(new[] { OwnAppTree, OwnPlainTree }));
            Assert.That(linkedIds, Is.EqualTo(enrolledIds));
        });
    }

    [Test]
    public async Task The_default_tenant_sees_bare_ids_in_both_reports()
    {
        var (control, status) = CreateFacades(
            new AllowingAccessGate(),
            new DefaultTenantContextResolver(),
            enrolledTrees: [DefaultTree],
            linkedTrees: [DefaultTree]);

        var enrolledIds = (await control.GetReplicationConfigAsync()).Trees.Select(tree => tree.TreeId).ToArray();
        var linkedIds = (await status.GetPeerStatusAsync(ReplicationPeerStatusQuery.All)).Peers.Select(link => link.TreeId).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(enrolledIds, Is.EqualTo(new[] { DefaultTree }));
            Assert.That(linkedIds, Is.EqualTo(enrolledIds));
        });
    }
}
