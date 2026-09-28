using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Shell.Areas.Cluster;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Cluster;

/// <summary>
/// The area's pure readings: logical names and their owners, the catalogue's
/// projection (which drops every physical shadow), the region roll-up, the
/// fault sentences and the number formats.
/// </summary>
[TestFixture]
public sealed class ClusterReadingTests
{
    [Test]
    [TestCase("orders", null, null, "orders", null)]
    [TestCase("a/crm/orders", null, "crm", "orders", "app crm")]
    [TestCase("t/acme/orders", "acme", null, "orders", "tenant acme")]
    [TestCase("t/acme/a/crm/orders/2026", "acme", "crm", "orders/2026", "app crm, tenant acme")]
    [TestCase("a/crm", null, null, "a/crm", null)]
    [TestCase("t//orders", null, null, "t//orders", null)]
    public void A_logical_name_names_its_owners(string treeId, string? tenant, string? app, string name, string? ownership)
    {
        var parsed = ClusterTreeName.Parse(treeId);

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Tenant, Is.EqualTo(tenant));
            Assert.That(parsed.App, Is.EqualTo(app));
            Assert.That(parsed.Name, Is.EqualTo(name));
            Assert.That(parsed.Ownership, Is.EqualTo(ownership));
            Assert.That(parsed.IsAppTree, Is.EqualTo(app is not null));
            Assert.That(parsed.TreeId, Is.EqualTo(treeId));
        });
    }

    [Test]
    public void The_catalogue_drops_resize_and_restore_shadows_and_orders_by_id()
    {
        var projected = ClusterTreeCatalog.Project(
        [
            ClusterTestContext.Tree("orders") with { IsAlias = true, PhysicalTreeId = "orders/resized/7f3a" },
            ClusterTestContext.Tree("orders/resized/7f3a"),
            ClusterTestContext.Tree("a/crm/invoices"),
            ClusterTestContext.Tree("a/crm/invoices-shadow") with { RestoreShadowOfTreeId = "a/crm/invoices" },
            ClusterTestContext.Tree("deleted") with { Lifecycle = TreeLifecycleState.SoftDeleted },
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(projected.Select(tree => tree.TreeId), Is.EqualTo(new[] { "a/crm/invoices", "deleted", "orders" }));
            Assert.That(projected.Single(tree => tree.TreeId == "orders").IsAliased, Is.True);
            Assert.That(projected[0].Name.App, Is.EqualTo("crm"));
            Assert.That(projected[1].Lifecycle, Is.EqualTo(TreeLifecycleState.SoftDeleted));
            Assert.That(projected[2].WalPartitions, Is.EqualTo(2));
            Assert.That(projected[2].VirtualShardCount, Is.EqualTo(4096));
        });
    }

    [Test]
    public void Peers_roll_up_by_region_and_are_stalled_only_when_every_link_is()
    {
        var peers = ClusterRegionPicture.RollUp(
        [
            Link("orders", "us-east", ReplicationLinkHealth.Stalled),
            Link("invoices", "us-east", ReplicationLinkHealth.Stalled, ReplicationLinkDirection.Inbound),
            Link("orders", "ap-south", ReplicationLinkHealth.Stalled),
            Link("invoices", "ap-south", ReplicationLinkHealth.Healthy),
            Link("orders", "eu-north", ReplicationLinkHealth.Lagging),
            Link("orders", "sa-east", ReplicationLinkHealth.Healthy),
            Link("orders", "af-south", ReplicationLinkHealth.Unknown),
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(peers.Select(peer => peer.RegionId), Is.EqualTo(new[] { "af-south", "ap-south", "eu-north", "sa-east", "us-east" }));
            Assert.That(peers.Where(peer => peer.IsStalled).Select(peer => peer.RegionId), Is.EqualTo(new[] { "us-east" }));
            Assert.That(peers.Select(peer => peer.Health), Is.EqualTo(new[]
            {
                ReplicationLinkHealth.Unknown, ReplicationLinkHealth.Lagging, ReplicationLinkHealth.Lagging,
                ReplicationLinkHealth.Healthy, ReplicationLinkHealth.Stalled,
            }));
            Assert.That(peers.Single(peer => peer.RegionId == "us-east").Trees, Is.EqualTo(2));
            Assert.That(new ClusterRegionPeer("x", 0, 0, 0, 0, 0).IsStalled, Is.False, "a peer with no link is not stalled");
        });
    }

    [Test]
    public void Faults_read_as_one_sentence()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClusterFaults.Describe(new LatticeAuthorizationDeniedException("secret detail")), Is.EqualTo(ClusterFaults.Denied));
            Assert.That(ClusterFaults.Describe(new KeyNotFoundException()), Is.EqualTo("The cluster does not know that tree."));
            Assert.That(ClusterFaults.Describe(new NotSupportedException()), Is.EqualTo("This cluster does not serve that operation."));
            Assert.That(ClusterFaults.Describe(new TimeoutException()), Is.EqualTo("The cluster did not answer in time."));
            Assert.That(ClusterFaults.Describe(new InvalidOperationException("A resize is already in flight.")), Is.EqualTo("A resize is already in flight."));
            Assert.That(ClusterFaults.Describe(new InvalidOperationException()), Is.EqualTo("The cluster could not complete the request."));
            Assert.That(ClusterFaults.IsDenied(new UnauthorizedAccessException()), Is.True);
            Assert.That(ClusterFaults.IsDenied(new InvalidOperationException()), Is.False);
            Assert.That(() => ClusterFaults.Describe(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Numbers_read_in_the_area_formats()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClusterFormat.Count(48210), Is.EqualTo("48,210"));
            Assert.That(ClusterFormat.Rate(3.25), Is.EqualTo("3.2").Or.EqualTo("3.3"));
            Assert.That(ClusterFormat.Bytes(512), Is.EqualTo("512 B"));
            Assert.That(ClusterFormat.Bytes(1536), Is.EqualTo("1.5 KiB"));
            Assert.That(ClusterFormat.Bytes(4L * 1024 * 1024 * 1024), Is.EqualTo("4.0 GiB"));
            Assert.That(ClusterFormat.Instant(new DateTimeOffset(2026, 9, 28, 14, 2, 11, TimeSpan.FromHours(1))), Is.EqualTo("2026-09-28 13:02:11 UTC"));
            Assert.That(ClusterFormat.Duration(TimeSpan.Zero), Is.EqualTo("none"));
            Assert.That(ClusterFormat.Duration(TimeSpan.FromDays(7)), Is.EqualTo("7 days"));
            Assert.That(ClusterFormat.Duration(TimeSpan.FromHours(3)), Is.EqualTo("3 hours"));
            Assert.That(ClusterFormat.Duration(TimeSpan.FromMinutes(1)), Is.EqualTo("1 minute"));
            Assert.That(ClusterFormat.Duration(TimeSpan.FromSeconds(90)), Is.EqualTo("90 seconds"));
            Assert.That(ClusterFormat.Plural(1, "leaf", "leaves"), Is.EqualTo("1 leaf"));
            Assert.That(ClusterFormat.Plural(2, "leaf", "leaves"), Is.EqualTo("2 leaves"));
            Assert.That(ClusterFormat.Plural(2, "tree"), Is.EqualTo("2 trees"));
        });
    }

    [Test]
    public void The_stylesheet_address_derives_from_the_one_content_base_path_and_ships()
    {
        var relative = ClusterAssets.Stylesheet[Orleans.Lattice.Explorer.Shell.Design.ShellDesignAssets.ContentBasePath.Length..];
        var file = Path.Combine(Orleans.Lattice.Testing.Hygiene.HygieneRepository.FindRepoRoot(), "src", "lattice.explorer", "Shell", "wwwroot", relative);

        Assert.Multiple(() =>
        {
            Assert.That(ClusterAssets.Stylesheet, Does.StartWith(Orleans.Lattice.Explorer.Shell.Design.ShellDesignAssets.ContentBasePath));
            Assert.That(File.Exists(file), Is.True, file);
        });
    }

    private static ReplicationPeerStatusEntry Link(string tree, string peer, ReplicationLinkHealth health, ReplicationLinkDirection direction = ReplicationLinkDirection.Outbound) =>
        new(tree, peer, direction, 0, 0, 0, TimeSpan.FromSeconds(1), 0, health);
}
