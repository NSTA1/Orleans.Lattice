using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Replication.ReplicationTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>
/// The Replication area's pure pieces: health roles and ordering, text formatting,
/// app ownership from the <c>a/{slug}/</c> prefix, addresses, filters, and the
/// roll-ups the estate diagram and the trees page draw.
/// </summary>
[TestFixture]
public sealed class ReplicationModelTests
{
    [TestCase(ReplicationLinkHealth.Healthy, LtStateRole.Healthy, "Healthy", 0)]
    [TestCase(ReplicationLinkHealth.Lagging, LtStateRole.Lagging, "Lagging", 2)]
    [TestCase(ReplicationLinkHealth.Stalled, LtStateRole.Stalled, "Stalled", 3)]
    [TestCase(ReplicationLinkHealth.Unknown, LtStateRole.Unknown, "Unknown", 1)]
    public void Every_health_has_a_role_a_label_a_severity_and_a_query_value(ReplicationLinkHealth health, LtStateRole role, string label, int severity)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationHealth.Role(health), Is.EqualTo(role));
            Assert.That(ReplicationHealth.Label(health), Is.EqualTo(label));
            Assert.That(ReplicationHealth.Severity(health), Is.EqualTo(severity));
            Assert.That(ReplicationHealth.QueryValue(health), Is.EqualTo(label.ToLowerInvariant()));
            Assert.That(ReplicationHealth.TryParse(label.ToUpperInvariant(), out var parsed), Is.True);
            Assert.That(parsed, Is.EqualTo(health));
        });
    }

    [Test]
    public void Health_orders_worst_first_and_rejects_unknown_query_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationHealth.WorstFirst, Is.EqualTo(new[] { ReplicationLinkHealth.Stalled, ReplicationLinkHealth.Lagging, ReplicationLinkHealth.Unknown, ReplicationLinkHealth.Healthy }));
            Assert.That(ReplicationHealth.Worse(ReplicationLinkHealth.Healthy, ReplicationLinkHealth.Lagging), Is.EqualTo(ReplicationLinkHealth.Lagging));
            Assert.That(ReplicationHealth.Worse(ReplicationLinkHealth.Stalled, ReplicationLinkHealth.Lagging), Is.EqualTo(ReplicationLinkHealth.Stalled));
            Assert.That(ReplicationHealth.TryParse("broken", out _), Is.False);
            Assert.That(ReplicationHealth.TryParse(null, out _), Is.False);
            Assert.That(ReplicationHealth.Role((ReplicationLinkHealth)42), Is.EqualTo(LtStateRole.Unknown));
        });
    }

    [Test]
    public void Format_writes_counts_sizes_backlogs_and_contact_times()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationFormat.Count(1204), Is.EqualTo("1,204"));
            Assert.That(ReplicationFormat.Count(1, "tree", "trees"), Is.EqualTo("1 tree"));
            Assert.That(ReplicationFormat.Count(3, "tree", "trees"), Is.EqualTo("3 trees"));
            Assert.That(ReplicationFormat.Bytes(-5), Is.EqualTo("0 B"));
            Assert.That(ReplicationFormat.Bytes(512), Is.EqualTo("512 B"));
            Assert.That(ReplicationFormat.Bytes(2048), Is.EqualTo("2.0 KB"));
            Assert.That(ReplicationFormat.Bytes(3_355_443), Is.EqualTo("3.2 MB"));
            Assert.That(ReplicationFormat.Bytes(200L * 1024 * 1024 * 1024), Is.EqualTo("200 GB"));
            Assert.That(ReplicationFormat.Backlog(0, 0), Is.EqualTo("Caught up"));
            Assert.That(ReplicationFormat.Backlog(1, 10), Is.EqualTo("1 entry, 10 B behind"));
            Assert.That(ReplicationFormat.Contact(null), Is.EqualTo("Never"));
            Assert.That(ReplicationFormat.Contact(TimeSpan.FromSeconds(-3)), Is.EqualTo("0 s ago"));
            Assert.That(ReplicationFormat.Contact(TimeSpan.FromSeconds(12)), Is.EqualTo("12 s ago"));
            Assert.That(ReplicationFormat.Contact(TimeSpan.FromMinutes(4.5)), Is.EqualTo("4 min ago"));
            Assert.That(ReplicationFormat.Contact(new TimeSpan(2, 5, 0)), Is.EqualTo("2 h 5 min ago"));
            Assert.That(ReplicationFormat.Contact(TimeSpan.FromDays(3)), Is.EqualTo("3 d ago"));
            Assert.That(ReplicationFormat.Direction(ReplicationLinkDirection.Inbound), Is.EqualTo("Inbound"));
            Assert.That(ReplicationFormat.Direction(ReplicationLinkDirection.Outbound), Is.EqualTo("Outbound"));
        });
    }

    [Test]
    public void Format_names_every_merge_mode_and_enrolment_source()
    {
        Assert.Multiple(() =>
        {
            foreach (var mode in Enum.GetValues<LatticeMergeMode>())
            {
                Assert.That(ReplicationFormat.MergeMode(mode), Is.Not.EqualTo(mode.ToString()).Or.EqualTo("Sequence"), mode.ToString());
            }

            Assert.That(ReplicationFormat.MergeMode(LatticeMergeMode.LwwRegister), Is.EqualTo("LWW register"));
            Assert.That(ReplicationFormat.MergeMode((LatticeMergeMode?)null), Is.EqualTo("None"));
            Assert.That(ReplicationFormat.MergeMode((LatticeMergeMode)99), Is.EqualTo("99"));
            Assert.That(ReplicationFormat.Source(ReplicationEnrollmentSource.Runtime), Is.EqualTo("Runtime"));
            Assert.That(ReplicationFormat.Source(ReplicationEnrollmentSource.Static), Is.EqualTo("Static"));
            Assert.That(ReplicationFormat.Source(ReplicationEnrollmentSource.RuntimeAndStatic), Is.EqualTo("Runtime and static"));
        });
    }

    [TestCase("a/crm/orders", "crm")]
    [TestCase("a/crm/orders/archive", "crm")]
    [TestCase("a/task-board/items", "task-board")]
    [TestCase("t/acme/a/task-board/tasks", "task-board")]
    [TestCase("t/acme/a/crm/orders/archive", "crm")]
    public void An_a_slug_prefix_names_the_owning_app(string tree, string slug)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationTreeOwnership.TryGetAppSlug(tree, out var owner), Is.True);
            Assert.That(owner, Is.EqualTo(slug));
            Assert.That(ReplicationTreeOwnership.IsAppOwned(tree), Is.True);
        });
    }

    [TestCase(null)]
    [TestCase("orders")]
    [TestCase("a/")]
    [TestCase("a/crm")]
    [TestCase("a/crm/")]
    [TestCase("a//orders")]
    [TestCase("a/CRM/orders")]
    [TestCase("ab/crm/orders")]
    [TestCase("t/acme/a")]
    [TestCase("t/acme/orders")]
    [TestCase("t/acme/a/crm")]
    [TestCase("t//a/crm/orders")]
    [TestCase("t/acme/")]
    [TestCase("x/acme/a/crm/orders")]
    public void Anything_else_is_not_app_owned(string? tree)
    {
        Assert.That(ReplicationTreeOwnership.TryGetAppSlug(tree, out _), Is.False);
    }

    [Test]
    public void The_app_link_is_the_apps_area_replication_page()
    {
        Assert.That(ReplicationTreeOwnership.AppReplicationAddress("crm").Format(), Is.EqualTo("/apps/crm/replication"));
    }

    [Test]
    public void Addresses_cover_the_estate_the_trees_and_one_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationAddresses.Estate.Format(), Is.EqualTo("/replication"));
            Assert.That(ReplicationAddresses.Trees.Format(), Is.EqualTo("/replication/trees"));
            Assert.That(ReplicationAddresses.ForRegion("us-east").Format(), Is.EqualTo("/replication?region=us-east"));
            Assert.That(ReplicationAddresses.ForApp("crm").Format(), Is.EqualTo("/replication?app=crm"));
            Assert.That(ReplicationAddresses.ForTree("a/crm/orders")!.Format(), Is.EqualTo("/replication/trees/a/crm/orders"));
            Assert.That(ReplicationAddresses.ForTree("Orders")!.Format(), Is.EqualTo("/replication/trees/%4Frders"));
            Assert.That(ReplicationAddresses.ForTree(""), Is.Null);
            Assert.That(ReplicationAddresses.ForTree("a//x"), Is.Null);
            Assert.That(ReplicationAddresses.TreeIdOf(ExplorerAddress.Parse("/replication/trees/a/crm/orders")), Is.EqualTo("a/crm/orders"));
            Assert.That(ReplicationAddresses.TreeIdOf(ExplorerAddress.Parse("/t/acme/replication/trees/orders")), Is.EqualTo("orders"));
            Assert.That(ReplicationAddresses.TreeIdOf(ExplorerAddress.Parse("/replication/trees")), Is.Null);
            Assert.That(ReplicationAddresses.TreeIdOf(ExplorerAddress.Parse("/data/trees/orders")), Is.Null);
            Assert.That(() => ReplicationAddresses.TreeIdOf(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Filters_read_from_the_address_and_ignore_values_that_name_nothing()
    {
        var filter = ReplicationFilter.From(ExplorerAddress.Parse("/replication?health=Stalled&region=ap-south&app=crm"));
        var unknown = ReplicationFilter.From(ExplorerAddress.Parse("/replication?health=broken&region="));

        Assert.Multiple(() =>
        {
            Assert.That(filter, Is.EqualTo(new ReplicationFilter(ReplicationLinkHealth.Stalled, "ap-south", "crm")));
            Assert.That(filter.IsActive, Is.True);
            Assert.That(unknown, Is.EqualTo(ReplicationFilter.None));
            Assert.That(unknown.IsActive, Is.False);
            Assert.That(() => ReplicationFilter.From(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_filter_matches_links_by_health_region_and_app()
    {
        var links = Estate().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(links.Count(ReplicationFilter.None.Matches), Is.EqualTo(links.Length));
            Assert.That(links.Where(new ReplicationFilter(ReplicationLinkHealth.Stalled, null, null).Matches).Select(link => link.TreeId), Is.EqualTo(new[] { "a/crm/contacts" }));
            Assert.That(links.Count(new ReplicationFilter(null, "ap-south", null).Matches), Is.EqualTo(3));
            Assert.That(links.Count(new ReplicationFilter(null, null, "billing").Matches), Is.EqualTo(2));
            Assert.That(links.Count(new ReplicationFilter(ReplicationLinkHealth.Healthy, "ap-south", "billing").Matches), Is.EqualTo(1));
            Assert.That(new ReplicationFilter(null, null, "crm").MatchesTree("orders"), Is.False);
            Assert.That(() => ReplicationFilter.None.Matches(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void An_edge_rolls_its_links_up_to_the_worst_health_and_the_summed_backlog()
    {
        var edge = ReplicationEdgeSummary.From(ReplicationLinkDirection.Outbound,
        [
            Link("a", "p", ReplicationLinkHealth.Healthy, entries: 5, bytes: 10, errors: 1),
            Link("b", "p", ReplicationLinkHealth.Stalled, entries: 7, bytes: 20, errors: 9),
            Link("b", "p", ReplicationLinkHealth.Lagging, entries: -3, bytes: long.MaxValue),
        ])!;

        Assert.Multiple(() =>
        {
            Assert.That(edge.Health, Is.EqualTo(ReplicationLinkHealth.Stalled));
            Assert.That(edge.Links, Is.EqualTo(3));
            Assert.That(edge.Trees, Is.EqualTo(2));
            Assert.That(edge.EntriesBehind, Is.EqualTo(12), "a negative backlog counts as none");
            Assert.That(edge.BytesBehind, Is.EqualTo(long.MaxValue), "the sum saturates rather than overflowing");
            Assert.That(edge.ConsecutiveErrors, Is.EqualTo(9));
            Assert.That(edge.Stalled, Is.EqualTo(1));
            Assert.That(edge.Lagging, Is.EqualTo(1));
            Assert.That(ReplicationEdgeSummary.From(ReplicationLinkDirection.Inbound, []), Is.Null);
            Assert.That(() => ReplicationEdgeSummary.From(ReplicationLinkDirection.Inbound, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Peers_are_summarised_worst_first_with_one_edge_per_direction()
    {
        var peers = ReplicationPeerSummary.Summarise(Estate());

        Assert.Multiple(() =>
        {
            Assert.That(peers.Select(peer => peer.PeerRegionId), Is.EqualTo(new[] { "ap-south", "sa-east", "us-east" }));
            Assert.That(peers[0].Health, Is.EqualTo(ReplicationLinkHealth.Stalled));
            Assert.That(peers[0].Outbound!.Health, Is.EqualTo(ReplicationLinkHealth.Stalled));
            Assert.That(peers[0].Inbound!.Health, Is.EqualTo(ReplicationLinkHealth.Lagging));
            Assert.That(peers[1].Inbound, Is.Null);
            Assert.That(peers[1].Health, Is.EqualTo(ReplicationLinkHealth.Unknown));
            Assert.That(new ReplicationPeerSummary("x", null, peers[2].Inbound).Health, Is.EqualTo(ReplicationLinkHealth.Healthy));
            Assert.That(new ReplicationPeerSummary("x", null, null).Health, Is.EqualTo(ReplicationLinkHealth.Unknown));
            Assert.That(() => ReplicationPeerSummary.Summarise(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void An_estate_counts_by_health_lists_peers_and_orders_worst_first()
    {
        var estate = new ReplicationEstate("eu-west", [.. Estate()], Truncated: false, DateTimeOffset.UnixEpoch);
        var ordered = ReplicationEstate.WorstFirst(estate.Links);

        Assert.Multiple(() =>
        {
            Assert.That(estate.Count(ReplicationLinkHealth.Healthy), Is.EqualTo(3));
            Assert.That(estate.Count(ReplicationLinkHealth.Stalled), Is.EqualTo(1));
            Assert.That(estate.PeerRegions, Is.EqualTo(new[] { "ap-south", "sa-east", "us-east" }));
            Assert.That(ordered.Select(link => link.Health).Take(3), Is.EqualTo(new[] { ReplicationLinkHealth.Stalled, ReplicationLinkHealth.Lagging, ReplicationLinkHealth.Unknown }));
        });
    }

    [Test]
    public void Tree_rows_join_enrolment_with_link_health_and_ownership()
    {
        var report = new ReplicationConfigReport(
        [
            Tree("orders"),
            Tree("a/crm/contacts"),
            Tree("static-only", source: ReplicationEnrollmentSource.Static),
            Tree("both", source: ReplicationEnrollmentSource.RuntimeAndStatic),
        ]);

        var rows = ReplicationTreeRow.Build(report, [.. Estate()]);

        Assert.Multiple(() =>
        {
            Assert.That(rows.Select(row => row.TreeId), Is.EqualTo(new[] { "a/crm/contacts", "both", "orders", "static-only" }));
            var crm = rows[0];
            Assert.That(crm.AppSlug, Is.EqualTo("crm"));
            Assert.That(crm.Health, Is.EqualTo(ReplicationLinkHealth.Stalled));
            Assert.That(crm.Links, Is.EqualTo(2));
            Assert.That(crm.IsToggleable, Is.False, "an app-owned tree follows its app");
            Assert.That(rows[1].IsToggleable, Is.True);
            Assert.That(rows[1].Health, Is.Null);
            Assert.That(rows[2].Health, Is.EqualTo(ReplicationLinkHealth.Healthy));
            Assert.That(rows[3].IsToggleable, Is.False, "a static-only tree cannot be disabled at runtime");
            Assert.That(crm.Matches(new ReplicationFilter(ReplicationLinkHealth.Stalled, "ap-south", "crm")), Is.True);
            Assert.That(crm.Matches(new ReplicationFilter(null, "us-east", null)), Is.False);
            Assert.That(rows[1].Matches(new ReplicationFilter(ReplicationLinkHealth.Healthy, null, null)), Is.False);
            Assert.That(() => ReplicationTreeRow.Build(null!, []), Throws.ArgumentNullException);
            Assert.That(() => crm.Matches(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Faults_carry_a_fixed_sentence_per_kind_and_never_the_server_detail()
    {
        var denied = ReplicationFault.From(new LatticeAuthorizationDeniedException("secret detail"), "replication status");
        var unserved = ReplicationFault.From(new NotSupportedException("secret detail"), "replication status");
        var failed = ReplicationFault.From(new InvalidOperationException("secret detail"), "replication status");

        Assert.Multiple(() =>
        {
            Assert.That(denied.Kind, Is.EqualTo(ReplicationFaultKind.Denied));
            Assert.That(unserved.Kind, Is.EqualTo(ReplicationFaultKind.NotServed));
            Assert.That(failed.Kind, Is.EqualTo(ReplicationFaultKind.Failed));
            Assert.That(new[] { denied.Message, unserved.Message, failed.Message }, Has.None.Contains("secret"));
            Assert.That(denied.Message, Is.EqualTo("You are not allowed to see replication status on this cluster."));
            Assert.That(() => ReplicationFault.From(null!, "x"), Throws.ArgumentNullException);
            Assert.That(() => ReplicationFault.From(new Exception(), " "), Throws.ArgumentException);
        });
    }

    [Test]
    public void A_read_is_a_value_or_a_fault()
    {
        var success = ReplicationRead<string>.Success("value");
        var failure = ReplicationRead<string>.Failure(ReplicationFault.NotServed("x"));

        Assert.Multiple(() =>
        {
            Assert.That(success.Succeeded, Is.True);
            Assert.That(success.Fault, Is.Null);
            Assert.That(failure.Succeeded, Is.False);
            Assert.That(failure.Value, Is.Null);
            Assert.That(() => ReplicationRead<string>.Success(null!), Throws.ArgumentNullException);
            Assert.That(() => ReplicationRead<string>.Failure(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_assets_derive_from_the_shell_content_base_and_the_options_have_defaults()
    {
        var options = new ReplicationOptions();

        Assert.Multiple(() =>
        {
            Assert.That(ReplicationAssets.Stylesheet, Is.EqualTo(Orleans.Lattice.Explorer.UI.Design.ShellDesignAssets.ContentBasePath + "replication/lattice-replication.css"));
            Assert.That(ReplicationAssets.ModuleSpecifier, Is.EqualTo("./" + ReplicationAssets.Module));
            Assert.That(options.CacheLifetime, Is.EqualTo(TimeSpan.FromSeconds(15)));
            Assert.That(options.RefreshInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(options.PageSize, Is.EqualTo(1000));
            Assert.That(options.MaxPages, Is.EqualTo(50));
        });
    }
}
