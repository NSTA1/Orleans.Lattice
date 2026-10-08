using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Tests.PeerStatus;

/// <summary>
/// Unit tests for <see cref="LatticeReplicationStatus"/> over a real, manually
/// clocked telemetry state: field mapping and derived health, the local region
/// id, fail-closed permission scoping, filters, paging (including skipping rows
/// the caller may not see), and the effective, tenant-qualified reporting of
/// tenant and app tree ids (issue #4000).
/// </summary>
[TestFixture]
public sealed class LatticeReplicationStatusTests
{
    private const string LocalRegion = "west";

    private static LatticeReplicationStatus CreateStatus(
        StatsBackedPeerStatusReader reader,
        ILatticeAccessGate? gate = null,
        ITenantContextResolver? tenantResolver = null,
        LatticeReplicationStatusOptions? options = null)
    {
        var replicationOptions = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        replicationOptions.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = LocalRegion });
        var statusOptions = Substitute.For<IOptionsMonitor<LatticeReplicationStatusOptions>>();
        statusOptions.CurrentValue.Returns(options ?? new LatticeReplicationStatusOptions());

        return new LatticeReplicationStatus(
            reader,
            new ReplicationAccessAuthorizer(gate ?? new AllowingAccessGate(), membership: null),
            tenantResolver ?? new DefaultTenantContextResolver(),
            replicationOptions,
            statusOptions);
    }

    private static async Task<List<ReplicationPeerStatusEntry>> ReadAllAsync(
        ILatticeReplicationStatus status,
        ReplicationPeerStatusQuery query,
        List<ReplicationPeerStatusPage>? pages = null)
    {
        var all = new List<ReplicationPeerStatusEntry>();
        var next = query;
        for (var guard = 0; guard < 100; guard++)
        {
            var page = await status.GetPeerStatusAsync(next);
            pages?.Add(page);
            all.AddRange(page.Peers);
            if (page.ContinuationToken is null)
            {
                return all;
            }

            next = query with { ContinuationToken = page.ContinuationToken };
        }

        throw new AssertionException("paging did not terminate");
    }

    [Test]
    public async Task GetPeerStatusAsync_maps_every_field_and_the_local_region()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordBacklog("orders", "east", entriesBehind: 12, bytesBehind: 3400);
        reader.Stats.RecordInFlight("orders", "east", depth: 2);
        reader.Stats.RecordSuccess("orders", "east");
        reader.Stats.Advance(TimeSpan.FromSeconds(7));
        reader.Stats.RecordInboundError("orders", "east");

        var page = await CreateStatus(reader).GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        Assert.That(page.LocalRegionId, Is.EqualTo(LocalRegion));
        Assert.That(page.ContinuationToken, Is.Null);
        Assert.That(page.Peers, Has.Count.EqualTo(2));
        var outbound = page.Peers[0];
        var inbound = page.Peers[1];
        Assert.Multiple(() =>
        {
            Assert.That(outbound.TreeId, Is.EqualTo("orders"));
            Assert.That(outbound.PeerRegionId, Is.EqualTo("east"));
            Assert.That(outbound.Direction, Is.EqualTo(ReplicationLinkDirection.Outbound));
            Assert.That(outbound.EntriesBehind, Is.EqualTo(12));
            Assert.That(outbound.BytesBehind, Is.EqualTo(3400));
            Assert.That(outbound.InFlight, Is.EqualTo(2));
            Assert.That(outbound.ConsecutiveErrors, Is.Zero);
            Assert.That(outbound.TimeSinceLastContact, Is.EqualTo(TimeSpan.FromSeconds(7)));
            Assert.That(outbound.Health, Is.EqualTo(ReplicationLinkHealth.Healthy));

            Assert.That(inbound.Direction, Is.EqualTo(ReplicationLinkDirection.Inbound));
            Assert.That(inbound.ConsecutiveErrors, Is.EqualTo(1));
            Assert.That(inbound.TimeSinceLastContact, Is.Null);
            Assert.That(inbound.Health, Is.EqualTo(ReplicationLinkHealth.Unknown));
        });
    }

    [Test]
    public async Task GetPeerStatusAsync_derives_health_from_the_configured_thresholds()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("orders", "east");
        reader.Stats.RecordBacklog("orders", "east", entriesBehind: 11, bytesBehind: 0);
        reader.Stats.RecordSuccess("audit", "east");
        reader.Stats.RecordBacklog("audit", "east", entriesBehind: 21, bytesBehind: 0);
        reader.Stats.RecordSuccess("calm", "east");
        var options = new LatticeReplicationStatusOptions { LaggingEntriesBehind = 10, StalledEntriesBehind = 20 };

        var page = await CreateStatus(reader, options: options).GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        var health = page.Peers.ToDictionary(p => p.TreeId, p => p.Health);
        Assert.That(health, Is.EqualTo(new Dictionary<string, ReplicationLinkHealth>
        {
            ["audit"] = ReplicationLinkHealth.Stalled,
            ["calm"] = ReplicationLinkHealth.Healthy,
            ["orders"] = ReplicationLinkHealth.Lagging,
        }));
    }

    [Test]
    public async Task GetPeerStatusAsync_maps_the_specific_cause_of_a_stalled_link()
    {
        var reader = new StatsBackedPeerStatusReader();
        var now = reader.Stats.Now;
        reader.Stats.RecordReseedRequired("trimmed", "east", now);
        reader.Stats.RecordDeadLetterFull("full-queue", "east", ReplicationContactDirection.Outbound, now);
        reader.Stats.RecordReseedRequired("both", "east", now);
        reader.Stats.RecordDeadLetterFull("both", "east", ReplicationContactDirection.Outbound, now);

        var links = await ReadAllAsync(CreateStatus(reader), ReplicationPeerStatusQuery.All);
        var reasons = links.ToDictionary(link => link.TreeId, link => link.StallReason);

        Assert.That(reasons, Is.EqualTo(new Dictionary<string, ReplicationLinkStallReason?>
        {
            ["both"] = ReplicationLinkStallReason.ReseedRequired,
            ["full-queue"] = ReplicationLinkStallReason.DeadLetterQueueFull,
            ["trimmed"] = ReplicationLinkStallReason.ReseedRequired,
        }));
    }

    [Test]
    public async Task GetPeerStatusAsync_omits_trees_the_caller_may_not_manage()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("orders", "east");
        reader.Stats.RecordSuccess("secret", "east");
        reader.Stats.RecordSuccess("inventory", "east");

        var page = await CreateStatus(reader, new TreeScopedAccessGate("orders", "inventory"))
            .GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        Assert.That(page.Peers.Select(p => p.TreeId), Is.EqualTo(new[] { "inventory", "orders" }));
    }

    [Test]
    public async Task GetPeerStatusAsync_denies_everything_to_an_unauthorized_caller()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("orders", "east");

        var page = await CreateStatus(reader, new DenyingAccessGate("no grant"))
            .GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        Assert.Multiple(() =>
        {
            Assert.That(page.Peers, Is.Empty);
            Assert.That(page.ContinuationToken, Is.Null);
            Assert.That(page.LocalRegionId, Is.EqualTo(LocalRegion));
        });
    }

    [Test]
    public async Task GetPeerStatusAsync_anonymous_is_denied_by_default()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("orders", "east");

        var page = await CreateStatus(reader, new AnonymousDenyingAccessGate())
            .GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        Assert.That(page.Peers, Is.Empty);
    }

    [Test]
    public async Task GetPeerStatusAsync_tree_filter_the_caller_may_not_manage_reads_nothing()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("secret", "east");

        var page = await CreateStatus(reader, new TreeScopedAccessGate("orders"))
            .GetPeerStatusAsync(new ReplicationPeerStatusQuery { TreeId = "secret" });

        Assert.Multiple(() =>
        {
            Assert.That(page.Peers, Is.Empty);
            Assert.That(page.ContinuationToken, Is.Null);
            Assert.That(reader.Reads, Is.Empty, "a denied tree filter must not read any telemetry");
        });
    }

    [Test]
    public async Task GetPeerStatusAsync_tree_filter_authorizes_once_and_reads_only_that_tree()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("orders", "east");
        reader.Stats.RecordSuccess("orders", "north");
        reader.Stats.RecordSuccess("audit", "east");
        var gate = new AllowingAccessGate();

        var page = await CreateStatus(reader, gate).GetPeerStatusAsync(new ReplicationPeerStatusQuery { TreeId = "orders" });

        Assert.Multiple(() =>
        {
            Assert.That(page.Peers.Select(p => (p.TreeId, p.PeerRegionId)), Is.EqualTo(new[] { ("orders", "east"), ("orders", "north") }));
            Assert.That(gate.AuthorizedTrees, Is.EqualTo(new[] { "orders" }));
            Assert.That(reader.Reads.Select(r => r.TreeId), Has.All.EqualTo("orders"));
        });
    }

    [Test]
    public async Task GetPeerStatusAsync_filters_by_peer_region()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("orders", "east");
        reader.Stats.RecordSuccess("orders", "north");

        var page = await CreateStatus(reader).GetPeerStatusAsync(new ReplicationPeerStatusQuery { PeerRegionId = "north" });

        Assert.That(page.Peers.Select(p => p.PeerRegionId), Is.EqualTo(new[] { "north" }));
    }

    [Test]
    public async Task GetPeerStatusAsync_authorizes_each_tree_once_per_page()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("orders", "east");
        reader.Stats.RecordSuccess("orders", "north");
        reader.Stats.RecordInboundSuccess("orders", "east");
        var gate = new AllowingAccessGate();

        await CreateStatus(reader, gate).GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        Assert.That(gate.AuthorizedTrees, Is.EqualTo(new[] { "orders" }));
    }

    [Test]
    public async Task GetPeerStatusAsync_pages_through_every_link_exactly_once()
    {
        var reader = new StatsBackedPeerStatusReader();
        foreach (var tree in new[] { "e", "a", "c", "b", "d" })
        {
            reader.Stats.RecordSuccess(tree, "east");
        }

        var pages = new List<ReplicationPeerStatusPage>();
        var all = await ReadAllAsync(CreateStatus(reader), new ReplicationPeerStatusQuery { PageSize = 2 }, pages);

        Assert.Multiple(() =>
        {
            Assert.That(all.Select(p => p.TreeId), Is.EqualTo(new[] { "a", "b", "c", "d", "e" }));
            Assert.That(pages.Select(p => p.Peers.Count), Is.EqualTo(new[] { 2, 2, 1 }));
            Assert.That(pages[^1].ContinuationToken, Is.Null);
        });
    }

    [Test]
    public async Task GetPeerStatusAsync_a_page_that_ends_the_report_carries_no_continuation()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("a", "east");
        reader.Stats.RecordSuccess("b", "east");

        var page = await CreateStatus(reader).GetPeerStatusAsync(new ReplicationPeerStatusQuery { PageSize = 2 });

        Assert.Multiple(() =>
        {
            Assert.That(page.Peers, Has.Count.EqualTo(2));
            Assert.That(page.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public async Task GetPeerStatusAsync_skips_hidden_rows_without_ending_the_page_early()
    {
        var reader = new StatsBackedPeerStatusReader();
        foreach (var tree in new[] { "a", "h1", "h2", "h3", "h4", "h5", "z" })
        {
            reader.Stats.RecordSuccess(tree, "east");
        }

        var pages = new List<ReplicationPeerStatusPage>();
        var all = await ReadAllAsync(
            CreateStatus(reader, new TreeScopedAccessGate("a", "z")),
            new ReplicationPeerStatusQuery { PageSize = 1 },
            pages);

        Assert.Multiple(() =>
        {
            Assert.That(all.Select(p => p.TreeId), Is.EqualTo(new[] { "a", "z" }));
            Assert.That(pages, Has.Count.EqualTo(2));
            Assert.That(pages.Select(p => p.Peers.Count), Has.All.EqualTo(1));
        });
    }

    [Test]
    public async Task GetPeerStatusAsync_continuation_encodes_only_a_row_the_caller_was_shown()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("a", "east");
        reader.Stats.RecordSuccess("hidden", "east");
        reader.Stats.RecordSuccess("z", "east");

        var page = await CreateStatus(reader, new TreeScopedAccessGate("a", "z"))
            .GetPeerStatusAsync(new ReplicationPeerStatusQuery { PageSize = 1 });

        var cursor = ReplicationPeerStatusContinuation.Decode(page.ContinuationToken);
        Assert.That(cursor?.Tree, Is.EqualTo("a"));
    }

    [Test]
    public async Task GetPeerStatusAsync_reports_a_tenant_tree_by_its_effective_qualified_id()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("t/acme/a/crm/contacts", "east");
        reader.Stats.RecordSuccess("t/acme/orders", "east");
        var tenant = FixedTenantContextResolver.For("acme");

        var page = await CreateStatus(reader, tenantResolver: tenant).GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        Assert.That(page.Peers.Select(p => p.TreeId), Is.EqualTo(new[] { "t/acme/a/crm/contacts", "t/acme/orders" }));
    }

    [Test]
    public async Task GetPeerStatusAsync_authorizes_the_effective_id_it_reports()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("t/acme/a/crm/contacts", "east");
        var gate = new AllowingAccessGate();

        await CreateStatus(reader, gate, FixedTenantContextResolver.For("acme")).GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        Assert.That(gate.AuthorizedTrees, Is.EqualTo(new[] { "t/acme/a/crm/contacts" }));
    }

    [Test]
    public async Task GetPeerStatusAsync_filters_on_a_tenant_local_app_tree_name()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("t/acme/a/crm/contacts", "east");
        reader.Stats.RecordSuccess("t/acme/a/crm/deals", "east");

        var page = await CreateStatus(reader, tenantResolver: FixedTenantContextResolver.For("acme"))
            .GetPeerStatusAsync(new ReplicationPeerStatusQuery { TreeId = "a/crm/contacts" });

        Assert.Multiple(() =>
        {
            Assert.That(page.Peers.Select(p => p.TreeId), Is.EqualTo(new[] { "t/acme/a/crm/contacts" }));
            Assert.That(reader.Reads.Select(r => r.TreeId), Has.All.EqualTo("t/acme/a/crm/contacts"));
        });
    }

    [Test]
    public async Task GetPeerStatusAsync_continuation_carries_the_effective_id_the_caller_was_shown()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("t/acme/a/crm/contacts", "east");
        reader.Stats.RecordSuccess("t/acme/a/crm/deals", "east");
        var status = CreateStatus(reader, tenantResolver: FixedTenantContextResolver.For("acme"));

        var pages = new List<ReplicationPeerStatusPage>();
        var all = await ReadAllAsync(status, new ReplicationPeerStatusQuery { PageSize = 1 }, pages);

        var cursor = ReplicationPeerStatusContinuation.Decode(pages[0].ContinuationToken);
        Assert.Multiple(() =>
        {
            Assert.That(all.Select(p => p.TreeId), Is.EqualTo(new[] { "t/acme/a/crm/contacts", "t/acme/a/crm/deals" }));
            Assert.That(cursor?.Tree, Is.EqualTo(pages[0].Peers[^1].TreeId));
        });
    }

    [Test]
    public async Task GetPeerStatusAsync_default_tenant_reports_app_trees_unchanged()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("a/crm/contacts", "east");

        var page = await CreateStatus(reader).GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        Assert.That(page.Peers.Single().TreeId, Is.EqualTo("a/crm/contacts"));
    }

    [Test]
    public async Task GetPeerStatusAsync_resolves_the_tenant_asynchronously_when_needed()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("t/acme/orders", "east");
        var resolver = Substitute.For<ITenantContextResolver>();
        resolver.ResolveCurrentAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<TenantId>(TenantId.Parse("acme")));

        var page = await CreateStatus(reader, tenantResolver: resolver).GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        Assert.That(page.Peers.Single().TreeId, Is.EqualTo("t/acme/orders"));
    }

    [Test]
    public void GetPeerStatusAsync_fails_closed_when_no_tenant_resolves()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("orders", "east");

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await CreateStatus(reader, tenantResolver: FixedTenantContextResolver.Denying)
                    .GetPeerStatusAsync(ReplicationPeerStatusQuery.All),
                Throws.TypeOf<LatticeTenantAccessDeniedException>());
            Assert.That(reader.Reads, Is.Empty);
        });
    }

    [Test]
    public void GetPeerStatusAsync_rejects_a_negative_page_size()
    {
        var status = CreateStatus(new StatsBackedPeerStatusReader());

        Assert.That(
            async () => await status.GetPeerStatusAsync(new ReplicationPeerStatusQuery { PageSize = -1 }),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void GetPeerStatusAsync_rejects_a_malformed_continuation()
    {
        var status = CreateStatus(new StatsBackedPeerStatusReader());

        Assert.That(
            async () => await status.GetPeerStatusAsync(new ReplicationPeerStatusQuery { ContinuationToken = "garbage" }),
            Throws.ArgumentException);
    }

    [Test]
    public async Task GetPeerStatusAsync_reads_one_row_past_the_page_to_learn_whether_more_follow()
    {
        var reader = new StatsBackedPeerStatusReader();
        reader.Stats.RecordSuccess("a", "east");

        await CreateStatus(reader).GetPeerStatusAsync(new ReplicationPeerStatusQuery { PageSize = 5 });

        Assert.That(reader.Reads.Single().Limit, Is.EqualTo(6));
    }

    [Test]
    public void GetPeerStatusAsync_honours_cancellation()
    {
        var status = CreateStatus(new StatsBackedPeerStatusReader());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await status.GetPeerStatusAsync(ReplicationPeerStatusQuery.All, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetPeerStatusAsync_stops_when_the_reader_does_not_advance()
    {
        var row = new ReplicationPeerStatusRow("a", "east", ReplicationContactDirection.Outbound, 0, 0, 0, 1d, 0);
        var reader = Substitute.For<IReplicationPeerStatusReader>();
        reader.ReadAsync(Arg.Any<ReplicationPeerStatusReadRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<ReplicationPeerStatusRow>>(new[] { row, row }));
        var replicationOptions = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        replicationOptions.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = LocalRegion });
        var statusOptions = Substitute.For<IOptionsMonitor<LatticeReplicationStatusOptions>>();
        statusOptions.CurrentValue.Returns(new LatticeReplicationStatusOptions());
        var status = new LatticeReplicationStatus(
            reader,
            new ReplicationAccessAuthorizer(new DenyingAccessGate("hidden"), membership: null),
            new DefaultTenantContextResolver(),
            replicationOptions,
            statusOptions);

        var page = await status.GetPeerStatusAsync(new ReplicationPeerStatusQuery { PageSize = 1 });

        Assert.That(page.Peers, Is.Empty);
    }

    [Test]
    public void GetPeerStatusAsync_null_query_throws()
    {
        var status = CreateStatus(new StatsBackedPeerStatusReader());

        Assert.That(async () => await status.GetPeerStatusAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var reader = new StatsBackedPeerStatusReader();
        var authorizer = new ReplicationAccessAuthorizer(new AllowingAccessGate(), membership: null);
        var tenant = new DefaultTenantContextResolver();
        var replication = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var status = Substitute.For<IOptionsMonitor<LatticeReplicationStatusOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeReplicationStatus(null!, authorizer, tenant, replication, status), Throws.ArgumentNullException);
            Assert.That(() => new LatticeReplicationStatus(reader, null!, tenant, replication, status), Throws.ArgumentNullException);
            Assert.That(() => new LatticeReplicationStatus(reader, authorizer, null!, replication, status), Throws.ArgumentNullException);
            Assert.That(() => new LatticeReplicationStatus(reader, authorizer, tenant, null!, status), Throws.ArgumentNullException);
            Assert.That(() => new LatticeReplicationStatus(reader, authorizer, tenant, replication, null!), Throws.ArgumentNullException);
        });
    }
}
