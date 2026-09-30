using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Samples.Explorer.Tests;

/// <summary>
/// Starts the sample's two-region estate in this process and checks what the
/// README promises: every Explorer area is visible to the bootstrap
/// administrator, the seeded tenants exist in both regions, and the enrolled
/// trees replicate from east to west.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed partial class EstateSmokeTests
{
    /// <summary>Every area but Telemetry, which needs a metrics backend the sample does not run.</summary>
    private static readonly string[] EstateAreas =
        ["access", "apps", "backups", "cluster", "data", "replication", "schema", "tenancy"];

    private static readonly TimeSpan ReplicationBudget = TimeSpan.FromSeconds(60);

    /// <summary>How long an approved cross-tenant grant may take to reach the tenant gate's compiled snapshot.</summary>
    private static readonly TimeSpan GrantBudget = TimeSpan.FromSeconds(30);

    private ExplorerSample _sample = null!;

    [OneTimeSetUp]
    public async Task StartAsync() => _sample = await SampleTestHost.StartAsync(minimal: false);

    [OneTimeTearDown]
    public async Task StopAsync()
    {
        if (_sample is not null)
        {
            await _sample.DisposeAsync();
            File.Delete(_sample.Console.ConfigPath);
        }
    }

    [Test]
    public void The_banner_lists_every_url_and_every_sample_identity()
    {
        using var writer = new StringWriter();
        SampleBanner.Write(writer, _sample, TimeSpan.FromSeconds(1.5));
        var banner = writer.ToString();

        Assert.That(banner, Does.Contain(_sample.Console.Url.ToString()));
        Assert.That(banner, Does.Contain(_sample.East.Plan.GrpcEndpoint.ToString()));
        Assert.That(banner, Does.Contain(_sample.West!.Plan.GrpcEndpoint.ToString()));
        foreach (var identity in new[]
        {
            SampleIdentities.Administrator, SampleIdentities.AcmeAdmin, SampleIdentities.GlobexAdmin,
            SampleIdentities.Alice, SampleIdentities.Bob, SampleIdentities.Carol,
        })
        {
            Assert.That(banner, Does.Contain(identity));
        }

        Assert.That(banner, Does.Contain("Started in 1.5s."));
        Assert.That(banner, Does.Contain("Press P to pause the peer link"));
        Assert.That(_sample.SeedLog, Has.Some.Contains("installed and enabled in tenant 'acme'"));
    }

    [Test]
    public async Task Every_seeded_group_is_defined_and_one_roster_group_is_left_to_create()
    {
        var membership = _sample.East.Services.GetRequiredService<Orleans.Lattice.Membership.ILatticeMembershipDirectory>();
        var seeded = new[]
        {
            SampleIdentities.OperatorsGroup, SampleIdentities.TaskEditorsGroup, SampleIdentities.TaskViewersGroup,
            SampleIdentities.VisitorsGroup, SampleIdentities.AcmeEditorsGroup,
        };

        foreach (var group in seeded)
        {
            Assert.That(await membership.GetGroupAsync(group), Is.Not.Null, $"'{group}' has a group record, so Access lists it and refuses it as a new id");
        }

        Assert.That(await membership.GetGroupAsync(SampleIdentities.AuditorsGroup), Is.Null, "the roster keeps one group for New group to create");
        Assert.That((await membership.GetGroupAsync(SampleIdentities.OperatorsGroup))!.DisplayName, Is.EqualTo("Floor Operators"));
    }

    [Test]
    public async Task Every_area_but_telemetry_is_visible_to_the_bootstrap_administrator()
    {
        var areas = DirectorySpine.ReadAreas(await SampleTestHost.GetHomeAsync(_sample));

        Assert.That(areas.Keys, Is.EquivalentTo(EstateAreas), "the spine shows exactly these areas");
        Assert.That(areas.Where(area => !area.Value).Select(area => area.Key), Is.Empty, "no area is shown as unavailable");
    }

    [Test]
    public void The_estate_runs_two_regions_and_the_console_connects_to_east()
    {
        Assert.That(_sample.Regions.Select(region => region.Id), Is.EqualTo(new[] { SampleIdentities.EastRegion, SampleIdentities.WestRegion }));
        Assert.That(_sample.ConsoleRegion, Is.SameAs(_sample.East));
        Assert.That(_sample.Console.Endpoint, Is.EqualTo(_sample.East.Plan.GrpcEndpoint));
        Assert.That(_sample.Sink, Is.Not.Null);
        Assert.That(_sample.Writer, Is.Not.Null);
    }

    [Test]
    public async Task A_backup_captured_in_east_resolves_through_the_sink_both_regions_share()
    {
        using var _ = LatticeCredentialContext.Use(
            SampleSeeder.BasicToken(SampleIdentities.Administrator), scheme: DemoBasicAuthenticator.Scheme);
        var capture = _sample.East.Services.GetRequiredService<ILatticeBackupCaptureService>();

        var result = await capture.CaptureAsync(
            new LatticeBackupCaptureRequest("smoke", BackupScopeSelector.WholeTree(SampleIdentities.FactoryFloorTree)));

        var westSink = _sample.West!.Services.GetRequiredService<ILatticeBackupSink>();
        Assert.That(westSink, Is.SameAs(_sample.Sink), "both regions resolve the one shared sink");
        Assert.That((await westSink.ProbeAsync(result.BackupId)).IsResolvable, Is.True, "west resolves east's backup");
    }

    [Test]
    public async Task A_tenant_admin_reaches_only_its_own_tenant()
    {
        using var _ = LatticeCredentialContext.Use(
            SampleSeeder.BasicToken(SampleIdentities.AcmeAdmin), scheme: DemoBasicAuthenticator.Scheme);
        var tenants = await _sample.East.Services.GetRequiredService<ILatticeTenantSelfService>().ListAccessibleTenantsAsync();

        Assert.That(tenants.Select(tenant => tenant.TenantId), Is.EqualTo(new[] { SampleIdentities.AcmeTenant }));
    }

    [Test]
    public async Task Both_regions_hold_the_seeded_tenants()
    {
        foreach (var region in _sample.Regions)
        {
            using var _ = LatticeCredentialContext.Use(
                SampleSeeder.BasicToken(SampleIdentities.Administrator), scheme: DemoBasicAuthenticator.Scheme);
            var tenants = await region.Services.GetRequiredService<ILatticeTenantSelfService>().ListAccessibleTenantsAsync();

            Assert.That(
                tenants.Select(tenant => tenant.TenantId),
                Is.SupersetOf(new[] { SampleIdentities.AcmeTenant, SampleIdentities.GlobexTenant }),
                $"the operator administers both seeded tenants in region '{region.Id}'");
        }
    }

    [Test]
    public async Task The_demo_tree_and_the_app_declared_tree_are_enrolled_in_both_regions()
    {
        var appTree = LatticeTenantTrees.Compose(TenantId.Parse(SampleIdentities.AcmeTenant), "a/task-board/tasks");
        foreach (var region in _sample.Regions)
        {
            var control = region.Services.GetRequiredService<ILatticeReplicationControl>();
            var enrolled = await SampleTestHost.EventuallyAsync(
                async () =>
                {
                    using var _ = LatticeSystemOrigin.Enter();
                    var report = await control.GetReplicationConfigAsync();
                    var enabled = report.Trees.Where(tree => tree.Enabled && !tree.Ambiguous).Select(tree => tree.TreeId).ToHashSet(StringComparer.Ordinal);
                    return enabled.Contains(SampleIdentities.FactoryFloorTree) && enabled.Contains(appTree);
                },
                ReplicationBudget);

            Assert.That(enrolled, Is.True, $"'{SampleIdentities.FactoryFloorTree}' and '{appTree}' are enrolled in region '{region.Id}'");
        }
    }

    [Test]
    public async Task The_seeded_demo_tree_and_the_acme_task_board_reach_west() =>
        Assert.That(
            await ExplorerSample.WaitForSeedOnPeerAsync(_sample.West!, ReplicationBudget),
            Is.True,
            "the last seeded machine and acme's last seeded card replicated from east to west");

    [Test]
    public async Task Pausing_the_peer_link_fails_shipping_and_resuming_it_delivers_the_backlog()
    {
        var key = "paused-" + Guid.NewGuid().ToString("N");
        var east = _sample.East.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(SampleIdentities.FactoryFloorTree);
        var west = _sample.West!.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(SampleIdentities.FactoryFloorTree);
        var status = _sample.East.Services.GetRequiredService<ILatticeReplicationStatus>();

        Assert.That(_sample.PeerLink.Pause(), Is.True);
        try
        {
            using (LatticeSystemOrigin.Enter())
            {
                await east.SetAsync(key, [4, 5, 6]);
            }

            var failing = await SampleTestHost.EventuallyAsync(
                async () =>
                {
                    using var _ = LatticeSystemOrigin.Enter();
                    var page = await status.GetPeerStatusAsync(new ReplicationPeerStatusQuery { TreeId = SampleIdentities.FactoryFloorTree }, CancellationToken.None);
                    return page.Peers.Any(peer => peer.Direction == ReplicationLinkDirection.Outbound && peer.ConsecutiveErrors > 0);
                },
                ReplicationBudget);
            Assert.That(failing, Is.True, "east's outbound link reports failed shipping while the link is paused");
        }
        finally
        {
            _sample.PeerLink.Resume();
        }

        var delivered = await SampleTestHost.EventuallyAsync(
            async () =>
            {
                using var _ = LatticeSystemOrigin.Enter();
                return await west.GetAsync(key) is { } value && value.SequenceEqual(new byte[] { 4, 5, 6 });
            },
            ReplicationBudget);
        Assert.That(delivered, Is.True, "the write made while paused reached west once the link resumed");
    }

    [Test]
    public async Task A_write_in_east_reaches_west()
    {
        var key = "smoke-" + Guid.NewGuid().ToString("N");
        var east = _sample.East.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(SampleIdentities.FactoryFloorTree);
        var west = _sample.West!.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(SampleIdentities.FactoryFloorTree);
        using (LatticeSystemOrigin.Enter())
        {
            await east.SetAsync(key, [1, 2, 3]);
        }

        var arrived = await SampleTestHost.EventuallyAsync(
            async () =>
            {
                using var _ = LatticeSystemOrigin.Enter();
                return await west.GetAsync(key) is { } value && value.SequenceEqual(new byte[] { 1, 2, 3 });
            },
            ReplicationBudget);

        Assert.That(arrived, Is.True, "the write replicated from east to west");
    }
}
