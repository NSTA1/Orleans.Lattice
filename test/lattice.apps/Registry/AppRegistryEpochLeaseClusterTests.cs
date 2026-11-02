using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// The app-registry epoch lease on a real two-silo cluster (#5713): every silo leases its compiled
/// snapshot from the epoch grain and is pushed every advance through its observer, so a silo that did
/// not commit a registry write serves grant evaluations with no epoch read, yet honours a revocation
/// committed on the other silo from the first evaluation after the write returns (#5595).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AppRegistryEpochLeaseClusterTests
{
    private const string Resource = "app.manifest.json";
    private static readonly AppSlug Crm = AppSlug.Parse("crm");
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private const string CrmManifest = """
        {
          "identity": { "slug": "crm", "version": "1.0.0" },
          "trees": [{ "name": "records" }],
          "roles": [{ "name": "reader", "operations": ["Read"], "scopes": [{ "tree": "records" }] }],
          "subscriptions": [],
          "mcpTools": []
        }
        """;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        var builder = new TestClusterBuilder(2);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDownAsync()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task A_revocation_committed_on_one_silo_is_honoured_by_the_other_silos_leased_snapshot()
    {
        var silos = _cluster.Silos.OfType<InProcessSiloHandle>().Select(s => s.SiloHost.Services).ToArray();
        var committing = silos[0];
        var other = silos[1].GetRequiredService<CompiledAppRegistrySnapshotMaintainer>();
        foreach (var silo in silos)
        {
            await silo.GetRequiredService<CompiledAppRegistrySnapshotMaintainer>().LeaseEstablished.WaitAsync(Bound);
        }

        var registry = committing.GetRequiredService<IAppRegistry>();
        using var origin = LatticeSystemOrigin.Enter();
        Assert.That((await registry.InstallAsync(new AppRegistryInstallRequest
        {
            Identity = new AppIdentity { Slug = Crm, Version = AppVersion.Parse("1.0.0") },
            Ceiling = AppCapabilityCeiling.Structural(LatticeOperation.Read),
            RoleBindings = [AppRoleBinding.Create("reader", "readers")],
        })).Succeeded, Is.True);
        Assert.That((await registry.EnableAsync(TenantId.Default, Crm)).Succeeded, Is.True);

        await other.EnsureCurrentAsync();
        Assert.Multiple(() =>
        {
            Assert.That(other.Current.GetEnabledTenantApps(TenantId.Default).Select(r => r.Slug), Is.EqualTo(new[] { Crm }));
            Assert.That(other.IsLeasedCurrent, Is.True, "the other silo now evaluates on its leased snapshot with no epoch read");
        });

        Assert.That((await registry.DisableAsync(TenantId.Default, Crm)).Succeeded, Is.True);

        await other.EnsureCurrentAsync();
        Assert.That(
            other.Current.GetEnabledTenantApps(TenantId.Default),
            Is.Empty,
            "the disable was pushed to the other silo before it returned, so the next evaluation never grants it");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeMembership();
            siloBuilder.AddLatticeAuth();
            siloBuilder.AddLatticeApps()
                .AddLatticeApp(Crm.Value, new FakeAppAssembly(Resource, CrmManifest), Resource);
        }
    }
}
