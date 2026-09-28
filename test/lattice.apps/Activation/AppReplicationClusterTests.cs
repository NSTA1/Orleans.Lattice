using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>Real app activation and runtime enrolment, with deterministic feed delivery between two clusters.</summary>
[TestFixture]
[Category("Integration")]
public sealed class AppReplicationClusterTests
{
    private TestCluster _source = null!;
    private TestCluster _peer = null!;
    private static readonly TenantId Tenant = TenantId.Parse("acme");
    private static readonly AppSlug Slug = AppSlug.Parse("notes");
    private const string Tree = "t/acme/a/notes/records";
    private const string Resource = "app.replication.json";
    private const string Manifest = """
        {
          "identity": { "slug": "notes", "version": "1.0.0" },
          "trees": [{ "name": "records" }],
          "roles": [],
          "replication": [{ "tree": "records", "mergeMode": "LwwRegister" }],
          "subscriptions": [],
          "mcpTools": []
        }
        """;

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        var source = new TestClusterBuilder(1);
        source.AddSiloBuilderConfigurator<SourceConfigurator>();
        _source = source.Build();
        await _source.DeployAsync();
        var peer = new TestClusterBuilder(1);
        peer.AddSiloBuilderConfigurator<PeerConfigurator>();
        _peer = peer.Build();
        await _peer.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDownAsync()
    {
        if (_source is not null)
        {
            await _source.StopAllSilosAsync();
            await _source.DisposeAsync();
        }
        if (_peer is not null)
        {
            await _peer.StopAllSilosAsync();
            await _peer.DisposeAsync();
        }
    }

    [Test]
    public async Task Enable_enrolment_and_tenant_app_data_converge_to_the_other_cluster()
    {
        var source = Services(_source);
        var peer = Services(_peer);
        var sourceAuthority = source.GetRequiredService<ILatticeReplicationConfigAuthority>();
        var peerAuthority = peer.GetRequiredService<ILatticeReplicationConfigAuthority>();
        using (LatticeSystemOrigin.Enter())
        {
            Assert.That(await sourceAuthority.GetTreeStatusAsync(Tree), Is.Null,
                "in-image registration alone must not enrol the tree");
            var registry = source.GetRequiredService<IAppRegistry>();
            var install = await registry.InstallAsync(new AppRegistryInstallRequest
            {
                Tenant = Tenant,
                Identity = new AppIdentity { Slug = Slug, Version = AppVersion.Parse("1.0.0") },
                Ceiling = AppCapabilityCeiling.Structural(LatticeOperation.Read | LatticeOperation.Write),
                RoleBindings = Array.Empty<AppRoleBinding>(),
            });
            Assert.That(install.Succeeded, Is.True, install.Message);
            var enabled = await source.GetRequiredService<IAppActivationPipeline>().EnableAsync(Tenant, Slug);
            Assert.That(enabled.Succeeded, Is.True, () => string.Join("; ", enabled.Diagnostics.Select(d => d.Message)));
            Assert.That((await sourceAuthority.GetTreeStatusAsync(Tree))?.Enabled, Is.True);
            Assert.That(await sourceAuthority.GetTreeStatusAsync("a/notes/records"), Is.Null);
            Assert.That(source.GetRequiredService<IOptions<LatticeReplicationOptions>>().Value.ReplicatedTrees,
                Does.Not.ContainKey(Tree));

            // Deliver the actual config WAL before app data, through the public receiver seam.
            var config = await CaptureAsync(source, LatticeSystemTreeNames.ReplicationConfig);
            Assert.That(config, Is.Not.Empty);
            await peer.GetRequiredService<IReplicationApplier>().ApplyBatchAsync(config);
            await TestPoll.UntilAsync(async () => (await peerAuthority.GetTreeStatusAsync(Tree))?.Enabled == true,
                "the app-authored runtime enrolment converges to the peer");

            var writer = source.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(Tree);
            var reader = peer.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(Tree);
            await writer.SetAsync("key", new byte[] { 3, 7, 6, 4 });
            var data = await CaptureAsync(source, Tree);
            Assert.That(data, Is.Not.Empty);
            Assert.That(data.All(entry => entry.TreeId == Tree), Is.True);
            await TestPoll.UntilAsync(async () =>
            {
                await peer.GetRequiredService<IReplicationApplier>().ApplyBatchAsync(data);
                return (await reader.GetAsync("key"))?.SequenceEqual(new byte[] { 3, 7, 6, 4 }) == true;
            }, "the dynamically enrolled tenant app tree replicates to the peer");
        }
    }

    private static async Task<List<WalRecord>> CaptureAsync(IServiceProvider services, string tree)
    {
        var records = new List<WalRecord>();
        await foreach (var entry in services.GetRequiredService<IChangeFeed>().Subscribe(tree, HybridLogicalClock.Zero))
            records.Add(entry);
        return records;
    }

    private static IServiceProvider Services(TestCluster cluster) =>
        cluster.Silos.OfType<InProcessSiloHandle>().Single().SiloHost.Services;

    private static void Configure(ISiloBuilder silo, string clusterId)
    {
        silo.AddLattice((builder, name) => builder.AddMemoryGrainStorage(name));
        silo.UseInMemoryReminderService();
        silo.AddLatticeMembership();
        silo.AddLatticeAuth();
        silo.AddLatticeReplication(options => options.ClusterId = clusterId, enableRuntimeConfig: true);
        silo.AddLatticeApps(options => options.ReconcileOnStartup = false)
            .AddLatticeApp("notes", new FakeAppAssembly(Resource, Manifest), Resource);
    }

    private sealed class SourceConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => AppReplicationClusterTests.Configure(siloBuilder, "app-source");
    }

    private sealed class PeerConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => AppReplicationClusterTests.Configure(siloBuilder, "app-peer");
    }
}
