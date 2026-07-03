using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Membership;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the
/// read-only state API, the membership add-on (with a deterministic in-test
/// authenticator), and the authorization add-on - then co-hosts the state-API
/// gRPC surface over that silo's live <see cref="ILatticeStateQuery"/> facade.
/// The gRPC binding's identity bridge is configured with the test scheme so an
/// inbound credential header resolves to a subject the enforcing
/// <see cref="ILatticeAccessGate"/> reasons over. Over in-memory grain storage;
/// no network or external store is involved. Proves the wire-level identity
/// bridge and auth-backed read visibility of issue #981.
/// </summary>
internal sealed class AuthGrpcStateClusterFixture
{
    /// <summary>A bootstrap administrator subject id configured on the silo (root-of-trust bypass).</summary>
    public const string BootstrapAdmin = "root-admin";

    /// <summary>The scheme the identity bridge is configured to strip and stamp.</summary>
    public const string CredentialScheme = ApiStateGrpcTestCredentialAuthenticator.Scheme;

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The state-API read facade the gRPC surface binds to.</summary>
    public ILatticeStateQuery Query => SiloServices.GetRequiredService<ILatticeStateQuery>();

    /// <summary>The silo-side authorization policy store (rule authoring).</summary>
    public ILatticeAuthorizationPolicyStore Store =>
        SiloServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    private CompiledPolicySnapshotMaintainer Maintainer =>
        SiloServices.GetRequiredService<CompiledPolicySnapshotMaintainer>();

    /// <summary>Deploys the cluster.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Stops and disposes the cluster.</summary>
    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// Authors one or more rules and forces a synchronous compiled-policy rebuild
    /// so a test observes them without polling the asynchronous change feed.
    /// </summary>
    public async Task GrantAsync(params LatticeAuthorizationRule[] rules)
    {
        foreach (var rule in rules)
        {
            await Store.PutRuleAsync(rule);
        }

        await Maintainer.RebuildNowAsync();
    }

    /// <summary>Forces a synchronous compiled-policy rebuild.</summary>
    public Task<long> RebuildPolicyAsync() => Maintainer.RebuildNowAsync();

    /// <summary>
    /// Stamps <paramref name="subject"/> as the ambient caller for the lifetime of
    /// the returned scope; used to author seed writes under the bootstrap admin.
    /// </summary>
    public static IDisposable AsSubject(string subject) =>
        LatticeCredentialContext.Use(subject, scheme: CredentialScheme);

    /// <summary>
    /// Registers <paramref name="treeId"/> and writes <paramref name="keys"/>
    /// (each a distinct single-byte value), authored under the bootstrap
    /// administrator so the write itself is authorized. Returns the grain handle.
    /// </summary>
    public async Task<ILattice> CreatePopulatedTreeAsync(string treeId, params string[] keys)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 4,
            ShardCount = 2,
        });

        var tree = Cluster.Client.GetGrain<ILattice>(treeId);
        using (AsSubject(BootstrapAdmin))
        {
            byte value = 0;
            foreach (var key in keys)
            {
                await tree.SetAsync(key, new[] { value++ });
            }
        }

        return tree;
    }

    /// <summary>
    /// Co-hosts the state-API gRPC surface over this fixture's live facade with
    /// the identity bridge configured for the test scheme. The transport-level
    /// authorizer (F-117) is left disabled so the test isolates the new
    /// visibility layer rather than the coarse transport gate.
    /// </summary>
    public async Task<GrpcStateHost> CreateGrpcHostAsync()
    {
        var facade = Query;
        var observer = SiloServices.GetRequiredService<ILatticeStateObserver>();
        var metrics = SiloServices.GetRequiredService<ILatticeStateMetricsObserver>();
        var hostBuilder = new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddLogging();
                    services.AddRouting();
                    services.AddSingleton(facade);
                    services.AddSingleton(observer);
                    services.AddSingleton(metrics);
                    services.AddLatticeStateApiGrpc(o =>
                    {
                        o.RequireAuthorization = false;
                        o.CredentialScheme = CredentialScheme;
                    });
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(e => e.MapLatticeStateApiGrpc());
                });
            });

        var host = await hostBuilder.StartAsync();
        var server = host.GetTestServer();
        var channel = GrpcChannel.ForAddress(server.BaseAddress, new GrpcChannelOptions
        {
            HttpHandler = server.CreateHandler(),
        });

        return new GrpcStateHost(host, channel);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.WalPartitions = 1;
            });
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeStateApi();
            siloBuilder.AddLatticeMembership();
            siloBuilder.Services
                .AddSingleton<ILatticeCredentialAuthenticator, ApiStateGrpcTestCredentialAuthenticator>();
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(BootstrapAdmin);
            });
        }
    }
}
