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

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the
/// write-capable data API, the membership add-on (with a deterministic in-test
/// authenticator), and the authorization add-on - then co-hosts the data-API
/// gRPC surface over that silo's live <see cref="ILatticeDataApi"/> facade. The
/// gRPC binding's identity bridge is configured with the test scheme so an
/// inbound credential header resolves to a subject the enforcing
/// <see cref="ILatticeAccessGate"/> reasons over on every mutation and read.
/// Over in-memory grain storage; no network or external store is involved.
/// </summary>
internal sealed class AuthGrpcDataClusterFixture
{
    /// <summary>A bootstrap administrator subject id configured on the silo (root-of-trust bypass).</summary>
    public const string BootstrapAdmin = "root-admin";

    /// <summary>The scheme the identity bridge is configured to strip and stamp.</summary>
    public const string CredentialScheme = ApiDataGrpcTestCredentialAuthenticator.Scheme;

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The data-API read-write facade the gRPC surface binds to.</summary>
    public ILatticeDataApi Api => SiloServices.GetRequiredService<ILatticeDataApi>();

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

    /// <summary>
    /// Stamps <paramref name="subject"/> as the ambient caller for the lifetime of
    /// the returned scope; used to author seed writes under the bootstrap admin.
    /// </summary>
    public static IDisposable AsSubject(string subject) =>
        LatticeCredentialContext.Use(subject, scheme: CredentialScheme);

    /// <summary>Registers an empty single-WAL-partition tree and returns the grain handle.</summary>
    public async Task<ILattice> RegisterTreeAsync(string treeId)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 4,
            ShardCount = 2,
            WalPartitions = 1,
        });

        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    /// <summary>
    /// Reads the raw value at <paramref name="key"/> under the bootstrap admin, so
    /// a test can assert durability independently of the caller-scoped facade read
    /// path.
    /// </summary>
    public async Task<byte[]?> ReadRawAsync(string treeId, string key)
    {
        var tree = Cluster.Client.GetGrain<ILattice>(treeId);
        using (AsSubject(BootstrapAdmin))
        {
            return await tree.GetAsync(key);
        }
    }

    /// <summary>
    /// Co-hosts the data-API gRPC surface over this fixture's live facade with the
    /// identity bridge configured for the test scheme. The transport-level
    /// authorizer is left disabled (<paramref name="requireAuthorization"/> is
    /// <see langword="false"/> by default) so the test isolates the per-tree /
    /// per-key enforcement of the gated <see cref="ILattice"/> surface rather than
    /// the coarse transport gate. Pass <see langword="true"/> to exercise the
    /// default-deny coarse gate instead.
    /// </summary>
    public async Task<GrpcDataHost> CreateGrpcHostAsync(bool requireAuthorization = false)
    {
        var facade = Api;
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
                    services.AddLatticeDataApiGrpc(o =>
                    {
                        o.RequireAuthorization = requireAuthorization;
                        o.CredentialScheme = CredentialScheme;
                    });
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(e => e.MapLatticeDataApiGrpc());
                });
            });

        var host = await hostBuilder.StartAsync();
        var server = host.GetTestServer();
        var channel = GrpcChannel.ForAddress(server.BaseAddress, new GrpcChannelOptions
        {
            HttpHandler = server.CreateHandler(),
        });

        return new GrpcDataHost(host, channel);
    }

    /// <summary>
    /// The tree-id prefix that <see cref="PrefixMergeModeResolver"/> declares as a
    /// cross-cluster-replicated <see cref="LatticeMergeMode.PnCounter"/> tree, so a
    /// mismatched-shape write against it exercises the origin write guard.
    /// </summary>
    public const string ReplicatedCounterPrefix = "replicated-counter-";

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
            siloBuilder.AddLatticeDataApi();
            siloBuilder.AddLatticeMembership();
            siloBuilder.Services
                .AddSingleton<ILatticeCredentialAuthenticator, ApiDataGrpcTestCredentialAuthenticator>();
            siloBuilder.Services
                .AddSingleton<ILatticeMergeModeResolver, PrefixMergeModeResolver>();
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(BootstrapAdmin);
            });
        }
    }

    /// <summary>
    /// Declares trees whose id starts with <see cref="ReplicatedCounterPrefix"/> as
    /// replicated under <see cref="LatticeMergeMode.PnCounter"/>; every other tree
    /// resolves to <see langword="null"/> (unenrolled), so the fixture's other tests
    /// are unaffected.
    /// </summary>
    private sealed class PrefixMergeModeResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            treeId.StartsWith(ReplicatedCounterPrefix, StringComparison.Ordinal)
                ? LatticeMergeMode.PnCounter
                : null;
    }
}

/// <summary>
/// Disposable handle to a co-hosted data-API gRPC server: the in-process host
/// plus a client channel and the resolved method definitions.
/// </summary>
internal sealed class GrpcDataHost : IAsyncDisposable
{
    private readonly IHost _host;

    public GrpcDataHost(IHost host, GrpcChannel channel)
    {
        _host = host;
        Channel = channel;
        Methods = host.Services.GetRequiredService<LatticeDataApiGrpcMethods>();
    }

    public GrpcChannel Channel { get; }

    public IServiceProvider Services => _host.Services;

    public LatticeDataApiGrpcMethods Methods { get; }

    public async ValueTask DisposeAsync()
    {
        Channel.Dispose();
        await _host.StopAsync();
        _host.Dispose();
    }
}
