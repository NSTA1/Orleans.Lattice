using System.Text;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Single-silo fixture for the state-API gRPC binding. Stands up a real
/// Orleans <see cref="TestCluster"/> (so the gRPC service runs over the actual
/// <see cref="ILatticeStateQuery"/> facade, not a stub) and exposes a helper
/// that co-hosts the gRPC surface in an in-process ASP.NET Core
/// <c>TestServer</c> bound to that same facade instance, with a configurable
/// authorizer so the auth tests can flip the allow / deny posture.
/// </summary>
internal sealed class GrpcStateClusterFixture
{
    public const int SmallMaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    public ILatticeStateQuery Query => SiloServices.GetRequiredService<ILatticeStateQuery>();

    /// <summary>
    /// Resolves the <see cref="ILatticeStateQuery"/> facade hosted on the silo at
    /// <paramref name="index"/>. Used by the multi-silo gRPC test to bind the
    /// gRPC surface to a facade on a silo other than the one the writing client
    /// happened to target, proving the binding reads cluster-distributed state.
    /// </summary>
    public ILatticeStateQuery QueryOnSilo(int index) =>
        Cluster.Silos.OfType<InProcessSiloHandle>().ElementAt(index).SiloHost.Services
            .GetRequiredService<ILatticeStateQuery>();

    public ILatticeStateObserver Observer => SiloServices.GetRequiredService<ILatticeStateObserver>();

    public async Task InitializeAsync(int siloCount = 1)
    {
        var builder = new TestClusterBuilder(initialSilosCount: (short)siloCount);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// Co-hosts the state-API gRPC surface over this fixture's live facade.
    /// </summary>
    /// <param name="authorizer">
    /// Authorizer to register; defaults to <see cref="AllowAllStateApiAuthorizer"/>.
    /// </param>
    /// <param name="requireAuthorization">Whether to enforce the authorizer.</param>
    /// <param name="facade">
    /// The facade the gRPC surface binds to; defaults to the primary silo's
    /// <see cref="Query"/>. Pass <see cref="QueryOnSilo"/> to host over a
    /// non-primary silo for multi-silo coverage.
    /// </param>
    public async Task<GrpcStateHost> CreateGrpcHostAsync(
        ILatticeStateApiAuthorizer? authorizer = null,
        bool requireAuthorization = false,
        ILatticeStateQuery? facade = null)
    {
        facade ??= Query;
        var observer = Observer;
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
                    if (authorizer is not null)
                    {
                        services.AddSingleton(authorizer);
                    }

                    services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = requireAuthorization);
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

    public async Task<ILattice> RegisterTreeAsync(string treeId, int shardCount)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            ShardCount = shardCount,
            MaxLeafKeys = SmallMaxLeafKeys,
            WalPartitions = 1,
        });

        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    public async Task<ILattice> CreatePopulatedTreeAsync(string treeId, int keyCount, int shardCount)
    {
        var tree = await RegisterTreeAsync(treeId, shardCount);
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync(KeyAt(i), Encoding.UTF8.GetBytes($"value-{i:D5}"));
        }

        return tree;
    }

    public static string KeyAt(int index) => $"key-{index:D5}";

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
            siloBuilder.AddLatticeStateApi(o =>
            {
                o.ChangeObservationPollInterval = TimeSpan.FromMilliseconds(25);
            });
        }
    }
}

/// <summary>
/// Disposable handle to a co-hosted state-API gRPC server: the in-process
/// host plus a client channel and the resolved method definitions.
/// </summary>
internal sealed class GrpcStateHost : IAsyncDisposable
{
    private readonly IHost _host;

    public GrpcStateHost(IHost host, GrpcChannel channel)
    {
        _host = host;
        Channel = channel;
        Methods = host.Services.GetRequiredService<LatticeStateGrpcMethods>();
    }

    public GrpcChannel Channel { get; }

    public LatticeStateGrpcMethods Methods { get; }

    public async ValueTask DisposeAsync()
    {
        Channel.Dispose();
        await _host.StopAsync();
        _host.Dispose();
    }
}
