using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using Orleans.Lattice.Schema;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// Single-silo fixture for the schema control-API gRPC binding. Stands up a real
/// Orleans <see cref="TestCluster"/> - with the core lattice, the schema
/// enforcement + versioning engine, and the schema control-API add-on - so the
/// gRPC service runs over the actual <see cref="ILatticeSchemaControl"/> facade
/// rather than a stub. Exposes a helper that co-hosts the gRPC surface in an
/// in-process ASP.NET Core <c>TestServer</c> bound to that same facade instance,
/// with a configurable authorizer so the auth tests can flip the allow / deny
/// posture.
/// </summary>
internal sealed class GrpcSchemaClusterFixture
{
    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The client-side grain factory used to seed and read trees.</summary>
    public IGrainFactory GrainFactory => Cluster.GrainFactory;

    /// <summary>The silo-side schema control facade under test.</summary>
    public ILatticeSchemaControl Control =>
        SiloServices.GetRequiredService<ILatticeSchemaControl>();

    /// <summary>Deploys the single-silo cluster.</summary>
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
    /// Co-hosts the schema control-API gRPC surface over this fixture's live
    /// facade in an in-process <c>TestServer</c>, returning a disposable handle
    /// with a client channel bound to the same server.
    /// </summary>
    /// <param name="authorizer">
    /// Authorizer to register; when <see langword="null"/> the binding's default
    /// <see cref="DenySchemaApiAuthorizer"/> stands, so a caller must pass a
    /// permissive authorizer to exercise the accept path.
    /// </param>
    /// <param name="requireAuthorization">Whether to enforce the authorizer.</param>
    public async Task<GrpcSchemaHost> CreateGrpcHostAsync(
        ILatticeSchemaApiAuthorizer? authorizer = null,
        bool requireAuthorization = true)
    {
        var control = Control;
        var hostBuilder = new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddLogging();
                    services.AddRouting();
                    services.AddSingleton(control);
                    if (authorizer is not null)
                    {
                        services.AddSingleton(authorizer);
                    }

                    services.AddLatticeSchemaApiGrpc(o => o.RequireAuthorization = requireAuthorization);
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(e => e.MapLatticeSchemaApiGrpc());
                });
            });

        var host = await hostBuilder.StartAsync();
        var server = host.GetTestServer();
        var channel = GrpcChannel.ForAddress(server.BaseAddress, new GrpcChannelOptions
        {
            HttpHandler = server.CreateHandler(),
        });

        return new GrpcSchemaHost(host, channel);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.AddLatticeSchemaVersioning();
            siloBuilder.AddLatticeSchemaApi();
        }
    }
}

/// <summary>
/// Disposable handle to a co-hosted schema control-API gRPC server: the
/// in-process host plus a client channel and a ready-built typed client.
/// </summary>
internal sealed class GrpcSchemaHost : IAsyncDisposable
{
    private readonly IHost _host;

    public GrpcSchemaHost(IHost host, GrpcChannel channel)
    {
        _host = host;
        Channel = channel;
        Client = LatticeSchemaApiGrpcClient.Create(channel.CreateCallInvoker(), host.Services);
        Methods = host.Services.GetRequiredService<LatticeSchemaGrpcMethods>();
    }

    /// <summary>The client channel bound to the in-process server.</summary>
    public GrpcChannel Channel { get; }

    /// <summary>The typed client over the channel, using the host's serializers.</summary>
    public LatticeSchemaApiGrpcClient Client { get; }

    /// <summary>The resolved method definitions, for raw-invoker header tests.</summary>
    public LatticeSchemaGrpcMethods Methods { get; }

    public async ValueTask DisposeAsync()
    {
        Channel.Dispose();
        await _host.StopAsync();
        _host.Dispose();
    }
}
