using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the
/// membership and authorization add-ons (with a deterministic in-test
/// authenticator), and the admin control facade - then co-hosts the auth-API
/// gRPC surface over that silo's live <see cref="ILatticeAuthAdmin"/> facade. The
/// gRPC binding's identity bridge is configured with the test scheme so an
/// inbound credential header resolves to a subject the facade's administrator
/// check reasons over on every operation. Over in-memory grain storage; no
/// network or external store is involved.
/// </summary>
internal sealed class AuthApiGrpcClusterFixture
{
    /// <summary>A bootstrap administrator subject id configured on the silo (root-of-trust bypass).</summary>
    public const string BootstrapAdmin = "root-admin";

    /// <summary>The scheme the identity bridge is configured to strip and stamp.</summary>
    public const string CredentialScheme = ApiAuthGrpcTestCredentialAuthenticator.Scheme;

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The admin control facade the gRPC surface binds to.</summary>
    public ILatticeAuthAdmin Admin => SiloServices.GetRequiredService<ILatticeAuthAdmin>();

    /// <summary>
    /// Polls <paramref name="condition"/> until it holds or a bounded timeout
    /// elapses, so a test observes an authored rule once the asynchronous
    /// compiled-policy snapshot has rebuilt without reaching into the auth
    /// package's internals. Fails the test if the condition never holds.
    /// </summary>
    public static async Task WaitUntilAsync(Func<Task<bool>> condition, string because)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(50);
        }

        Assert.Fail($"Condition was not met within the timeout: {because}");
    }

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
    /// Co-hosts the auth-API gRPC surface over this fixture's live facade with the
    /// identity bridge configured for the test scheme.
    /// </summary>
    /// <param name="requireAuthorization">
    /// Whether the transport meta-authorizer is enforced. Left
    /// <see langword="false"/> by default so a test isolates the facade's own
    /// administrator check; pass <see langword="true"/> to exercise the coarse
    /// gate.
    /// </param>
    /// <param name="authorizer">
    /// An optional transport meta-authorizer to register before the binding's
    /// default-deny fallback. When <see langword="null"/> the binding's default
    /// <see cref="DenyAllAuthApiAuthorizer"/> applies.
    /// </param>
    public async Task<GrpcAuthHost> CreateGrpcHostAsync(
        bool requireAuthorization = false,
        ILatticeAuthApiAuthorizer? authorizer = null)
    {
        var facade = Admin;
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
                    if (authorizer is not null)
                    {
                        services.AddSingleton(authorizer);
                    }

                    services.AddLatticeAuthApiGrpc(o =>
                    {
                        o.RequireAuthorization = requireAuthorization;
                        o.CredentialScheme = CredentialScheme;
                    });
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(e => e.MapLatticeAuthApiGrpc());
                });
            });

        var host = await hostBuilder.StartAsync();
        var server = host.GetTestServer();
        var channel = GrpcChannel.ForAddress(server.BaseAddress, new GrpcChannelOptions
        {
            HttpHandler = server.CreateHandler(),
        });

        return new GrpcAuthHost(host, channel);
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
            siloBuilder.AddLatticeMembership();
            siloBuilder.Services
                .AddSingleton<ILatticeCredentialAuthenticator, ApiAuthGrpcTestCredentialAuthenticator>();
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(BootstrapAdmin);
            });
            siloBuilder.AddLatticeAuthApi();
        }
    }
}

/// <summary>
/// Disposable handle to a co-hosted auth-API gRPC server: the in-process host
/// plus a client channel, the resolved method definitions, and factories for
/// subject-scoped clients / invokers that stamp the caller credential header.
/// </summary>
internal sealed class GrpcAuthHost : IAsyncDisposable
{
    private readonly IHost _host;

    public GrpcAuthHost(IHost host, GrpcChannel channel)
    {
        _host = host;
        Channel = channel;
        Methods = host.Services.GetRequiredService<LatticeAuthApiGrpcMethods>();
    }

    public GrpcChannel Channel { get; }

    public IServiceProvider Services => _host.Services;

    public LatticeAuthApiGrpcMethods Methods { get; }

    /// <summary>
    /// Builds a call invoker that stamps <paramref name="subject"/> as the
    /// <c>authorization</c> credential header on every call (via a metadata
    /// interceptor), or an unadorned invoker when <paramref name="subject"/> is
    /// <see langword="null"/> (an anonymous caller).
    /// </summary>
    public CallInvoker InvokerFor(string? subject)
    {
        var invoker = Channel.CreateCallInvoker();
        if (subject is null)
        {
            return invoker;
        }

        return invoker.Intercept(metadata =>
        {
            metadata.Add("authorization", $"{AuthApiGrpcClusterFixture.CredentialScheme} {subject}");
            return metadata;
        });
    }

    /// <summary>
    /// Builds a public <see cref="LatticeAuthApiGrpcClient"/> whose calls stamp
    /// <paramref name="subject"/> as the credential header, over the host's
    /// Orleans serializer provider so the wire marshallers match the server.
    /// </summary>
    public LatticeAuthApiGrpcClient ClientFor(string? subject) =>
        LatticeAuthApiGrpcClient.Create(InvokerFor(subject), Services);

    public async ValueTask DisposeAsync()
    {
        Channel.Dispose();
        await _host.StopAsync();
        _host.Dispose();
    }
}
