using System.Net;
using System.Net.Sockets;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Client;
using Orleans.Hosting;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// A reusable integration harness that stands up the repository-context MCP
/// server exactly the way a production host does -
/// <c>AddLatticeMcp(...)</c> + <c>AddRepoContextTools()</c> +
/// <c>MapLatticeMcp()</c> - co-hosted on an in-memory Lattice cluster (localhost
/// clustering, memory grain storage / reminders, and the core tree), served over
/// an in-process ASP.NET Core <see cref="TestServer"/>, and reachable with a real
/// <see cref="McpClient"/> over the streamable-HTTP transport using the test
/// server's <see cref="HttpClient"/>. It exists so every repository-context tool
/// sub-issue (#1431 / #1432 / #1433) and the end-to-end smoke (#1436) asserts
/// tool discovery, authorization gating, and request / response over the real MCP
/// protocol instead of unit-testing grain calls in isolation - and asserts the
/// fail-closed discovery seam uniformly through the shared
/// <see cref="RepoContextMcpAuthPosture"/> presets.
/// </summary>
/// <remarks>
/// <para>
/// <b>Determinism.</b> Each harness co-hosts its own single-silo cluster with a
/// fresh random cluster id and its own in-memory store, so a test starts from a
/// clean slate and parallel fixtures never share cluster state. Dispose the
/// harness (<c>await using</c>) to tear the silo and web host down.
/// </para>
/// <para>
/// <b>Auth posture.</b> The posture is driven through deterministic stub
/// collaborators (a stub credential bridge and a stub permission resolver) plus
/// the opt-in <c>AllowAllMcpAuthorizer</c>, so the only thing that scopes the
/// advertised tool set is the caller's resolved group grant - exactly the
/// fail-closed seam #1428 installs. <c>RequireAuthorization</c> is disabled on
/// the endpoint purely so a loopback client can reach the transport; discovery
/// stays fail-closed underneath.
/// </para>
/// <para>
/// <b>Extensibility.</b> A fixture that needs extra facades or tool modules
/// supplies <see cref="RepoContextMcpHarnessOptions.ConfigureSilo"/> /
/// <see cref="RepoContextMcpHarnessOptions.ConfigureServices"/> without
/// re-implementing bring-up.
/// </para>
/// </remarks>
public sealed class RepoContextMcpHarness : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly List<HttpClient> _clients = new();

    private RepoContextMcpHarness(WebApplication app) => _app = app;

    /// <summary>
    /// The co-hosted cluster's grain factory, so a fixture can seed or read
    /// Lattice trees (for example <c>GetGrain&lt;ILattice&gt;("...")</c>)
    /// directly, off the MCP path, to arrange or assert tool behaviour.
    /// </summary>
    public IGrainFactory GrainFactory => _app.Services.GetRequiredService<IGrainFactory>();

    /// <summary>The harness's root service provider (the web host's provider).</summary>
    public IServiceProvider Services => _app.Services;

    /// <summary>
    /// Brings up a harness under the supplied <paramref name="options"/> (or the
    /// defaults when omitted) and returns it once the silo and MCP endpoint are
    /// started and ready to serve.
    /// </summary>
    /// <param name="options">The harness configuration, or <see langword="null"/> for defaults.</param>
    /// <param name="cancellationToken">Cancels the bring-up.</param>
    /// <returns>The started harness.</returns>
    public static async Task<RepoContextMcpHarness> StartAsync(
        RepoContextMcpHarnessOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new RepoContextMcpHarnessOptions();
        var clusterId = $"repocontext-mcp-{Guid.NewGuid():N}";

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseTestServer();

        var (siloPort, gatewayPort) = ReserveSiloPorts();
        builder.Host.UseOrleans(silo =>
        {
            silo.UseLocalhostClustering(siloPort, gatewayPort);
            silo.Configure<Orleans.Configuration.ClusterOptions>(o =>
            {
                o.ClusterId = clusterId;
                o.ServiceId = clusterId;
            });
            silo.AddMemoryGrainStorageAsDefault();
            silo.UseInMemoryReminderService();
            silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));

            options.ConfigureSilo?.Invoke(silo);
        });

        options.ConfigureServices?.Invoke(builder.Services);

        // Drive the auth posture deterministically: stub the identity + permission
        // collaborators and open the coarse authorizer, so the only thing that
        // scopes the advertised tools is the caller's resolved group grant. These
        // are registered before AddLatticeMcp so its TryAdd registrations no-op and
        // the stubs win.
        var (credential, access) = ResolvePosture(options.Posture);
        builder.Services.AddSingleton<ILatticeApiMcpCredentialBridge>(
            new RepoContextMcpStubCredentialBridge(credential));
        builder.Services.AddSingleton<ILatticeApiMcpPermissionResolver>(
            new RepoContextMcpStubPermissionResolver(access));
        builder.Services.AddSingleton<ILatticeApiMcpAuthorizer>(new AllowAllMcpAuthorizer());

        builder.Services.AddLatticeMcp(o => o.RequireAuthorization = false);
        builder.Services.AddRepoContextTools();

        var app = builder.Build();
        app.MapLatticeMcp();
        await app.StartAsync(cancellationToken).ConfigureAwait(false);

        return new RepoContextMcpHarness(app);
    }

    /// <summary>
    /// Creates an <see cref="HttpClient"/> bound to the in-process test server.
    /// The client is owned by the harness and disposed with it.
    /// </summary>
    /// <returns>A test-server-bound HTTP client.</returns>
    public HttpClient CreateHttpClient()
    {
        var client = _app.GetTestServer().CreateClient();
        _clients.Add(client);
        return client;
    }

    /// <summary>
    /// Connects a real <see cref="McpClient"/> to the harness's MCP endpoint over
    /// the streamable-HTTP transport, using the test server's
    /// <see cref="HttpClient"/>. The returned client owns its HTTP client, so
    /// disposing the MCP client releases it.
    /// </summary>
    /// <param name="cancellationToken">Cancels the connect / initialise handshake.</param>
    /// <returns>An initialised MCP client scoped to the harness's auth posture.</returns>
    public async Task<McpClient> ConnectAsync(CancellationToken cancellationToken = default)
    {
        var httpClient = _app.GetTestServer().CreateClient();
        var transport = new HttpClientTransport(
            new HttpClientTransportOptions
            {
                Endpoint = httpClient.BaseAddress!,
                TransportMode = HttpTransportMode.StreamableHttp,
            },
            httpClient,
            NullLoggerFactory.Instance,
            ownsHttpClient: true);

        return await McpClient.CreateAsync(transport, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        foreach (var client in _clients)
        {
            client.Dispose();
        }

        await _app.StopAsync().ConfigureAwait(false);
        await _app.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Maps a posture to the credential the stub bridge stamps and the group
    /// access set the stub resolver returns. The repository-context group shares
    /// the data read/write mask, so a granted caller (reader or writer) is
    /// offered the read-only <c>repocontext_health</c> probe; an unauthenticated
    /// caller resolves to anonymous and is offered nothing.
    /// </summary>
    private static (LatticeCredential? Credential, LatticeApiMcpAccessSet Access) ResolvePosture(
        RepoContextMcpAuthPosture posture) => posture switch
    {
        RepoContextMcpAuthPosture.Unauthenticated
            => (null, LatticeApiMcpAccessSet.None),
        RepoContextMcpAuthPosture.Reader
            => (new LatticeCredential("repocontext-reader"),
                LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.RepoContext)),
        RepoContextMcpAuthPosture.Writer
            => (new LatticeCredential("repocontext-writer"),
                LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.RepoContext)),
        _ => (null, LatticeApiMcpAccessSet.None),
    };

    /// <summary>
    /// Reserves two free loopback TCP ports for the silo and gateway endpoints so
    /// parallel harnesses never collide on the localhost-clustering defaults.
    /// </summary>
    private static (int SiloPort, int GatewayPort) ReserveSiloPorts()
        => (FreeTcpPort(), FreeTcpPort());

    private static int FreeTcpPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
