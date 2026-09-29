using System.Net;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Components.Server;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// One running Explorer web head, hosted in-process on Kestrel over HTTPS at an
/// ephemeral loopback port: the exact code path a consumer runs
/// (<c>AddLatticeExplorerWeb</c> and <c>MapLatticeExplorer</c>), served from the
/// published static-asset content root.
/// </summary>
/// <remarks>
/// <para>
/// Every head gets its own configuration document, written before it starts, and
/// never reads the process environment: the suite runs several heads in one
/// process, so a process-wide variable could not say which cluster each one talks
/// to. A head built with no endpoint is the first-run head.
/// </para>
/// <para>
/// A head is built by <see cref="StartAsync"/> with hooks around the head's own
/// registration, which is how the test world adds a silo and its gRPC surface and
/// how the hostile-app head swaps in its app facades.
/// </para>
/// </remarks>
internal sealed class ExplorerHead : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly ExplorerHostFaultRecorder _faults;
    private readonly string _stateDirectory;

    private ExplorerHead(WebApplication app, Uri baseUri, ExplorerHostFaultRecorder faults, string stateDirectory)
    {
        _app = app;
        BaseUri = baseUri;
        _faults = faults;
        _stateDirectory = stateDirectory;
    }

    /// <summary>The HTTPS base address a browser navigates to, ending in <c>/</c>.</summary>
    public Uri BaseUri { get; }

    /// <summary>The head's services, including a co-hosted silo's.</summary>
    public IServiceProvider Services => _app.Services;

    /// <summary>
    /// What the head logged at warning level or above since the last read, or
    /// <see langword="null"/>. Reading clears the buffer.
    /// </summary>
    public string? DescribeFaults() => _faults.DescribeFaults();

    /// <summary>The absolute address of a head-relative <paramref name="path"/>.</summary>
    /// <param name="path">A path such as <c>/data</c> or <c>data</c>.</param>
    public string Url(string path) => new Uri(BaseUri, path.TrimStart('/')).ToString();

    /// <summary>Builds and starts a head.</summary>
    /// <param name="options">What the head connects to and how it is composed.</param>
    public static async Task<ExplorerHead> StartAsync(ExplorerHeadOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var publishRoot = await ExplorerPublishedAssets.EnsureAsync();
        var stateDirectory = Path.Combine(Path.GetTempPath(), "lattice-explorer-uihead-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(stateDirectory);

        var configPath = Path.Combine(stateDirectory, "explorer-config.json");
        if (options.Endpoint is { } endpoint)
        {
            await new JsonExplorerConfigStore(new ExplorerConfigStoreOptions { FilePath = configPath })
                .SaveAsync(new ExplorerConfiguration
                {
                    Endpoint = endpoint,
                    TransportMode = ExplorerTransportMode.InsecureLoopbackDev,
                    AllowUnencryptedHttp2 = true,
                });
        }

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            // MapStaticAssets finds "{ApplicationName}.staticwebassets.endpoints.json"
            // beside the test assembly and serves from the published wwwroot.
            ApplicationName = typeof(ExplorerHead).Assembly.GetName().Name,
            ContentRootPath = publishRoot,
        });

        builder.Logging.ClearProviders();
        var faults = new ExplorerHostFaultRecorder();
        builder.Logging.AddProvider(faults);

        var webPort = LoopbackEndpoints.ReservePort();
        var certificate = LoopbackEndpoints.CreateServerCertificate();
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(IPAddress.Loopback, webPort, listen =>
            {
                listen.Protocols = HttpProtocols.Http1AndHttp2;
                listen.UseHttps(certificate);
            });
            options.ConfigureKestrel?.Invoke(kestrel);
        });

        options.ConfigureBuilder?.Invoke(builder);

        // Before the head: every contract it supplies is registered with TryAdd, so a
        // registration made here wins.
        options.ConfigureServices?.Invoke(builder.Services);

        builder.Services.AddLatticeExplorerWeb(web =>
        {
            web.ConfigFilePath = configPath;
            web.UseEnvironmentBootstrap = false;
            web.AllowInteractiveEndpointConfiguration = options.AllowInteractiveEndpointConfiguration;
        });

        options.ConfigureServicesAfterHead?.Invoke(builder.Services);

        builder.Services.Configure<CircuitOptions>(circuit =>
        {
            // Nothing here depends on reconnecting, and a retained circuit keeps its
            // whole render tree alive: dozens of retained circuits is what used to
            // exhaust the in-process head part way through the lane.
            circuit.DisconnectedCircuitMaxRetained = 0;
            circuit.DisconnectedCircuitRetentionPeriod = TimeSpan.FromSeconds(1);
            circuit.DetailedErrors = true;
        });

        var app = builder.Build();
        app.UseAntiforgery();
        options.ConfigureApp?.Invoke(app);
        app.MapLatticeExplorer();

        await app.StartAsync();

        var head = new ExplorerHead(app, new Uri($"https://127.0.0.1:{webPort}/"), faults, stateDirectory);
        await head.VerifyFrameworkAssetServedAsync();
        return head;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
        try
        {
            Directory.Delete(_stateDirectory, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private async Task VerifyFrameworkAssetServedAsync()
    {
        // If the Blazor bootstrap script does not serve, no circuit ever connects and
        // every assertion degrades to a bare locator timeout. A 200 here proves the
        // published content root is wired.
        using var handler = new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator,
        };
        using var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(30) };
        var asset = new Uri(BaseUri, "_framework/blazor.web.js");
        using var response = await client.GetAsync(asset);
        if (response.StatusCode != HttpStatusCode.OK)
        {
            throw new InvalidOperationException(
                $"The Explorer web head returned HTTP {(int)response.StatusCode} for '{asset}'. The Blazor "
                + "framework asset is not being served, so no interactive circuit can start. The static web "
                + "assets were not materialised into the published content root.");
        }
    }
}
