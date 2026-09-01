using System.Net;
using System.Net.Http;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Components.Server;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Hosts the Orleans.Lattice Explorer web head in-process on Kestrel, bound to an
/// ephemeral loopback port, for the duration of the UI-test run. Playwright drives
/// a real browser against the resulting <see cref="BaseUri"/>.
/// <para>
/// The head is started in its disconnected, signed-out state: no silo, no gRPC
/// backend, no Azurite, no docker. The shell frame, the area tab strip, the catalog
/// pane, and the sign-in affordance all render without a Lattice backend - which is
/// all a reflow and accessibility baseline needs. This keeps the suite free of
/// out-of-process infrastructure while still exercising the exact web-head code path
/// a consumer runs (<c>AddLatticeExplorerWeb</c> + <c>MapLatticeExplorer</c>).
/// </para>
/// <para>
/// A single instance is shared across every UI fixture: the design-system breakpoint
/// observer keys off the real browser viewport, so one long-lived head serves every
/// viewport width without a per-fixture restart. The host is disposed once, after the
/// whole run, by <see cref="ExplorerAppHostSetup"/>.
/// </para>
/// <para>
/// <b>Static web assets are served from a published content root</b> produced by
/// <see cref="ExplorerPublishedAssets"/>. A plain <c>dotnet build</c> does not
/// materialise the framework and RCL assets portably (see that type's remarks), so the
/// fixture publishes them once and hosts from the result. On startup the framework
/// bootstrap asset is verified to serve <c>200 OK</c>, turning a misconfigured content
/// root into a clear fixture failure instead of an opaque locator timeout.
/// </para>
/// </summary>
public sealed class ExplorerAppHost : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly string _publishRoot;

    private ExplorerAppHost(WebApplication app, Uri baseUri, string publishRoot)
    {
        _app = app;
        BaseUri = baseUri;
        _publishRoot = publishRoot;
    }

    /// <summary>The loopback base address Playwright should navigate to.</summary>
    public Uri BaseUri { get; }

    /// <summary>
    /// Builds and starts the Explorer web head on an ephemeral loopback port and
    /// returns a handle exposing the resolved <see cref="BaseUri"/>. The port is
    /// chosen by binding <c>http://127.0.0.1:0</c>, so concurrent runs never collide
    /// on a fixed port. Before returning, the framework bootstrap asset
    /// (<c>_framework/blazor.web.js</c>) is verified to serve <c>200 OK</c>, so a
    /// broken static-asset content root fails here with a clear message rather than as
    /// an opaque locator timeout deep inside a test.
    /// </summary>
    /// <param name="configureServices">
    /// An optional hook run <b>before</b> <c>AddLatticeExplorerWeb</c>, so a
    /// registration it makes wins the head's own <c>TryAdd</c> for the same contract.
    /// This is the seam the end-to-end journey suite composes its world through -
    /// a catalog reader, the tenancy seams, an extra area plugin - without a second
    /// copy of the hosting code and without perturbing the default head every other
    /// fixture measures. Omit it for the default disconnected, signed-out head.
    /// </param>
    /// <param name="configureApp">
    /// An optional hook run after <c>MapLatticeExplorer</c>, for a test-only endpoint
    /// the harness drives world state through.
    /// </param>
    public static async Task<ExplorerAppHost> StartAsync(
        Action<IServiceCollection>? configureServices = null,
        Action<WebApplication>? configureApp = null)
    {
        var publishRoot = await ExplorerPublishedAssets.EnsureAsync();

        // Seed a first-run endpoint so the shell renders past its configuration gate
        // deterministically, independent of any persisted browser localStorage. The
        // Explorer's ConfigurationGate blocks the entire shell (tab strip, catalog,
        // nav) behind a "Connect to the state API" dialog until Session.IsConfigured
        // is true. On a developer machine an endpoint persisted to the browser's
        // localStorage by an earlier run makes the gate pass; a fresh CI browser
        // profile has empty storage, so the gate would sit on the dialog forever and
        // no [role=tab] would ever attach. EnvironmentExplorerBootstrap reads this
        // variable as an in-memory-only seed (never written back, never carrying a
        // credential): a secure loopback endpoint validates, IsConfigured flips true,
        // and the shell renders in its disconnected/signed-out state - exactly the
        // baseline this suite needs. The endpoint is deliberately unreachable; the
        // connection faults gracefully rather than throwing, which is the correct
        // no-backend state to reflow and accessibility-scan.
        Environment.SetEnvironmentVariable(
            EnvironmentExplorerBootstrap.EndpointVariable, "https://localhost:65535");

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            // Pin the application name so MapStaticAssets locates its endpoint manifest
            // ("{ApplicationName}.staticwebassets.endpoints.json") under the content
            // root - under `dotnet test` the app name does not default to this value.
            // Point the content root (and thus wwwroot) at the published output, where
            // the framework and RCL assets exist as real files with relative paths.
            ApplicationName = typeof(ExplorerAppHost).Assembly.GetName().Name,
            ContentRootPath = publishRoot,
        });

        builder.Logging.ClearProviders();

        // Bind port 0 and read back what the OS assigned.
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        // Before AddLatticeExplorerWeb, deliberately: every contract the head
        // supplies is registered with TryAdd, so a journey's own catalog reader or
        // tenancy seam only wins if it is already in the collection when the head
        // registers its default.
        configureServices?.Invoke(builder.Services);

        builder.Services.AddLatticeExplorerWeb();

        // Retain a disconnected circuit briefly - but only briefly.
        //
        // This is the difference between a lane that runs in one process and one that
        // dies part way through. Every UI test opens one to three fresh browser
        // contexts, and closing a context only drops the SignalR connection: Blazor
        // Server then keeps the whole circuit - every component instance, every
        // captured render tree, the scoped services behind them - alive for
        // DisconnectedCircuitRetentionPeriod (three minutes by default), up to
        // DisconnectedCircuitMaxRetained (a hundred). A suite of forty browser tests
        // therefore accumulates dozens of live circuits inside the in-process head at
        // once, and the test host is killed for memory long before the suite ends -
        // which reads as "Test host process crashed" with no test result to blame,
        // and is why every fixture passes alone while the whole lane aborts.
        //
        // Nothing under test depends on reconnection, so retaining nothing is both
        // safe and much closer to what these tests mean: each test gets a genuinely
        // new circuit, and the previous one is collected as soon as its page closes.
        //
        // Tried at four retained for thirty seconds, on the theory that a socket
        // blip mid-test would otherwise destroy a circuit the client could have
        // recovered. That was reasoning rather than evidence, and the measured
        // behaviour went the other way: the journey shards began failing with the
        // shell never reporting itself measured - a circuit that never establishes
        // at all, which is what memory pressure in this head looks like, and is the
        // very failure retaining nothing exists to prevent. Reverted; do not raise
        // it again without measuring the head's memory first.
        builder.Services.Configure<CircuitOptions>(options =>
        {
            options.DisconnectedCircuitMaxRetained = 0;
            options.DisconnectedCircuitRetentionPeriod = TimeSpan.FromSeconds(1);

            // Say what actually went wrong. A server-side exception on a Blazor circuit
            // otherwise reaches the browser as "There was an unhandled exception on the
            // current circuit, so this circuit will be terminated" and nothing else -
            // and because the circuit is then dead, every later assertion in the test
            // fails against a frozen page rather than against the fault. In a test host
            // there is no reason to withhold the detail, and having it turns a whole
            // afternoon of bisecting into reading one stack trace.
            options.DetailedErrors = true;
        });

        var app = builder.Build();

        app.UseAntiforgery();
        app.MapLatticeExplorer();
        configureApp?.Invoke(app);

        await app.StartAsync();

        var address = ResolveAddress(app);
        await VerifyFrameworkAssetServedAsync(address);
        return new ExplorerAppHost(app, address, publishRoot);
    }

    private static async Task VerifyFrameworkAssetServedAsync(Uri baseUri)
    {
        // The single most valuable pre-flight check: if the framework bootstrap script
        // does not serve, the interactive circuit never connects and every layout
        // assertion degrades to a bare locator timeout with no server-side signal. A
        // 200 here proves the static-asset content root is wired correctly on this OS.
        var assetUri = new Uri(baseUri, "_framework/blazor.web.js");
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };

        HttpResponseMessage response;
        try
        {
            response = await client.GetAsync(assetUri);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"The Explorer web head could not serve '{assetUri}'. Without the Blazor Web "
                + "framework bootstrap script the interactive server circuit never connects and "
                + "the shell never renders. See the inner exception.",
                ex);
        }

        if (response.StatusCode != HttpStatusCode.OK)
        {
            throw new InvalidOperationException(
                $"The Explorer web head returned HTTP {(int)response.StatusCode} for '{assetUri}', "
                + "not 200. The Blazor Web framework asset is not being served, so the interactive "
                + "server circuit will never connect and the shell will not render. This usually "
                + "means the static web assets were not materialised into the published content "
                + "root.");
        }
    }

    private static Uri ResolveAddress(WebApplication app)
    {
        var addresses = app.Services
            .GetRequiredService<IServer>()
            .Features
            .Get<IServerAddressesFeature>()
            ?.Addresses;

        var address = addresses?.FirstOrDefault();
        if (string.IsNullOrEmpty(address))
        {
            throw new InvalidOperationException(
                "The Explorer web head started but reported no server address, so the UI tests "
                + "have nothing to navigate to.");
        }

        // Normalize 0.0.0.0 / [::] to an explicit loopback host for the browser.
        var uri = new Uri(address);
        if (uri.Host is "0.0.0.0" or "::" or "[::]")
        {
            uri = new UriBuilder(uri) { Host = IPAddress.Loopback.ToString() }.Uri;
        }

        return uri;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();

        try
        {
            Directory.Delete(_publishRoot, recursive: true);
        }
        catch (IOException)
        {
            // Best-effort cleanup of the temp publish directory; a leftover temp folder
            // must never fail the run.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
