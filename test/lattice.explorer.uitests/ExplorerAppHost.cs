using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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
/// </summary>
public sealed class ExplorerAppHost : IAsyncDisposable
{
    private readonly WebApplication _app;

    private ExplorerAppHost(WebApplication app, Uri baseUri)
    {
        _app = app;
        BaseUri = baseUri;
    }

    /// <summary>The loopback base address Playwright should navigate to.</summary>
    public Uri BaseUri { get; }

    /// <summary>
    /// Builds and starts the Explorer web head on an ephemeral loopback port and
    /// returns a handle exposing the resolved <see cref="BaseUri"/>. The port is
    /// chosen by binding <c>http://127.0.0.1:0</c>, so concurrent runs never collide
    /// on a fixed port.
    /// </summary>
    public static async Task<ExplorerAppHost> StartAsync()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            // Pin the application name and content root to this test assembly and its
            // output directory. MapStaticAssets locates its endpoint manifest as
            // "{ApplicationName}.staticwebassets.endpoints.json" under the content
            // root, and under `dotnet test` neither the app name nor the working
            // directory default to those values - so without both the Explorer's
            // stylesheets 404 and the shell renders unstyled, defeating any geometry
            // assertion.
            ApplicationName = typeof(ExplorerAppHost).Assembly.GetName().Name,
            ContentRootPath = AppContext.BaseDirectory,
        });

        builder.Logging.ClearProviders();

        // The Explorer's static web assets (its stylesheet, favicon, and interop
        // scripts, shipped as RCLs under _content/...) are what give the shell its
        // real geometry. WebApplication only auto-maps these in the Development
        // environment, so opt static web assets in explicitly - otherwise the CSS
        // never loads and boundingBox() would measure an unstyled document.
        builder.WebHost.UseStaticWebAssets();

        // Ephemeral loopback port: bind port 0 and read back what the OS assigned.
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        builder.Services.AddLatticeExplorerWeb();

        var app = builder.Build();

        app.UseAntiforgery();
        app.MapLatticeExplorer();

        await app.StartAsync();

        var address = ResolveAddress(app);
        return new ExplorerAppHost(app, address);
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
    }
}
