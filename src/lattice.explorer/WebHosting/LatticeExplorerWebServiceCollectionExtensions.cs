using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.Core.Metrics;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Topology;
using Orleans.Lattice.Explorer.UI.Authentication;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// Registration entry point for the embeddable Orleans.Lattice Explorer web head.
/// A single <see cref="AddLatticeExplorerWeb(IServiceCollection, Action{LatticeExplorerWebOptions})"/>
/// call wires up everything the standalone head registers, so a consumer can
/// co-host the read-only explorer inside their own ASP.NET application.
/// </summary>
public static class LatticeExplorerWebServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Orleans.Lattice Explorer web head: Razor components with
    /// interactive server components, the shared explorer UI, the state-API
    /// connection seam, the configuration backing store plus environment
    /// bootstrap, the catalog / metrics / topology / data / dead-letter / history
    /// / session services, the browser-backed UI preference store, the capability
    /// store, the Backups and Access areas, and the cookie / data-protection auth
    /// plumbing. Map the endpoints with
    /// <see cref="LatticeExplorerWebEndpointRouteBuilderExtensions.MapLatticeExplorer"/>.
    /// </summary>
    /// <param name="services">The consuming application's service collection.</param>
    /// <param name="configure">
    /// An optional callback to configure the mount point, configuration file path,
    /// and environment-bootstrap behaviour.
    /// </param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    public static IServiceCollection AddLatticeExplorerWeb(
        this IServiceCollection services,
        Action<LatticeExplorerWebOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new LatticeExplorerWebOptions();
        configure?.Invoke(options);
        services.TryAddSingleton(options);

        // The web head is Blazor Server: the server process holds the gRPC channel
        // to the cluster's state API and the browser renders over the SignalR
        // circuit. Interactive server components host the shared UI class library.
        services.AddRazorComponents()
            .AddInteractiveServerComponents();

        // The config backing store, shared connection, and session live in DI. The
        // JSON store path is taken from the options, else the LATTICE_EXPLORER_CONFIG
        // environment variable, else the per-user local app-data default.
        var configFilePath = ResolveConfigFilePath(options);
        services.AddExplorerConfiguration(storeOptions =>
        {
            if (!string.IsNullOrWhiteSpace(configFilePath))
            {
                storeOptions.FilePath = configFilePath;
            }
        });

        if (options.UseEnvironmentBootstrap)
        {
            // Launcher-friendly first-run bootstrap: seed the endpoint (and an
            // optional sign-in credential) from environment variables when nothing
            // is persisted yet.
            services.AddExplorerEnvironmentBootstrap();
        }

        services.AddExplorerCatalog();
        services.AddExplorerMetrics();
        services.AddExplorerTopology();
        services.AddExplorerData();
        services.AddExplorerDeadLetter();
        services.AddExplorerHistory();
        services.AddExplorerSession();

        // The web head persists UI preferences to the browser's localStorage (Data
        // Protection-encrypted), overriding the in-memory fallback backing store.
        services.AddScoped<IUiPreferenceBackingStore, ProtectedLocalStoragePreferenceBackingStore>();

        // Authentication. The credential rests in an HttpOnly + Secure cookie
        // encrypted with Data Protection (no browser storage); the login dialog
        // posts to the server endpoints so the password never crosses the circuit.
        services.AddDataProtection();
        services.AddHttpContextAccessor();
        services.TryAddSingleton<ICredentialStore, CookieCredentialStore>();
        services.TryAddSingleton(new ExplorerAuthUiOptions
        {
            UseServerFormPost = true,
            LoginPath = options.BaseHref + "auth/login",
            LogoutPath = options.BaseHref + "auth/logout",
        });
        services.AddExplorerAuth();

        // The Backups management area: the backup control-API client, its catalog
        // reader, and the capability probe that gates the area and its actions.
        services.AddExplorerBackup();

        // The Access (membership & access-control) management area: the auth-admin
        // control-API client, its membership and policy services, and the
        // capability probe that gates the area.
        services.AddExplorerAccess();

        return services;
    }

    private static string? ResolveConfigFilePath(LatticeExplorerWebOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.ConfigFilePath))
        {
            return options.ConfigFilePath;
        }

        var fromEnvironment = Environment.GetEnvironmentVariable(
            EnvironmentExplorerBootstrap.ConfigPathVariable);
        return string.IsNullOrWhiteSpace(fromEnvironment) ? null : fromEnvironment;
    }
}
