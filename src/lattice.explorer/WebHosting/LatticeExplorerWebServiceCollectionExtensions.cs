using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.DataProtection;
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
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Core.Topology;
using Orleans.Lattice.Explorer.DesignSystem;
using Orleans.Lattice.Explorer.MyTenant;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.UI.Authentication;
using Orleans.Lattice.Explorer.UI.Plugins;

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

        // The plugin host and the two adapters that publish the Explorer's own
        // selection, connection, tenant and preference state onto the plugin
        // contract. Which areas the shell surfaces is decided further down, by
        // which area plugins this head registers.
        services.AddExplorerPluginAdapters();

        // The per-selection tier: the metrics, topology, data and dead-letter
        // surfaces a tree or view resolves to, and the tag-index browser a
        // tag-index selection resolves to. Registered as ordinary plugins, so
        // the detail panel enumerates and gates them exactly as the shell does
        // the area tier.
        services.AddExplorerSelectionPlugins();

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

        // The adaptive shell's viewport seam: one breakpoint per circuit, driven
        // by LatticeAdaptiveRoot and read by every design-system primitive.
        services.AddLatticeExplorerDesignSystem();

        // The web head persists UI preferences to the browser's localStorage (Data
        // Protection-encrypted), overriding the in-memory fallback backing store.
        services.AddScoped<IUiPreferenceBackingStore, ProtectedLocalStoragePreferenceBackingStore>();

        // Authentication. The credential rests in an HttpOnly + Secure cookie
        // encrypted with Data Protection (no browser storage); the login dialog
        // posts to the server endpoints so the password never crosses the circuit.
        ConfigureDataProtection(services, options);
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
        // reader, and the access gate that gates the area and its per-scope
        // actions, plus the plugin registration that surfaces it in the shell.
        services.AddExplorerBackup();
        services.AddExplorerBackupsPlugin();

        // The Access (membership & access-control) management area: the auth-admin
        // control-API client, its membership and policy services, and the access
        // gate that gates the area, plus the plugin registration that surfaces it.
        services.AddExplorerAccess();
        services.AddExplorerAccessPlugin();

        // The My Tenant self-service area, for a tenant administrator. Its
        // registration must follow AddExplorerAccess(): AddExplorerTenantView()
        // registers a fail-closed placeholder platform-operator gate with
        // TryAdd, and Access registers the real one, so calling them the other
        // way round keeps the placeholder and every tenant switch quietly does
        // nothing. On a cluster without the tenancy add-on the plugin's gate
        // reports the surface unavailable and no My Tenant tab is rendered.
        services.AddExplorerTenantView();
        services.AddExplorerMyTenant();
        services.AddExplorerMyTenantPlugin();

        // The Schema management area: the schema control-API client, its policy,
        // versioning, and compliance services, and the access gate that gates the
        // area. The head opts the area into the shell by registering its plugin,
        // so withholding it renders no Schema tab at all - the services stay wired
        // either way, so it can be surfaced without new plumbing.
        services.AddExplorerSchema();
        if (options.EnableSchemaArea)
        {
            services.AddExplorerSchemaPlugin();
        }

        return services;
    }

    /// <summary>
    /// Registers ASP.NET Data Protection. Without configuration this is exactly the
    /// framework default (a per-instance, ephemeral key ring). When a host supplies
    /// <see cref="LatticeExplorerWebOptions.DataProtectionKeyRingBlobUri"/> the key
    /// ring is instead persisted to shared Azure Blob Storage so every replica
    /// shares one ring and can decrypt one another's OpenID Connect session cookie
    /// - the load-bearing piece that lets a signed-in operator survive a failover
    /// between replicas. Fail-closed: a blob URI with no credential is a
    /// misconfiguration, not a silent fall-through to the ephemeral ring.
    /// </summary>
    private static void ConfigureDataProtection(IServiceCollection services, LatticeExplorerWebOptions options)
    {
        var dataProtection = services.AddDataProtection();

        if (options.DataProtectionKeyRingBlobUri is { } blobUri)
        {
            if (options.DataProtectionKeyRingCredential is not { } credential)
            {
                throw new InvalidOperationException(
                    $"{nameof(LatticeExplorerWebOptions)}.{nameof(LatticeExplorerWebOptions.DataProtectionKeyRingBlobUri)} is set but "
                    + $"{nameof(LatticeExplorerWebOptions.DataProtectionKeyRingCredential)} is null. Persisting the Data Protection key "
                    + "ring to shared blob storage requires a TokenCredential (for example DefaultAzureCredential or a managed-identity "
                    + "credential).");
            }

            dataProtection.PersistKeysToAzureBlobStorage(blobUri, credential);
        }

        if (!string.IsNullOrWhiteSpace(options.DataProtectionApplicationName))
        {
            dataProtection.SetApplicationName(options.DataProtectionApplicationName);
        }

        options.ConfigureDataProtection?.Invoke(dataProtection);
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
