using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Session;
namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// Registration entry point for the embeddable Orleans.Lattice Explorer web head.
/// A single <see cref="AddLatticeExplorerWeb(IServiceCollection, Action{LatticeExplorerWebOptions})"/>
/// call wires up everything the standalone head registers, so a consumer can
/// co-host the Explorer inside their own ASP.NET application. Every area is
/// compiled in and decides its own visibility from its facade's capability probe,
/// failing closed; there is no plugin or area registration API.
/// </summary>
public static class LatticeExplorerWebServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Orleans.Lattice Explorer web head: Razor components with
    /// interactive server components, the state-API connection seam, the
    /// configuration backing store plus environment bootstrap, the browser-backed
    /// UI preference store, the cookie / data-protection auth plumbing, the tenant
    /// view, and the Explorer UI itself - its navigation and session chrome, its
    /// credential-aware transport, the Lattice App frame host, and every native area. Map the endpoints with
    /// <see cref="LatticeExplorerWebEndpointRouteBuilderExtensions.MapLatticeExplorer"/>.
    /// </summary>
    /// <remarks>
    /// The web head is multi-user, so two seams the single-operator desktop head
    /// leaves open are closed by default here: the environment <b>credential</b>
    /// seed is withheld (see
    /// <see cref="LatticeExplorerWebOptions.AllowEnvironmentCredentialSeed"/>), and
    /// the shared configuration store refuses browser-driven writes (see
    /// <see cref="LatticeExplorerWebOptions.AllowInteractiveEndpointConfiguration"/>).
    /// The secret-free endpoint seed is unaffected, so a head is still configured
    /// by <c>LATTICE_EXPLORER_ENDPOINT</c> or a pre-provisioned document.
    /// In Development, the framework's record of an unhandled circuit exception is never
    /// filtered out, so a terminated circuit always leaves its stack trace in the log.
    /// </remarks>
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
        // to the cluster and the browser renders over the SignalR circuit. The app
        // frame host relays each frame request (up to 128 KiB of UTF-8) to .NET as
        // one JS interop message, which the SignalR default of 32 KiB would refuse.
        services.AddRazorComponents()
            .AddInteractiveServerComponents()
            .AddHubOptions(hub => hub.MaximumReceiveMessageSize = Math.Max(
                hub.MaximumReceiveMessageSize ?? 0,
                MinimumCircuitMessageBytes));

        // A circuit the framework terminates is logged at Error with its exception; in
        // Development that record is never filtered out, so a dead console always leaves
        // its stack trace behind (issue #4011).
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IConfigureOptions<LoggerFilterOptions>, ExplorerCircuitErrorLogging>());
        // The config backing store, shared connection, and session live in DI. The
        // JSON store path is taken from the options, else the LATTICE_EXPLORER_CONFIG
        // environment variable, else the per-user local app-data default.
        var configFilePath = ResolveConfigFilePath(options);

        if (!options.AllowInteractiveEndpointConfiguration)
        {
            // Security: the persisted configuration is one process-wide document
            // shared by every browser, and it names the endpoint every circuit
            // dials and every sign-in is challenged against. Nothing upstream
            // authenticates the caller who writes it, so an anonymous visitor could
            // repoint the whole head at a host they control. Wrap the store so
            // writes are refused at the one seam every writer funnels through,
            // registered ahead of AddExplorerConfiguration so its TryAdd of the
            // raw JSON store is skipped. Reads are untouched, so the endpoint still
            // arrives from the environment or a pre-provisioned document.
            services.TryAddSingleton<IExplorerConfigStore>(sp =>
                new ReadOnlyExplorerConfigStore(
                    new JsonExplorerConfigStore(sp.GetRequiredService<ExplorerConfigStoreOptions>())));
        }

        services.AddExplorerConfiguration(storeOptions =>
        {
            if (!string.IsNullOrWhiteSpace(configFilePath))
            {
                storeOptions.FilePath = configFilePath;
            }
        });

        if (!options.AllowEnvironmentCredentialSeed)
        {
            // Security: the web head is multi-user and its credential store is per
            // browser, so it is empty for every anonymous visitor. Seeding the
            // process-wide environment credential there would sign each of them in
            // as the operator. Register the no-op seed ahead of the bootstrap below
            // so its TryAdd of the environment credential seed is skipped; the
            // secret-free endpoint seed it also registers is left in place.
            services.TryAddSingleton<IExplorerCredentialSeed, NullExplorerCredentialSeed>();
        }

        if (options.UseEnvironmentBootstrap)
        {
            // Launcher-friendly first-run bootstrap: seed the endpoint (and, when
            // explicitly opted in above, a sign-in credential) from environment
            // variables when nothing is persisted yet.
            services.AddExplorerEnvironmentBootstrap();
        }

        // The per-user preference contract the chrome's appearance reads, persisted
        // to the browser's localStorage (Data Protection-encrypted) rather than the
        // in-memory fallback backing store.
        services.AddExplorerSession();
        services.AddScoped<IUiPreferenceBackingStore, ProtectedLocalStoragePreferenceBackingStore>();

        // Authentication. The credential rests in an HttpOnly + Secure cookie
        // encrypted with Data Protection (no browser storage); the sign-in dialog
        // posts to the server endpoints so the password never crosses the circuit.
        // The session chrome's own default paths are base-relative; the head
        // registers them rooted at its base href, ahead of the Shell's TryAdd.
        ConfigureDataProtection(services, options);
        services.AddHttpContextAccessor();
        services.TryAddSingleton<ICredentialStore, CookieCredentialStore>();
        services.TryAddSingleton(new SessionSignInOptions
        {
            UseServerFormPost = true,
            LoginPath = options.BaseHref + SessionSignInOptions.DefaultLoginPath,
            LogoutPath = options.BaseHref + SessionSignInOptions.DefaultLogoutPath,
        });

        // The connection dialog's edit, test and save follow the same opt-in as the
        // store: its Test connection dials whatever the visitor typed, so a head
        // that refuses browser writes offers no test either.
        services.TryAddSingleton(new SessionEndpointConfigurationOptions
        {
            AllowInteractiveEndpointConfiguration = options.AllowInteractiveEndpointConfiguration,
        });
        services.AddExplorerAuth();

        // The Explorer itself: chrome, session, credential-aware transport, the
        // app frame host and every native area. Nothing here is an extension
        // point: the areas are compiled in (epic #3807, E1 and E2).
        services.AddLatticeExplorerShell();

        // Tenancy. Registered AFTER the Shell, because Core adds its fallbacks with
        // TryAdd: the Shell's accessible-tenant list and platform-operator gate
        // must win over Core's active-tenant-only list and fail-closed gate.
        services.AddExplorerTenantView();

        return services;
    }

    /// <summary>
    /// The smallest SignalR receive limit the circuit may run with: a frame
    /// request is at most 128 KiB of UTF-8, relayed as one interop message, plus
    /// the envelope around it.
    /// </summary>
    internal const long MinimumCircuitMessageBytes = 160 * 1024;
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
