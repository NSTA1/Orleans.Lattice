using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.Core.Metrics;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Topology;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.DesignSystem;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.UI.Authentication;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer;

/// <summary>
/// Configures and builds the MAUI application that hosts the explorer UI in a
/// <c>BlazorWebView</c> on the Windows desktop head.
/// </summary>
public static class MauiProgram
{
    /// <summary>
    /// Creates and configures the <see cref="MauiApp"/> for the Windows desktop head.
    /// </summary>
    public static MauiApp CreateMauiApp()
    {
        var builder = MauiApp.CreateBuilder();
        builder
            .UseMauiApp<App>()
            .ConfigureFonts(fonts =>
            {
                fonts.AddFont("OpenSans-Regular.ttf", "OpenSansRegular");
            });

        builder.Services.AddMauiBlazorWebView();

        // The config store, shared connection, and session. The JSON store lives
        // in the MAUI per-user app-data directory on the Windows desktop head,
        // unless a launcher overrides the path via LATTICE_EXPLORER_CONFIG.
        builder.Services.AddExplorerConfiguration(options =>
        {
            var configOverride = Environment.GetEnvironmentVariable(
                EnvironmentExplorerBootstrap.ConfigPathVariable);
            options.FilePath = !string.IsNullOrWhiteSpace(configOverride)
                ? configOverride
                : Path.Combine(
                    FileSystem.AppDataDirectory,
                    ExplorerConfigStoreOptions.DefaultFileName);
        });

        // Launcher-friendly first-run bootstrap: seed the endpoint (and an
        // optional sign-in credential) from environment variables when nothing is
        // persisted yet.
        builder.Services.AddExplorerEnvironmentBootstrap();
        builder.Services.AddExplorerCatalog();
        builder.Services.AddExplorerMetrics();
        builder.Services.AddExplorerTopology();
        builder.Services.AddExplorerData();
        builder.Services.AddExplorerDeadLetter();
        builder.Services.AddExplorerHistory();
        builder.Services.AddExplorerSession();

        // The adaptive shell's viewport seam: one breakpoint per rendered shell,
        // driven by LatticeAdaptiveRoot and read by every design-system
        // primitive.
        builder.Services.AddLatticeExplorerDesignSystem();

        // The desktop head persists UI preferences to the platform preference
        // store, overriding the in-memory fallback backing store.
        builder.Services.AddScoped<IUiPreferenceBackingStore, MauiPreferenceBackingStore>();

        // Authentication. The desktop head rests the credential on the machine via
        // DPAPI (per-user encrypted) and signs in fully in-process.
        builder.Services.AddSingleton<ICredentialStore>(
            new DpapiCredentialStore(Path.Combine(FileSystem.AppDataDirectory, "credential.bin")));
        builder.Services.AddSingleton(new ExplorerAuthUiOptions { UseServerFormPost = false });
        builder.Services.AddExplorerAuth();

        // The plugin host and the adapters that publish the Explorer's own
        // selection, connection, tenant and preference state onto the plugin
        // contract. Which areas the shell surfaces is decided by which area
        // plugins this head registers below.
        builder.Services.AddExplorerPluginAdapters();

        // The Backups management area (see the web head for the rationale).
        builder.Services.AddExplorerBackup();
        builder.Services.AddExplorerBackupsPlugin();

        // The Access (membership & access-control) area (see the web head).
        builder.Services.AddExplorerAccess();
        builder.Services.AddExplorerAccessPlugin();

        // The Schema management area (see the web head). Its services are wired
        // but its plugin is not registered, so the desktop head renders no Schema
        // tab - the head opts in by registering the plugin.
        builder.Services.AddExplorerSchema();

#if DEBUG
        builder.Services.AddBlazorWebViewDeveloperTools();
        builder.Logging.AddDebug();
#endif

        return builder.Build();
    }
}
