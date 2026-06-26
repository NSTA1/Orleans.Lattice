using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.Metrics;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Topology;
using Orleans.Lattice.Explorer.UI.Authentication;

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
        builder.Services.AddExplorerSession();

        // The desktop head persists UI preferences to the platform preference
        // store, overriding the in-memory fallback backing store.
        builder.Services.AddScoped<IUiPreferenceBackingStore, MauiPreferenceBackingStore>();

        // Authentication. The desktop head rests the credential on the machine via
        // DPAPI (per-user encrypted) and signs in fully in-process.
        builder.Services.AddSingleton<ICredentialStore>(
            new DpapiCredentialStore(Path.Combine(FileSystem.AppDataDirectory, "credential.bin")));
        builder.Services.AddSingleton(new ExplorerAuthUiOptions { UseServerFormPost = false });
        builder.Services.AddExplorerAuth();

#if DEBUG
        builder.Services.AddBlazorWebViewDeveloperTools();
        builder.Logging.AddDebug();
#endif

        return builder.Build();
    }
}
