using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.Metrics;
using Orleans.Lattice.Explorer.Core.Topology;

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
        // in the MAUI per-user app-data directory on the Windows desktop head.
        builder.Services.AddExplorerConfiguration(options =>
            options.FilePath = Path.Combine(
                FileSystem.AppDataDirectory,
                ExplorerConfigStoreOptions.DefaultFileName));
        builder.Services.AddExplorerCatalog();
        builder.Services.AddExplorerMetrics();
        builder.Services.AddExplorerTopology();
        builder.Services.AddExplorerData();

#if DEBUG
        builder.Services.AddBlazorWebViewDeveloperTools();
        builder.Logging.AddDebug();
#endif

        return builder.Build();
    }
}
