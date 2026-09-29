using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Framing;
using Orleans.Lattice.Explorer.UI.Framing.Broker;

namespace Orleans.Lattice.Explorer.UI;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// The app frame host and bridge broker (X1, issue #3817). The loader and broker are
    /// scoped, one per circuit, and resolve that circuit's credential-aware
    /// <see cref="ILatticeAppWorkspace"/> and <see cref="ILatticeAppBridge"/> (registered by
    /// the transport item, T1) optionally, so a host without them refuses every launch and
    /// request rather than failing to start. Only the bundle cache is a singleton, and it
    /// holds verified bytes and nothing else.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddAppFrame(IServiceCollection services)
    {
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<AppFrameBundleCache>();
        services.TryAddScoped<IAppFrameHostContext, DefaultAppFrameHostContext>();

        services.TryAddScoped(provider => new AppFrameBundleLoader(
            provider.GetService<ILatticeAppWorkspace>(),
            provider.GetRequiredService<AppFrameBundleCache>(),
            provider.GetService<ILogger<AppFrameBundleLoader>>() ?? NullLogger<AppFrameBundleLoader>.Instance));

        services.TryAddScoped(provider => new AppBridgeBroker(
            provider.GetService<ILatticeAppBridge>(),
            provider.GetRequiredService<AppFrameBundleLoader>(),
            provider.GetService<IAppFrameHostContext>(),
            provider.GetService<LtToastService>(),
            provider.GetRequiredService<TimeProvider>(),
            provider.GetService<ILogger<AppBridgeBroker>>() ?? NullLogger<AppBridgeBroker>.Instance));
    }
}
