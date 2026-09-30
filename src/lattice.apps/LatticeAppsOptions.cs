namespace Orleans.Lattice.Apps;

/// <summary>
/// Options for the <c>Orleans.Lattice.Apps</c> add-on registered by
/// <see cref="LatticeAppsServiceCollectionExtensions.AddLatticeApps(Microsoft.Extensions.DependencyInjection.IServiceCollection, Action{LatticeAppsOptions})"/>.
/// </summary>
public sealed class LatticeAppsOptions
{
    /// <summary>Default value for <see cref="StartupRetryDelay"/> (250 milliseconds).</summary>
    public static readonly TimeSpan DefaultStartupRetryDelay = TimeSpan.FromMilliseconds(250);

    /// <summary>Default value for <see cref="StartupRetryMaxDelay"/> (30 seconds).</summary>
    public static readonly TimeSpan DefaultStartupRetryMaxDelay = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Whether each silo re-reconciles every enabled app in the background once it starts, so
    /// its trees and rules re-converge with the in-image manifests. Defaults to <c>true</c>.
    /// Failures are recorded against the app and never affect silo startup.
    /// </summary>
    public bool ReconcileOnStartup { get; set; } = true;

    /// <summary>
    /// The initial delay before retrying the startup registry read while the silo is not yet
    /// ready to serve it; doubles on each retry up to <see cref="StartupRetryMaxDelay"/>.
    /// Must be positive. A delay longer than a timer can wait (<c>0xFFFFFFFE</c>
    /// milliseconds, about 49.7 days) is held to that ceiling.
    /// </summary>
    public TimeSpan StartupRetryDelay { get; set; } = DefaultStartupRetryDelay;

    /// <summary>
    /// The upper bound on the startup retry delay. Must be positive and not less than
    /// <see cref="StartupRetryDelay"/>. <see cref="TimeSpan.MaxValue"/> leaves the doubling
    /// uncapped up to the timer ceiling (<c>0xFFFFFFFE</c> milliseconds, about 49.7 days),
    /// which is where every retry delay is held.
    /// </summary>
    public TimeSpan StartupRetryMaxDelay { get; set; } = DefaultStartupRetryMaxDelay;
}
