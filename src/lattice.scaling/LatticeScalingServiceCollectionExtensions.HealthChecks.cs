using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Orleans.Lattice.Scaling;

public static partial class LatticeScalingServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="LatticeScalingHealthCheck"/> on the supplied
    /// <see cref="IHealthChecksBuilder"/>. The check reads the
    /// <see cref="ILatticeScalingSignal"/> facade registered by
    /// <see cref="AddLatticeScalingSignal(Orleans.Hosting.ISiloBuilder, System.Action{LatticeScalingSignalOptions})"/>,
    /// so this extension must be called <i>after</i> <c>AddLatticeScalingSignal</c>
    /// on the same host.
    /// </summary>
    /// <param name="builder">The ASP.NET Core health-checks builder.</param>
    /// <param name="name">
    /// Registered name for the health check; defaults to
    /// <see cref="LatticeScalingHealthCheckOptions.DefaultName"/>. Named
    /// <see cref="LatticeScalingHealthCheckOptions"/> bound under the same name
    /// are honoured.
    /// </param>
    /// <param name="failureStatus">
    /// Optional override for the <see cref="HealthStatus"/> reported when the
    /// check throws. Defaults to <see cref="HealthStatus.Unhealthy"/> when
    /// <see langword="null"/>. The aggregate result returned by a successful
    /// invocation is unaffected; <c>Degraded</c> and <c>Unhealthy</c> are
    /// derived from the configured thresholds rather than from this parameter.
    /// </param>
    /// <param name="tags">Optional tags applied to the registration (e.g. <c>"ready"</c>).</param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="builder"/> is <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// The check is registered on the underlying
    /// <see cref="IServiceCollection"/> as a singleton via
    /// <see cref="ServiceCollectionDescriptorExtensions.TryAddSingleton{TService}(IServiceCollection)"/>.
    /// The check itself is stateless, but a singleton avoids re-allocating the
    /// instance on every probe and lets a host that pre-registers its own
    /// instance win the registration.
    /// </remarks>
    public static IHealthChecksBuilder AddLatticeScalingHealthCheck(
        this IHealthChecksBuilder builder,
        string? name = null,
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.TryAddSingleton<LatticeScalingHealthCheck>();

        return builder.AddCheck<LatticeScalingHealthCheck>(
            name ?? LatticeScalingHealthCheckOptions.DefaultName,
            failureStatus,
            tags ?? Array.Empty<string>());
    }
}
