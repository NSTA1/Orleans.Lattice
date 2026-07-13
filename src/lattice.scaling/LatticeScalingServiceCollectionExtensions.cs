using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Extension methods for configuring the opt-in <c>Orleans.Lattice.Scaling</c>
/// autoscaling signal on an Orleans silo.
/// </summary>
public static class LatticeScalingServiceCollectionExtensions
{
    /// <summary>
    /// Adds the <c>Orleans.Lattice.Scaling</c> read-only autoscaling signal to
    /// the silo. Registers the <see cref="ILatticeScalingSignal"/> facade as a
    /// singleton (via
    /// <see cref="ServiceCollectionDescriptorExtensions.TryAddSingleton{TService, TImplementation}(IServiceCollection)"/>
    /// so later issues can substitute a richer collector-backed implementation),
    /// binds <see cref="LatticeScalingSignalOptions"/>, and ensures a
    /// <see cref="TimeProvider"/> is available for sampling timestamps.
    /// <para>
    /// The scaffold registration performs no live pressure collection; the
    /// facade returns a well-formed zero signal. Safe to call more than once -
    /// the <c>TryAdd</c> registrations are idempotent.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder to register the signal on.</param>
    /// <param name="configure">
    /// Optional delegate to configure <see cref="LatticeScalingSignalOptions"/>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="builder"/> is <see langword="null"/>.
    /// </exception>
    public static ISiloBuilder AddLatticeScalingSignal(
        this ISiloBuilder builder,
        Action<LatticeScalingSignalOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = builder.Services.AddOptions<LatticeScalingSignalOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        builder.Services.TryAddSingleton(TimeProvider.System);
        builder.Services.TryAddSingleton<ILatticeScalingSignal, StubLatticeScalingSignal>();

        return builder;
    }
}
