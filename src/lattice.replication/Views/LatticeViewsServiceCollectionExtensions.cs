using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication.Views;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Extension methods for configuring asynchronous materialised views on an
/// Orleans silo.
/// </summary>
public static class LatticeViewsServiceCollectionExtensions
{
    /// <summary>
    /// Adds materialised-view maintenance to the silo. Registers the view
    /// catalog, the <see cref="ILatticeViewFactory"/>, the per-view options
    /// validator, and the hosted activation service, and applies any startup view
    /// declarations from <paramref name="configure"/>.
    /// <para>
    /// Must be called <i>after</i> <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>:
    /// views tail the per-shard write-ahead log through the commit-log reader the
    /// WAL provider registers, so a WAL provider must be present.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional startup view declarations.</param>
    public static ISiloBuilder AddLatticeViews(
        this ISiloBuilder builder,
        Action<LatticeViewRegistrationBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var registrationBuilder = new LatticeViewRegistrationBuilder();
        configure?.Invoke(registrationBuilder);

        builder.Services.AddOptions<LatticeViewOptions>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeViewOptions>, LatticeViewOptionsValidator>());

        builder.Services.TryAddSingleton<IViewCatalog, ViewCatalog>();
        builder.Services.TryAddSingleton<ILatticeViewFactory, LatticeViewFactory>();
        builder.Services.TryAddSingleton<IReadOnlyList<StartupViewRegistration>>(
            _ => registrationBuilder.Registrations);

        // Fail fast at silo start when a view's replication mode is inconsistent
        // with the replicated-trees configuration (DeriveLocally + view tree
        // replicated = two writers; ShipView + view tree not replicated = consumers
        // never receive it). Registered before the activation service below so the
        // throw lands before a maintainer can act on a misconfigured view.
        builder.Services.AddSingleton<IHostedService, LatticeViewReplicationStartupValidator>();

        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, ViewActivationService>());

        return builder;
    }

    /// <summary>
    /// Configures <see cref="LatticeViewOptions"/> for a specific view identified
    /// by <paramref name="viewName"/>. These settings override the global
    /// defaults for that view only.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="viewName">The logical view name to configure.</param>
    /// <param name="configure">Mutates the named options instance.</param>
    public static ISiloBuilder ConfigureLatticeView(
        this ISiloBuilder builder,
        string viewName,
        Action<LatticeViewOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.Configure(viewName, configure);
        return builder;
    }
}
