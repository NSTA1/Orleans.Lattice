using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Views;

namespace Orleans.Lattice;

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
    /// A view tails the per-shard write-ahead log through the commit-log reader,
    /// so the host must register a WAL-backed lattice by calling
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice"/> (which registers
    /// the commit-log reader and the in-memory WAL baseline) before
    /// <c>AddLatticeViews</c>. The view maintainer pins the source WAL through the
    /// consumer-cursor registry, so this call folds in
    /// <see cref="LatticeServiceCollectionExtensions.AddWalCursorRegistry"/> (the
    /// in-memory default plus the leaf-cursor reporter); the call is idempotent, so
    /// a host that wired its own cursor registry up first is unaffected.
    /// <c>AddLatticeReplication</c> is <i>not</i> required for a local
    /// (<see cref="LatticeViewReplicationMode.DeriveLocally"/>) view; it is only
    /// needed when a view ships its tree across clusters
    /// (<see cref="LatticeViewReplicationMode.ShipView"/>).
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

        // The maintainer pins the source WAL GC against its applied frontier via
        // the consumer-cursor registry, so views require it. Fold in the in-memory
        // default (plus the leaf-cursor reporter) the same way AddLatticeReplication
        // does; idempotent, so a host that registered its own registry first wins.
        builder.AddWalCursorRegistry();

        builder.Services.AddOptions<LatticeViewOptions>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeViewOptions>, LatticeViewOptionsValidator>());

        builder.Services.TryAddSingleton<HistoryRowCodec>();
        builder.Services.TryAddSingleton(
            static sp => new HistoryLatticeViewProjection(sp.GetRequiredService<HistoryRowCodec>()));
        builder.Services.TryAddSingleton<IViewCatalog, ViewCatalog>();
        builder.Services.TryAddSingleton<IViewSourceGuard, ViewSourceGuard>();
        builder.Services.TryAddSingleton<ILatticeViewFactory, LatticeViewFactory>();
        builder.Services.TryAddSingleton<IReadOnlyList<StartupViewRegistration>>(
            _ => registrationBuilder.Registrations);

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
