using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State;

/// <summary>
/// Extension methods for registering the optional
/// <c>Orleans.Lattice.Api.State</c> add-on on an Orleans silo.
/// </summary>
public static class LatticeApiStateServiceCollectionExtensions
{
    /// <summary>
    /// Adds the read-only cluster state API to the silo. At the scaffolding
    /// stage this only binds an (empty) <see cref="LatticeApiStateOptions"/>
    /// instance and registers an idempotency marker; it adds no query,
    /// observe, or transport behaviour and imposes zero cost on the
    /// read/write path.
    /// <para>
    /// Must be called <i>after</i>
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice(ISiloBuilder, Action{ISiloBuilder, string})"/>:
    /// the core registration is the source of truth for the tree registry,
    /// per-shard digests, and the options system this API reads. Calling it
    /// first fails fast with a clear message, mirroring how the replication
    /// add-on guards its ordering relative to the core registration.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeApiStateOptions"/>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <c>AddLattice(...)</c> has not been called on the same
    /// builder before this call.
    /// </exception>
    public static ISiloBuilder AddLatticeStateApi(
        this ISiloBuilder builder,
        Action<LatticeApiStateOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: AddLattice registers the core options validator
        // (IValidateOptions<LatticeOptions>). Its absence means the state API
        // would have no tree registry / digest surface to read, so fail fast
        // at registration with an actionable message rather than failing
        // obscurely at silo start.
        if (!builder.Services.Any(d => d.ServiceType == typeof(IValidateOptions<LatticeOptions>)))
        {
            throw new InvalidOperationException(
                "AddLatticeStateApi() must be called after AddLattice(). Register the core " +
                "lattice (siloBuilder.AddLattice(...)) before adding the state API.");
        }

        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        // Ensure the options instance is always resolvable even when the
        // caller passes no configure delegate.
        builder.Services.AddOptions<LatticeApiStateOptions>();

        // The transport-agnostic read facade. Registered as a silo singleton
        // that every transport binding (gRPC now, MCP later) adapts over.
        builder.Services.TryAddSingleton<ILatticeStateQuery, LatticeStateQuery>();

        // The transport-agnostic live change-observation facade. Tails the
        // tree's durable WAL by cursor and yields change notifications; every
        // transport binding's subscription RPC adapts over this surface.
        builder.Services.TryAddSingleton<ILatticeStateObserver, LatticeStateObserver>();

        // The transport-agnostic live metrics-observation facade. Samples
        // low-cardinality per-tree aggregates on a cadence and delta-encodes
        // them; the dashboard's live gauges adapt over this surface.
        builder.Services.TryAddSingleton<ILatticeStateMetricsObserver, LatticeStateMetricsObserver>();

        // Idempotency marker: the structural wiring (added by later issues)
        // runs once regardless of how many times the host calls this method.
        // A repeat call still layers any supplied configure delegate above,
        // matching how the sibling add-ons treat repeated registration.
        builder.Services.TryAddSingleton<LatticeApiStateMarker>();

        return builder;
    }

    /// <summary>
    /// Internal singleton whose sole purpose is to make a repeated
    /// <see cref="AddLatticeStateApi"/> call a no-op for the structural
    /// wiring while still layering any supplied options delegate.
    /// </summary>
    internal sealed class LatticeApiStateMarker
    {
    }
}
