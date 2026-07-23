using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Extension methods for registering the optional
/// <c>Orleans.Lattice.Api.Replication</c> replication control facade on an
/// Orleans silo.
/// </summary>
public static class LatticeApiReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Adds the transport-agnostic replication control facade to the silo:
    /// binds <see cref="LatticeApiReplicationOptions"/>, registers the
    /// fail-closed <see cref="ReplicationAccessAuthorizer"/> and the
    /// <see cref="ILatticeReplicationControl"/> singleton every transport binding
    /// (gRPC and MCP) adapts over, and registers an idempotency marker. It adds no
    /// transport behaviour of its own.
    /// <para>
    /// Must be called <i>after</i> the replication config authority is registered
    /// (<c>AddLatticeReplication(...).ReplicateLatticeReplicationConfig()</c>):
    /// that seam is the source of truth for the enable / disable / status
    /// operations this facade drives. Calling it first fails fast with a clear
    /// message, mirroring how the sibling control-API add-ons guard their ordering
    /// relative to the core registration.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeApiReplicationOptions"/>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the replication config authority has not been registered on the
    /// same builder before this call.
    /// </exception>
    public static ISiloBuilder AddLatticeReplicationApi(
        this ISiloBuilder builder,
        Action<LatticeApiReplicationOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: the config authority is registered by
        // ReplicateLatticeReplicationConfig() (which itself requires
        // AddLatticeReplication). Its absence means the facade would have no engine
        // seam to drive, so fail fast at registration with an actionable message
        // rather than failing obscurely at silo start.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ILatticeReplicationConfigAuthority)))
        {
            throw new InvalidOperationException(
                "AddLatticeReplicationApi() must be called after the replication config authority is " +
                "registered. Call siloBuilder.AddLatticeReplication(...).ReplicateLatticeReplicationConfig() " +
                "before adding the replication control API.");
        }

        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        // Ensure the options instance is always resolvable even when the caller
        // passes no configure delegate.
        builder.Services.AddOptions<LatticeApiReplicationOptions>();

        // The fail-closed authorization seam the facade consults before touching
        // the engine. Resolves the always-present core access gate and the optional
        // membership context.
        builder.Services.TryAddSingleton(sp => new ReplicationAccessAuthorizer(
            sp.GetRequiredService<ILatticeAccessGate>(),
            sp.GetService<ILatticeMembershipContext>()));

        // The transport-agnostic control facade. Registered as a silo singleton
        // that every transport binding (gRPC and MCP) adapts over.
        builder.Services.TryAddSingleton<ILatticeReplicationControl, LatticeReplicationControl>();

        // Idempotency marker: the structural wiring runs once regardless of how
        // many times the host calls this method. A repeat call still layers any
        // supplied configure delegate above, matching how the sibling add-ons treat
        // repeated registration.
        builder.Services.TryAddSingleton<LatticeApiReplicationMarker>();

        return builder;
    }

    /// <summary>
    /// Internal singleton whose sole purpose is to make a repeated
    /// <see cref="AddLatticeReplicationApi"/> call a no-op for the structural
    /// wiring while still layering any supplied options delegate.
    /// </summary>
    internal sealed class LatticeApiReplicationMarker
    {
    }
}
