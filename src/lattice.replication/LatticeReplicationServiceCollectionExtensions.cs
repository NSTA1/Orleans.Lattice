using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Extension methods for configuring <c>Orleans.Lattice.Replication</c> on an
/// Orleans silo.
/// </summary>
public static class LatticeReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Adds <c>Orleans.Lattice.Replication</c> to the silo. Registers the
    /// no-op <see cref="IReplicationTransport"/> as the default and binds the
    /// supplied <paramref name="configure"/> delegate to the unnamed
    /// <see cref="LatticeReplicationOptions"/> instance. Replace the transport
    /// registration after this call (e.g. with an HTTP or gRPC implementation)
    /// to enable real cross-cluster shipping.
    /// </summary>
    public static ISiloBuilder AddLatticeReplication(
        this ISiloBuilder builder,
        Action<LatticeReplicationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        // ConfigureAll so the cluster-wide setup is the baseline for every
        // named (per-tree) options instance; per-tree overrides registered via
        // ConfigureLatticeReplication(treeName, ...) layer on top. The
        // commit-time observer resolves options via Get(treeId), so the
        // baseline must be visible to every named lookup - not only the
        // default instance.
        builder.Services.ConfigureAll(configure);
        builder.Services.TryAddSingleton<IReplicationTransport, NoOpReplicationTransport>();
        builder.Services.TryAddSingleton<IReplogSink, ShardedReplogSink>();
        builder.Services.TryAddSingleton<IChangeFeed, ChangeFeed>();
        builder.Services.TryAddSingleton<ReplicationPeerStats>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IMutationObserver, ReplicationMutationObserver>());
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeReplicationOptions>, LatticeReplicationOptionsValidator>());
        return builder;
    }

    /// <summary>
    /// Configures global <see cref="LatticeReplicationOptions"/> that apply to
    /// all replicated trees unless a per-tree override is registered.
    /// </summary>
    public static ISiloBuilder ConfigureLatticeReplication(
        this ISiloBuilder builder,
        Action<LatticeReplicationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.ConfigureAll(configure);
        return builder;
    }

    /// <summary>
    /// Configures <see cref="LatticeReplicationOptions"/> for a specific tree
    /// identified by <paramref name="treeName"/>. These settings override the
    /// global defaults for that tree only.
    /// </summary>
    public static ISiloBuilder ConfigureLatticeReplication(
        this ISiloBuilder builder,
        string treeName,
        Action<LatticeReplicationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.Configure(treeName, configure);
        return builder;
    }
}
