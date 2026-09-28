using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Registers the peer-status read path on a silo: the per-silo
/// <see cref="ReplicationPeerStatusGrainService"/> and the cluster-wide
/// <see cref="IReplicationPeerStatusReader"/> that fans out to it. Called by the
/// replication status facade's registration; idempotent.
/// </summary>
internal static class ReplicationPeerStatusReadPath
{
    /// <summary>
    /// Adds the read path to <paramref name="builder"/> once. A repeated call is a
    /// no-op, so the grain service is never registered twice.
    /// </summary>
    /// <param name="builder">The silo builder. Must not be <see langword="null"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    public static ISiloBuilder AddReplicationPeerStatusReadPath(this ISiloBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (builder.Services.Any(d => d.ServiceType == typeof(ReplicationPeerStatusReadPathMarker)))
        {
            return builder;
        }

        builder.Services.AddSingleton<ReplicationPeerStatusReadPathMarker>();
        builder.AddGrainService<ReplicationPeerStatusGrainService>();
        builder.Services.TryAddSingleton<IReplicationPeerStatusReader, ClusterReplicationPeerStatusReader>();
        return builder;
    }

    /// <summary>Idempotency marker for <see cref="AddReplicationPeerStatusReadPath"/>.</summary>
    internal sealed class ReplicationPeerStatusReadPathMarker
    {
    }
}
