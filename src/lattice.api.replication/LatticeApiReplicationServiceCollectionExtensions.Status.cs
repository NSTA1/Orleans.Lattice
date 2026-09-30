using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication;

public static partial class LatticeApiReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Adds the read-only replication peer-status facade to the silo: binds and
    /// validates <see cref="LatticeReplicationStatusOptions"/>, registers the
    /// cluster-wide peer-status read path (a per-silo grain service plus the
    /// reader that fans out to it), the fail-closed
    /// <see cref="ReplicationAccessAuthorizer"/>, and the
    /// <see cref="ILatticeReplicationStatus"/> singleton every transport binding
    /// adapts over. Independent of <see cref="AddLatticeReplicationApi"/>: the
    /// status facade reads telemetry and needs no config authority, and
    /// <see cref="ILatticeReplicationControl"/> is unaffected by it.
    /// <para>
    /// Must be called <i>after</i> <c>AddLatticeReplication(...)</c>, which
    /// registers the per-peer telemetry state this facade reads. Calling it first
    /// fails fast with a clear message. Repeated calls are idempotent for the
    /// structural wiring and still layer any supplied options delegate.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeReplicationStatusOptions"/>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <c>AddLatticeReplication(...)</c> has not been called on the same
    /// builder before this call.
    /// </exception>
    public static ISiloBuilder AddLatticeReplicationStatusApi(
        this ISiloBuilder builder,
        Action<LatticeReplicationStatusOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (!builder.Services.Any(d => d.ServiceType == typeof(ReplicationPeerStats)))
        {
            throw new InvalidOperationException(
                "AddLatticeReplicationStatusApi() must be called after AddLatticeReplication(...), which "
                + "registers the per-peer replication telemetry the status facade reads.");
        }

        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        builder.Services.AddOptions<LatticeReplicationStatusOptions>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeReplicationStatusOptions>, LatticeReplicationStatusOptionsValidator>());

        builder.AddReplicationPeerStatusReadPath();

        builder.Services.TryAddSingleton(sp => new ReplicationAccessAuthorizer(
            sp.GetRequiredService<ILatticeAccessGate>(),
            sp.GetService<ILatticeMembershipContext>()));

        builder.Services.TryAddSingleton<ILatticeReplicationStatus, LatticeReplicationStatus>();

        return builder;
    }
}
