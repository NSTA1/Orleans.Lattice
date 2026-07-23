using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Orleans.Lattice.Replication;

public static partial class LatticeReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="LatticeReplicationHealthCheck"/> on the supplied
    /// <see cref="IHealthChecksBuilder"/>. The check reads the cluster-wide
    /// singleton <see cref="ReplicationPeerStats"/> registered by
    /// <see cref="AddLatticeReplication(ISiloBuilder, Action{LatticeReplicationOptions}, bool)"/>,
    /// so this extension must be called <i>after</i> <c>AddLatticeReplication</c>
    /// on the same <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="builder">The ASP.NET Core health-checks builder.</param>
    /// <param name="name">
    /// Registered name for the health check; defaults to
    /// <see cref="LatticeReplicationHealthCheckOptions.DefaultName"/>. Named
    /// <see cref="LatticeReplicationHealthCheckOptions"/> bound under the
    /// same name are honoured.
    /// </param>
    /// <param name="failureStatus">
    /// Optional override for the <see cref="HealthStatus"/> reported when
    /// the check throws. Defaults to <see cref="HealthStatus.Unhealthy"/>
    /// when <see langword="null"/>. The aggregate result returned by a
    /// successful invocation is unaffected; <c>Degraded</c> and
    /// <c>Unhealthy</c> are derived from the per-peer thresholds rather
    /// than from this parameter.
    /// </param>
    /// <param name="tags">Optional tags applied to the registration (e.g. <c>"ready"</c>).</param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining.</returns>
    /// <remarks>
    /// <see cref="LatticeReplicationHealthCheck"/> is registered on the
    /// underlying <see cref="IServiceCollection"/> as a <b>singleton</b>
    /// rather than the default transient lifetime that
    /// <see cref="HealthChecksBuilderAddCheckExtensions.AddCheck{T}(IHealthChecksBuilder, string, HealthStatus?, IEnumerable{string}?)"/>
    /// applies. The check holds per-peer "first-degraded-at" state used to
    /// drive
    /// <see cref="LatticeReplicationHealthCheckOptions.UnhealthyAfter"/>
    /// escalation; a fresh instance per probe would reset that state on
    /// every readiness call and the sustained-degraded escalation path
    /// would never fire.
    /// </remarks>
    public static IHealthChecksBuilder AddLatticeReplicationHealthCheck(
        this IHealthChecksBuilder builder,
        string? name = null,
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Promote the check to a singleton so the per-peer "first-degraded-at"
        // map survives between probes (the default AddCheck<T> registration is
        // transient, which would reset escalation state on every readiness
        // call). TryAddSingleton so a host that pre-registers its own instance
        // wins the registration; AddCheck<T>(...) below resolves the same
        // singleton on every probe.
        builder.Services.TryAddSingleton<LatticeReplicationHealthCheck>();

        return builder.AddCheck<LatticeReplicationHealthCheck>(
            name ?? LatticeReplicationHealthCheckOptions.DefaultName,
            failureStatus,
            tags ?? Array.Empty<string>());
    }
}

