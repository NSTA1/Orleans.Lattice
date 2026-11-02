using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The single authority for the region id this silo serves, shared by every
/// tenancy component that compares a tenant's per-region status against "here":
/// the residency snapshot and gates, the region backfill coordinator, its
/// dispatcher and status-change listener, and the backfill service.
/// </summary>
/// <remarks>
/// <para>
/// Allowed regions, residency and per-region lifecycle are keyed by
/// <em>replication</em> region ids (the peers' cluster ids), so on a replicated
/// host the local region is the replication cluster id
/// (<see cref="ILatticeReplicationContext.LocalReplicaId"/>), never the Orleans
/// <see cref="ClusterOptions.ClusterId"/>. The two commonly differ (for example
/// <c>Cluster__Id = estate-west</c> with <c>Replication__ClusterId = west</c>);
/// reading the Orleans id there stranded every configured tenant short of Online
/// (issue #5575). A single-cluster host has no replication id, so the Orleans
/// cluster id is the only region it can name.
/// </para>
/// <para>
/// The id is read from host configuration only, never from a wire-supplied value.
/// It fails closed: when it cannot be resolved (an empty replication cluster id on
/// a replicated host, or an empty Orleans cluster id on a single-cluster host),
/// <see cref="RegionId"/> throws rather than defaulting to a placeholder region,
/// and <see cref="TenantLocalRegionStartupValidator"/> refuses to start the silo.
/// </para>
/// </remarks>
internal sealed class TenantLocalRegion
{
    private readonly string? _regionId;
    private readonly string _unresolvedReason;

    /// <summary>Resolves the local region from host configuration.</summary>
    /// <param name="clusterOptions">The Orleans cluster options; used only on a single-cluster host.</param>
    /// <param name="replicationContext">The replication configuration seam.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantLocalRegion(IOptions<ClusterOptions> clusterOptions, ILatticeReplicationContext replicationContext)
    {
        ArgumentNullException.ThrowIfNull(clusterOptions);
        ArgumentNullException.ThrowIfNull(replicationContext);

        if (replicationContext.IsReplicationEnabled)
        {
            _regionId = Normalize(replicationContext.LocalReplicaId);
            _unresolvedReason =
                "replication is enabled but LatticeReplicationOptions.ClusterId is empty. Tenancy identifies this "
                + "silo's region by its replication cluster id, the id allowed regions and residency name; set "
                + "Replication:ClusterId to the region id.";
        }
        else
        {
            _regionId = Normalize(clusterOptions.Value.ClusterId);
            _unresolvedReason =
                "replication is not enabled and the Orleans ClusterOptions.ClusterId is empty. A single-cluster "
                + "host identifies its region by its Orleans cluster id; set it to the region id.";
        }
    }

    private TenantLocalRegion(string regionId)
    {
        _regionId = regionId;
        _unresolvedReason = string.Empty;
    }

    /// <summary>Creates an authority fixed to <paramref name="regionId"/>, for tests and benchmarks.</summary>
    /// <param name="regionId">The region id; must not be <c>null</c> or empty.</param>
    internal static TenantLocalRegion ForRegion(string regionId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(regionId);
        return new TenantLocalRegion(regionId);
    }

    /// <summary>
    /// <c>true</c> when the local region id resolved from configuration; otherwise
    /// <c>false</c>, and every region-scoped tenancy decision is refused.
    /// </summary>
    public bool IsResolved => _regionId is not null;

    /// <summary>The region id this silo serves.</summary>
    /// <exception cref="InvalidOperationException">The local region could not be resolved.</exception>
    public string RegionId => _regionId ?? throw new InvalidOperationException(UnresolvedMessage);

    /// <summary>Explains why the region could not be resolved; meaningful only when <see cref="IsResolved"/> is <c>false</c>.</summary>
    internal string UnresolvedMessage => "Tenancy cannot resolve this silo's region id: " + _unresolvedReason;

    private static string? Normalize(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;
}
