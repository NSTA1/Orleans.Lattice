using MultiSiteManufacturing.Host.Replication;

namespace MultiSiteManufacturing.Host.Replication.Grains;

/// <summary>
/// Singleton grain that aggregates per-silo replication activity
/// snapshots into a cluster-wide view. Each silo's
/// <see cref="ReplicationActivityTracker"/> pushes its local snapshot
/// every two seconds via <see cref="ReportAsync"/>; the layout component
/// polls <see cref="SnapshotAsync"/> on a one-second cadence and renders
/// the merged result.
/// </summary>
/// <remarks>
/// The grain exists because the package's <c>ReplicationShipperGrain</c>
/// activates on a single silo per <c>(tree, peer)</c> pair, so its meter
/// measurements only fire in that silo's process. A browser sticky-pinned
/// to a different silo would otherwise see an empty status strip despite
/// replication actually flowing. Aggregating in a single addressable
/// grain - rather than per-silo broadcast or stream fan-out - keeps the
/// cross-silo hop to one round-trip per Blazor poll.
/// </remarks>
internal interface IClusterReplicationActivityGrain : IGrainWithIntegerKey
{
    /// <summary>Fixed integer key used to address the singleton grain.</summary>
    public const long SingletonKey = 0;

    /// <summary>
    /// Records the most recent local snapshot from the silo identified by
    /// <paramref name="siloId"/>. Subsequent reports from the same silo
    /// replace the previous entry. Snapshots older than the grain's
    /// stale-after window are dropped from the merged view returned by
    /// <see cref="SnapshotAsync"/>.
    /// </summary>
    /// <param name="siloId">Cluster-qualified silo id (e.g. <c>"us-a"</c>).</param>
    /// <param name="snapshot">Local-silo snapshot from the per-silo tracker.</param>
    Task ReportAsync(string siloId, ReplicationActivitySnapshot snapshot);

    /// <summary>
    /// Returns the merged cluster-wide snapshot. Per-peer counters are
    /// summed across silos and timestamps are taken as the maximum, so the
    /// indicator reflects "the freshest contact any silo has had with this
    /// peer" rather than the local silo's slice.
    /// </summary>
    Task<ReplicationActivitySnapshot> SnapshotAsync();
}
