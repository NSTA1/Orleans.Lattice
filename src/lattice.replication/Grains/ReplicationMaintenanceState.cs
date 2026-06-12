namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Persistent state for IReplicationMaintenanceGrain. Tracks the
/// last-completed cadence ticks so a silo restart resumes the GC and
/// fall-off-log probes on the configured cadence rather than firing
/// both immediately on every reactivation.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationMaintenanceState)]
internal sealed class ReplicationMaintenanceState
{
    /// <summary>
    /// Wall-clock ticks (UtcNow) of the most-recent successful WAL
    /// garbage-collection pass. Used by the phase pump to skip the
    /// GC step until MaintenanceGcInterval has elapsed.
    /// </summary>
    [Id(0)]
    public long LastGcTicks { get; set; }

    /// <summary>
    /// Wall-clock ticks (UtcNow) of the most-recent fall-off-the-log
    /// probe iteration. Used by the phase pump to skip the probe until
    /// MaintenanceFallOffCheckInterval has elapsed.
    /// </summary>
    [Id(1)]
    public long LastFallOffCheckTicks { get; set; }

    /// <summary>
    /// Wall-clock ticks (UtcNow) of the most-recent successful
    /// atomic-batch buffer orphan-sweep pass. Used by the phase pump
    /// to skip the orphan sweep until the half-cadence relative to
    /// MaintenanceGcInterval has elapsed. Default <c>0</c> fires the
    /// sweep on the first phase tick after activation.
    /// </summary>
    [Id(2)]
    public long LastOrphanSweepTicks { get; set; }
}
