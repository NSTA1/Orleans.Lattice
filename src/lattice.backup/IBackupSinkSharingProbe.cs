namespace Orleans.Lattice.Backup;

/// <summary>
/// Backup-local seam consulted by the startup sink guard and the periodic
/// backup-health monitor so a <b>replicated</b> tree's backups can be checked
/// against the deployment fact "every cluster reads the same backup store",
/// while the backup package stays replication-unaware. The backup package cannot
/// reference the replication package (that would invert the intended layering:
/// backup depends only on core lattice), so this interface is the capture-side
/// analogue of <see cref="IRestoreSagaDispatcher"/>: backup declares the seam and
/// the replication package supplies the implementation that actually knows the
/// peer set and can talk to it.
/// <para>
/// Whether an external sink is genuinely shared is not locally provable - two
/// regions can hold identical-looking connection strings that resolve to
/// different accounts - so the real implementation writes a tiny per-cluster
/// marker into its own configured sink and reads every peer's marker back out of
/// that same sink. A marker that is missing while its peer is demonstrably up is
/// proof the sink is not shared; a marker that is missing while its peer is
/// unreachable is merely undecided.
/// </para>
/// <para>
/// A default no-op implementation (<see cref="NoBackupSinkSharingProbe"/>) is
/// registered by
/// <see cref="LatticeBackupServiceCollectionExtensions.AddLatticeBackup(Orleans.Hosting.ISiloBuilder, System.Action{LatticeBackupOptions})"/>
/// that always reports <see cref="BackupSinkSharingStatus.NotApplicable"/>, which
/// is correct for a single-cluster deployment where the replication package is
/// not wired: no peers exist, so there is nothing to share a sink with.
/// </para>
/// </summary>
public interface IBackupSinkSharingProbe
{
    /// <summary>
    /// The most recent verdict this probe produced, or <see langword="null"/>
    /// when it has never run. Read by the per-backup health verification path so
    /// annotating a health report costs no I/O: sink sharing is a slow-moving
    /// deployment fact, refreshed once per health sweep rather than once per
    /// backup.
    /// </summary>
    BackupSinkSharingReport? LastReport { get; }

    /// <summary>
    /// Runs the probe now and returns the fresh verdict, also publishing it to
    /// <see cref="LastReport"/>. Implementations must be inert - returning
    /// <see cref="BackupSinkSharingStatus.NotApplicable"/> without any sink or
    /// network I/O - when no tree is replicated or the deployment has no peers,
    /// so a single-cluster host is never charged for a cross-cluster check it
    /// cannot need.
    /// </summary>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>The fresh sharing verdict. Never <see langword="null"/>.</returns>
    Task<BackupSinkSharingReport> ProbeAsync(CancellationToken cancellationToken = default);
}
