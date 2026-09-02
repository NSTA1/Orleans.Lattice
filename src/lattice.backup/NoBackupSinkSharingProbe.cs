namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="IBackupSinkSharingProbe"/> registered by
/// <see cref="LatticeBackupServiceCollectionExtensions.AddLatticeBackup(Orleans.Hosting.ISiloBuilder, System.Action{LatticeBackupOptions})"/>.
/// Never probes and always reports
/// <see cref="BackupSinkSharingStatus.NotApplicable"/>, which is the correct
/// behaviour for a single-cluster deployment where the replication package is not
/// wired: there are no peers, so there is no sink to share and nothing a
/// cross-cluster probe could discover. A multi-cluster host replaces this
/// registration with the replication package's implementation that writes a
/// per-cluster marker and reads every peer's marker back.
/// </summary>
internal sealed class NoBackupSinkSharingProbe : IBackupSinkSharingProbe
{
    private static readonly BackupSinkSharingReport Inert = new(
        BackupSinkSharingStatus.NotApplicable,
        clusterId: string.Empty,
        peerCount: 0,
        unconfirmedPeerClusterIds: [],
        probedAtUtc: DateTimeOffset.MinValue,
        explanation: "Cross-cluster backup sink sharing is not probed: the replication package is not wired into this host, so the deployment has no peer clusters.");

    /// <inheritdoc />
    public BackupSinkSharingReport? LastReport => null;

    /// <inheritdoc />
    public Task<BackupSinkSharingReport> ProbeAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(Inert);
}
