namespace Orleans.Lattice.Backup;

/// <summary>
/// The verdict of the cross-cluster backup-sink sharing probe: whether the sink
/// this cluster captures into is demonstrably the <b>same</b> store every peer
/// cluster reads from. A coordinated restore of a replicated tree is
/// all-or-nothing across every peer, and each peer resolves the manifest chain
/// from its own configured sink, so a per-cluster (non-shared) sink silently
/// produces backups that can never be restored.
/// <para>
/// The probe is deliberately three-valued rather than a boolean: an absent peer
/// marker is only evidence of a non-shared sink when the peer is otherwise known
/// to be up. A peer that is simply offline yields
/// <see cref="Unverified"/>, never a false accusation.
/// </para>
/// </summary>
public enum BackupSinkSharingStatus
{
    /// <summary>
    /// The probe does not apply here and nothing was measured: no tree is
    /// replicated, the deployment has no peers, the replication package is not
    /// wired, or the probe is disabled. This is the value a single-cluster
    /// deployment always reports, and the default for a backup captured before
    /// the probe existed.
    /// </summary>
    NotApplicable = 0,

    /// <summary>
    /// Every current peer's sink marker was read back from this cluster's own
    /// configured sink, so the sink is demonstrably shared across the peer set and
    /// a coordinated restore can resolve the same backup from every cluster.
    /// </summary>
    Shared = 1,

    /// <summary>
    /// Sharing could be neither confirmed nor refuted: at least one peer left no
    /// marker and was not reachable over the saga control channel, so it may
    /// simply not be running yet. Not a fault - the periodic backup-health sweep
    /// re-probes and resolves the verdict once the peer is up.
    /// </summary>
    Unverified = 2,

    /// <summary>
    /// At least one peer is reachable (so it is up and running the replication
    /// stack) yet its sink marker is absent from this cluster's sink, or is
    /// present but does not attest to that peer. The sink is therefore <b>not</b>
    /// shared with that peer and a coordinated restore of a replicated tree would
    /// abort. Backups captured here are not restorable fleet-wide.
    /// </summary>
    NotShared = 3,
}
